//! Persisted blocks (port of the `Block` part of `tsdb/block.go`; the meta types are in [`crate::meta`]).
//!
//! A [`Block`] is a cheap-to-clone handle (Go `*Block`). Its readers ([`Block::index`], [`Block::chunks`],
//! [`Block::tombstones`]) keep the block open (Go's pending readers): [`Block::close`] waits until all of them
//! are closed or dropped. The index and chunk files are mmapped; chunks given to queries point into the mapping
//! without a copy ([`BlockChunk`]) and keep the mapping alive while they live.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use chunkenc::{AnyIterator, Chunk, Encoding, Pool};
use model::labels::ScratchBuilder;
use model::matcher::Matcher;
use storage::{Context, Error, LabelHints, Meta, Sentinel, SeriesRef};
use tsdb_format::chunks;
use tsdb_format::index::{self, BoxPostings, Postings, PostingsDecoder, StringIter};
use tsdb_format::tombstones::{self, Interval, MemTombstones};

use crate::compact::Compactor;
use crate::meta::{BlockMeta, INDEX_FILENAME, META_FILENAME, Ulid, read_meta_file, write_meta_file};
use crate::querier::{label_names_with_matchers, label_values_with_matchers, postings_for_matchers};
use crate::reader::{BlockReader, ChunkOrIterable, ChunkReader, IndexReader};
use crate::{Logger, chunks_err, index_err, tomb_err};

static CLOSING: Sentinel = Sentinel::new("block is closing");
/// Go `ErrClosing`: returned when a block is in the process of being closed.
pub const ERR_CLOSING: Error = Error::Sentinel(&CLOSING);

/// Go `PostingsDecoderFactory`: the postings decoder for a block.
pub type PostingsDecoderFactory = Arc<dyn Fn(&BlockMeta) -> PostingsDecoder + Send + Sync>;

/// Go `chunkDir`.
pub fn chunk_dir(dir: &Path) -> PathBuf {
    dir.join("chunks")
}

// ---------------------------------------------------------------------------------------------------------------
// The index file reader as an IndexReader.

impl IndexReader for index::Reader {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        Box::new(index::Reader::symbols(self))
    }
    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        index::Reader::sorted_label_values(self, ctx, name, hints, matchers).map_err(index_err)
    }
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        index::Reader::label_values(self, ctx, name, hints, matchers).map_err(index_err)
    }
    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        index::Reader::postings(self, ctx, name, values).map_err(index_err)
    }
    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        index::Reader::postings_for_label_matching(self, ctx, name, &|s| m(s))
    }
    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_> {
        index::Reader::postings_for_all_label_values(self, ctx, name)
    }
    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        index::Reader::sorted_postings(self, p)
    }
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        index::Reader::sharded_postings(self, p, shard_index, shard_count)
    }
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        index::Reader::series(self, r, builder, Some(chks)).map_err(index_err)
    }
    fn label_names(&self, ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        index::Reader::label_names(self, ctx, matchers).map_err(index_err)
    }
    fn label_names_for(&self, ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error> {
        index::Reader::label_names_for(self, ctx, postings).map_err(index_err)
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Block chunks.

/// A chunk of a block: its bytes point into the mmapped chunk segment (no copy). Read-only: `appender` fails.
pub struct BlockChunk {
    enc: Encoding,
    data: *const u8,
    len: usize,
    _r: Arc<chunks::Reader>,
}

// SAFETY: the pointer refers to immutable bytes owned by the `chunks::Reader` behind the `Arc` (an mmapped file or
// an owned buffer), which is `Send + Sync`; the bytes are never written.
unsafe impl Send for BlockChunk {}
// SAFETY: see `Send`.
unsafe impl Sync for BlockChunk {}

impl BlockChunk {
    /// The chunk at `r` of the reader (Go `Reader.ChunkOrIterable`).
    pub fn read(r: &Arc<chunks::Reader>, cref: chunks::ChunkRef) -> Result<BlockChunk, Error> {
        let (enc, data) = r.chunk_bytes(cref).map_err(chunks_err)?;
        if !chunkenc::is_valid_encoding(enc) {
            return Err(Error::msg(format!("invalid chunk encoding \"{}\"", enc.as_str())));
        }
        Ok(BlockChunk { enc, data: data.as_ptr(), len: data.len(), _r: r.clone() })
    }
}

impl fmt::Debug for BlockChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockChunk").field("encoding", &self.enc).field("len", &self.len).finish()
    }
}

impl Chunk for BlockChunk {
    fn bytes(&self) -> &[u8] {
        // SAFETY: `data` and `len` describe a slice of the reader's segment bytes, which live (unmoved and
        // unchanged) as long as the reader, and `self._r` keeps the reader alive.
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
    fn encoding(&self) -> Encoding {
        self.enc
    }
    fn appender(&mut self) -> Result<chunkenc::Appender, chunkenc::Error> {
        Err(chunkenc::Error::msg("block chunks are read-only"))
    }
    fn iterator<'a>(&'a self, it: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        let b = self.bytes();
        let res = match it {
            Some(it) => it.reset_to(self.enc, b),
            None => AnyIterator::new(self.enc, b),
        };
        // The encoding was checked when the chunk was read.
        res.unwrap_or(AnyIterator::Nop(chunkenc::NopIterator))
    }
    fn num_samples(&self) -> usize {
        let b = self.bytes();
        if b.len() < 2 {
            return 0;
        }
        let n = u16::from_be_bytes([b[0], b[1]]);
        match self.enc {
            chunkenc::ENC_HISTOGRAM_ST | chunkenc::ENC_FLOAT_HISTOGRAM_ST => {
                (n & chunkenc::st::HISTOGRAM_ST_SAMPLE_COUNT_MASK) as usize
            }
            _ => n as usize,
        }
    }
    fn compact(&mut self) {}
    fn reset(&mut self, _stream: Vec<u8>) {
        // Block chunks are immutable views of the chunk files.
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// The chunk segment files of a block as a [`ChunkReader`].
#[derive(Clone)]
pub struct ChunksFileReader(pub Arc<chunks::Reader>);

impl ChunkReader for ChunksFileReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        Ok(ChunkOrIterable::Chunk(Arc::new(BlockChunk::read(&self.0, meta.chunk_ref)?)))
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Block.

struct BlockState {
    closing: bool,
    pending_readers: usize,
    meta: BlockMeta,
    tombstones: Arc<dyn tombstones::Reader>,
    num_bytes_tombstone: i64,
    num_bytes_meta: i64,
}

struct BlockInner {
    dir: PathBuf,
    ulid: Ulid,
    min_time: i64,
    max_time: i64,
    // Symbol table size in bytes, kept to avoid recalculation.
    symbol_table_size: u64,
    chunkr: Arc<chunks::Reader>,
    indexr: Arc<index::Reader>,
    logger: Logger,
    num_bytes_chunks: i64,
    num_bytes_index: i64,
    state: Mutex<BlockState>,
    readers_done: Condvar,
}

impl BlockInner {
    fn lock(&self) -> MutexGuard<'_, BlockState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn start_read(&self) -> Result<(), Error> {
        let mut s = self.lock();
        if s.closing {
            return Err(ERR_CLOSING);
        }
        s.pending_readers += 1;
        Ok(())
    }

    fn done_read(&self) {
        let mut s = self.lock();
        s.pending_readers = s.pending_readers.saturating_sub(1);
        if s.pending_readers == 0 {
            self.readers_done.notify_all();
        }
    }
}

/// Go `Block`: a directory of time series data covering a continuous time range. Clones share the block.
#[derive(Clone)]
pub struct Block {
    inner: Arc<BlockInner>,
}

/// Go `OpenBlock`: opens the block in the directory. The chunk pool is accepted for API compatibility; block
/// chunks point into the mmapped files and need no pool.
pub fn open_block(
    logger: Option<Logger>,
    dir: &Path,
    pool: Option<Arc<dyn Pool>>,
    postings_decoder_factory: Option<&PostingsDecoderFactory>,
) -> Result<Block, Error> {
    let logger = logger.unwrap_or_else(Logger::nop);
    let (meta, size_meta) = read_meta_file(dir)?;
    let cr = chunks::new_dir_reader(&chunk_dir(dir), pool).map_err(chunks_err)?;
    let decoder = match postings_decoder_factory {
        Some(f) => f(&meta),
        None => index::decode_postings_raw,
    };
    let ir = index::Reader::open(&dir.join(INDEX_FILENAME), decoder).map_err(index_err)?;
    let (tr, size_tomb) = tombstones::read_tombstones(dir).map_err(tomb_err)?;

    let symbol_table_size = ir.symbol_table_size();
    let num_bytes_chunks = cr.size();
    let num_bytes_index = ir.size();
    Ok(Block {
        inner: Arc::new(BlockInner {
            dir: dir.to_path_buf(),
            ulid: meta.ulid,
            min_time: meta.min_time,
            max_time: meta.max_time,
            symbol_table_size,
            chunkr: Arc::new(cr),
            indexr: Arc::new(ir),
            logger,
            num_bytes_chunks,
            num_bytes_index,
            state: Mutex::new(BlockState {
                closing: false,
                pending_readers: 0,
                meta,
                tombstones: Arc::new(tr),
                num_bytes_tombstone: size_tomb,
                num_bytes_meta: size_meta,
            }),
            readers_done: Condvar::new(),
        }),
    })
}

impl Block {
    /// Go `Close`: closes the on-disk block. It blocks as long as there are readers reading from the block. The
    /// files are unmapped when the last chunk taken from the block is dropped.
    pub fn close(&self) -> Result<(), Error> {
        let mut s = self.inner.lock();
        s.closing = true;
        while s.pending_readers > 0 {
            s = self.inner.readers_done.wait(s).unwrap_or_else(|p| p.into_inner());
        }
        let r = s.tombstones.close().map_err(tomb_err);
        drop(s);
        r
    }

    /// Go `String`: the ULID.
    pub fn ulid(&self) -> Ulid {
        self.inner.ulid
    }

    /// Go `Dir`: the directory of the block.
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// Go `MinTime`.
    pub fn min_time(&self) -> i64 {
        self.inner.min_time
    }

    /// Go `MaxTime`.
    pub fn max_time(&self) -> i64 {
        self.inner.max_time
    }

    /// Go `GetSymbolTableSize`: the symbol table size in the index of this block.
    pub fn get_symbol_table_size(&self) -> u64 {
        self.inner.symbol_table_size
    }

    /// Whether two handles refer to the same open block.
    pub fn ptr_eq(&self, o: &Block) -> bool {
        Arc::ptr_eq(&self.inner, &o.inner)
    }

    /// The number of readers that keep the block open (for tests).
    #[doc(hidden)]
    pub fn pending_readers(&self) -> usize {
        self.inner.lock().pending_readers
    }

    /// Go `setCompactionFailed`.
    pub fn set_compaction_failed(&self) -> Result<(), Error> {
        let mut s = self.inner.lock();
        s.meta.compaction.failed = true;
        let n = write_meta_file(&self.inner.dir, &mut s.meta)?;
        s.num_bytes_meta = n;
        Ok(())
    }

    /// Marks the block deletable and writes its meta file (part of Go `CompactWithBlockPopulator`). Returns the
    /// write error.
    pub(crate) fn set_deletable(&self) -> Result<(), Error> {
        let mut s = self.inner.lock();
        s.meta.compaction.deletable = true;
        let r = write_meta_file(&self.inner.dir, &mut s.meta);
        s.num_bytes_meta = *r.as_ref().unwrap_or(&0);
        r.map(|_| ())
    }

    /// Go `Delete`: deletes the matching series between mint and maxt in the block.
    pub fn delete(&self, ctx: &Context, mint: i64, maxt: i64, ms: &[Matcher]) -> Result<(), Error> {
        let mut s = self.inner.lock();
        if s.closing {
            return Err(ERR_CLOSING);
        }
        let ir: &dyn IndexReader = &*self.inner.indexr;
        let mut p = postings_for_matchers(ctx, ir, ms).map_err(|e| Error::wrap("select series", e))?;

        // Choose only valid postings which have chunks in the time-range.
        let stones = MemTombstones::new();
        let mut chks: Vec<Meta> = Vec::new();
        let mut builder = ScratchBuilder::default();
        while p.next() {
            ir.series(p.at(), &mut builder, &mut chks)?;
            for chk in &chks {
                if chk.overlaps_closed_interval(mint, maxt) {
                    // Delete only until the current values and not beyond.
                    let (tmin, tmax) = clamp_interval(mint, maxt, chks[0].min_time, chks[chks.len() - 1].max_time);
                    stones.add_interval(p.at(), &[Interval::new(tmin, tmax)]);
                    break;
                }
            }
        }
        if let Some(e) = p.err() {
            return Err(index_err(e));
        }
        drop(p);

        s.tombstones
            .iter(&mut |id, ivs| {
                for iv in ivs {
                    stones.add_interval(id, &[*iv]);
                }
                Ok(())
            })
            .map_err(tomb_err)?;
        s.meta.stats.num_tombstones = stones.total();
        s.tombstones = Arc::new(stones);

        let n = tombstones::write_file(&self.inner.logger, &self.inner.dir, &*s.tombstones).map_err(tomb_err)?;
        s.num_bytes_tombstone = n;
        let n = write_meta_file(&self.inner.dir, &mut s.meta)?;
        s.num_bytes_meta = n;
        Ok(())
    }

    /// Go `CleanTombstones`: removes the tombstones and rewrites the block (only if there are any tombstones).
    /// Returns the ULIDs of the new blocks (empty when the result block is empty) and whether the parent block can
    /// be deleted safely.
    pub fn clean_tombstones(&self, dest: &Path, c: &dyn Compactor) -> Result<(Vec<Ulid>, bool), Error> {
        let tombs = self.inner.lock().tombstones.clone();
        let mut num_stones = 0usize;
        // The iteration function only returns Ok.
        let _ = tombs.iter(&mut |_, ivs| {
            num_stones += ivs.len();
            Ok(())
        });
        if num_stones == 0 {
            return Ok((Vec::new(), false));
        }
        let meta = self.meta();
        let uids = c.write(dest, self, meta.min_time, meta.max_time, Some(&meta))?;
        Ok((uids, true))
    }

    /// Go `Snapshot`: creates a snapshot of the block into `dir` (hard links).
    pub fn snapshot(&self, dir: &Path) -> Result<(), Error> {
        let io = |p: &str, e: std::io::Error| Error::wrap(p, Error::other(e));
        let block_dir = dir.join(self.inner.ulid.to_string());
        std::fs::create_dir_all(&block_dir).map_err(|e| io("create snapshot block dir", e))?;
        let chunks_dir = chunk_dir(&block_dir);
        std::fs::create_dir_all(&chunks_dir).map_err(|e| io("create snapshot chunk dir", e))?;

        // Hardlink meta, index and tombstones.
        for fname in [META_FILENAME, INDEX_FILENAME, tombstones::TOMBSTONES_FILENAME] {
            std::fs::hard_link(self.inner.dir.join(fname), block_dir.join(fname))
                .map_err(|e| io(&format!("create snapshot {fname}"), e))?;
        }

        // Hardlink the chunks.
        let cur_chunk_dir = chunk_dir(&self.inner.dir);
        let mut files: Vec<_> = std::fs::read_dir(&cur_chunk_dir)
            .map_err(|e| io("ReadDir the current chunk dir", e))?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect();
        files.sort();
        for f in files {
            std::fs::hard_link(cur_chunk_dir.join(&f), chunks_dir.join(&f)).map_err(|e| io("hardlink a chunk", e))?;
        }
        Ok(())
    }

    /// Go `OverlapsClosedInterval`: whether the block overlaps [mint, maxt].
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        // The block itself is a half-open interval [pb.meta.MinTime, pb.meta.MaxTime).
        self.inner.min_time <= maxt && mint < self.inner.max_time
    }

    /// Go `LabelNames`: all the unique label names present in the block, sorted.
    pub fn label_names(&self, ctx: &Context) -> Result<Vec<String>, Error> {
        IndexReader::label_names(&*self.inner.indexr, ctx, &[])
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner.ulid, f)
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner.ulid, f)
    }
}

impl BlockReader for Block {
    /// Go `Index`: a new index reader against the block data.
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        self.inner.start_read()?;
        Ok(Box::new(BlockIndexReader { ir: self.inner.indexr.clone(), b: Releaser::new(self.inner.clone()) }))
    }

    /// Go `Chunks`: a new chunk reader against the block data.
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        self.inner.start_read()?;
        Ok(Box::new(BlockChunkReader {
            cr: ChunksFileReader(self.inner.chunkr.clone()),
            b: Releaser::new(self.inner.clone()),
        }))
    }

    /// Go `Tombstones`: a new tombstone reader against the block data.
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        self.inner.start_read()?;
        let t = self.inner.lock().tombstones.clone();
        Ok(Arc::new(BlockTombstoneReader { t, b: Releaser::new(self.inner.clone()) }))
    }

    /// Go `Meta`.
    fn meta(&self) -> BlockMeta {
        self.inner.lock().meta.clone()
    }

    /// Go `Size`: the number of bytes that the block takes up.
    fn size(&self) -> i64 {
        let s = self.inner.lock();
        self.inner.num_bytes_chunks + self.inner.num_bytes_index + s.num_bytes_tombstone + s.num_bytes_meta
    }
}

/// Releases one pending reader of a block once (on `close` or on drop).
struct Releaser {
    b: Arc<BlockInner>,
    done: AtomicBool,
}

impl Releaser {
    fn new(b: Arc<BlockInner>) -> Releaser {
        Releaser { b, done: AtomicBool::new(false) }
    }
    fn release(&self) {
        if !self.done.swap(true, Ordering::AcqRel) {
            self.b.done_read();
        }
    }
}

impl Drop for Releaser {
    fn drop(&mut self) {
        self.release();
    }
}

/// Go `blockIndexReader`.
struct BlockIndexReader {
    ir: Arc<index::Reader>,
    b: Releaser,
}

impl BlockIndexReader {
    fn wrap(&self, e: Error) -> Error {
        Error::wrap(format_args!("block: {}", self.b.b.ulid), e)
    }
}

impl IndexReader for BlockIndexReader {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        IndexReader::symbols(&*self.ir)
    }

    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let r = if matchers.is_empty() {
            IndexReader::sorted_label_values(&*self.ir, ctx, name, hints, &[])
        } else {
            self.label_values(ctx, name, hints, matchers).map(|mut st| {
                st.sort_unstable();
                st
            })
        };
        r.map_err(|e| self.wrap(e))
    }

    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        if matchers.is_empty() {
            return IndexReader::label_values(&*self.ir, ctx, name, hints, &[]).map_err(|e| self.wrap(e));
        }
        label_values_with_matchers(ctx, &*self.ir, name, hints, matchers)
    }

    fn postings(&self, ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        IndexReader::postings(&*self.ir, ctx, name, values).map_err(|e| self.wrap(e))
    }

    fn postings_for_label_matching(
        &self,
        ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        IndexReader::postings_for_label_matching(&*self.ir, ctx, name, m)
    }

    fn postings_for_all_label_values(&self, ctx: &Context, name: &str) -> BoxPostings<'_> {
        IndexReader::postings_for_all_label_values(&*self.ir, ctx, name)
    }

    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        IndexReader::sorted_postings(&*self.ir, p)
    }

    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        IndexReader::sharded_postings(&*self.ir, p, shard_index, shard_count)
    }

    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        IndexReader::series(&*self.ir, r, builder, chks).map_err(|e| self.wrap(e))
    }

    fn label_names(&self, ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        if matchers.is_empty() {
            return IndexReader::label_names(&*self.ir, ctx, &[]);
        }
        label_names_with_matchers(ctx, &*self.ir, matchers)
    }

    fn label_names_for(&self, ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error> {
        IndexReader::label_names_for(&*self.ir, ctx, postings)
    }

    fn close(&self) -> Result<(), Error> {
        self.b.release();
        Ok(())
    }
}

/// Go `blockChunkReader`.
struct BlockChunkReader {
    cr: ChunksFileReader,
    b: Releaser,
}

impl ChunkReader for BlockChunkReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        self.cr.chunk_or_iterable(meta)
    }
    fn close(&self) -> Result<(), Error> {
        self.b.release();
        Ok(())
    }
}

/// Go `blockTombstoneReader`.
struct BlockTombstoneReader {
    t: Arc<dyn tombstones::Reader>,
    b: Releaser,
}

impl tombstones::Reader for BlockTombstoneReader {
    fn get(&self, r: SeriesRef) -> Result<tombstones::Intervals, tombstones::Error> {
        self.t.get(r)
    }
    fn iter(
        &self,
        f: &mut dyn FnMut(SeriesRef, &tombstones::Intervals) -> Result<(), tombstones::Error>,
    ) -> Result<(), tombstones::Error> {
        self.t.iter(f)
    }
    fn total(&self) -> u64 {
        self.t.total()
    }
    fn close(&self) -> Result<(), tombstones::Error> {
        self.b.release();
        Ok(())
    }
}

/// Go `clampInterval`.
pub fn clamp_interval(mut a: i64, mut b: i64, mint: i64, maxt: i64) -> (i64, i64) {
    if a < mint {
        a = mint;
    }
    if b > maxt {
        b = maxt;
    }
    (a, b)
}
