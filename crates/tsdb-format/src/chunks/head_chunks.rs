//! Port of `tsdb/chunks/head_chunks.go`: the head chunk files (`chunks_head/000001`, ...) and the
//! ChunkDiskMapper that writes them and reads them back through mmap.

use super::*;
use crate::encoding::{Crc32, uvarint, uvarint_size};
use crate::fileutil::MmapFile;
use chunkenc::{Encoding, Pool};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Go `MagicHeadChunks`: 4 bytes at the beginning of a head chunk file.
pub const MAGIC_HEAD_CHUNKS: u32 = 0x0130BC91;
const HEAD_CHUNKS_FORMAT_V1: u8 = 1;

/// Go `MintMaxtSize`: the size of the mint/maxt for head chunk file and chunks.
pub const MINT_MAXT_SIZE: usize = 8;
/// Go `SeriesRefSize`: the size of series reference on disk.
pub const SERIES_REF_SIZE: usize = 8;
/// Go `HeadChunkFileHeaderSize`: the total size of the header for a head chunk file.
pub const HEAD_CHUNK_FILE_HEADER_SIZE: usize = SEGMENT_HEADER_SIZE;
/// Go `MaxHeadChunkFileSize`: the max size of a head chunk file (128 MiB).
pub const MAX_HEAD_CHUNK_FILE_SIZE: usize = 128 * 1024 * 1024;
/// Go `CRCSize`: the size of crc32 sum on disk.
pub const CRC_SIZE: usize = 4;
/// Go `MaxHeadChunkMetaSize`: the max size of an mmapped chunk minus the chunk data.
pub const MAX_HEAD_CHUNK_META_SIZE: usize = SERIES_REF_SIZE
    + 2 * MINT_MAXT_SIZE
    + CHUNK_ENCODING_SIZE
    + MAX_CHUNK_LENGTH_FIELD_SIZE
    + CRC_SIZE;
/// Go `MinWriteBufferSize`: the minimum write buffer size allowed (64 KiB).
pub const MIN_WRITE_BUFFER_SIZE: usize = 64 * 1024;
/// Go `MaxWriteBufferSize`: the maximum write buffer size allowed (8 MiB).
pub const MAX_WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024;
/// Go `DefaultWriteBufferSize` (4 MiB).
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
/// Go `DefaultWriteQueueSize`: the size of the in-memory queue used before flushing chunks to disk. 0
/// disables the queue.
pub const DEFAULT_WRITE_QUEUE_SIZE: usize = 0;
/// Go `HeadChunkFilePreallocationSize` (non-Windows): the size a new head chunk file is preallocated to.
pub const HEAD_CHUNK_FILE_PREALLOCATION_SIZE: i64 = (MIN_WRITE_BUFFER_SIZE * 2) as i64;

/// Go `OutOfOrderMask`: the high bit of the encoding byte marks out-of-order chunks in head chunk files.
pub const OUT_OF_ORDER_MASK: u8 = 0b1000_0000;

/// Go `ChunkDiskMapperRef`: the location of a head chunk on disk. The upper 4 bytes hold the index of the
/// head chunk file and the lower 4 bytes the byte offset in the file where the chunk starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChunkDiskMapperRef(pub u64);

impl ChunkDiskMapperRef {
    /// Go `newChunkDiskMapperRef`.
    pub fn new(seq: u64, offset: u64) -> ChunkDiskMapperRef {
        ChunkDiskMapperRef((seq << 32) | offset)
    }

    /// Go `Unpack`: (file sequence, offset).
    pub fn unpack(self) -> (usize, usize) {
        ((self.0 >> 32) as usize, ((self.0 << 32) >> 32) as usize)
    }

    /// Go `GreaterThanOrEqualTo`.
    pub fn greater_than_or_equal_to(self, r: ChunkDiskMapperRef) -> bool {
        let (s1, o1) = self.unpack();
        let (s2, o2) = r.unpack();
        s1 > s2 || (s1 == s2 && o1 >= o2)
    }

    /// Go `GreaterThan`.
    pub fn greater_than(self, r: ChunkDiskMapperRef) -> bool {
        let (s1, o1) = self.unpack();
        let (s2, o2) = r.unpack();
        s1 > s2 || (s1 == s2 && o1 > o2)
    }
}

/// Go `chunkPos`: the position in the head chunk files. Not thread-safe; a lock must protect it.
#[derive(Debug, Default)]
struct ChunkPos {
    /// Index of chunk file.
    seq: u64,
    /// Offset within chunk file.
    offset: u64,
    /// When true then the next chunk will be written to a new file.
    cut_file: bool,
}

impl ChunkPos {
    /// Go `getNextChunkRef`: the ref the chunk will have once written, and whether a new file must be cut
    /// before writing it. Must be called in the order in which chunks are written.
    fn get_next_chunk_ref(&mut self, chk_len: u64) -> (ChunkDiskMapperRef, bool) {
        let bytes_to_write = Self::bytes_to_write_for_chunk(chk_len);
        let mut cut_file = false;
        if self.should_cut_new_file(bytes_to_write) {
            self.to_new_file();
            self.cut_file = false;
            cut_file = true;
        }
        let chk_offset = self.offset;
        self.offset += bytes_to_write;
        (ChunkDiskMapperRef::new(self.seq, chk_offset), cut_file)
    }

    /// Go `toNewFile`.
    #[allow(clippy::wrong_self_convention)]
    fn to_new_file(&mut self) {
        self.seq += 1;
        self.offset = SEGMENT_HEADER_SIZE as u64;
    }

    /// Go `cutFileOnNextChunk`.
    fn cut_file_on_next_chunk(&mut self) {
        self.cut_file = true;
    }

    /// Go `setSeq`.
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    /// Go `shouldCutNewFile`.
    fn should_cut_new_file(&self, bytes_to_write: u64) -> bool {
        if self.cut_file {
            return true;
        }
        self.offset == 0 || // First head chunk file.
            self.offset + bytes_to_write > MAX_HEAD_CHUNK_FILE_SIZE as u64 // Exceeds the max head chunk file size.
    }

    /// Go `bytesToWriteForChunk`: bytes written for a chunk of the given data length, including all meta data.
    fn bytes_to_write_for_chunk(chk_len: u64) -> u64 {
        // Headers.
        let mut bytes = (SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE + CHUNK_ENCODING_SIZE) as u64;
        // Size of chunk length encoded as uvarint.
        bytes += uvarint_size(chk_len) as u64;
        // Chunk length.
        bytes += chk_len;
        // crc32.
        bytes += CRC_SIZE as u64;
        bytes
    }
}

/// Go `mmappedChunkFile`: mmap access to an entire head chunk file. The mapping is shared so that readers can
/// use it without holding the lock; it is unmapped when the last user drops it.
#[derive(Debug)]
struct MmappedChunkFile {
    byte_slice: Arc<MmapFile>,
    /// Max timestamp among all of this file's chunks.
    maxt: i64,
}

impl MmappedChunkFile {
    fn bytes(&self) -> &[u8] {
        self.byte_slice.bytes()
    }
}

/// Writer state (Go fields guarded by `writePathMtx`).
struct WriteState {
    dir_file: Option<File>,
    /// File being written to, and its path.
    cur_file: Option<(File, PathBuf)>,
    /// Used for the size retention.
    cur_file_maxt: i64,
    /// Buffer used to write the header of the chunk.
    byte_buf: [u8; MAX_HEAD_CHUNK_META_SIZE],
    /// Writer for the current open file.
    chk_writer: Option<BufWriter>,
    crc32: Crc32,
    /// Whether the maxt is set for all mmapped files (after `iterate_all_chunks`).
    file_maxt_set: bool,
}

/// Reader state (Go fields guarded by `readPathMtx`). The file number on disk is the key.
struct ReadState {
    mmapped_chunk_files: BTreeMap<usize, MmappedChunkFile>,
    /// Index of the current open file being appended to; 0 if no file is active.
    cur_file_sequence: usize,
}

/// State shared with the write queue worker.
struct Inner {
    dir: PathBuf,
    write_buffer_size: usize,
    write: Mutex<WriteState>,
    read: RwLock<ReadState>,
    /// Bytes written in the current open file.
    cur_file_offset: AtomicU64,
    /// Chunks are flushed to disk in batches; they are served from this buffer until flushed.
    chunk_buffer: ChunkBuffer,
    /// Used when fetching a chunk from disk to allocate a chunk.
    pool: Arc<dyn Pool>,
    closed: AtomicBool,
}

/// Go `ChunkDiskMapper`: writes the head block chunks to disk and gives access to them through mmapped files.
pub struct ChunkDiskMapper {
    inner: Arc<Inner>,
    /// The file position which will eventually be reached once the write queue has been processed. Locking it
    /// serializes `write_chunk` calls.
    evtl_pos: Mutex<ChunkPos>,
    write_queue: Option<ChunkWriteQueue>,
}

impl fmt::Debug for ChunkDiskMapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkDiskMapper")
            .field("dir", &self.inner.dir)
            .finish()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

fn rlock<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|p| p.into_inner())
}

fn wlock<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|p| p.into_inner())
}

/// Where a chunk was found.
enum Located {
    Mem(Arc<dyn Chunk>),
    Mmap(Arc<MmapFile>, Encoding, usize, usize),
}

impl ChunkDiskMapper {
    /// Go `NewChunkDiskMapper`: a ChunkDiskMapper against `dir` (created if missing). With no pool, a new
    /// `chunkenc::DefaultPool` is used. `write_queue_size` 0 disables the write queue.
    /// NOTE: `iterate_all_chunks` must be called at least once after creating it to set the maxt of all files.
    pub fn new(
        dir: &Path,
        pool: Option<Arc<dyn Pool>>,
        write_buffer_size: usize,
        write_queue_size: usize,
    ) -> Result<ChunkDiskMapper, Error> {
        // Validate write buffer size.
        if !(MIN_WRITE_BUFFER_SIZE..=MAX_WRITE_BUFFER_SIZE).contains(&write_buffer_size) {
            return Err(Error::Msg(format!(
                "ChunkDiskMapper write buffer size should be between {MIN_WRITE_BUFFER_SIZE} and {MAX_WRITE_BUFFER_SIZE} (actual: {write_buffer_size})"
            )));
        }
        if !write_buffer_size.is_multiple_of(1024) {
            return Err(Error::Msg(format!(
                "ChunkDiskMapper write buffer size should be a multiple of 1024 (actual: {write_buffer_size})"
            )));
        }
        std::fs::create_dir_all(dir)?;
        let dir_file = crate::fileutil::open_dir(dir)?;

        let inner = Arc::new(Inner {
            dir: dir.to_path_buf(),
            write_buffer_size,
            write: Mutex::new(WriteState {
                dir_file: Some(dir_file),
                cur_file: None,
                cur_file_maxt: 0,
                byte_buf: [0; MAX_HEAD_CHUNK_META_SIZE],
                chk_writer: None,
                crc32: Crc32::new(),
                file_maxt_set: false,
            }),
            read: RwLock::new(ReadState {
                mmapped_chunk_files: BTreeMap::new(),
                cur_file_sequence: 0,
            }),
            cur_file_offset: AtomicU64::new(0),
            chunk_buffer: ChunkBuffer::new(),
            pool: pool.unwrap_or_else(|| Arc::new(chunkenc::new_pool())),
            closed: AtomicBool::new(false),
        });

        let write_queue = if write_queue_size > 0 {
            let w = inner.clone();
            let f: WriteChunkF =
                Arc::new(move |series_ref, mint, maxt, chk, r, is_ooo, cut_file| {
                    let chk = chk.expect("chunk write job without a chunk");
                    w.write_chunk(series_ref, mint, maxt, chk, r, is_ooo, cut_file)
                });
            Some(ChunkWriteQueue::new(write_queue_size, f))
        } else {
            None
        };

        let mut m = ChunkDiskMapper {
            inner,
            evtl_pos: Mutex::new(ChunkPos::default()),
            write_queue,
        };
        if let Err(e) = m.open_mmap_files() {
            if let Some(q) = &m.write_queue {
                q.stop();
            }
            m.inner.closed.store(true, Ordering::SeqCst);
            return Err(e);
        }
        Ok(m)
    }

    /// Go `ApplyOutOfOrderMask`.
    pub fn apply_out_of_order_mask(source_encoding: Encoding) -> Encoding {
        Encoding(source_encoding.0 | OUT_OF_ORDER_MASK)
    }

    /// Go `IsOutOfOrderChunk`.
    pub fn is_out_of_order_chunk(e: Encoding) -> bool {
        e.0 & OUT_OF_ORDER_MASK != 0
    }

    /// Go `RemoveMasks`.
    pub fn remove_masks(source_encoding: Encoding) -> Encoding {
        Encoding(source_encoding.0 & !OUT_OF_ORDER_MASK)
    }

    /// The directory of the head chunk files.
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    /// Go `openMMapFiles`: opens all files in the directory for mmapping.
    fn open_mmap_files(&mut self) -> Result<(), Error> {
        let mut rs = wlock(&self.inner.read);
        rs.mmapped_chunk_files.clear();
        let res = (|| -> Result<u64, Error> {
            let files = list_chunk_files(&self.inner.dir)?;
            let files = repair_last_chunk_file(files)?;

            for (&seq, fn_) in &files {
                let f = MmapFile::open(fn_).map_err(|e| {
                    Error::Io(e).wrap(&format!("mmap files, file: {}", fn_.display()))
                })?;
                rs.mmapped_chunk_files.insert(
                    seq,
                    MmappedChunkFile {
                        byte_slice: Arc::new(f),
                        maxt: 0,
                    },
                );
            }

            // Check for gaps in the files.
            let indices: Vec<usize> = rs.mmapped_chunk_files.keys().copied().collect();
            let Some(&first) = indices.first() else {
                return Ok(0);
            };
            let mut last_seq = first;
            for &seq in &indices[1..] {
                if seq != last_seq + 1 {
                    return Err(Error::Msg(format!(
                        "found unsequential head chunk files {} (index: {}) and {} (index: {})",
                        files[&last_seq].display(),
                        last_seq,
                        files[&seq].display(),
                        seq
                    )));
                }
                last_seq = seq;
            }

            for (i, b) in &rs.mmapped_chunk_files {
                let b = b.bytes();
                if b.len() < HEAD_CHUNK_FILE_HEADER_SIZE {
                    return Err(Error::Msg(format!(
                        "{}: invalid head chunk file header: {}",
                        files[i].display(),
                        err_invalid_size()
                    )));
                }
                // Verify magic number.
                let m = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
                if m != MAGIC_HEAD_CHUNKS {
                    return Err(Error::Msg(format!(
                        "{}: invalid magic number {m:x}",
                        files[i].display()
                    )));
                }
                // Verify chunk format version.
                let v = b[MAGIC_CHUNKS_SIZE];
                if v != CHUNKS_FORMAT_V1 {
                    return Err(Error::Msg(format!(
                        "{}: invalid chunk format version {v}",
                        files[i].display()
                    )));
                }
            }
            Ok(last_seq as u64)
        })();
        match res {
            Ok(last_seq) => {
                lock(&self.evtl_pos).set_seq(last_seq);
                Ok(())
            }
            Err(e) => {
                rs.mmapped_chunk_files.clear();
                Err(e)
            }
        }
    }

    /// Go `WriteChunk`: writes the chunk to disk (through the write queue if enabled) and returns its ref. The
    /// callback gets the result of the write.
    pub fn write_chunk(
        &self,
        series_ref: HeadSeriesRef,
        mint: i64,
        maxt: i64,
        chk: Arc<dyn Chunk>,
        is_ooo: bool,
        callback: Option<WriteCallback>,
    ) -> ChunkDiskMapperRef {
        // The evtl_pos lock must be held to serialize the calls to get_next_chunk_ref() and the writing of the
        // chunk (either with or without queue).
        let mut pos = lock(&self.evtl_pos);
        let (r, cut_file) = pos.get_next_chunk_ref(chk.bytes().len() as u64);

        if let Some(q) = &self.write_queue {
            let job = ChunkWriteJob {
                cut_file,
                series_ref,
                mint,
                maxt,
                chk: Some(chk),
                r#ref: r,
                is_ooo,
                callback,
            };
            if let Err((
                e,
                ChunkWriteJob {
                    callback: Some(cb), ..
                },
            )) = q.try_add_job(job)
            {
                cb(Err(e));
            }
            return r;
        }

        let res = self
            .inner
            .write_chunk(series_ref, mint, maxt, &chk, r, is_ooo, cut_file);
        if let Some(cb) = callback {
            cb(res);
        }
        r
    }

    /// Go `CutNewFile`: a new file will be created the next time a chunk is written.
    pub fn cut_new_file(&self) {
        lock(&self.evtl_pos).cut_file_on_next_chunk();
    }

    /// Go `IsQueueEmpty`.
    pub fn is_queue_empty(&self) -> bool {
        match &self.write_queue {
            None => true,
            Some(q) => q.queue_is_empty(),
        }
    }

    /// Go `Chunk`: the chunk at `r`. A chunk that is still queued or buffered is shared; a chunk read from disk
    /// gets its own copy of the data (from the pool), so it stays valid after the file is unmapped.
    pub fn chunk(&self, r: ChunkDiskMapperRef) -> Result<Arc<dyn Chunk>, Error> {
        match self.locate(r)? {
            Located::Mem(c) => Ok(c),
            Located::Mmap(m, enc, s, e) => {
                let (sgm_index, _) = r.unpack();
                let c = self
                    .inner
                    .pool
                    .get(enc, m.bytes()[s..e].to_vec())
                    .map_err(|err| {
                        Error::from(CorruptionErr {
                            dir: self.inner.dir.clone(),
                            file_index: sgm_index as i64,
                            err: Error::Chunk(err),
                        })
                    })?;
                Ok(Arc::from(c))
            }
        }
    }

    /// Like `chunk`, but calls `f` with the encoding and the data of the chunk without copying it (the bytes of
    /// an mmapped file stay mapped during the call). Iterate the data with `chunkenc::AnyIterator::new`.
    pub fn chunk_bytes_with<R>(
        &self,
        r: ChunkDiskMapperRef,
        f: impl FnOnce(Encoding, &[u8]) -> R,
    ) -> Result<R, Error> {
        Ok(match self.locate(r)? {
            Located::Mem(c) => f(c.encoding(), c.bytes()),
            Located::Mmap(m, enc, s, e) => f(enc, &m.bytes()[s..e]),
        })
    }

    fn locate(&self, r: ChunkDiskMapperRef) -> Result<Located, Error> {
        // The read lock is held for the whole lookup so that close() cannot run in between.
        let rs = rlock(&self.inner.read);
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }

        if let Some(q) = &self.write_queue
            && let Some(c) = q.get(r)
        {
            return Ok(Located::Mem(c));
        }

        let (sgm_index, chk_start) = r.unpack();
        // We skip the series ref and the mint/maxt beforehand.
        let chk_start = chk_start + SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE;

        // If it is the current open file, then the chunks can be in the buffer too.
        if sgm_index == rs.cur_file_sequence
            && let Some(c) = self.inner.chunk_buffer.get(r)
        {
            return Ok(Located::Mem(c));
        }

        let corruption = |file_index: i64, err: Error| -> Error {
            Error::from(CorruptionErr {
                dir: self.inner.dir.clone(),
                file_index,
                err,
            })
        };

        let Some(mmap_file) = rs.mmapped_chunk_files.get(&sgm_index) else {
            if sgm_index > rs.cur_file_sequence {
                return Err(corruption(
                    -1,
                    Error::Msg(format!(
                        "head chunk file index {sgm_index} more than current open file"
                    )),
                ));
            }
            return Err(corruption(
                sgm_index as i64,
                Error::Msg(format!(
                    "head chunk file index {sgm_index} does not exist on disk"
                )),
            ));
        };
        let b = mmap_file.bytes();
        let fi = sgm_index as i64;

        if chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE > b.len() {
            return Err(corruption(
                fi,
                Error::Msg(format!(
                    "head chunk file doesn't include enough bytes to read the chunk size data field - required:{}, available:{}",
                    chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE,
                    b.len()
                )),
            ));
        }

        // Encoding. ChunkDiskMapper uses only the last 7 bits for the encoding.
        let chk_enc = Self::remove_masks(Encoding(b[chk_start]));
        // Data length. With the minimum chunk length this should never cause us reading over the end of the
        // slice.
        let chk_data_len_start = chk_start + CHUNK_ENCODING_SIZE;
        let (chk_data_len, n) = uvarint(
            &b[chk_data_len_start..(chk_data_len_start + MAX_CHUNK_LENGTH_FIELD_SIZE).min(b.len())],
        );
        if n <= 0 {
            return Err(corruption(
                fi,
                Error::Msg(format!("reading chunk length failed with {n}")),
            ));
        }
        if chk_data_len > i64::MAX as u64 {
            return Err(corruption(
                fi,
                Error::Msg(format!(
                    "chunk length {chk_data_len} exceeds supported size"
                )),
            ));
        }
        let n = n as usize;
        let chk_data_len = chk_data_len as usize;
        if chk_data_len_start > i64::MAX as usize - n - chk_data_len {
            return Err(corruption(
                fi,
                Error::Msg(format!(
                    "chunk data end overflows supported size (start={chk_data_len_start}, len={chk_data_len}, n={n})"
                )),
            ));
        }

        // Verify the chunk data end.
        let chk_data_end = chk_data_len_start + n + chk_data_len;
        if chk_data_end > b.len() {
            return Err(corruption(
                fi,
                Error::Msg(format!(
                    "head chunk file doesn't include enough bytes to read the chunk - required:{}, available:{}",
                    chk_data_end,
                    b.len()
                )),
            ));
        }

        // Check the CRC.
        if chk_data_end + CRC_SIZE > b.len() {
            // Go panics on the out of range slice here; report it as corruption instead.
            return Err(corruption(
                fi,
                Error::Msg(format!(
                    "head chunk file doesn't include enough bytes to read the chunk - required:{}, available:{}",
                    chk_data_end + CRC_SIZE,
                    b.len()
                )),
            ));
        }
        let sum = &b[chk_data_end..chk_data_end + CRC_SIZE];
        if let Err(e) = check_crc32(
            &b[chk_start - (SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE)..chk_data_end],
            sum,
        ) {
            return Err(corruption(fi, e));
        }

        Ok(Located::Mmap(
            mmap_file.byte_slice.clone(),
            chk_enc,
            chk_data_end - chk_data_len,
            chk_data_end,
        ))
    }

    /// Go `IterateAllChunks`: iterates all mmapped files (in order of the file number) and all the chunks in
    /// them, calling `f(series_ref, chunk_ref, mint, maxt, num_samples, encoding, is_ooo)`. It returns on the
    /// first error. It must be called at least once after `new` to set the maxt of all files.
    pub fn iterate_all_chunks<F>(&self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(
            HeadSeriesRef,
            ChunkDiskMapperRef,
            i64,
            i64,
            u16,
            Encoding,
            bool,
        ) -> Result<(), Error>,
    {
        let mut ws = lock(&self.inner.write);
        let res = self.iterate_all_chunks_locked(&mut f);
        ws.file_maxt_set = true;
        drop(ws);
        res
    }

    fn iterate_all_chunks_locked<F>(&self, f: &mut F) -> Result<(), Error>
    where
        F: FnMut(
            HeadSeriesRef,
            ChunkDiskMapperRef,
            i64,
            i64,
            u16,
            Encoding,
            bool,
        ) -> Result<(), Error>,
    {
        let dir = &self.inner.dir;
        let corruption = |file_index: usize, err: Error| -> Error {
            Error::from(CorruptionErr {
                dir: dir.clone(),
                file_index: file_index as i64,
                err,
            })
        };

        // Iterate files in ascending order. The mappings are taken out of the lock so that `f` can read chunks.
        let (files, cur_seq): (Vec<(usize, Arc<MmapFile>, i64)>, usize) = {
            let rs = rlock(&self.inner.read);
            (
                rs.mmapped_chunk_files
                    .iter()
                    .map(|(&s, m)| (s, m.byte_slice.clone(), m.maxt))
                    .collect(),
                rs.cur_file_sequence,
            )
        };
        for (seg_id, mmap, file_maxt) in files {
            let b = mmap.bytes();
            let mut maxt_seen = file_maxt;
            let mut file_end = b.len();
            if seg_id == cur_seq {
                file_end = (self.cur_file_size() as usize).min(b.len());
            }
            let mut idx = HEAD_CHUNK_FILE_HEADER_SIZE;
            let res = (|| -> Result<(), Error> {
                while idx < file_end {
                    if file_end - idx < MAX_HEAD_CHUNK_META_SIZE {
                        // Check for all 0s which marks the end of the file.
                        if b[idx..file_end].iter().all(|&x| x == 0) {
                            // End of segment chunk file content.
                            break;
                        }
                        return Err(corruption(
                            seg_id,
                            Error::Msg(format!(
                                "head chunk file has some unread data, but doesn't include enough bytes to read the chunk header - required:{}, available:{}, file:{}",
                                idx + MAX_HEAD_CHUNK_META_SIZE,
                                file_end,
                                seg_id
                            )),
                        ));
                    }
                    let chunk_ref = ChunkDiskMapperRef::new(seg_id as u64, idx as u64);

                    let start_idx = idx;
                    let series_ref =
                        u64::from_be_bytes(b[idx..idx + SERIES_REF_SIZE].try_into().unwrap());
                    idx += SERIES_REF_SIZE;
                    let mint = i64::from_be_bytes(b[idx..idx + MINT_MAXT_SIZE].try_into().unwrap());
                    idx += MINT_MAXT_SIZE;
                    let maxt = i64::from_be_bytes(b[idx..idx + MINT_MAXT_SIZE].try_into().unwrap());
                    idx += MINT_MAXT_SIZE;

                    // Files are preallocated. As series ref always starts from 1, we assume it being 0 to be the
                    // end of the actual file data. Additionally we are checking mint and maxt just to be sure.
                    if series_ref == 0 && mint == 0 && maxt == 0 {
                        break;
                    }

                    let chk_enc = Encoding(b[idx]);
                    idx += CHUNK_ENCODING_SIZE;
                    let (data_len, n) = uvarint(&b[idx..idx + MAX_CHUNK_LENGTH_FIELD_SIZE]);
                    // Go adds n even when it is not positive.
                    idx = (idx as isize + n) as usize;

                    let num_samples = u16::from_be_bytes([b[idx], b[idx + 1]]);
                    // Skip the data. In the beginning we only checked for the chunk meta size. Now that we have
                    // added the chunk data length, we check for sufficient bytes again.
                    let end = (idx as u128) + data_len as u128;
                    if end + CRC_SIZE as u128 > file_end as u128 {
                        return Err(corruption(
                            seg_id,
                            Error::Msg(format!(
                                "head chunk file doesn't include enough bytes to read the chunk header - required:{}, available:{}, file:{}",
                                end + CRC_SIZE as u128,
                                file_end,
                                seg_id
                            )),
                        ));
                    }
                    idx = end as usize;

                    // Check CRC.
                    let sum = &b[idx..idx + CRC_SIZE];
                    if let Err(e) = check_crc32(&b[start_idx..idx], sum) {
                        return Err(corruption(seg_id, e));
                    }
                    idx += CRC_SIZE;

                    if maxt > maxt_seen {
                        maxt_seen = maxt;
                    }
                    let is_ooo = Self::is_out_of_order_chunk(chk_enc);
                    // Extract the encoding from the byte. ChunkDiskMapper uses only the last 7 bits for the
                    // encoding.
                    let chk_enc = Self::remove_masks(chk_enc);
                    if let Err(err) = f(
                        series_ref,
                        chunk_ref,
                        mint,
                        maxt,
                        num_samples,
                        chk_enc,
                        is_ooo,
                    ) {
                        if let Error::Corruption(mut c) = err {
                            c.dir = dir.clone();
                            c.file_index = seg_id as i64;
                            return Err(Error::Corruption(c));
                        }
                        return Err(err);
                    }
                }

                if idx > file_end {
                    // It should be equal to the slice length.
                    return Err(corruption(
                        seg_id,
                        Error::Msg(format!(
                            "head chunk file doesn't include enough bytes to read the last chunk data - required:{}, available:{}, file:{}",
                            idx, file_end, seg_id
                        )),
                    ));
                }
                Ok(())
            })();
            if maxt_seen != file_maxt
                && let Some(m) = wlock(&self.inner.read).mmapped_chunk_files.get_mut(&seg_id)
                && maxt_seen > m.maxt
            {
                m.maxt = maxt_seen;
            }
            res?;
        }
        Ok(())
    }

    /// Go `Truncate`: deletes the head chunk files with numbers less than `file_no`. The current file is never
    /// deleted; if it has chunks, a new file is cut on the next write.
    pub fn truncate(&self, file_no: u32) -> Result<(), Error> {
        let (chk_file_indices_len, removed_files) = {
            let rs = rlock(&self.inner.read);
            // Sorted file indices, else if files deletion fails in between, it can lead to unsequential files.
            let mut removed = Vec::new();
            for &seq in rs.mmapped_chunk_files.keys() {
                if seq == rs.cur_file_sequence || seq as u32 >= file_no {
                    break;
                }
                removed.push(seq);
            }
            (rs.mmapped_chunk_files.len(), removed)
        };

        // Cut a new file only if the current file has some chunks.
        if self.cur_file_size() > HEAD_CHUNK_FILE_HEADER_SIZE as u64 {
            // There is a known race here between the check of cur_file_size() and cut_new_file(): a new file
            // could already be cut, which only results in an empty file that does no harm.
            self.cut_new_file();
        }
        let n_removed = removed_files.len();
        let res = self.inner.delete_files(removed_files);

        if chk_file_indices_len == n_removed {
            // All files were deleted. Reset the current sequence.
            let mut pos = lock(&self.evtl_pos);
            // We can safely reset the sequence only if the write queue is empty. If it's not empty, then there
            // may be a job in the queue that will create a new segment file with an ID generated before the
            // sequence reset. queue_is_empty() must be called while holding evtl_pos to avoid a race with
            // write_chunk().
            if self.write_queue.as_ref().is_none_or(|q| q.queue_is_empty()) {
                match &res {
                    Ok(()) => pos.set_seq(0),
                    // In case of error, set it to the last file number on the disk that was not deleted.
                    Err((pending, _)) => pos.set_seq(*pending.last().unwrap() as u64),
                }
            }
        }
        res.map_err(|(_, e)| e)
    }

    /// Go `DeleteCorrupted`: deletes all the head chunk files after the one which had the corruption
    /// (including the corrupt file).
    pub fn delete_corrupted(&self, original_err: &Error) -> Result<(), Error> {
        let Some(cerr) = original_err.as_corruption() else {
            return Err(Error::Msg(format!("cannot handle error: {original_err}")));
        };
        // Delete all the head chunk files following the corrupt head chunk file.
        let mut segs = Vec::new();
        let mut last_seq = 0usize;
        {
            let rs = rlock(&self.inner.read);
            for &seg in rs.mmapped_chunk_files.keys() {
                if seg as i64 >= cerr.file_index {
                    segs.push(seg);
                } else if seg > last_seq {
                    last_seq = seg;
                }
            }
        }
        let res = self.inner.delete_files(segs);
        let mut pos = lock(&self.evtl_pos);
        match res {
            Ok(()) => {
                pos.set_seq(last_seq as u64);
                Ok(())
            }
            Err((pending, e)) => {
                // In case of error, set it to the last file number on the disk that was not deleted.
                pos.set_seq(*pending.last().unwrap() as u64);
                Err(e)
            }
        }
    }

    /// Go `Size`: the size of the chunk files.
    pub fn size(&self) -> Result<i64, Error> {
        Ok(crate::fileutil::dir_size(&self.inner.dir)?)
    }

    fn cur_file_size(&self) -> u64 {
        self.inner.cur_file_offset.load(Ordering::SeqCst)
    }

    /// Go `Close`: closes all the open files. Chunks cannot be read afterwards.
    pub fn close(&self) -> Result<(), Error> {
        // Locking the eventual position lock blocks write_chunk().
        let _pos = lock(&self.evtl_pos);
        if let Some(q) = &self.write_queue {
            q.stop();
        }
        // write_chunk locks the write state first and then the read state for cutting files. The lock order
        // must not be reversed here, else it can cause deadlocks.
        let mut ws = lock(&self.inner.write);
        let mut rs = wlock(&self.inner.read);
        if self.inner.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.closed.store(true, Ordering::SeqCst);

        rs.mmapped_chunk_files.clear();
        let fin = self.inner.finalize_cur_file(&mut ws).err();
        ws.dir_file = None;
        match join_errors(vec![fin]) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    // ----------------------------------------------------------------------------------------------------------
    // Internals for tests (Go tests read the unexported fields).

    #[doc(hidden)]
    pub fn test_cur_file_sequence(&self) -> usize {
        rlock(&self.inner.read).cur_file_sequence
    }

    #[doc(hidden)]
    pub fn test_chk_writer_buffered(&self) -> usize {
        lock(&self.inner.write)
            .chk_writer
            .as_ref()
            .map_or(0, |w| w.buffered())
    }

    #[doc(hidden)]
    pub fn test_cur_file_name(&self) -> Option<PathBuf> {
        lock(&self.inner.write)
            .cur_file
            .as_ref()
            .map(|(_, p)| p.clone())
    }

    /// The numbers of the mmapped files, in order.
    #[doc(hidden)]
    pub fn test_mmapped_chunk_files(&self) -> Vec<usize> {
        rlock(&self.inner.read)
            .mmapped_chunk_files
            .keys()
            .copied()
            .collect()
    }

    #[doc(hidden)]
    pub fn test_file_maxt_set(&self) -> bool {
        lock(&self.inner.write).file_maxt_set
    }

    /// Go `cdm.cut()`.
    #[doc(hidden)]
    pub fn test_cut(&self) -> Result<(usize, usize), Error> {
        let mut ws = lock(&self.inner.write);
        self.inner.cut(&mut ws)
    }

    /// Go `cdm.evtlPos.toNewFile()`.
    #[doc(hidden)]
    pub fn test_evtl_pos_to_new_file(&self) {
        lock(&self.evtl_pos).to_new_file();
    }

    /// The write queue, if enabled.
    #[doc(hidden)]
    pub fn test_write_queue(&self) -> Option<&ChunkWriteQueue> {
        self.write_queue.as_ref()
    }
}

impl Drop for ChunkDiskMapper {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Inner {
    /// Go `writeChunk`.
    #[allow(clippy::too_many_arguments)]
    fn write_chunk(
        &self,
        series_ref: HeadSeriesRef,
        mint: i64,
        maxt: i64,
        chk: &Arc<dyn Chunk>,
        r: ChunkDiskMapperRef,
        is_ooo: bool,
        cut_file: bool,
    ) -> Result<(), Error> {
        let mut guard = lock(&self.write);
        let ws = &mut *guard;
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }

        if cut_file {
            self.cut_and_expect_ref(ws, r)?;
        }

        let data = chk.bytes();
        // If the chunk is not smaller than the buffer, no need to flush here, as we have to flush at the end
        // (to not keep partial chunks in the buffer).
        let available = ws.chk_writer.as_ref().map_or(0, |w| w.available());
        if data.len() + MAX_HEAD_CHUNK_META_SIZE < self.write_buffer_size
            && available < MAX_HEAD_CHUNK_META_SIZE + data.len()
        {
            self.flush_buffer(ws)?;
        }

        ws.crc32.reset();
        let buf = &mut ws.byte_buf;
        let mut bytes_written = 0;
        buf[bytes_written..bytes_written + 8].copy_from_slice(&series_ref.to_be_bytes());
        bytes_written += SERIES_REF_SIZE;
        buf[bytes_written..bytes_written + 8].copy_from_slice(&mint.to_be_bytes());
        bytes_written += MINT_MAXT_SIZE;
        buf[bytes_written..bytes_written + 8].copy_from_slice(&maxt.to_be_bytes());
        bytes_written += MINT_MAXT_SIZE;
        let mut enc = chk.encoding();
        if is_ooo {
            enc = ChunkDiskMapper::apply_out_of_order_mask(enc);
        }
        buf[bytes_written] = enc.0;
        bytes_written += CHUNK_ENCODING_SIZE;
        bytes_written += put_uvarint(&mut buf[bytes_written..], data.len() as u64);

        let header = ws.byte_buf;
        self.write_and_append_to_crc32(ws, &header[..bytes_written])?;
        self.write_and_append_to_crc32(ws, data)?;
        let sum = ws.crc32.sum32().to_be_bytes();
        self.write(ws, &sum)?;

        if maxt > ws.cur_file_maxt {
            ws.cur_file_maxt = maxt;
        }

        self.chunk_buffer.put(r, chk.clone());

        if data.len() + MAX_HEAD_CHUNK_META_SIZE >= self.write_buffer_size {
            // The chunk was bigger than the buffer itself. Flushing to not keep partial chunks in buffer.
            self.flush_buffer(ws)?;
        }
        Ok(())
    }

    /// Go `cutAndExpectRef`: cuts a new file and checks that its position matches the chunk ref.
    fn cut_and_expect_ref(
        &self,
        ws: &mut WriteState,
        chk_ref: ChunkDiskMapperRef,
    ) -> Result<(), Error> {
        let (seq, offset) = self.cut(ws)?;
        let (exp_seq, exp_offset) = chk_ref.unpack();
        if seq != exp_seq || offset != exp_offset {
            return Err(Error::Msg(format!(
                "expected newly cut file to have sequence:offset {exp_seq}:{exp_offset}, got {seq}:{offset}"
            )));
        }
        Ok(())
    }

    /// Go `cut`: creates a new mmapped file and returns its sequence and the offset to start writing chunks.
    fn cut(&self, ws: &mut WriteState) -> Result<(usize, usize), Error> {
        let had_cur_file = ws.cur_file.is_some();
        // Sync current tail to disk and close.
        self.finalize_cur_file(ws)?;

        let dir_file = ws.dir_file.as_ref().ok_or(Error::Closed)?;
        let (offset, new_file, path, seq) = cut_segment_file(
            &self.dir,
            dir_file,
            MAGIC_HEAD_CHUNKS,
            HEAD_CHUNKS_FORMAT_V1,
            HEAD_CHUNK_FILE_PREALLOCATION_SIZE,
        )?;
        let seq = seq as usize;

        self.cur_file_offset.store(offset as u64, Ordering::SeqCst);

        if had_cur_file {
            let mut rs = wlock(&self.read);
            let cur = rs.cur_file_sequence;
            if let Some(m) = rs.mmapped_chunk_files.get_mut(&cur) {
                m.maxt = ws.cur_file_maxt;
            }
        }

        // The file is mapped at the max size so that chunks written later are visible without remapping.
        let mmap_file = MmapFile::open_with_size(&path, MAX_HEAD_CHUNK_FILE_SIZE as i64)?;

        {
            let mut rs = wlock(&self.read);
            rs.cur_file_sequence = seq;
            rs.mmapped_chunk_files.insert(
                seq,
                MmappedChunkFile {
                    byte_slice: Arc::new(mmap_file),
                    maxt: 0,
                },
            );
        }
        ws.cur_file = Some((new_file, path));
        if ws.chk_writer.is_none() {
            ws.chk_writer = Some(BufWriter::new(self.write_buffer_size));
        }
        ws.cur_file_maxt = 0;
        Ok((seq, offset))
    }

    /// Go `finalizeCurFile`: writes all pending data to the current file, syncs and closes it.
    fn finalize_cur_file(&self, ws: &mut WriteState) -> Result<(), Error> {
        if ws.cur_file.is_none() {
            return Ok(());
        }
        self.flush_buffer(ws)?;
        let (f, _) = ws.cur_file.take().unwrap();
        f.sync_all()?;
        Ok(())
    }

    fn write(&self, ws: &mut WriteState, b: &[u8]) -> Result<(), Error> {
        let (Some(w), Some((f, _))) = (&mut ws.chk_writer, &mut ws.cur_file) else {
            return Err(Error::msg("no head chunk file to write to"));
        };
        let (n, err) = w.write(f, b);
        self.cur_file_offset.fetch_add(n as u64, Ordering::SeqCst);
        match err {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    fn write_and_append_to_crc32(&self, ws: &mut WriteState, b: &[u8]) -> Result<(), Error> {
        self.write(ws, b)?;
        ws.crc32.write(b);
        Ok(())
    }

    /// Go `flushBuffer`: flushes the buffered chunks to the file and clears the chunk buffer.
    fn flush_buffer(&self, ws: &mut WriteState) -> Result<(), Error> {
        if let (Some(w), Some((f, _))) = (&mut ws.chk_writer, &mut ws.cur_file) {
            w.flush(f)?;
        }
        self.chunk_buffer.clear();
        Ok(())
    }

    /// Go `deleteFiles`: deletes the given files in order. On error, returns the sorted files that were not
    /// deleted from disk.
    fn delete_files(&self, mut removed_files: Vec<usize>) -> Result<(), (Vec<usize>, Error)> {
        removed_files.sort_unstable();
        {
            let mut rs = wlock(&self.read);
            for seq in &removed_files {
                // Dropping the entry unmaps the file once no reader uses it.
                rs.mmapped_chunk_files.remove(seq);
            }
        }
        // We actually delete the files separately to not block the read lock for long.
        for (i, &seq) in removed_files.iter().enumerate() {
            if let Err(e) = std::fs::remove_file(segment_file(&self.dir, seq as i64)) {
                return Err((removed_files[i..].to_vec(), e.into()));
            }
        }
        Ok(())
    }
}

/// Go `listChunkFiles`: the head chunk files of `dir` by number.
fn list_chunk_files(dir: &Path) -> Result<BTreeMap<usize, PathBuf>, Error> {
    Ok(read_seq_dir(dir)?
        .into_iter()
        .map(|(s, p)| (s as usize, p))
        .collect())
}

/// Go `HardLinkChunkFiles`: hard links the chunk files of `src` into `dst`. Does nothing if `src` does not
/// exist; creates `dst` if needed.
pub fn hard_link_chunk_files(src: &Path, dst: &Path) -> Result<(), Error> {
    match std::fs::metadata(src) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::Io(e).wrap("check source chunks dir")),
        Ok(_) => {}
    }
    std::fs::create_dir_all(dst).map_err(|e| Error::Io(e).wrap("set up destination chunks dir"))?;
    let files = list_chunk_files(src).map_err(|e| e.wrap("list chunks"))?;
    for file_path in files.values() {
        let file_name = file_path.file_name().unwrap_or_default();
        std::fs::hard_link(src.join(file_name), dst.join(file_name))
            .map_err(|e| Error::Io(e).wrap("hardlink a chunk"))?;
    }
    Ok(())
}

/// Go `repairLastChunkFile`: deletes the last file if it is empty or its magic number is 0. Because the files
/// are not fsynced when created, an abrupt shutdown can leave an empty file at the end.
fn repair_last_chunk_file(
    mut files: BTreeMap<usize, PathBuf>,
) -> Result<BTreeMap<usize, PathBuf>, Error> {
    let Some((&last_file, path)) = files.iter().next_back() else {
        return Ok(files);
    };
    if last_file == 0 {
        return Ok(files);
    }
    let mut f = File::open(path)
        .map_err(|e| Error::Io(e).wrap("open file during last head chunk file repair"))?;
    let mut buf = [0u8; MAGIC_CHUNKS_SIZE];
    // Go: a single Read call.
    let size = match io::Read::read(&mut f, &mut buf) {
        Ok(n) => n,
        Err(e) => {
            return Err(
                Error::Io(e).wrap("failed to read magic number during last head chunk file repair")
            );
        }
    };
    drop(f);

    // We either don't have enough bytes for the magic number or the magic number is 0. A wrong magic number is
    // not checked here: that error goes up so that the Head repairs properly.
    if size < MAGIC_CHUNKS_SIZE || u32::from_be_bytes(buf) == 0 {
        // Corrupt file, hence remove it.
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != io::ErrorKind::NotFound
        {
            return Err(Error::Io(e)
                .wrap("delete corrupted, empty head chunk file during last file repair"));
        }
        files.remove(&last_file);
    }
    Ok(files)
}

/// Number of shards of the chunk buffer.
const IN_BUFFER_SHARDS: usize = 128;

/// Go `chunkBuffer`: a thread safe lookup table for chunks by their ref.
struct ChunkBuffer {
    shards: Vec<RwLock<HashMap<ChunkDiskMapperRef, Arc<dyn Chunk>>>>,
}

impl ChunkBuffer {
    fn new() -> ChunkBuffer {
        ChunkBuffer {
            shards: (0..IN_BUFFER_SHARDS)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }

    fn put(&self, r: ChunkDiskMapperRef, chk: Arc<dyn Chunk>) {
        wlock(&self.shards[(r.0 % IN_BUFFER_SHARDS as u64) as usize]).insert(r, chk);
    }

    fn get(&self, r: ChunkDiskMapperRef) -> Option<Arc<dyn Chunk>> {
        rlock(&self.shards[(r.0 % IN_BUFFER_SHARDS as u64) as usize])
            .get(&r)
            .cloned()
    }

    /// Empties all shards (the maps keep their capacity for the next batch).
    fn clear(&self) {
        for s in &self.shards {
            wlock(s).clear();
        }
    }
}
