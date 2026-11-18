//! Port of `tsdb/chunks/chunks.go`: the block chunk segment files (`chunks/000001`, ...) with their Writer and
//! Reader.

use super::*;
use crate::encoding::{Crc32, uvarint};
use crate::fileutil::MmapFile;
use chunkenc::{Encoding, Pool};
use std::io::Seek;
use std::sync::Arc;

/// Go `DefaultChunkSegmentSize`: the default chunks segment size.
pub const DEFAULT_CHUNK_SEGMENT_SIZE: i64 = 512 * 1024 * 1024;

/// The size of the write buffer of a segment (Go: 8 MiB).
const WRITER_BUFFER_SIZE: usize = 8 * 1024 * 1024;

/// Go `WriterOption`.
#[derive(Clone, Copy, Debug)]
pub enum WriterOption {
    SegmentSize(i64),
    UncachedIo(bool),
}

/// Go `WithUncachedIO`. Note: uncached (direct) I/O is accepted but writes go through the page cache with a
/// plain buffered writer for now.
pub fn with_uncached_io(enabled: bool) -> WriterOption {
    WriterOption::UncachedIo(enabled)
}

/// Go `WithSegmentSize`: values <= 0 select `DEFAULT_CHUNK_SEGMENT_SIZE`.
pub fn with_segment_size(segment_size: i64) -> WriterOption {
    WriterOption::SegmentSize(if segment_size <= 0 {
        DEFAULT_CHUNK_SEGMENT_SIZE
    } else {
        segment_size
    })
}

/// Go `Writer`: writes block chunks to sequentially numbered segment files.
#[derive(Debug)]
pub struct Writer {
    dir: PathBuf,
    dir_file: Option<File>,
    /// The current tail segment (the only open one).
    tail: Option<File>,
    /// Number of segments cut so far (Go `len(w.files)`).
    n_files: usize,
    wbuf: Option<BufWriter>,
    n: i64,
    crc32: Crc32,
    segment_size: i64,
    use_uncached_io: bool,
}

/// Go `NewWriter`: a new writer against the given directory (created if missing).
pub fn new_writer(dir: &Path, opts: &[WriterOption]) -> Result<Writer, Error> {
    let mut segment_size = DEFAULT_CHUNK_SEGMENT_SIZE;
    let mut use_uncached_io = false;
    for o in opts {
        match *o {
            WriterOption::SegmentSize(s) => segment_size = s,
            WriterOption::UncachedIo(b) => use_uncached_io = b,
        }
    }
    std::fs::create_dir_all(dir)?;
    let dir_file = crate::fileutil::open_dir(dir)?;
    Ok(Writer {
        dir: dir.to_path_buf(),
        dir_file: Some(dir_file),
        tail: None,
        n_files: 0,
        wbuf: None,
        n: 0,
        crc32: Crc32::new(),
        segment_size,
        use_uncached_io,
    })
}

impl Writer {
    /// Whether uncached I/O was requested (it is not used yet).
    pub fn uses_uncached_io(&self) -> bool {
        self.use_uncached_io
    }

    /// Go `finalizeTail`: writes all pending data to the tail file, truncates the preallocated rest and closes
    /// it.
    fn finalize_tail(&mut self) -> Result<(), Error> {
        let Some(mut tf) = self.tail.take() else {
            return Ok(());
        };
        if let Some(wbuf) = &mut self.wbuf {
            wbuf.flush(&mut tf)?;
        }
        tf.sync_all()?;
        // As the file was pre-allocated, we truncate any superfluous zero bytes.
        let off = tf.stream_position()?;
        tf.set_len(off)?;
        Ok(())
    }

    fn cut(&mut self) -> Result<(), Error> {
        // Sync current tail to disk and close.
        self.finalize_tail()?;
        let dir_file = self
            .dir_file
            .as_ref()
            .ok_or_else(|| Error::msg("writer closed"))?;
        let (n, f, _, _) = cut_segment_file(
            &self.dir,
            dir_file,
            MAGIC_CHUNKS,
            CHUNKS_FORMAT_V1,
            self.segment_size,
        )?;
        self.n = n as i64;
        self.tail = Some(f);
        self.n_files += 1;
        if self.wbuf.is_none() {
            self.wbuf = Some(BufWriter::new(WRITER_BUFFER_SIZE));
        }
        Ok(())
    }

    fn write(&mut self, b: &[u8]) -> Result<(), Error> {
        let (Some(wbuf), Some(tail)) = (&mut self.wbuf, &mut self.tail) else {
            return Err(Error::msg("no segment to write to"));
        };
        let (n, err) = wbuf.write(tail, b);
        self.n += n as i64;
        match err {
            Some(e) => Err(e.into()),
            None => Ok(()),
        }
    }

    /// Go `WriteChunks`: writes as many chunks as possible to the current segment, cuts a new segment when the
    /// current one is full and writes the rest there. Sets `ref` of every chunk.
    pub fn write_chunks(&mut self, chks: &mut [Meta]) -> Result<(), Error> {
        let mut batch_size: i64 = 0;
        let mut batch_start = 0;
        // Batches as [start, end) ranges of chks.
        let mut batches: Vec<(usize, usize)> = vec![(0, 0)];
        let mut first_batch = true;

        for (i, chk) in chks.iter().enumerate() {
            let data = chk
                .chunk
                .as_ref()
                .ok_or_else(|| Error::msg("chunk data missing"))?
                .bytes()
                .len();
            // Each chunk contains: data length + encoding + the data itself + crc32.
            // The data length is a variable length field so use the maximum possible value.
            let chk_size =
                (MAX_CHUNK_LENGTH_FIELD_SIZE + CHUNK_ENCODING_SIZE + data + CRC32_SIZE) as i64;
            batch_size += chk_size;

            // Cut a new batch when it is not the first chunk (to avoid empty segments) and the batch is too large
            // to fit in the current segment.
            let mut cut_new_batch =
                i != 0 && batch_size + SEGMENT_HEADER_SIZE as i64 > self.segment_size;

            // If the segment already has some data then the first batch size calculation should account for that.
            if first_batch && self.n > SEGMENT_HEADER_SIZE as i64 {
                cut_new_batch = batch_size + self.n > self.segment_size;
                if cut_new_batch {
                    first_batch = false;
                }
            }

            if cut_new_batch {
                batch_start = i;
                batches.push((i, i));
                batch_size = chk_size;
            }
            *batches.last_mut().unwrap() = (batch_start, i + 1);
        }

        // Create a new segment when one doesn't already exist.
        if self.n == 0 {
            self.cut()?;
        }

        let nb = batches.len();
        for (i, (s, e)) in batches.into_iter().enumerate() {
            self.write_chunks_batch(&mut chks[s..e])?;
            // Cut a new segment only when there are more chunks to write.
            if i < nb - 1 {
                self.cut()?;
            }
        }
        Ok(())
    }

    /// Go `writeChunks`: writes the chunks into the current segment irrespective of the segment size limit.
    fn write_chunks_batch(&mut self, chks: &mut [Meta]) -> Result<(), Error> {
        if chks.is_empty() {
            return Ok(());
        }
        let seq = self.seq() as u64;
        for chk in chks.iter_mut() {
            chk.r#ref = new_block_chunk_ref(seq, self.n as u64);
            let c = chk
                .chunk
                .as_deref()
                .ok_or_else(|| Error::msg("chunk data missing"))?;
            let data = c.bytes();
            let enc = c.encoding().0;

            let mut lb = [0u8; MAX_CHUNK_LENGTH_FIELD_SIZE];
            let n = put_uvarint(&mut lb, data.len() as u64);
            self.write(&lb[..n])?;
            self.write(&[enc])?;
            self.write(data)?;

            self.crc32.reset();
            self.crc32.write(&[enc]);
            self.crc32.write(data);
            let sum = self.crc32.sum32().to_be_bytes();
            self.write(&sum)?;
        }
        Ok(())
    }

    fn seq(&self) -> usize {
        self.n_files - 1
    }

    /// Go `Close`: finalizes the tail segment and closes the directory.
    pub fn close(&mut self) -> Result<(), Error> {
        self.finalize_tail()?;
        // Close dir file.
        self.dir_file = None;
        Ok(())
    }
}

/// Go `binary.PutUvarint` into a fixed buffer.
pub(crate) fn put_uvarint(buf: &mut [u8], mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        buf[i] = (x as u8) | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i + 1
}

/// The bytes of one segment: an mmapped file or an owned buffer.
#[derive(Debug)]
enum Segment {
    Mmap(MmapFile),
    Bytes(Vec<u8>),
}

impl Segment {
    fn bytes(&self) -> &[u8] {
        match self {
            Segment::Mmap(m) => m.bytes(),
            Segment::Bytes(b) => b,
        }
    }
}

/// Go `Reader`: reads chunks from the segment files of a block.
pub struct Reader {
    bs: Vec<Segment>,
    /// The total size of bytes in the reader.
    size: i64,
    pool: Arc<dyn Pool>,
}

impl fmt::Debug for Reader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("segments", &self.bs.len())
            .field("size", &self.size)
            .finish()
    }
}

/// Go `newReader`: checks the header of every segment.
fn new_reader(bs: Vec<Segment>, pool: Arc<dyn Pool>) -> Result<Reader, Error> {
    let mut size = 0i64;
    for (i, b) in bs.iter().enumerate() {
        let b = b.bytes();
        if b.len() < SEGMENT_HEADER_SIZE {
            return Err(Error::Msg(format!(
                "invalid segment header in segment {i}: {}",
                err_invalid_size()
            )));
        }
        // Verify magic number.
        let m = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        if m != MAGIC_CHUNKS {
            return Err(Error::Msg(format!("invalid magic number {m:x}")));
        }
        // Verify chunk format version.
        let v = b[MAGIC_CHUNKS_SIZE];
        if v != CHUNKS_FORMAT_V1 {
            return Err(Error::Msg(format!("invalid chunk format version {v}")));
        }
        size += b.len() as i64;
    }
    Ok(Reader { bs, size, pool })
}

/// Go `NewDirReader`: a reader against the sequentially numbered files in `dir`. With no pool, a new
/// `chunkenc::DefaultPool` is used.
pub fn new_dir_reader(dir: &Path, pool: Option<Arc<dyn Pool>>) -> Result<Reader, Error> {
    let files = sequence_files(dir)?;
    let pool = pool.unwrap_or_else(|| Arc::new(chunkenc::new_pool()));
    let mut bs = Vec::with_capacity(files.len());
    for f in files {
        let m = MmapFile::open(&f).map_err(|e| Error::Io(e).wrap("mmap files"))?;
        bs.push(Segment::Mmap(m));
    }
    new_reader(bs, pool)
}

/// A reader over in-memory segments, with the same header checks as `new_dir_reader`.
pub fn new_bytes_reader(
    segments: Vec<Vec<u8>>,
    pool: Option<Arc<dyn Pool>>,
) -> Result<Reader, Error> {
    let pool = pool.unwrap_or_else(|| Arc::new(chunkenc::new_pool()));
    new_reader(segments.into_iter().map(Segment::Bytes).collect(), pool)
}

impl Reader {
    /// A reader over in-memory segments without header checks (Go `&Reader{bs: ...}` in tests).
    #[doc(hidden)]
    pub fn from_raw_segments(segments: Vec<Vec<u8>>) -> Reader {
        Reader {
            bs: segments.into_iter().map(Segment::Bytes).collect(),
            size: 0,
            pool: Arc::new(chunkenc::new_pool()),
        }
    }

    /// Go `Close`: unmaps the segments.
    pub fn close(self) -> Result<(), Error> {
        Ok(())
    }

    /// Go `Size`: the total size of the segments.
    pub fn size(&self) -> i64 {
        self.size
    }

    /// The encoding and the data of the chunk at `r`, checked against its CRC32, without a copy (the bytes
    /// borrow the mmapped segment). Iterate them with `chunkenc::AnyIterator::new(enc, bytes)`.
    pub fn chunk_bytes(&self, r: ChunkRef) -> Result<(Encoding, &[u8]), Error> {
        let (sgm_index, chk_start) = unpack_block_chunk_ref(r);
        if sgm_index >= self.bs.len() {
            return Err(Error::Msg(format!(
                "segment index {sgm_index} out of range"
            )));
        }
        let sgm = self.bs[sgm_index].bytes();

        if chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE > sgm.len() {
            return Err(Error::Msg(format!(
                "segment doesn't include enough bytes to read the chunk size data field - required:{}, available:{}",
                chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE,
                sgm.len()
            )));
        }
        // With the minimum chunk length this should never cause us reading over the end of the slice.
        let (chk_data_len, n) = uvarint(&sgm[chk_start..chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE]);
        if n <= 0 {
            return Err(Error::Msg(format!("reading chunk length failed with {n}")));
        }
        let chk_enc_start = chk_start + n as usize;
        let chk_end = chk_enc_start as u128
            + (CHUNK_ENCODING_SIZE + CRC32_SIZE) as u128
            + chk_data_len as u128;
        if chk_end > sgm.len() as u128 {
            return Err(Error::Msg(format!(
                "segment doesn't include enough bytes to read the chunk - required:{chk_end}, available:{}",
                sgm.len()
            )));
        }
        let chk_end = chk_end as usize;
        let chk_data_start = chk_enc_start + CHUNK_ENCODING_SIZE;
        let chk_data_end = chk_end - CRC32_SIZE;

        check_crc32(
            &sgm[chk_enc_start..chk_data_end],
            &sgm[chk_data_end..chk_end],
        )?;
        Ok((
            Encoding(sgm[chk_enc_start]),
            &sgm[chk_data_start..chk_data_end],
        ))
    }

    /// Go `ChunkOrIterable`: the chunk at `meta.ref`. Block chunks never return an iterable. The data is copied
    /// into a chunk taken from the pool (return it with `pool.put` to reuse its buffer).
    pub fn chunk_or_iterable(&self, meta: &Meta) -> Result<Box<dyn Chunk>, Error> {
        self.chunk(meta.r#ref)
    }

    /// The chunk at `r` (see `chunk_or_iterable`).
    pub fn chunk(&self, r: ChunkRef) -> Result<Box<dyn Chunk>, Error> {
        let (enc, data) = self.chunk_bytes(r)?;
        Ok(self.pool.get(enc, data.to_vec())?)
    }

    /// The pool chunks are taken from.
    pub fn pool(&self) -> &Arc<dyn Pool> {
        &self.pool
    }
}
