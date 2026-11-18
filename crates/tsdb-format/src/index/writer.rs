//! Port of the writer half of `tsdb/index/index.go`: index format v2, byte-identical to Go.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use model::labels::{self, Labels};

use super::{CRC32_SIZE, Error, FORMAT_V2, MAGIC_INDEX, SERIES_BYTE_ALIGN, SeriesRef, Symbols};
use crate::chunks::{ChunkRef, Meta};
use crate::context::Context;
use crate::encoding::{Crc32, Decbuf, Encbuf, crc32c};
use crate::fileutil::{self, MmapFile};

/// Go `PostingsEncoder`: encodes one postings list.
pub type PostingsEncoder = fn(&mut Encbuf, &[u32]) -> Result<(), Error>;

/// Go `indexTOCLen`.
pub const INDEX_TOC_LEN: usize = 6 * 8 + CRC32_SIZE;

/// Size of the in-memory write buffers. Go uses 4 MiB; the bytes written do not depend on it.
const FILE_BUFFER_SIZE: usize = 1 << 18;
const ENC_BUFFER_SIZE: usize = 1 << 16;

/// Go `indexWriterStage`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    None = 0,
    Symbols = 1,
    Series = 2,
    Done = 3,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Stage::None => "none",
            Stage::Symbols => "symbols",
            Stage::Series => "series",
            Stage::Done => "done",
        }
    }

    fn prev(self) -> Stage {
        match self {
            Stage::None | Stage::Symbols => Stage::None,
            Stage::Series => Stage::Symbols,
            Stage::Done => Stage::Series,
        }
    }
}

/// Go `TOC`: the index table of contents, where each section of the index starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Toc {
    pub symbols: u64,
    pub series: u64,
    pub label_indices: u64,
    pub label_indices_table: u64,
    pub postings: u64,
    pub postings_table: u64,
}

/// Go `NewTOCFromByteSlice`: the TOC parsed from the end of the index bytes.
pub fn new_toc_from_byte_slice(bs: &[u8]) -> Result<Toc, Error> {
    if bs.len() < INDEX_TOC_LEN {
        return Err(crate::encoding::Error::InvalidSize.into());
    }
    let b = &bs[bs.len() - INDEX_TOC_LEN..];
    let exp_crc = u32::from_be_bytes(b[b.len() - 4..].try_into().unwrap());
    let mut d = Decbuf::new(&b[..b.len() - 4]);
    if d.crc32() != exp_crc {
        return Err(Error::wrap(
            "read TOC",
            crate::encoding::Error::InvalidChecksum,
        ));
    }
    let toc = Toc {
        symbols: d.be64(),
        series: d.be64(),
        label_indices: d.be64(),
        label_indices_table: d.be64(),
        postings: d.be64(),
        postings_table: d.be64(),
    };
    match d.err() {
        Some(e) => Err(e.into()),
        None => Ok(toc),
    }
}

/// Go `EncodePostingsRaw`: the "basic" postings list encoding format with no compression:
/// `<BE uint32 len X><BE uint32 0><BE uint32 1>...<BE uint32 X-1>`.
pub fn encode_postings_raw(e: &mut Encbuf, offs: &[u32]) -> Result<(), Error> {
    e.put_be32int(offs.len() as i64);
    for &off in offs {
        e.put_be32(off);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// FileWriter.

/// Go `FileWriter`: a buffered file writer that tracks its position.
pub struct FileWriter {
    fbuf: BufWriter<File>,
    pos: u64,
    name: PathBuf,
}

impl FileWriter {
    /// Go `NewFileWriter`.
    pub fn new(name: &Path) -> Result<FileWriter, Error> {
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(name)?;
        Ok(FileWriter {
            fbuf: BufWriter::with_capacity(FILE_BUFFER_SIZE, f),
            pos: 0,
            name: name.to_path_buf(),
        })
    }

    /// Go `Pos`.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Go `Write`.
    pub fn write(&mut self, bufs: &[&[u8]]) -> Result<(), Error> {
        for b in bufs {
            self.fbuf.write_all(b)?;
            self.pos += b.len() as u64;
            // For now the index file must not grow beyond 64GiB. Some of the fixed-sized offset references in v1
            // are only 4 bytes large.
            if self.pos > 16 * u32::MAX as u64 {
                let name = model::unicode::quote(&self.name.to_string_lossy());
                return Err(Error::with_cause(
                    format!("{name} {}", Error::index_exceeds_64gib()),
                    Error::index_exceeds_64gib(),
                ));
            }
        }
        Ok(())
    }

    /// Go `Flush`.
    pub fn flush(&mut self) -> Result<(), Error> {
        self.fbuf.flush()?;
        Ok(())
    }

    /// Go `WriteAt`.
    pub fn write_at(&mut self, buf: &[u8], pos: u64) -> Result<(), Error> {
        self.flush()?;
        self.fbuf.get_ref().write_all_at(buf, pos)?;
        Ok(())
    }

    /// Go `AddPadding`: adds zero byte padding until the file size is a multiple of `size`.
    pub fn add_padding(&mut self, size: u64) -> Result<(), Error> {
        let p = self.pos % size;
        if p == 0 {
            return Ok(());
        }
        let p = size - p;
        const ZEROS: [u8; 16] = [0; 16];
        let zeros: Vec<u8>;
        let pad: &[u8] = if p as usize <= ZEROS.len() {
            &ZEROS[..p as usize]
        } else {
            zeros = vec![0; p as usize];
            &zeros
        };
        self.write(&[pad])
            .map_err(|e| Error::wrap("add padding", e))
    }

    /// Go `Close`: flushes, syncs and closes the file.
    pub fn close(mut self) -> Result<(), Error> {
        self.flush()?;
        self.fbuf.get_ref().sync_all()?;
        Ok(())
    }

    /// Go `Remove`.
    pub fn remove(&self) -> Result<(), Error> {
        fs::remove_file(&self.name)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Writer.

/// Go `Writer`: writes an index file in format v2. Call `add_symbol` for all symbols in sorted order, then
/// `add_series` for all series in label order, then `close`.
pub struct Writer {
    ctx: Context,

    // For the main index file.
    f: Option<FileWriter>,
    // Temporary file for postings.
    fp: Option<FileWriter>,
    // Temporary file for posting offsets table.
    fpo: Option<FileWriter>,
    cnt_po: u64,

    toc: Toc,
    stage: Stage,
    postings_start: u64, // Due to padding, can differ from TOC entry.

    // Reusable memory.
    buf1: Encbuf,
    buf2: Encbuf,

    num_symbols: usize,
    symbols: Option<Symbols>,
    symbol_file: Option<MmapFile>,
    last_symbol: String,
    symbol_cache: HashMap<String, u32>, // From symbol to index in table.

    label_names: HashMap<String, u64>, // Label names, and their usage.

    // Hold last series to validate that clients insert new series in order.
    last_series: Labels,
    last_series_ref: SeriesRef,

    // Hold last added chunk reference to make sure that chunks are ordered properly.
    last_chunk_ref: ChunkRef,

    crc32: Crc32,

    postings_encoder: PostingsEncoder,
}

/// Go `os.RemoveAll`: removes a file or a directory tree; a missing path is not an error.
fn remove_all(p: &Path) -> io::Result<()> {
    match fs::symlink_metadata(p) {
        Ok(md) if md.is_dir() => fs::remove_dir_all(p),
        Ok(_) => fs::remove_file(p),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

impl Writer {
    /// Go `NewWriter`: a writer to `path` using the default (raw) postings encoder.
    pub fn new(ctx: &Context, path: &Path) -> Result<Writer, Error> {
        Writer::new_with_encoder(ctx, path, encode_postings_raw)
    }

    /// Go `NewWriterWithEncoder`: serializes data in format version 2, encoding each postings list with
    /// `encoder`.
    pub fn new_with_encoder(
        ctx: &Context,
        path: &Path,
        encoder: PostingsEncoder,
    ) -> Result<Writer, Error> {
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let df = fileutil::open_dir(dir)?;

        remove_all(path).map_err(|e| Error::wrap("remove any existing index at path", e))?;

        // Main index file we are building.
        let f = FileWriter::new(path)?;
        // Temporary file for postings.
        let fp = FileWriter::new(&with_suffix(path, "_tmp_p"))?;
        // Temporary file for posting offset table.
        let fpo = FileWriter::new(&with_suffix(path, "_tmp_po"))?;
        df.sync_all().map_err(|e| Error::wrap("sync dir", e))?;

        let mut iw = Writer {
            ctx: ctx.clone(),
            f: Some(f),
            fp: Some(fp),
            fpo: Some(fpo),
            cnt_po: 0,
            toc: Toc::default(),
            stage: Stage::None,
            postings_start: 0,
            buf1: Encbuf::with_capacity(ENC_BUFFER_SIZE),
            buf2: Encbuf::with_capacity(ENC_BUFFER_SIZE),
            num_symbols: 0,
            symbols: None,
            symbol_file: None,
            last_symbol: String::new(),
            symbol_cache: HashMap::with_capacity(1 << 10),
            label_names: HashMap::with_capacity(1 << 8),
            last_series: Labels::default(),
            last_series_ref: 0,
            last_chunk_ref: 0,
            crc32: Crc32::new(),
            postings_encoder: encoder,
        };
        iw.write_meta()?;
        Ok(iw)
    }

    #[inline]
    fn f(&mut self) -> &mut FileWriter {
        self.f.as_mut().expect("index writer closed")
    }

    fn pos(&self) -> u64 {
        self.f.as_ref().map_or(0, |f| f.pos)
    }

    fn write_buf1(&mut self) -> Result<(), Error> {
        let f = self.f.as_mut().expect("index writer closed");
        f.write(&[self.buf1.get()])
    }

    fn write_at_buf1(&mut self, pos: u64) -> Result<(), Error> {
        let f = self.f.as_mut().expect("index writer closed");
        f.write_at(self.buf1.get(), pos)
    }

    fn ctx_err(&self) -> Option<Error> {
        self.ctx.err().map(Error::context)
    }

    /// Go `ensureStage`: handles transitions between write stages and ensures that writer methods are called in
    /// a valid order.
    fn ensure_stage(&mut self, s: Stage) -> Result<(), Error> {
        if let Some(e) = self.ctx_err() {
            return Err(e);
        }
        if self.stage == s {
            return Ok(());
        }
        if self.stage < s.prev() {
            // A stage has been skipped.
            self.ensure_stage(s.prev())?;
        }
        if self.stage > s {
            return Err(Error::new(format!(
                "invalid stage {}, currently at {}",
                model::unicode::quote(s.as_str()),
                model::unicode::quote(self.stage.as_str())
            )));
        }

        // Mark start of sections in table of contents.
        match s {
            Stage::Symbols => {
                self.toc.symbols = self.pos();
                self.start_symbols()?;
            }
            Stage::Series => {
                self.finish_symbols()?;
                self.toc.series = self.pos();
            }
            Stage::Done => {
                self.toc.label_indices = self.pos();
                // LabelIndices generation depends on the posting offset table produced at this stage.
                self.write_postings_to_tmp_files()?;

                self.toc.postings = self.pos();
                self.write_postings()?;

                self.toc.label_indices_table = self.pos();

                self.toc.postings_table = self.pos();
                self.write_postings_offset_table()?;
                self.write_toc()?;
            }
            Stage::None => {}
        }
        self.stage = s;
        Ok(())
    }

    fn write_meta(&mut self) -> Result<(), Error> {
        self.buf1.reset();
        self.buf1.put_be32(MAGIC_INDEX);
        self.buf1.put_byte(FORMAT_V2 as u8);
        self.write_buf1()
    }

    /// Go `AddSeries`: adds the series one at a time along with its chunks.
    pub fn add_series(
        &mut self,
        r#ref: SeriesRef,
        lset: &Labels,
        chunks: &[Meta],
    ) -> Result<(), Error> {
        self.ensure_stage(Stage::Series)?;
        if labels::compare(lset, &self.last_series) <= 0 {
            return Err(Error::new(format!(
                "out-of-order series added with label set {}, last label set {}",
                model::unicode::quote(&lset.to_string()),
                model::unicode::quote(&self.last_series.to_string())
            )));
        }
        if r#ref < self.last_series_ref && !self.last_series.is_empty() {
            return Err(Error::new(format!(
                "series with reference greater than {} already added",
                r#ref
            )));
        }

        let mut last_chunk_ref = self.last_chunk_ref;
        let mut last_max_t: i64 = 0;
        for (ix, c) in chunks.iter().enumerate() {
            if c.r#ref < last_chunk_ref {
                return Err(Error::new(format!(
                    "unsorted chunk reference: {}, previous: {}",
                    c.r#ref, last_chunk_ref
                )));
            }
            last_chunk_ref = c.r#ref;

            if ix > 0 && c.min_time <= last_max_t {
                return Err(Error::new(format!(
                    "chunk minT {} is not higher than previous chunk maxT {}",
                    c.min_time, last_max_t
                )));
            }
            if c.max_time < c.min_time {
                return Err(Error::new(format!(
                    "chunk maxT {} is less than minT {}",
                    c.max_time, c.min_time
                )));
            }
            last_max_t = c.max_time;
        }

        // We add padding to 16 bytes to increase the addressable space we get through 4 byte series references.
        self.f()
            .add_padding(SERIES_BYTE_ALIGN)
            .map_err(|e| Error::wrap("failed to write padding bytes", e))?;

        if self.pos() % SERIES_BYTE_ALIGN != 0 {
            return Err(Error::new(format!(
                "series write not 16-byte aligned at {}",
                self.pos()
            )));
        }

        self.buf2.reset();
        self.buf2.put_uvarint(lset.len() as i64);

        {
            let (buf2, symbol_cache, label_names) =
                (&mut self.buf2, &self.symbol_cache, &mut self.label_names);
            lset.validate(|name, value| {
                let Some(&name_index) = symbol_cache.get(name) else {
                    return Err(Error::new(format!(
                        "symbol entry for {} does not exist",
                        model::unicode::quote(name)
                    )));
                };
                match label_names.get_mut(name) {
                    Some(n) => *n += 1,
                    None => {
                        label_names.insert(name.to_string(), 1);
                    }
                }
                buf2.put_uvarint32(name_index);

                let Some(&value_index) = symbol_cache.get(value) else {
                    return Err(Error::new(format!(
                        "symbol entry for {} does not exist",
                        model::unicode::quote(value)
                    )));
                };
                buf2.put_uvarint32(value_index);
                Ok(())
            })?;
        }

        self.buf2.put_uvarint(chunks.len() as i64);

        if let Some(c) = chunks.first() {
            self.buf2.put_varint64(c.min_time);
            self.buf2
                .put_uvarint64(c.max_time.wrapping_sub(c.min_time) as u64);
            self.buf2.put_uvarint64(c.r#ref);
            let mut t0 = c.max_time;
            let mut ref0 = c.r#ref as i64;

            for c in &chunks[1..] {
                self.buf2.put_uvarint64(c.min_time.wrapping_sub(t0) as u64);
                self.buf2
                    .put_uvarint64(c.max_time.wrapping_sub(c.min_time) as u64);
                t0 = c.max_time;

                self.buf2.put_varint64((c.r#ref as i64).wrapping_sub(ref0));
                ref0 = c.r#ref as i64;
            }
        }

        self.buf1.reset();
        self.buf1.put_uvarint(self.buf2.len() as i64);

        self.buf2.put_hash(&mut self.crc32);

        let f = self.f.as_mut().expect("index writer closed");
        f.write(&[self.buf1.get(), self.buf2.get()])
            .map_err(|e| Error::wrap("write series data", e))?;

        self.last_series.copy_from(lset);
        self.last_series_ref = r#ref;
        self.last_chunk_ref = last_chunk_ref;
        Ok(())
    }

    fn start_symbols(&mut self) -> Result<(), Error> {
        // We are at toc.symbols. Leave 4 bytes of space for the length, and another 4 for the number of symbols
        // which will both be calculated later.
        self.f().write(&[b"alenblen"])
    }

    /// Go `AddSymbol`: symbols must be added in strictly increasing order.
    pub fn add_symbol(&mut self, sym: &str) -> Result<(), Error> {
        self.ensure_stage(Stage::Symbols)?;
        if self.num_symbols != 0 && sym <= self.last_symbol.as_str() {
            return Err(Error::new(format!(
                "symbol {} out-of-order",
                model::unicode::quote(sym)
            )));
        }
        self.last_symbol.clear();
        self.last_symbol.push_str(sym);
        self.symbol_cache
            .insert(sym.to_string(), self.num_symbols as u32);
        self.num_symbols += 1;
        self.buf1.reset();
        self.buf1.put_uvarint_str(sym);
        self.write_buf1()
    }

    fn finish_symbols(&mut self) -> Result<(), Error> {
        let symbol_table_size = self.pos() - self.toc.symbols - 4;
        // The symbol table's <len> part is 4 bytes. So the total symbol table size must be less than or equal to
        // 2^32-1.
        if symbol_table_size > u32::MAX as u64 {
            let e = Error::symbol_table_too_large();
            return Err(Error::with_cause(format!("{e}: {symbol_table_size}"), e));
        }

        // Write out the length and symbol count.
        self.buf1.reset();
        self.buf1.put_be32int(symbol_table_size as i64);
        self.buf1.put_be32int(self.num_symbols as i64);
        self.write_at_buf1(self.toc.symbols)?;

        let hash_pos = self.pos();
        // Leave space for the hash. We can only calculate it now that the number of symbols is known, so mmap
        // and do it from there.
        self.f().write(&[b"hash"])?;
        self.f().flush()?;

        let name = self.f.as_ref().unwrap().name.clone();
        let sf = MmapFile::open(&name)?;
        let hash = crc32c(&sf.bytes()[self.toc.symbols as usize + 4..hash_pos as usize]);
        self.buf1.reset();
        self.buf1.put_be32(hash);
        self.write_at_buf1(hash_pos)?;

        // Load in the symbol table efficiently for the rest of the index writing.
        let symbols = Symbols::new(sf.bytes(), FORMAT_V2, self.toc.symbols as usize)
            .map_err(|e| Error::wrap("read symbols", e));
        self.symbol_file = Some(sf);
        self.symbols = Some(symbols?);
        Ok(())
    }

    /// Go `writePostingsOffsetTable`.
    fn write_postings_offset_table(&mut self) -> Result<(), Error> {
        // Ensure everything is in the temporary file.
        self.fpo.as_mut().unwrap().flush()?;

        let start_pos = self.pos();
        // Leave 4 bytes of space for the length, which will be calculated later.
        self.f().write(&[b"alen"])?;

        // Copy over the tmp posting offset table, however we need to adjust the offsets.
        let adjustment = self.postings_start;

        self.buf1.reset();
        self.crc32.reset();
        self.buf1.put_be32int(self.cnt_po as i64); // Count.
        self.buf1.write_to_hash(&mut self.crc32);
        self.write_buf1()?;

        let fpo = self.fpo.as_ref().unwrap();
        let mf = MmapFile::open(&fpo.name)?;
        let mut d = Decbuf::new_raw(mf.bytes(), fpo.pos as usize);
        let mut cnt = self.cnt_po;
        let f = self.f.as_mut().unwrap();
        while d.err().is_none() && cnt > 0 {
            self.buf1.reset();
            self.buf1.put_uvarint(d.uvarint()); // Keycount.
            self.buf1.put_uvarint_bytes(d.uvarint_bytes()); // Label name.
            self.buf1.put_uvarint_bytes(d.uvarint_bytes()); // Label value.
            self.buf1
                .put_uvarint64(d.uvarint64().wrapping_add(adjustment)); // Offset.
            self.buf1.write_to_hash(&mut self.crc32);
            f.write(&[self.buf1.get()])?;
            cnt -= 1;
        }
        if let Some(e) = d.err() {
            return Err(e.into());
        }

        // Cleanup temporary file.
        drop(mf);
        let fpo = self.fpo.take().unwrap();
        let name = fpo.name.clone();
        fpo.close()?;
        fs::remove_file(&name)?;

        self.write_length_and_hash(start_pos)
            .map_err(|e| Error::wrap("postings offset table length/crc32 write error", e))
    }

    fn write_length_and_hash(&mut self, start_pos: u64) -> Result<(), Error> {
        self.buf1.reset();
        let l = self.pos() - start_pos - 4;
        if l > u32::MAX as u64 {
            let e = Error::postings_offset_table_too_large();
            return Err(Error::with_cause(format!("{e}: {l}"), e));
        }
        self.buf1.put_be32int(l as i64);
        self.write_at_buf1(start_pos)
            .map_err(|e| Error::wrap("write length from buffer error", e))?;

        // Write out the hash.
        self.buf1.reset();
        self.buf1.put_hash_sum(&self.crc32);
        self.write_buf1()
            .map_err(|e| Error::wrap("write buffer's crc32 error", e))
    }

    fn write_toc(&mut self) -> Result<(), Error> {
        self.buf1.reset();
        self.buf1.put_be64(self.toc.symbols);
        self.buf1.put_be64(self.toc.series);
        self.buf1.put_be64(self.toc.label_indices);
        self.buf1.put_be64(self.toc.label_indices_table);
        self.buf1.put_be64(self.toc.postings);
        self.buf1.put_be64(self.toc.postings_table);
        self.buf1.put_hash(&mut self.crc32);
        self.write_buf1()
    }

    /// Go `writePostingsToTmpFiles`: writes the postings lists and their offset table entries to the temporary
    /// files, reading the series back from the index file.
    fn write_postings_to_tmp_files(&mut self) -> Result<(), Error> {
        let mut names: Vec<String> = self.label_names.keys().cloned().collect();
        names.sort_unstable();

        self.f().flush()?;
        let fname = self.f.as_ref().unwrap().name.clone();
        let mf = MmapFile::open(&fname)?;
        let bytes = mf.bytes();
        let label_indices = self.toc.label_indices;

        // Write out the special all posting.
        let mut offsets: Vec<u32> = Vec::new();
        let mut d = Decbuf::new_raw(bytes, label_indices as usize);
        d.skip(self.toc.series as usize);
        while !d.is_empty() {
            d.consume_padding();
            let start_pos = label_indices - d.len() as u64;
            if start_pos % SERIES_BYTE_ALIGN != 0 {
                return Err(Error::new(format!(
                    "series not 16-byte aligned at {start_pos}"
                )));
            }
            offsets.push((start_pos / SERIES_BYTE_ALIGN) as u32);
            // Skip to next series.
            let x = d.uvarint();
            d.skip((x as usize).wrapping_add(CRC32_SIZE));
            if let Some(e) = d.err() {
                return Err(e.into());
            }
        }
        self.write_posting("", "", &offsets)?;
        let max_postings = offsets.len() as u64; // No label name can have more postings than this.
        drop(offsets);

        // The symbol lookups below borrow the symbols mmap while write_posting borrows self.
        let symbols = self.symbols.take();
        let symbol_file = self.symbol_file.take();
        let res = self.write_label_postings(
            bytes,
            &names,
            max_postings,
            symbols.as_ref(),
            symbol_file.as_ref(),
        );
        self.symbols = symbols;
        self.symbol_file = symbol_file;
        res
    }

    fn write_label_postings(
        &mut self,
        bytes: &[u8],
        mut names: &[String],
        max_postings: u64,
        symbols: Option<&Symbols>,
        symbol_file: Option<&MmapFile>,
    ) -> Result<(), Error> {
        let label_indices = self.toc.label_indices;
        let (symbols, symbol_bytes) = match (symbols, symbol_file) {
            (Some(s), Some(f)) => (s, f.bytes()),
            _ => return Err(Error::new("symbols not loaded")),
        };
        while !names.is_empty() {
            let mut batch_names: Vec<&str> = Vec::new();
            let mut c: u64 = 0;
            // Try to bunch up label names into one loop, but avoid using more memory than a single label name can.
            while let Some(name) = names.first() {
                let uses = self.label_names[name.as_str()];
                if uses + c > max_postings {
                    if c > 0 {
                        break;
                    }
                    return Err(Error::new(format!(
                        "corruption detected when writing postings to index: label {} has {} uses, but maxPostings is {}",
                        model::unicode::quote(name),
                        uses,
                        max_postings
                    )));
                }
                batch_names.push(name);
                c += uses;
                names = &names[1..];
            }

            let mut name_symbols: HashSet<u32> = HashSet::with_capacity(batch_names.len());
            for name in &batch_names {
                let Some(&sid) = self.symbol_cache.get(*name) else {
                    return Err(Error::new(format!(
                        "symbol entry for {} does not exist",
                        model::unicode::quote(name)
                    )));
                };
                name_symbols.insert(sid);
            }
            // Label name -> label value -> positions.
            let mut postings: HashMap<u32, HashMap<u32, Vec<u32>>> = HashMap::new();

            let mut d = Decbuf::new_raw(bytes, label_indices as usize);
            d.skip(self.toc.series as usize);
            while !d.is_empty() {
                d.consume_padding();
                let start_pos = label_indices - d.len() as u64;
                let l = d.uvarint(); // Length of this series in bytes.
                let start_len = d.len();

                // See if label names we want are in the series.
                let num_labels = d.uvarint();
                for _ in 0..num_labels {
                    let lno = d.uvarint() as u32;
                    let lvo = d.uvarint() as u32;
                    if name_symbols.contains(&lno) {
                        postings
                            .entry(lno)
                            .or_default()
                            .entry(lvo)
                            .or_default()
                            .push((start_pos / SERIES_BYTE_ALIGN) as u32);
                    }
                }
                // Skip to next series.
                let consumed = start_len - d.len();
                d.skip((l as usize).wrapping_sub(consumed).wrapping_add(CRC32_SIZE));
                if let Some(e) = d.err() {
                    return Err(e.into());
                }
            }

            for name in &batch_names {
                // Write out postings for this label name.
                let Some(&sid) = self.symbol_cache.get(*name) else {
                    return Err(Error::new(format!(
                        "symbol entry for {} does not exist",
                        model::unicode::quote(name)
                    )));
                };
                let Some(vm) = postings.get(&sid) else {
                    continue;
                };
                let mut values: Vec<u32> = vm.keys().copied().collect();
                // Symbol numbers are in order, so the strings will also be in order.
                values.sort_unstable();
                for v in values {
                    let value = symbols.lookup_bytes(symbol_bytes, v)?;
                    self.write_posting_bytes(name.as_bytes(), value, &vm[&v])?;
                }
            }
            if let Some(e) = self.ctx_err() {
                return Err(e);
            }
        }
        Ok(())
    }

    fn write_posting(&mut self, name: &str, value: &str, offs: &[u32]) -> Result<(), Error> {
        self.write_posting_bytes(name.as_bytes(), value.as_bytes(), offs)
    }

    fn write_posting_bytes(
        &mut self,
        name: &[u8],
        value: &[u8],
        offs: &[u32],
    ) -> Result<(), Error> {
        let fp = self.fp.as_mut().unwrap();
        // Align beginning to 4 bytes for more efficient postings list scans.
        fp.add_padding(4)?;

        // Write out postings offset table to temporary file as we go.
        self.buf1.reset();
        self.buf1.put_uvarint(2);
        self.buf1.put_uvarint_bytes(name);
        self.buf1.put_uvarint_bytes(value);
        self.buf1.put_uvarint64(fp.pos); // This is relative to the postings tmp file, not the final index file.
        self.fpo.as_mut().unwrap().write(&[self.buf1.get()])?;
        self.cnt_po += 1;

        self.buf1.reset();
        (self.postings_encoder)(&mut self.buf1, offs)?;

        self.buf2.reset();
        let l = self.buf1.len();
        if l as u64 > u32::MAX as u64 {
            return Err(Error::new(format!("posting size exceeds 4 bytes: {l}")));
        }
        self.buf2.put_be32int(l as i64);
        self.buf1.put_hash(&mut self.crc32);
        self.fp
            .as_mut()
            .unwrap()
            .write(&[self.buf2.get(), self.buf1.get()])
    }

    fn write_postings(&mut self) -> Result<(), Error> {
        // There's padding in the tmp file, make sure it actually works.
        self.f().add_padding(4)?;
        self.postings_start = self.pos();

        // Copy temporary file into main index.
        let fp = self.fp.as_mut().unwrap();
        fp.flush()?;
        let fp_pos = fp.pos;
        let src = fp.fbuf.get_mut();
        src.seek(SeekFrom::Start(0))?;
        // Don't need to calculate a checksum, so can copy directly.
        let f = self.f.as_mut().unwrap();
        let n = io::copy(src, &mut f.fbuf)?;
        if n != fp_pos {
            return Err(Error::new(format!(
                "wrote {fp_pos} bytes to posting temporary file, but only read back {n}"
            )));
        }
        f.pos += n;

        let fp = self.fp.take().unwrap();
        let name = fp.name.clone();
        fp.close()?;
        fs::remove_file(&name)?;
        Ok(())
    }

    /// Go `Close`: finishes the index (postings, offset table, TOC) and closes all files. Even if finishing
    /// fails, all the files are closed.
    pub fn close(mut self) -> Result<(), Error> {
        let ensure_err = self.ensure_stage(Stage::Done);

        self.symbol_file = None;
        if let Some(fp) = self.fp.take() {
            fp.close()?;
        }
        if let Some(fpo) = self.fpo.take() {
            fpo.close()?;
        }
        if let Some(f) = self.f.take() {
            f.close()?;
        }
        ensure_err
    }
}
