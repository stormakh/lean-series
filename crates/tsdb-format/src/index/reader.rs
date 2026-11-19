//! Port of the reader half of `tsdb/index/index.go`: index formats v1, v2 and v3 over an mmapped file or a byte
//! buffer, without copies on the query path.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use model::labels::{Label, ScratchBuilder, stable_hash};
use model::matcher::Matcher;

use super::{
    BigEndianPostings, BoxPostings, CHECK_CONTEXT_EVERY_N_ITERATIONS, Ctx, Error, FORMAT_V1,
    FORMAT_V2, FORMAT_V3, HEADER_LEN, LabelHints, ListPostings, MAGIC_INDEX, Postings,
    SERIES_BYTE_ALIGN, SeriesRef, Toc, empty_postings, err_postings, merge,
    new_toc_from_byte_slice,
};
use crate::chunks::Meta;
use crate::encoding::{self, Decbuf};
use crate::fileutil::MmapFile;

/// Go `symbolFactor`: the symbols and postings offset tables keep every this many entries in memory.
const SYMBOL_FACTOR: usize = 32;

/// Go `PostingsDecoder`: decodes one postings list (the contents of a `Decbuf` at the list) and returns its
/// number of elements and the postings, which may borrow the index bytes.
pub type PostingsDecoder = for<'a> fn(Decbuf<'a>) -> Result<(usize, BoxPostings<'a>), Error>;

/// Go `DecodePostingsRaw`: a postings list for `d` and its number of elements (no copy).
pub fn decode_postings_raw(mut d: Decbuf<'_>) -> Result<(usize, BoxPostings<'_>), Error> {
    let n = d.be32int();
    let l = d.get();
    if let Some(e) = d.err() {
        return Err(e.into());
    }
    if l.len() as i64 != 4 * n {
        return Err(Error::new(format!(
            "unexpected postings length, should be {} bytes for {} postings, got {} bytes",
            4 * n,
            n,
            l.len()
        )));
    }
    Ok((n as usize, Box::new(BigEndianPostings::new(l))))
}

/// Label data written by Prometheus is valid UTF-8; invalid bytes are replaced (copying only then).
#[inline]
fn yolo_str(b: &[u8]) -> Cow<'_, str> {
    String::from_utf8_lossy(b)
}

// ---------------------------------------------------------------------------------------------------------------
// StringIter.

/// Go `StringIter`: iterates over a sorted list of strings.
pub trait StringIter {
    /// Advances the iterator and returns true if another value was found.
    fn next(&mut self) -> bool;
    /// The value at the current iterator position.
    fn at(&self) -> &str;
    /// The last error of the iterator.
    fn err(&self) -> Option<Error>;
}

/// Go `stringListIter` (`NewStringListIter`): a `StringIter` over a sorted list of strings.
#[derive(Clone, Debug, Default)]
pub struct StringListIter {
    l: std::vec::IntoIter<String>,
    cur: String,
}

impl StringListIter {
    pub fn new(s: Vec<String>) -> StringListIter {
        StringListIter {
            l: s.into_iter(),
            cur: String::new(),
        }
    }
}

/// Go `NewStringListIter`.
pub fn new_string_list_iter(s: Vec<String>) -> StringListIter {
    StringListIter::new(s)
}

impl StringIter for StringListIter {
    fn next(&mut self) -> bool {
        match self.l.next() {
            Some(s) => {
                self.cur = s;
                true
            }
            None => false,
        }
    }
    fn at(&self) -> &str {
        &self.cur
    }
    fn err(&self) -> Option<Error> {
        None
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Symbols.

/// Go `Symbols`: the symbol table of an index, with the offset of every 32nd symbol in memory. The bytes are
/// passed to each call (the table does not own them).
#[derive(Clone, Debug, Default)]
pub struct Symbols {
    version: i32,
    off: usize,
    offsets: Vec<usize>,
    seen: usize,
}

impl Symbols {
    /// Go `NewSymbols`: reads the symbol table at `off` in `bs`.
    pub fn new(bs: &[u8], version: i32, off: usize) -> Result<Symbols, Error> {
        let mut s = Symbols {
            version,
            off,
            offsets: Vec::new(),
            seen: 0,
        };
        let mut d = Decbuf::new_at(bs, off, true);
        let orig_len = d.len();
        let cnt = d.be32int().max(0) as usize;
        let base_pos = off + 4;
        s.offsets = Vec::with_capacity(1 + cnt / SYMBOL_FACTOR);
        while d.err().is_none() && s.seen < cnt {
            if s.seen % SYMBOL_FACTOR == 0 {
                s.offsets.push(base_pos + orig_len - d.len());
            }
            d.uvarint_bytes(); // The symbol.
            s.seen += 1;
        }
        if let Some(e) = d.err() {
            return Err(e.into());
        }
        Ok(s)
    }

    /// Go `Lookup`: the symbol with index `o` (v2+) or at byte offset `o` (v1), without a copy.
    pub fn lookup_bytes<'a>(&self, bs: &'a [u8], o: u32) -> Result<&'a [u8], Error> {
        let mut d = Decbuf::new(bs);
        if self.version == FORMAT_V1 {
            d.skip(o as usize);
        } else {
            if o as usize >= self.seen {
                return Err(Error::new(format!("unknown symbol offset {o}")));
            }
            d.skip(self.offsets[o as usize / SYMBOL_FACTOR]);
            // Walk until we find the one we want.
            for _ in 0..(o as usize % SYMBOL_FACTOR) {
                d.uvarint_bytes();
            }
        }
        let sym = d.uvarint_bytes();
        if let Some(e) = d.err() {
            return Err(e.into());
        }
        Ok(sym)
    }

    /// Go `Lookup`.
    pub fn lookup<'a>(&self, bs: &'a [u8], o: u32) -> Result<Cow<'a, str>, Error> {
        self.lookup_bytes(bs, o).map(yolo_str)
    }

    /// Go `ReverseLookup`: the index (v2+) or byte offset (v1) of `sym`.
    pub fn reverse_lookup(&self, bs: &[u8], sym: &str) -> Result<u32, Error> {
        if self.offsets.is_empty() {
            return Err(Error::new(format!(
                "unknown symbol {} - no symbols",
                model::unicode::quote(sym)
            )));
        }
        let sym_b = sym.as_bytes();
        let i = self.offsets.partition_point(|&off| {
            // Any decoding errors here will be lost, however we already read through all of this at startup.
            let mut d = Decbuf::new(bs);
            d.skip(off);
            d.uvarint_bytes() <= sym_b
        });
        let mut d = Decbuf::new(bs);
        let i = i.saturating_sub(1);
        d.skip(self.offsets[i]);
        let mut res = i * SYMBOL_FACTOR;
        let mut last_len = 0;
        let mut last_symbol: &[u8] = &[];
        while d.err().is_none() && res <= self.seen {
            last_len = d.len();
            last_symbol = d.uvarint_bytes();
            if last_symbol >= sym_b {
                break;
            }
            res += 1;
        }
        if let Some(e) = d.err() {
            return Err(e.into());
        }
        if last_symbol != sym_b {
            return Err(Error::new(format!(
                "unknown symbol {}",
                model::unicode::quote(sym)
            )));
        }
        if self.version == FORMAT_V1 {
            return Ok((bs.len() - last_len) as u32);
        }
        Ok(res as u32)
    }

    /// Go `Size`: the in-memory size of the offsets table in bytes.
    pub fn size(&self) -> usize {
        self.offsets.len() * 8
    }

    /// Go `Iter`: an iterator over all symbols, in order.
    pub fn iter<'a>(&self, bs: &'a [u8]) -> SymbolsIter<'a> {
        let mut d = Decbuf::new_at(bs, self.off, true);
        let cnt = d.be32int();
        SymbolsIter {
            d,
            cnt,
            cur: Cow::Borrowed(""),
            err: None,
        }
    }
}

/// Go `symbolsIter`.
pub struct SymbolsIter<'a> {
    d: Decbuf<'a>,
    cnt: i64,
    cur: Cow<'a, str>,
    err: Option<Error>,
}

impl StringIter for SymbolsIter<'_> {
    fn next(&mut self) -> bool {
        if self.cnt == 0 || self.err.is_some() {
            return false;
        }
        let b = self.d.uvarint_bytes();
        self.cnt -= 1;
        if let Some(e) = self.d.err() {
            self.err = Some(e.into());
            return false;
        }
        self.cur = yolo_str(b);
        true
    }
    fn at(&self) -> &str {
        &self.cur
    }
    fn err(&self) -> Option<Error> {
        self.err.clone()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Postings offset table.

/// Go `ReadPostingsOffsetTable`: reads the postings offset table at `off` and calls `f` for each entry with the
/// label name, label value (both borrowed from `bs`), the postings offset and the entry's offset within the
/// table. If `f` returns an error, decoding stops and the error is returned.
pub fn read_postings_offset_table<'a>(
    bs: &'a [u8],
    off: u64,
    mut f: impl FnMut(&'a [u8], &'a [u8], u64, usize) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut d = Decbuf::new_at(bs, off as usize, true);
    let start_len = d.len();
    let mut cnt = d.be32();

    while d.err().is_none() && !d.is_empty() && cnt > 0 {
        let offset_pos = start_len - d.len();

        let key_count = d.uvarint();
        if key_count != 2 {
            return Err(Error::new(format!(
                "unexpected number of keys for postings offset table {key_count}"
            )));
        }
        let name = d.uvarint_bytes();
        let value = d.uvarint_bytes();
        let o = d.uvarint64();
        if d.err().is_some() {
            break;
        }
        f(name, value, o, offset_pos)?;
        cnt -= 1;
    }
    match d.err() {
        Some(e) => Err(e.into()),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Reader.

/// The bytes of an index: an mmapped file or an owned buffer.
enum Data {
    Mmap(MmapFile),
    Owned(Vec<u8>),
}

impl Data {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            Data::Mmap(m) => m.bytes(),
            Data::Owned(v) => v,
        }
    }
}

/// Go `postingOffset`: a label value (as a byte range of the index) and the offset of its entry in the postings
/// offset table.
#[derive(Clone, Copy, Debug)]
struct PostingOffset {
    value_start: usize,
    value_len: u32,
    off: usize,
}

/// A cheap hasher for the `u32` symbol keys of `name_symbols` (multiplicative hashing; keys come from the
/// index file, and the map is small).
#[derive(Clone, Copy, Default)]
struct U32Hash;

#[derive(Default)]
struct U32Hasher(u64);

impl std::hash::BuildHasher for U32Hash {
    type Hasher = U32Hasher;
    fn build_hasher(&self) -> U32Hasher {
        U32Hasher(0)
    }
}

impl std::hash::Hasher for U32Hasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(8) ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    fn write_u32(&mut self, i: u32) {
        self.0 = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// Go `Range`: a byte range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Range {
    pub start: i64,
    pub end: i64,
}

/// Go `Reader`: reads an index file (format v1, v2 or v3).
pub struct Reader {
    data: Data,
    toc: Toc,

    /// Map of label name to a list of some label values' positions in the offset table. The first and last
    /// values for each name are always present.
    postings: HashMap<String, Vec<PostingOffset>>,
    /// For the v1 format, label name -> label value -> offset.
    postings_v1: HashMap<String, HashMap<String, u64>>,

    symbols: Symbols,
    /// Cache of the label name symbol lookups, as there are not many and they are half of all lookups.
    name_symbols: HashMap<u32, String, U32Hash>,

    decode_postings: PostingsDecoder,

    version: i32,
}

/// Go `Decoder`: decoding methods for the v1 and v2 index file format.
#[derive(Clone, Copy)]
pub struct Decoder {
    pub decode_postings: PostingsDecoder,
}

impl Decoder {
    /// Go `LabelNamesOffsetsFor`: the offsets of the name symbols of a series entry, in stored order.
    pub fn label_names_offsets_for(b: &[u8]) -> Result<Vec<u32>, Error> {
        let mut d = Decbuf::new(b);
        let k = d.uvarint();
        let mut offsets = Vec::with_capacity(k.clamp(0, 1 << 16) as usize);
        for _ in 0..k {
            offsets.push(d.uvarint() as u32);
            let _ = d.uvarint(); // Skip the label value.
            if let Some(e) = d.err() {
                return Err(Error::wrap("read series label offsets", e));
            }
        }
        match d.err() {
            Some(e) => Err(e.into()),
            None => Ok(offsets),
        }
    }

    /// Go `Series`: decodes a series entry from `b` into `builder` and `chks`, looking up symbols with
    /// `lookup`. Skips reading chunks metadata if `chks` is `None`.
    pub fn series<'s>(
        b: &[u8],
        mut lookup: impl FnMut(u32) -> Result<Cow<'s, str>, Error>,
        builder: &mut ScratchBuilder,
        chks: Option<&mut Vec<Meta>>,
    ) -> Result<(), Error> {
        builder.reset();
        let mut chks = chks;
        if let Some(c) = chks.as_deref_mut() {
            c.clear();
        }

        let mut d = Decbuf::new(b);
        let k = d.uvarint();
        for _ in 0..k {
            let lno = d.uvarint() as u32;
            let lvo = d.uvarint() as u32;
            if let Some(e) = d.err() {
                return Err(Error::wrap("read series label offsets", e));
            }
            let ln = lookup(lno).map_err(|e| Error::wrap("lookup label name", e))?;
            let lv = lookup(lvo).map_err(|e| Error::wrap("lookup label value", e))?;
            builder.add(&ln, &lv);
        }

        // Skip reading chunks metadata if chks is None.
        let Some(chks) = chks else {
            return d.err().map_or(Ok(()), |e| Err(e.into()));
        };

        // Read the chunks meta data.
        let k = d.uvarint();
        if k == 0 {
            return d.err().map_or(Ok(()), |e| Err(e.into()));
        }

        let mut t0 = d.varint64();
        let maxt = (d.uvarint64() as i64).wrapping_add(t0);
        let mut ref0 = d.uvarint64() as i64;

        chks.push(Meta {
            chunk_ref: ref0 as u64,
            chunk: None,
            min_time: t0,
            max_time: maxt,
        });
        t0 = maxt;

        for i in 1..k {
            let mint = (d.uvarint64() as i64).wrapping_add(t0);
            let maxt = (d.uvarint64() as i64).wrapping_add(mint);

            ref0 = ref0.wrapping_add(d.varint64());
            t0 = maxt;

            if let Some(e) = d.err() {
                return Err(Error::wrap(&format!("read meta for chunk {i}"), e));
            }
            chks.push(Meta {
                chunk_ref: ref0 as u64,
                chunk: None,
                min_time: mint,
                max_time: maxt,
            });
        }
        d.err().map_or(Ok(()), |e| Err(e.into()))
    }
}

impl Reader {
    /// Go `NewReader`: a reader over the index bytes `b`. It handles the different format versions.
    pub fn new(b: Vec<u8>, decoder: PostingsDecoder) -> Result<Reader, Error> {
        Reader::new_from(Data::Owned(b), decoder)
    }

    /// Go `NewFileReader`: a reader over the mmapped index file at `path`.
    pub fn open(path: &Path, decoder: PostingsDecoder) -> Result<Reader, Error> {
        let f = MmapFile::open(path)?;
        // On error the mapping is closed when `f` is dropped.
        Reader::new_from(Data::Mmap(f), decoder)
    }

    fn new_from(data: Data, decode_postings: PostingsDecoder) -> Result<Reader, Error> {
        let b = data.bytes();
        // Verify header.
        if b.len() < HEADER_LEN {
            return Err(Error::wrap("index header", encoding::Error::InvalidSize));
        }
        let m = u32::from_be_bytes(b[0..4].try_into().unwrap());
        if m != MAGIC_INDEX {
            return Err(Error::new(format!("invalid magic number {m:x}")));
        }
        let version = b[4] as i32;
        if !matches!(version, FORMAT_V1 | FORMAT_V2 | FORMAT_V3) {
            return Err(Error::new(format!("unknown index file version {version}")));
        }

        let toc = new_toc_from_byte_slice(b).map_err(|e| Error::wrap("read TOC", e))?;
        let symbols = Symbols::new(b, version, toc.symbols as usize)
            .map_err(|e| Error::wrap("read symbols", e))?;

        let mut postings: HashMap<String, Vec<PostingOffset>> = HashMap::new();
        let mut postings_v1: HashMap<String, HashMap<String, u64>> = HashMap::new();
        let base = b.as_ptr() as usize;

        if version == FORMAT_V1 {
            // Earlier V1 formats don't have a sorted postings offset table, so load the whole offset table into
            // memory.
            read_postings_offset_table(b, toc.postings_table, |name, value, off, _| {
                let name = yolo_str(name);
                if !postings_v1.contains_key(name.as_ref()) {
                    postings_v1.insert(name.to_string(), HashMap::new());
                    postings.insert(name.to_string(), Vec::new()); // Used to get a list of label names in places.
                }
                postings_v1
                    .get_mut(name.as_ref())
                    .unwrap()
                    .insert(yolo_str(value).into_owned(), off);
                Ok(())
            })
            .map_err(|e| Error::wrap("read postings table", e))?;
        } else {
            let mut last: Option<(&[u8], PostingOffset)> = None;
            let mut value_count = 0usize;
            let mut cur_name: &[u8] = &[];
            let mut cur_list: Option<Vec<PostingOffset>> = None;
            // For the postings offset table we keep every label name but only every nth label value (plus the
            // first and last one), to save memory.
            read_postings_offset_table(b, toc.postings_table, |name, value, _, off| {
                let po = PostingOffset {
                    value_start: value.as_ptr() as usize - base,
                    value_len: value.len() as u32,
                    off,
                };
                if cur_list.is_none() || name != cur_name {
                    // Next label name. Entries are sorted by name, so a name never comes back.
                    if let Some(mut l) = cur_list.take() {
                        // Always include last value for each label name.
                        if let Some((_, lpo)) = last.take() {
                            l.push(lpo);
                        }
                        l.shrink_to_fit();
                        postings.insert(yolo_str(cur_name).into_owned(), l);
                    }
                    cur_name = name;
                    cur_list = Some(Vec::new());
                    value_count = 0;
                }
                if value_count % SYMBOL_FACTOR == 0 {
                    cur_list.as_mut().unwrap().push(po);
                    last = None;
                } else {
                    last = Some((name, po));
                }
                value_count += 1;
                Ok(())
            })
            .map_err(|e| Error::wrap("read postings table", e))?;
            if let Some(mut l) = cur_list.take() {
                if let Some((_, lpo)) = last.take() {
                    l.push(lpo);
                }
                l.shrink_to_fit();
                postings.insert(yolo_str(cur_name).into_owned(), l);
            }
        }

        let mut name_symbols = HashMap::with_capacity_and_hasher(postings.len(), U32Hash);
        for k in postings.keys() {
            if k.is_empty() {
                continue;
            }
            let off = symbols
                .reverse_lookup(b, k)
                .map_err(|e| Error::wrap("reverse symbol lookup", e))?;
            name_symbols.insert(off, k.clone());
        }

        Ok(Reader {
            data,
            toc,
            postings,
            postings_v1,
            symbols,
            name_symbols,
            decode_postings,
            version,
        })
    }

    #[inline]
    fn b(&self) -> &[u8] {
        self.data.bytes()
    }

    #[inline]
    fn po_value(&self, po: &PostingOffset) -> &[u8] {
        &self.b()[po.value_start..po.value_start + po.value_len as usize]
    }

    /// Go `Version`: the file format version of the index.
    pub fn version(&self) -> i32 {
        self.version
    }

    /// The table of contents.
    pub fn toc(&self) -> &Toc {
        &self.toc
    }

    /// Go `PostingsRanges`: the byte range in the index file of every postings list.
    pub fn postings_ranges(&self) -> Result<HashMap<Label, Range>, Error> {
        let b = self.b();
        let mut m = HashMap::new();
        read_postings_offset_table(b, self.toc.postings_table, |name, value, off, _| {
            let d = Decbuf::new_at(b, off as usize, true);
            if let Some(e) = d.err() {
                return Err(e.into());
            }
            m.insert(
                Label::new(yolo_str(name).into_owned(), yolo_str(value).into_owned()),
                Range {
                    start: off as i64 + 4,
                    end: off as i64 + 4 + d.len() as i64,
                },
            );
            Ok(())
        })
        .map_err(|e| Error::wrap("read postings table", e))?;
        Ok(m)
    }

    /// Go `Close`: releases the underlying bytes (dropping the reader does the same).
    pub fn close(self) -> Result<(), Error> {
        Ok(())
    }

    fn lookup_symbol(&self, o: u32) -> Result<Cow<'_, str>, Error> {
        if let Some(s) = self.name_symbols.get(&o) {
            return Ok(Cow::Borrowed(s));
        }
        self.symbols.lookup(self.b(), o)
    }

    /// Go `Symbols`: an iterator over the symbols that exist within the index.
    pub fn symbols(&self) -> SymbolsIter<'_> {
        self.symbols.iter(self.b())
    }

    /// Go `SymbolTableSize`: the symbol table size in bytes.
    pub fn symbol_table_size(&self) -> u64 {
        self.symbols.size() as u64
    }

    /// Go `SortedLabelValues`: the values that exist for the label name `name`, sorted.
    pub fn sorted_label_values(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let mut values = self.label_values(ctx, name, hints, matchers)?;
        if self.version == FORMAT_V1 {
            values.sort_unstable();
        }
        Ok(values)
    }

    /// Go `LabelValues`: the values that exist for the label name `name` (sorted, except for v1). Matchers are
    /// not supported.
    pub fn label_values(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        if !matchers.is_empty() {
            return Err(Error::new(format!(
                "matchers parameter is not implemented: {}",
                fmt_matchers(matchers)
            )));
        }

        if self.version == FORMAT_V1 {
            let Some(e) = self.postings_v1.get(name) else {
                return Ok(Vec::new());
            };
            // The offsets are held in a map, so they are read in an arbitrary order. Stopping at the limit
            // would return an arbitrary subset, which a caller asking for the smallest values cannot use.
            let early_limit = LabelHints::allows_early_stop(hints);
            let mut values = Vec::with_capacity(e.len());
            for k in e.keys() {
                if early_limit && values.len() as i64 >= hints.map_or(0, |h| h.limit) {
                    break;
                }
                values.push(k.clone());
            }
            if !early_limit {
                values = super::apply_limit(hints, values);
            }
            return Ok(values);
        }
        let Some(e) = self.postings.get(name) else {
            return Ok(Vec::new());
        };
        if e.is_empty() {
            return Ok(Vec::new());
        }

        let limit = hints.map_or(0, |h| h.limit.max(0) as usize);
        let mut values_length = e.len() * SYMBOL_FACTOR;
        if limit > 0 && values_length > limit {
            values_length = limit;
        }
        let mut values = Vec::with_capacity(values_length);
        let last_val = self.po_value(&e[e.len() - 1]);
        self.traverse_posting_offsets(ctx, e[0].off, |val, _| {
            if limit > 0 && values.len() >= limit {
                return Ok(false);
            }
            values.push(yolo_str(val).into_owned());
            Ok(val != last_val)
        })?;
        Ok(values)
    }

    /// Go `LabelNamesFor`: all the label names of the series referred to by the postings, sorted.
    pub fn label_names_for(
        &self,
        ctx: &dyn Ctx,
        postings: &mut dyn Postings,
    ) -> Result<Vec<String>, Error> {
        // Gather the name offsets in the symbol table first.
        let mut offsets_map: HashSet<u32> = HashSet::new();
        let mut i = 0usize;
        while postings.next() {
            let id = postings.at();
            i += 1;
            if i % CHECK_CONTEXT_EVERY_N_ITERATIONS == 0 {
                if let Some(e) = ctx.err() {
                    return Err(e);
                }
            }

            let offset = self.series_offset(id);
            let d = Decbuf::new_uvarint_at(self.b(), offset);
            let buf = d.get();
            if let Some(e) = d.err() {
                return Err(Error::wrap("get buffer for series", e));
            }

            let offsets = Decoder::label_names_offsets_for(buf)
                .map_err(|e| Error::wrap("get label name offsets", e))?;
            offsets_map.extend(offsets);
        }

        // Lookup the unique symbols.
        let mut names = Vec::with_capacity(offsets_map.len());
        for off in offsets_map {
            let name = self
                .lookup_symbol(off)
                .map_err(|e| Error::wrap("lookup symbol in LabelNamesFor", e))?;
            names.push(name.into_owned());
        }
        names.sort_unstable();
        Ok(names)
    }

    /// In version 2 series IDs are no longer exact references but series are 16-byte padded and the ID is the
    /// multiple of 16 of the actual position.
    #[inline]
    fn series_offset(&self, id: SeriesRef) -> usize {
        if self.version != FORMAT_V1 {
            id.wrapping_mul(SERIES_BYTE_ALIGN) as usize
        } else {
            id as usize
        }
    }

    /// Go `Series`: reads the series with the given ID and writes its labels into `builder` and its chunk metas
    /// into `chks` (when given).
    pub fn series(
        &self,
        id: SeriesRef,
        builder: &mut ScratchBuilder,
        chks: Option<&mut Vec<Meta>>,
    ) -> Result<(), Error> {
        let d = Decbuf::new_uvarint_at(self.b(), self.series_offset(id));
        if let Some(e) = d.err() {
            return Err(e.into());
        }
        builder.reset();
        Decoder::series(d.get(), |o| self.lookup_symbol(o), builder, chks)
            .map_err(|e| Error::wrap("read series", e))
    }

    /// Go `traversePostingOffsets`: traverses the postings offset table, starting at `off`, and calls `cb` with
    /// every label value and postings offset. If `cb` returns false (or an error), the traversing is
    /// interrupted.
    fn traverse_posting_offsets<'s>(
        &'s self,
        ctx: &dyn Ctx,
        off: usize,
        mut cb: impl FnMut(&'s [u8], u64) -> Result<bool, Error>,
    ) -> Result<(), Error> {
        // Don't CRC32 the entire postings offset table, this is very slow so hope any issues were caught at
        // startup.
        let mut d = Decbuf::new_at(self.b(), self.toc.postings_table as usize, false);
        d.skip(off);
        let mut skip = 0usize;
        let mut ctx_err = ctx.err();
        while d.err().is_none() && ctx_err.is_none() {
            if skip == 0 {
                // These are always the same number of bytes, and it's faster to skip than to parse.
                skip = d.len();
                d.uvarint(); // Keycount.
                d.uvarint_bytes(); // Label name.
                skip -= d.len();
            } else {
                d.skip(skip);
            }
            let v = d.uvarint_bytes(); // Label value.
            let postings_off = d.uvarint64(); // Offset.
            if !cb(v, postings_off)? {
                break;
            }
            ctx_err = ctx.err();
        }
        if let Some(e) = d.err() {
            return Err(Error::wrap("get postings offset entry", e));
        }
        if let Some(e) = ctx_err {
            return Err(Error::wrap("get postings offset entry", e));
        }
        Ok(())
    }

    fn decode_at(&self, postings_off: u64) -> Result<BoxPostings<'_>, Error> {
        let d = Decbuf::new_at(self.b(), postings_off as usize, true);
        let (_, p) = (self.decode_postings)(d).map_err(|e| Error::wrap("decode postings", e))?;
        Ok(p)
    }

    /// Go `Postings`: the postings of the series with label `name` equal to any of `values`.
    pub fn postings<S: AsRef<str>>(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        values: &[S],
    ) -> Result<BoxPostings<'_>, Error> {
        if self.version == FORMAT_V1 {
            let Some(e) = self.postings_v1.get(name) else {
                return Ok(empty_postings());
            };
            let mut res = Vec::with_capacity(values.len());
            for v in values {
                let Some(&postings_off) = e.get(v.as_ref()) else {
                    continue;
                };
                // Read from the postings table.
                res.push(self.decode_at(postings_off)?);
            }
            return Ok(merge(res));
        }

        let Some(e) = self.postings.get(name) else {
            return Ok(empty_postings());
        };
        if values.is_empty() {
            return Ok(empty_postings());
        }

        // Values must be in order so we can step through the table on disk.
        let mut vals: Vec<&[u8]> = values.iter().map(|v| v.as_ref().as_bytes()).collect();
        vals.sort_unstable();
        let mut res = Vec::with_capacity(vals.len());
        let mut value_index = 0;
        let first = self.po_value(&e[0]);
        while value_index < vals.len() && vals[value_index] < first {
            // Discard values before the start.
            value_index += 1;
        }
        while value_index < vals.len() {
            let mut value = vals[value_index];

            let mut i = e.partition_point(|po| self.po_value(po) < value);
            if i == e.len() {
                // We're past the end.
                break;
            }
            if i > 0 && self.po_value(&e[i]) != value {
                // Need to look from previous entry.
                i -= 1;
            }
            let next_value = e.get(i + 1).map(|po| self.po_value(po));

            self.traverse_posting_offsets(ctx, e[i].off, |val, postings_off| {
                while val >= value {
                    if val == value {
                        // Read from the postings table.
                        res.push(self.decode_at(postings_off)?);
                    }
                    value_index += 1;
                    if value_index == vals.len() {
                        break;
                    }
                    value = vals[value_index];
                }
                if next_value.is_none_or(|nv| value >= nv) || value_index == vals.len() {
                    // Need to go to a later postings offset entry, if there is one.
                    return Ok(false);
                }
                Ok(true)
            })?;
        }

        Ok(merge(res))
    }

    /// Go `PostingsForLabelMatching`: the postings of all values of `name` for which `match_fn` is true.
    pub fn postings_for_label_matching(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        match_fn: &dyn Fn(&str) -> bool,
    ) -> BoxPostings<'_> {
        self.postings_for_label_matching_inner(ctx, name, Some(match_fn))
    }

    /// Go `PostingsForAllLabelValues`.
    pub fn postings_for_all_label_values(&self, ctx: &dyn Ctx, name: &str) -> BoxPostings<'_> {
        self.postings_for_label_matching_inner(ctx, name, None)
    }

    fn postings_for_label_matching_inner(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        match_fn: Option<&dyn Fn(&str) -> bool>,
    ) -> BoxPostings<'_> {
        if self.version == FORMAT_V1 {
            return self.postings_for_label_matching_v1(ctx, name, match_fn);
        }

        let Some(e) = self.postings.get(name).filter(|e| !e.is_empty()) else {
            return empty_postings();
        };

        let mut postings_estimate = 0;
        if match_fn.is_none() {
            // The caller wants all postings for name.
            postings_estimate = e.len() * SYMBOL_FACTOR;
        }

        let last_val = self.po_value(&e[e.len() - 1]);
        let mut its = Vec::with_capacity(postings_estimate);
        if let Err(err) = self.traverse_posting_offsets(ctx, e[0].off, |val, postings_off| {
            if match_fn.is_none_or(|m| m(&yolo_str(val))) {
                // We want this postings iterator since the value is a match.
                its.push(self.decode_at(postings_off)?);
            }
            Ok(val != last_val)
        }) {
            return err_postings(err);
        }
        merge(its)
    }

    fn postings_for_label_matching_v1(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        match_fn: Option<&dyn Fn(&str) -> bool>,
    ) -> BoxPostings<'_> {
        let Some(e) = self.postings_v1.get(name).filter(|e| !e.is_empty()) else {
            return empty_postings();
        };

        let mut its = Vec::new();
        let mut count = 1;
        for (val, &offset) in e {
            if count % CHECK_CONTEXT_EVERY_N_ITERATIONS == 0 && ctx.err().is_some() {
                return err_postings(ctx.err().unwrap_or_else(Error::canceled));
            }
            count += 1;
            if match_fn.is_some_and(|m| !m(val)) {
                continue;
            }
            // Read from the postings table.
            match self.decode_at(offset) {
                Ok(p) => its.push(p),
                Err(err) => return err_postings(err),
            }
        }
        merge(its)
    }

    /// Go `SortedPostings`: the postings reordered so that the backing series are sorted (they already are).
    pub fn sorted_postings<'a>(&self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        p
    }

    /// Go `ShardedPostings`: the postings filtered to those whose series' labels hash to `shard_index` out of
    /// `shard_count`.
    pub fn sharded_postings<'a>(
        &self,
        mut p: BoxPostings<'a>,
        shard_index: u64,
        shard_count: u64,
    ) -> BoxPostings<'a> {
        let mut out: Vec<SeriesRef> = Vec::with_capacity(128);
        let mut buf_lbls = ScratchBuilder::default();

        while p.next() {
            let id = p.at();
            // Get the series labels (no chunks).
            if self.series(id, &mut buf_lbls, None).is_err() {
                return err_postings(Error::new(format!("series {id} not found")));
            }
            // Check if the series belong to the shard.
            if stable_hash(&buf_lbls.labels()) % shard_count != shard_index {
                continue;
            }
            out.push(id);
        }
        if let Some(e) = p.err() {
            return err_postings(Error::wrap("expand postings", e));
        }
        Box::new(ListPostings::from_shared(std::sync::Arc::new(out)))
    }

    /// Go `Size`: the size of the index file.
    pub fn size(&self) -> i64 {
        self.b().len() as i64
    }

    /// Go `LabelNames`: all the unique label names present in the index, sorted. Matchers are not supported.
    pub fn label_names(&self, _ctx: &dyn Ctx, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        if !matchers.is_empty() {
            return Err(Error::new(format!(
                "matchers parameter is not implemented: {}",
                fmt_matchers(matchers)
            )));
        }
        let mut label_names: Vec<String> = self
            .postings
            .keys()
            .filter(|n| !n.is_empty())
            .cloned()
            .collect();
        label_names.sort_unstable();
        Ok(label_names)
    }
}

/// Go `%+v` of a `[]*labels.Matcher`.
fn fmt_matchers(ms: &[Matcher]) -> String {
    let parts: Vec<String> = ms.iter().map(|m| m.to_string()).collect();
    format!("[{}]", parts.join(" "))
}
