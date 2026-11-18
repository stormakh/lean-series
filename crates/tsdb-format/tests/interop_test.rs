//! Byte compatibility with upstream Go Prometheus (both directions). `scripts/interop/tsdb-format/run.sh` runs the
//! Go generator (`scripts/interop/tsdb-format/gen`), which writes TSDB files to `bench/tmp/w1-tsdbfmt/go` and their
//! content dump to `bench/tmp/w1-tsdbfmt/go.dump.txt`. This test:
//! 1. reads the Go files with the Rust readers and checks that the Rust dump equals Go's dump;
//! 2. decodes and re-encodes every WAL record and checks identical bytes;
//! 3. writes the same content with the Rust writers to `bench/tmp/w1-tsdbfmt/rust` (index, chunks, chunks_head,
//!    WALs with their own checkpoints, tombstones). The Go verifier then reads those files.
//! Skipped when the Go output does not exist.

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use std::sync::Arc;

use chunkenc::{Chunk, Encoding};
use model::labels::{Labels, ScratchBuilder};
use tsdb_format::chunks::{self, ChunkDiskMapper, ChunkDiskMapperRef, DEFAULT_WRITE_BUFFER_SIZE, Meta};
use tsdb_format::context::Context;
use tsdb_format::index::{self, Postings as _, StringIter as _};
use tsdb_format::record::{Decoder, Encoder, Type};
use tsdb_format::tombstones::{self, Interval};
use tsdb_format::wlog::{self, Reader, WL, compression};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w1-tsdbfmt")
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

#[test]
fn interop_with_go() {
    let root = root();
    let go = root.join("go");
    if !go.exists() {
        eprintln!("skipped: {} does not exist (run scripts/interop/tsdb-format/run.sh)", go.display());
        return;
    }
    let want = fs::read_to_string(root.join("go.dump.txt")).unwrap();
    let got = dump(&go);
    fs::write(root.join("go.dump-by-rust.txt"), &got).unwrap();
    compare_dumps("Rust reading the Go files", &want, &got);

    let out = root.join("rust");
    let _ = fs::remove_dir_all(&out);
    fs::create_dir_all(&out).unwrap();
    rewrite(&go, &out);
    // Rust reads its own files back with the same content.
    let again = dump(&out);
    compare_dumps("Rust reading the Rust files", &want, &again);
}

fn compare_dumps(what: &str, want: &str, got: &str) {
    if want == got {
        eprintln!("ok   {what}: same content ({} dump lines)", got.lines().count());
        return;
    }
    let (wl, gl): (Vec<&str>, Vec<&str>) = (want.lines().collect(), got.lines().collect());
    let mut shown = 0;
    for i in 0..wl.len().max(gl.len()) {
        let (a, b) = (wl.get(i).copied().unwrap_or(""), gl.get(i).copied().unwrap_or(""));
        if a != b && shown < 5 {
            eprintln!("line {}:\n  go:   {:.200}\n  rust: {:.200}", i + 1, a, b);
            shown += 1;
        }
    }
    panic!("{what}: dump differs from Go's ({} vs {} lines)", wl.len(), gl.len());
}

/// Decodes a WAL record with the Rust decoder and encodes it again with the Rust encoder.
fn reencode(dec: &mut Decoder, rec: &[u8]) -> Vec<u8> {
    let t = dec.r#type(rec);
    let enc = Encoder { enable_st_storage: matches!(t, Type::SamplesV2 | Type::HistogramSamplesV2 | Type::FloatHistogramSamplesV2) };
    let mut b = Vec::new();
    match t {
        Type::Series => {
            let mut v = Vec::new();
            dec.series(rec, &mut v).unwrap();
            enc.series(&v, &mut b);
        }
        Type::Samples | Type::SamplesV2 => {
            let mut v = Vec::new();
            dec.samples(rec, &mut v).unwrap();
            enc.samples(&v, &mut b);
        }
        Type::Tombstones => {
            let mut v = Vec::new();
            dec.tombstones(rec, &mut v).unwrap();
            enc.tombstones(&v, &mut b);
        }
        Type::Exemplars => {
            let mut v = Vec::new();
            dec.exemplars(rec, &mut v).unwrap();
            enc.exemplars(&v, &mut b);
        }
        Type::MmapMarkers => {
            let mut v = Vec::new();
            dec.mmap_markers(rec, &mut v).unwrap();
            enc.mmap_markers(&v, &mut b);
        }
        Type::Metadata => {
            let mut v = Vec::new();
            dec.metadata(rec, &mut v).unwrap();
            enc.metadata(&v, &mut b);
        }
        Type::HistogramSamples | Type::HistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.histogram_samples(rec, &mut v).unwrap();
            let custom = enc.histogram_samples(&v, &mut b);
            assert!(custom.is_empty(), "custom bucket histograms in a regular V1 record");
        }
        Type::CustomBucketsHistogramSamples => {
            let mut v = Vec::new();
            dec.histogram_samples(rec, &mut v).unwrap();
            enc.custom_buckets_histogram_samples(&v, &mut b);
        }
        Type::FloatHistogramSamples | Type::FloatHistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.float_histogram_samples(rec, &mut v).unwrap();
            let custom = enc.float_histogram_samples(&v, &mut b);
            assert!(custom.is_empty(), "custom bucket histograms in a regular V1 record");
        }
        Type::CustomBucketsFloatHistogramSamples => {
            let mut v = Vec::new();
            dec.float_histogram_samples(rec, &mut v).unwrap();
            enc.custom_buckets_float_histogram_samples(&v, &mut b);
        }
        Type::MinValidTime => {
            let mint = dec.min_valid_time(rec).unwrap();
            enc.min_valid_time(mint, &mut b);
        }
        Type::Unknown => panic!("unknown record type {}", rec[0]),
    }
    b
}

fn dump_tombstones(dir: &Path, out: &mut String) {
    let (tr, _) = tombstones::read_tombstones(dir).unwrap();
    let mut stones: Vec<(u64, Vec<Interval>)> = Vec::new();
    tr.iter(&mut |r, ivs| {
        stones.push((r, ivs.to_vec()));
        Ok(())
    })
    .unwrap();
    stones.sort_by_key(|s| s.0);
    for (r, ivs) in stones {
        let is: Vec<String> = ivs.iter().map(|i| format!("{}:{}", i.mint, i.maxt)).collect();
        let _ = writeln!(out, "tombstone {r} {}", is.join(" "));
    }
}

const COMPRESSIONS: [(&str, compression::Type); 3] =
    [("none", compression::Type::None), ("snappy", compression::Type::Snappy), ("zstd", compression::Type::Zstd)];

/// All records of the numbered segments in `dir` (not the checkpoint).
fn read_records(dir: &Path) -> Vec<Vec<u8>> {
    let mut r = Reader::new(wlog::new_segments_reader(dir).unwrap());
    let mut out = Vec::new();
    while r.next() {
        out.push(r.record().to_vec());
    }
    if let Some(e) = r.err() {
        panic!("read {}: {e}", dir.display());
    }
    out
}

fn dump_wals(dir: &Path, out: &mut String) {
    let mut dec = Decoder::new(Default::default());
    for (name, _) in COMPRESSIONS {
        let wdir = dir.join(format!("wal-{name}"));
        let (first, last) = wlog::segments(&wdir).unwrap();
        let _ = writeln!(out, "wal {name} segments {first} {last}");
        for rec in read_records(&wdir) {
            let _ = writeln!(out, "rec {name} {} {}", rec[0], hex(&rec));
        }
        dump_checkpoint(&mut dec, name, &wdir, out);
    }
}

fn make_checkpoint(w: &WL, to: i64) {
    let keep = |id: u64| id % 3 != 0;
    wlog::checkpoint(w, 0, to, &keep, CHECKPOINT_MINT, false, true).unwrap();
}

fn dump_checkpoint(dec: &mut Decoder, name: &str, wdir: &Path, out: &mut String) {
    let (cp, idx) = wlog::last_checkpoint(wdir).unwrap();
    let _ = writeln!(out, "checkpoint {name} {} {idx}", cp.file_name().unwrap().to_string_lossy());
    let mut md = Vec::new();
    for rec in read_records(&cp) {
        if dec.r#type(&rec) == Type::Metadata {
            dec.metadata(&rec, &mut md).unwrap();
            continue;
        }
        let _ = writeln!(out, "cprec {name} {} {}", rec[0], hex(&rec));
    }
    // Go writes the metadata records from a map, in random order: dump them sorted.
    md.sort_by_key(|m| m.r#ref);
    for m in md {
        let _ = writeln!(out, "cpmeta {name} {} {} {}\t{}", m.r#ref, m.r#type, m.unit, m.help);
    }
}

const WAL_SEGMENT_SIZE: usize = 4 * 32 * 1024;
const COMPRESSED_WAL_SEGMENT_SIZE: usize = 32 * 32 * 1024;
const CHECKPOINT_MINT: i64 = 1_000_000;

/// Re-encodes every Go record (asserting identical bytes) and writes them to Rust WALs, then makes a checkpoint
/// with the same parameters as the Go generator.
fn rewrite_wals(go: &Path, out: &Path) {
    let mut dec = Decoder::new(Default::default());
    for (name, c) in COMPRESSIONS {
        let recs = read_records(&go.join(format!("wal-{name}")));
        let size = if c == compression::Type::None { WAL_SEGMENT_SIZE } else { COMPRESSED_WAL_SEGMENT_SIZE };
        let w = WL::new_size(out.join(format!("wal-{name}")), size, c).unwrap();
        for (i, rec) in recs.iter().enumerate() {
            if i == recs.len() / 2 {
                w.next_segment().unwrap();
            }
            let again = reencode(&mut dec, rec);
            assert!(again == *rec, "wal-{name} record {i} (type {}): Rust re-encoding differs from Go", rec[0]);
            w.log(rec).unwrap();
        }
        let (_, last) = wlog::segments(w.dir()).unwrap();
        make_checkpoint(&w, last - 1);
        w.close().unwrap();
    }
    eprintln!("ok   every WAL record decodes and re-encodes to the same bytes");
}

struct HeadChunk {
    series: u64,
    r: ChunkDiskMapperRef,
    mint: i64,
    maxt: i64,
    num_samples: u16,
    enc: u8,
    ooo: bool,
    bytes: Vec<u8>,
}

fn read_head_chunks(dir: &Path) -> Vec<HeadChunk> {
    let cdm = ChunkDiskMapper::new(&dir.join("chunks_head"), None, DEFAULT_WRITE_BUFFER_SIZE, 0).unwrap();
    let mut out = Vec::new();
    cdm.iterate_all_chunks(|series, r, mint, maxt, num_samples, enc, ooo| {
        out.push(HeadChunk { series, r, mint, maxt, num_samples, enc: enc.0, ooo, bytes: Vec::new() });
        Ok(())
    })
    .unwrap();
    for c in &mut out {
        c.bytes = cdm.chunk_bytes_with(c.r, |_, b| b.to_vec()).unwrap();
    }
    cdm.close().unwrap();
    out
}

fn dump_head_chunks(dir: &Path, out: &mut String) {
    for c in read_head_chunks(dir) {
        let _ = writeln!(
            out,
            "headchunk {} {} {} {} {} {} {} {}",
            c.series,
            c.r.0,
            c.mint,
            c.maxt,
            c.num_samples,
            c.enc,
            c.ooo,
            hex(&c.bytes)
        );
    }
}

/// Writes the Go head chunks with the Rust ChunkDiskMapper, cutting a new file where Go's file number changes.
fn rewrite_head_chunks(go: &Path, out: &Path) {
    let cdm = ChunkDiskMapper::new(&out.join("chunks_head"), None, DEFAULT_WRITE_BUFFER_SIZE, 0).unwrap();
    cdm.iterate_all_chunks(|_, _, _, _, _, _, _| Ok(())).unwrap();
    let mut prev_file = None;
    for c in read_head_chunks(go) {
        let file = c.r.unpack().0;
        if prev_file.is_some_and(|p| p != file) {
            cdm.cut_new_file();
        }
        prev_file = Some(file);
        let chk: Arc<dyn Chunk> = Arc::from(chunkenc::from_data(Encoding(c.enc), c.bytes).unwrap());
        let r = cdm.write_chunk(c.series, c.mint, c.maxt, chk, c.ooo, None);
        assert_eq!(r, c.r, "head chunk ref differs from Go's");
    }
    cdm.close().unwrap();
}

const CHUNK_SEGMENT_SIZE: i64 = 64 * 1024;

fn lstr(ls: &Labels) -> String {
    let mut s = String::new();
    ls.range(|n, v| {
        if !s.is_empty() {
            s.push(';');
        }
        s.push_str(n);
        s.push('=');
        s.push_str(v);
    });
    s
}

/// One series of a block: its ref in the index, labels and chunks (ref, mint, maxt, encoding, bytes).
struct BlockSeries {
    r: u64,
    labels: Labels,
    chunks: Vec<(u64, i64, i64, u8, Vec<u8>)>,
}

/// Reads the index and the block chunks of `dir`, writing the index part of the dump to `out`.
fn read_block(dir: &Path, out: &mut String) -> (Vec<String>, Vec<BlockSeries>) {
    let ctx = index::background();
    let ir = index::Reader::open(&dir.join("index"), index::decode_postings_raw).unwrap();
    let _ = writeln!(out, "index version {}", ir.version());
    let mut symbols = Vec::new();
    let mut it = ir.symbols();
    while it.next() {
        symbols.push(it.at().to_string());
        let _ = writeln!(out, "symbol {}", it.at());
    }
    assert!(it.err().is_none());
    let names = ir.label_names(ctx, &[]).unwrap();
    let _ = writeln!(out, "labelnames {}", names.join(" "));
    for n in &names {
        for v in ir.sorted_label_values(ctx, n, None, &[]).unwrap() {
            let mut p = ir.postings(ctx, n, &[v.as_str()]).unwrap();
            let refs: Vec<String> = index::expand_postings(&mut *p).unwrap().iter().map(|r| r.to_string()).collect();
            let _ = writeln!(out, "postings {n}\t{v}\t{}", refs.join(" "));
        }
    }
    let cr = chunks::new_dir_reader(&dir.join("chunks"), None).unwrap();
    let (k, v) = index::all_postings_key();
    let mut all = ir.postings(ctx, k, &[v]).unwrap();
    let mut b = ScratchBuilder::new(0);
    let mut metas = Vec::new();
    let mut series = Vec::new();
    while all.next() {
        metas.clear();
        ir.series(all.at(), &mut b, Some(&mut metas)).unwrap();
        let labels = b.labels();
        let cs: Vec<String> = metas.iter().map(|m| format!("{}:{}:{}", m.r#ref, m.min_time, m.max_time)).collect();
        let _ = writeln!(out, "series {}\t{}\t{}", all.at(), lstr(&labels), cs.join(" "));
        let mut s = BlockSeries { r: all.at(), labels, chunks: Vec::new() };
        for m in &metas {
            let (enc, bytes) = cr.chunk_bytes(m.r#ref).unwrap();
            let _ = writeln!(out, "chunk {} {} {}", m.r#ref, enc.0, hex(bytes));
            s.chunks.push((m.r#ref, m.min_time, m.max_time, enc.0, bytes.to_vec()));
        }
        series.push(s);
    }
    assert!(all.err().is_none());
    (symbols, series)
}

/// Writes the Go block (chunks and index) with the Rust writers.
fn rewrite_block(go: &Path, out: &Path) {
    let (symbols, series) = read_block(go, &mut String::new());
    let mut cw = chunks::new_writer(&out.join("chunks"), &[chunks::with_segment_size(CHUNK_SEGMENT_SIZE)]).unwrap();
    let mut iw = index::Writer::new(&Context::background(), &out.join("index")).unwrap();
    for s in &symbols {
        iw.add_symbol(s).unwrap();
    }
    for s in series {
        let mut metas: Vec<Meta> = s
            .chunks
            .iter()
            .map(|(_, mint, maxt, enc, b)| Meta {
                r#ref: 0,
                chunk: Some(chunkenc::from_data(Encoding(*enc), b.clone()).unwrap()),
                min_time: *mint,
                max_time: *maxt,
            })
            .collect();
        cw.write_chunks(&mut metas).unwrap();
        for (m, c) in metas.iter().zip(&s.chunks) {
            assert_eq!(m.r#ref, c.0, "block chunk ref differs from Go's");
        }
        iw.add_series(s.r, &s.labels, &metas).unwrap();
    }
    cw.close().unwrap();
    iw.close().unwrap();
}

fn dump(dir: &Path) -> String {
    let mut out = String::new();
    read_block(dir, &mut out);
    dump_head_chunks(dir, &mut out);
    dump_wals(dir, &mut out);
    dump_tombstones(dir, &mut out);
    out
}

fn rewrite(go: &Path, out: &Path) {
    rewrite_block(go, out);
    rewrite_head_chunks(go, out);
    rewrite_wals(go, out);
    let (tr, _) = tombstones::read_tombstones(go).unwrap();
    tombstones::write_file(&Default::default(), out, &tr).unwrap();
}
