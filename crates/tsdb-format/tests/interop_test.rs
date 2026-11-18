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

fn dump(_dir: &Path) -> String {
    unimplemented!()
}

fn rewrite(_go: &Path, _out: &Path) {
    unimplemented!()
}

#[test]
fn tmp_wal_only() {
    let root = root();
    let go = root.join("go");
    let want: String = fs::read_to_string(root.join("go.dump.txt"))
        .unwrap()
        .lines()
        .filter(|l| ["wal ", "rec ", "checkpoint ", "cprec ", "cpmeta ", "tombstone "].iter().any(|p| l.starts_with(p)))
        .map(|l| format!("{l}\n"))
        .collect();
    let mut got = String::new();
    dump_wals(&go, &mut got);
    dump_tombstones(&go, &mut got);
    compare_dumps("wal go", &want, &got);
    let out = root.join("rust");
    let _ = fs::remove_dir_all(&out);
    fs::create_dir_all(&out).unwrap();
    rewrite_wals(&go, &out);
    let (tr, _) = tombstones::read_tombstones(&go).unwrap();
    tombstones::write_file(&Default::default(), &out, &tr).unwrap();
    let mut got = String::new();
    dump_wals(&out, &mut got);
    dump_tombstones(&out, &mut got);
    compare_dumps("wal rust", &want, &got);
}
