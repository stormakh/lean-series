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

fn dump(_dir: &Path) -> String {
    unimplemented!()
}

fn rewrite(_go: &Path, _out: &Path) {
    unimplemented!()
}
