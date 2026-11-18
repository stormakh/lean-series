#!/usr/bin/env bash
# Byte compatibility check of crates/tsdb-format with upstream Go Prometheus, in both directions.
# 1. Go (gen/, upstream code) writes an index, block chunk segments, head chunk files, WALs (none, snappy, zstd;
#    all record types) with a checkpoint each, and tombstones to bench/tmp/w1-tsdbfmt/go, and their content dump
#    (read back with upstream readers) to bench/tmp/w1-tsdbfmt/go.dump.txt.
# 2. crates/tsdb-format/tests/interop_test.rs reads the Go files with the Rust readers and checks the same dump,
#    re-encodes every WAL record and checks identical bytes, then writes the same content with the Rust writers
#    to bench/tmp/w1-tsdbfmt/rust (and makes its own checkpoints).
# 3. Go reads the Rust files with upstream readers, checks the same dump, and checks that index, chunks,
#    chunks_head and the uncompressed WAL are byte-identical to the Go files.
# Usage: scripts/interop/tsdb-format/run.sh (from anywhere inside the repository).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
out="$root/bench/tmp/w1-tsdbfmt"
rm -rf -- "${out:?}"
mkdir -p "$out"
GOPROXY=off bash "$here/go.sh" -C "$here/gen" run . write "$out"
cd "$root"
~/.cargo/bin/cargo test -p tsdb-format --test interop_test -- --nocapture
GOPROXY=off bash "$here/go.sh" -C "$here/gen" run . verify "$out"
