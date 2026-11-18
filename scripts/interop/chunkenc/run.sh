#!/usr/bin/env bash
# Byte compatibility check of the Rust chunk encodings with upstream Go (tsdb/chunkenc).
# 1. The Go generator (gen/) encodes fixed sample sequences with upstream code and writes one manifest per chunk
#    to bench/tmp/w0-hist/interop/.
# 2. crates/chunkenc/tests/interop_test.rs re-encodes the same samples and decodes the Go bytes, and compares.
# Usage: scripts/interop/chunkenc/run.sh (from anywhere inside the repository).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
out="$root/bench/tmp/w0-hist/interop"
rm -rf -- "${out:?}"
GOPROXY=off bash "$here/go.sh" -C "$here/gen" run . "$out"
cd "$root"
~/.cargo/bin/cargo test -p chunkenc --test interop_test -- --nocapture
