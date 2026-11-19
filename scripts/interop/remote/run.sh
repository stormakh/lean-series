#!/usr/bin/env bash
# Byte compatibility of the Rust remote read/write protobuf and Snappy code with upstream Go.
# 1. The Go generator (gen/) encodes fixed WriteRequest v1, write v2 Request (Marshal and OptimizedMarshal),
#    ReadRequest, ReadResponse and ChunkedReadResponse with upstream code (gogo protobuf, golang/snappy) and
#    writes them to bench/tmp/w3-remote/interop/.
# 2. crates/remote/tests/interop_test.rs builds the same messages, asserts identical bytes, decodes Go's bytes,
#    and writes rust_*.bin files.
# 3. The Go generator runs again and decodes the Rust files (and re-encodes them to Go's bytes).
# Usage: scripts/interop/remote/run.sh (from anywhere inside the repository).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
out="$root/bench/tmp/w3-remote/interop"
rm -rf -- "${out:?}"
GOPROXY=off bash "$here/go.sh" -C "$here/gen" run . "$out"
cd "$root"
~/.cargo/bin/cargo test -p remote --test interop_test -- --nocapture
GOPROXY=off bash "$here/go.sh" -C "$here/gen" run . "$out"
