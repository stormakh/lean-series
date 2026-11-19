#!/usr/bin/env bash
# Memory (PSS) of the Rust TSDB vs the upstream Go TSDB (embedded, no server) for the same workload:
# <series> series with 4 labels, 120 samples each at 15 s (one full chunk per series), Snappy WAL.
# Go is measured after runtime.GC() (its best case). Usage: bench/tsdb-mem/run.sh [series...] (default 1000 100000)
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="$root/bench/tmp/w2-tsdb/mem"
rm -rf -- "${out:?}"
mkdir -p "$out"
cd "$root"
~/.cargo/bin/cargo build --release -p tsdb --example tsdb_mem 2>/dev/null
(cd "$here" && GOPROXY=off bash "$root/scripts/interop/tsdb-format/go.sh" build -o "$out/gomem" .)
for n in "${@:-1000 100000}"; do
  for x in $n; do
    "$out/gomem" "$out/go$x" "$x" 120
    "$root/target/release/examples/tsdb_mem" "$out/rs$x" "$x" 120
  done
done
