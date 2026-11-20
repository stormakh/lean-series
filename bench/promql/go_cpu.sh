#!/bin/bash
# Go CPU time per query of upstream BenchmarkRangeQuery cases: the benchmark runs with GOMAXPROCS=1
# (-test.cpu 1), so the garbage collector runs on the same CPU and ns/op is the CPU time per query, GC included.
# Usage: bench/promql/go_cpu.sh [bench regexp] (default: the hundred-series cases with 1000 steps).
set -e
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
. "$(cd "$(dirname "$0")/../.." && pwd)/tools/goenv.sh"
OUT=${OUT:-$ROOT/bench/tmp/w2-promql}
mkdir -p "$OUT"
BIN=$OUT/promql.test
if [ ! -x "$BIN" ]; then
  (cd "$(readlink -f "$ROOT/upstream")" && go test -c -o "$BIN" ./promql/)
fi
mkdir -p "$OUT/gotmp"
export TMPDIR="$OUT/gotmp"
cd "$(readlink -f "$ROOT/upstream")/promql"
"$BIN" -test.run XXX -test.cpu 1 -test.benchtime "${N:-5}x" \
  -test.bench "${1:-BenchmarkRangeQuery/expr=.*hundred.*,steps=1000\$}" | grep ns/op
