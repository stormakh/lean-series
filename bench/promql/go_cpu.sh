#!/bin/bash
# CPU time (user+sys, all threads incl. GC) per query of upstream BenchmarkRangeQuery cases.
# Each case runs in its own process with -benchtime=${N}x; the setup cost is removed by subtracting a run of a
# trivial case. Output: "<case> <cpu ms per query>".
set -e
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
. /home/kamrot/code/prometheus-rs/tools/goenv.sh
OUT=${OUT:-$ROOT/bench/tmp/w2-promql}
mkdir -p "$OUT"
BIN=$OUT/promql.test
if [ ! -x "$BIN" ]; then
  (cd "$(readlink -f "$ROOT/upstream")" && go test -c -o "$BIN" ./promql/)
fi
N=${N:-10}
cpu() {
  local TIMEFORMAT='%U %S'
  { time "$BIN" -test.run XXX -test.bench "$1" -test.benchtime ${N}x > /dev/null 2>&1 ; } 2> "$OUT/time.txt"
  awk '{print $1+$2}' "$OUT/time.txt"
}
BASE=$(cpu 'BenchmarkRangeQuery/expr=a_one,steps=1$')
echo "baseline $BASE s"
while read -r CASE; do
  [ -z "$CASE" ] && continue
  T=$(cpu "BenchmarkRangeQuery/$CASE\$")
  echo "$CASE $(awk -v t="$T" -v b="$BASE" -v n="$N" 'BEGIN{printf "%.2f", (t-b)*1000/(n+1)}')"
done <<'EOF'
expr=a_hundred,steps=1000
expr=rate\(a_hundred\[1m\]\),steps=1000
expr=rate\(a_hundred\[1d\]\),steps=1000
expr=a_hundred_-_b_hundred,steps=1000
expr=abs\(a_hundred\),steps=1000
expr=sum\(a_hundred\),steps=1000
expr=sum_without_\(l\)\(h_hundred\),steps=1000
expr=sum_without_\(l\)\(rate\(a_hundred\[1m\]\)\),steps=1000
expr=histogram_quantile\(0.9,_rate\(h_hundred\[5m\]\)\),steps=1000
expr=topk\(5,_a_hundred\),steps=1000
expr=rate\(a_hundred\[1m\]\)_\+_rate\(b_hundred\[1m\]\),steps=1000
expr=timestamp\(a_hundred\),steps=1000
EOF
