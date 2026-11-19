#!/usr/bin/env bash
# Data dir compatibility of crates/tsdb with real Prometheus 3.16.0, in both directions.
#
# A. Rust writes, Prometheus reads:
#    1. `tsdb_interop write` (crates/tsdb/examples) writes a data dir with the Rust DB: 5 h of float, histogram
#       and float histogram series with stale markers, so that at least one block is compacted, deletes some
#       data (tombstones in a block and in the head), and closes (WAL, chunks_head and the block stay).
#    2. The Rust DB dumps the dir in `promtool tsdb dump` form; real promtool dumps a copy; the dumps must be equal.
#    3. Real Prometheus opens another copy and answers a range query over all series; every float sample must be
#       bit-identical, every histogram must have the same count and sum.
# B. Prometheus writes, Rust reads:
#    1. promtool creates blocks from OpenMetrics; Prometheus scrapes itself (native histograms on) and
#       node_exporter for 40 s into the same dir; the admin API deletes data (tombstones in a block and the
#       head); Prometheus stops (WAL and chunks_head stay).
#    2. promtool and the Rust DB dump copies of the dir; the dumps must be equal.
# Ports 9700 (Prometheus) and 9701 (node_exporter). Data in bench/tmp/w2-tsdb/interop.
# Usage: scripts/interop/tsdb/run.sh (from anywhere inside the repository).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
bin="$root/upstream-bin"
[ -d "$bin" ] || bin="$HOME/code/prometheus-rs/upstream-bin"
prom="$bin/prometheus-3.16.0.linux-amd64"
nodeexp="$bin/node_exporter-1.12.1.linux-amd64/node_exporter"
out="$root/bench/tmp/w2-tsdb/interop"
rm -rf -- "${out:?}"
mkdir -p "$out"
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

cd "$root"
# ONLY_PROM=1 runs only the Prometheus side of B (to check the harness without the Rust DB).
ONLY_PROM="${ONLY_PROM:-0}"
if [ "$ONLY_PROM" != 1 ]; then
  ~/.cargo/bin/cargo build --release -p tsdb --example tsdb_interop
fi
rs="$root/target/release/examples/tsdb_interop"

wait_up() { for _ in $(seq 1 100); do curl -sf "$1" >/dev/null && return 0; sleep 0.2; done; echo "not up: $1"; return 1; }
stop() { kill -TERM "$1"; wait "$1" || true; }
start_prom() { # <data dir> <config> <log>
  "$prom/prometheus" --config.file="$2" --storage.tsdb.path="$1" --web.listen-address=127.0.0.1:9700 \
    --web.enable-admin-api --storage.tsdb.retention.time=10y >"$3" 2>&1 &
  PROM_PID=$!
  pids+=("$PROM_PID")
  wait_up http://127.0.0.1:9700/-/ready
}

if [ "$ONLY_PROM" != 1 ]; then
echo "== A. Rust writes, Prometheus reads"
A="$out/from-rust"
"$rs" write "$A"
ls "$A" "$A/wal" "$A/chunks_head" >"$out/a-layout.txt"
blocks=$(find "$A" -maxdepth 2 -name meta.json | wc -l)
tomb=$(find "$A" -mindepth 2 -maxdepth 2 -name tombstones -size +5c | wc -l)
heads=$(find "$A/chunks_head" -type f -size +8c | wc -l)
echo "blocks=$blocks blocks_with_tombstones=$tomb chunks_head_files=$heads wal_segments=$(ls "$A/wal" | grep -c '^[0-9]')"
[ "$blocks" -ge 1 ] && [ "$tomb" -ge 1 ] && [ "$heads" -ge 1 ]
cp -a "$A" "$out/a-rust-copy"; cp -a "$A" "$out/a-promtool-copy"; cp -a "$A" "$out/a-prom-copy"
"$rs" dump "$out/a-rust-copy" >"$out/a-rust.txt"
"$prom/promtool" tsdb dump "$out/a-promtool-copy" >"$out/a-promtool.txt"
python3 "$here/check.py" same "$out/a-rust.txt" "$out/a-promtool.txt"
printf 'global: {scrape_interval: 1h}\n' >"$out/empty.yml"
start_prom "$out/a-prom-copy" "$out/empty.yml" "$out/a-prom.log"
python3 "$here/check.py" api "$out/a-rust.txt" http://127.0.0.1:9700
stop "$PROM_PID"
fi

echo "== B. Prometheus writes, Rust reads"
B="$out/from-prom"
now_ms=$(($(date +%s) * 1000))
python3 "$here/check.py" gen-om "$out/om.txt" $((now_ms - 3 * 3600 * 1000))
"$prom/promtool" tsdb create-blocks-from openmetrics "$out/om.txt" "$B" >"$out/b-promtool-create.log"
"$nodeexp" --web.listen-address=127.0.0.1:9701 >"$out/node.log" 2>&1 &
NODE_PID=$!
pids+=("$NODE_PID")
wait_up http://127.0.0.1:9701/metrics
start_prom "$B" "$here/prom.yml" "$out/b-prom.log"
sleep 40
api=http://127.0.0.1:9700/api/v1/admin/tsdb/delete_series
end_s=$(date +%s)
curl -sf -X POST "$api" --data-urlencode 'match[]=interop_gauge{kind="tiny"}' \
  --data-urlencode "start=$((now_ms / 1000 - 5 * 3600))" --data-urlencode "end=$((now_ms / 1000 - 4 * 3600))"
curl -sf -X POST "$api" --data-urlencode 'match[]=node_cpu_seconds_total' \
  --data-urlencode "start=$((end_s - 20))" --data-urlencode "end=$((end_s - 10))"
stop "$PROM_PID"
stop "$NODE_PID"
cp -a "$B" "$out/b-rust-copy"; cp -a "$B" "$out/b-promtool-copy"
btomb=$(find "$B" -mindepth 2 -maxdepth 2 -name tombstones -size +5c | wc -l)
bheads=$(find "$B/chunks_head" -type f -size +8c | wc -l)
echo "blocks=$(find "$B" -maxdepth 2 -name meta.json | wc -l) blocks_with_tombstones=$btomb chunks_head_files=$bheads"
[ "$btomb" -ge 1 ] && [ "$bheads" -ge 1 ]
"$prom/promtool" tsdb dump "$out/b-promtool-copy" >"$out/b-promtool.txt"
if [ "$ONLY_PROM" = 1 ]; then cp "$out/b-promtool.txt" "$out/b-rust.txt"; else "$rs" dump "$out/b-rust-copy" >"$out/b-rust.txt"; fi
python3 "$here/check.py" same "$out/b-promtool.txt" "$out/b-rust.txt"
echo "interop tsdb: OK"
