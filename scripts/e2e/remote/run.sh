#!/usr/bin/env bash
# End-to-end remote write checks against real Prometheus 3.16.0. Ports 9931-9939, data in bench/tmp/w3-remote/e2e.
#
# Part 1 (receiver): Prometheus scrapes itself every second (with native histograms) and remote-writes to the Rust
#   write handler (crates/remote/examples/rw_e2e.rs `receive`), once with remote write 1.0 and once with 2.0.
#   Prometheus is stopped (SIGTERM flushes its queues), restarted on the same data dir without scraping, and its
#   API is asked for every sample; the Rust receiver must have received exactly the same samples.
# Part 2 (sender): the Rust queue manager (rw_e2e `send`) reads a WAL that the Rust TSDB record encoder wrote and
#   sends it with 1.0 and 2.0 to Prometheus started with --web.enable-remote-write-receiver; Prometheus' API must
#   return the same samples.
# Usage: scripts/e2e/remote/run.sh [part1|part2] (default both). Every process started here is stopped at the end.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
prom="${PROMETHEUS_BIN:-$(cd "$(dirname "$0")/../../.." && pwd)/upstream-bin/prometheus-3.16.0.linux-amd64/prometheus}"
tmp="$root/bench/tmp/w3-remote/e2e"
parts="${1:-all}"
pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

wait_http() {
  for _ in $(seq 1 100); do curl -sf "$1" >/dev/null 2>&1 && return 0; sleep 0.2; done
  echo "timeout waiting for $1"; return 1
}

stop_pid() {
  kill -TERM "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

rm -rf -- "${tmp:?}"
mkdir -p "$tmp"
cd "$root"
~/.cargo/bin/cargo build -q -p remote --example rw_e2e
bin="$root/target/debug/examples/rw_e2e"
rc=0

if [[ "$parts" == all || "$parts" == part1 ]]; then
  out="$tmp/part1"
  mkdir -p "$out/data"
  cat > "$out/prometheus.yml" <<EOF
global:
  scrape_interval: 1s
  scrape_native_histograms: true
scrape_configs:
  - job_name: self
    static_configs:
      - targets: ['127.0.0.1:9933']
remote_write:
  - url: http://127.0.0.1:9931/v1
    name: rw1
    send_exemplars: true
    send_native_histograms: true
    queue_config: {batch_send_deadline: 1s}
  - url: http://127.0.0.1:9931/v2
    name: rw2
    protobuf_message: io.prometheus.write.v2.Request
    send_exemplars: true
    send_native_histograms: true
    queue_config: {batch_send_deadline: 1s}
EOF
  "$bin" receive 127.0.0.1:9931 "$out" > "$out/receiver.log" 2>&1 &
  rpid=$!; pids+=("$rpid")
  "$prom" --config.file="$out/prometheus.yml" --storage.tsdb.path="$out/data" \
    --web.listen-address=127.0.0.1:9933 > "$out/prometheus.log" 2>&1 &
  ppid=$!; pids+=("$ppid")
  wait_http http://127.0.0.1:9933/-/ready
  sleep 20
  stop_pid "$ppid"
  touch "$out/stop"
  wait "$rpid" || true
  cat "$out/receiver.log"
  grep -iE "level=(error|warn)" "$out/prometheus.log" | grep -i remote | head -5 || true
  # Restart Prometheus on the same data (no scraping, no remote write) and read everything back.
  printf 'global:\n  scrape_interval: 1h\n' > "$out/readonly.yml"
  "$prom" --config.file="$out/readonly.yml" --storage.tsdb.path="$out/data" \
    --web.listen-address=127.0.0.1:9933 > "$out/prometheus2.log" 2>&1 &
  ppid=$!; pids+=("$ppid")
  wait_http http://127.0.0.1:9933/-/ready
  curl -sf -G http://127.0.0.1:9933/api/v1/query --data-urlencode 'query={__name__=~".+"}[1h]' > "$out/api.json"
  stop_pid "$ppid"
  python3 "$here/compare.py" "$out/v1.jsonl" "$out/api.json" || rc=1
  python3 "$here/compare.py" "$out/v2.jsonl" "$out/api.json" || rc=1
fi

if [[ "$parts" == all || "$parts" == part2 ]]; then
  out="$tmp/part2"
  mkdir -p "$out/data"
  printf 'global:\n  scrape_interval: 1h\n' > "$out/prometheus.yml"
  for proto in v1 v2; do
    rm -rf -- "${out:?}/data"
    "$prom" --config.file="$out/prometheus.yml" --storage.tsdb.path="$out/data" --web.enable-remote-write-receiver \
      --web.listen-address=127.0.0.1:9934 > "$out/prometheus-$proto.log" 2>&1 &
    ppid=$!; pids+=("$ppid")
    wait_http http://127.0.0.1:9934/-/ready
    "$bin" send "$proto" http://127.0.0.1:9934/api/v1/write "$out/wal-$proto" "$out/sent-$proto.jsonl" \
      > "$out/sender-$proto.log" 2>&1 || { cat "$out/sender-$proto.log"; rc=1; }
    cat "$out/sender-$proto.log"
    curl -sf -G http://127.0.0.1:9934/api/v1/query --data-urlencode 'query={__name__=~".+"}[1h]' > "$out/api-$proto.json"
    stop_pid "$ppid"
    python3 "$here/compare.py" "$out/sent-$proto.jsonl" "$out/api-$proto.json" || rc=1
  done
fi

if ss -ltn | grep -qE '127.0.0.1:99(3[1-9])\b'; then echo "ports still open:"; ss -ltnp | grep -E '127.0.0.1:993'; fi
[[ $rc == 0 ]] && echo "E2E OK" || echo "E2E FAILED"
exit $rc
