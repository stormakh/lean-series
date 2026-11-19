#!/bin/sh
# Rules + notifier end to end: real Prometheus 3.16.0 and prometheus-rs (examples/rules_e2e) scrape the same static
# target, evaluate scripts/e2e/rules/rules.yml and send alerts to two fake Alertmanagers; compare.py compares the
# alert JSON. Ports: 9900 target, 9901 fake AM (Go), 9902 fake AM (Rust), 9903 Prometheus.
# usage: scripts/e2e/rules/run.sh [seconds]
set -eu
R=$(cd "$(dirname "$0")/../../.." && pwd)
S=$R/scripts/e2e/rules
T=$R/bench/tmp/w3-rules/e2e
SECS=${1:-15}
mkdir -p "$T/www"
rm -f "$T"/*.log "$T"/*.yml "$T"/*.pid
rm -rf "$T/prom-data" "$T/rs-data"
cp "$S/metrics.txt" "$T/www/metrics"
cp "$S/rules.yml" "$T/rules.yml"
for side in go rs; do
  port=9901; [ $side = rs ] && port=9902
  sed -e "s|RULES|$T/rules.yml|" -e "s|AMPORT|$port|" "$S/prometheus.yml.in" > "$T/prometheus-$side.yml"
done

cleanup() {
  for p in "$T"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
  rm -f "$T"/*.pid
}
trap cleanup EXIT

python3 -m http.server 9900 --bind 127.0.0.1 --directory "$T/www" >/dev/null 2>&1 & echo $! > "$T/www.pid"
python3 "$S/fake_am.py" 9901 "$T/am-go.log" & echo $! > "$T/am-go.pid"
python3 "$S/fake_am.py" 9902 "$T/am-rs.log" & echo $! > "$T/am-rs.pid"
sleep 0.5

"$R/upstream-bin/prometheus-3.16.0.linux-amd64/prometheus" --config.file="$T/prometheus-go.yml" \
  --storage.tsdb.path="$T/prom-data" --web.listen-address=127.0.0.1:9903 \
  --web.external-url=http://127.0.0.1:9903/ > "$T/prometheus.log" 2>&1 & echo $! > "$T/prom.pid"

"$R/target/debug/examples/rules_e2e" "$T/prometheus-rs.yml" http://127.0.0.1:9903 "$SECS" "$T/rs-data" > "$T/rs.out" 2> "$T/rs.log"

kill "$(cat "$T/prom.pid")" 2>/dev/null || true
sleep 1
cat "$T/rs.out"
python3 "$S/compare.py" "$T/am-go.log" "$T/am-rs.log"
