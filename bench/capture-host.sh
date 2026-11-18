#!/bin/bash
# Records the Prometheus API requests that Lean Sight sends for the Node Exporter Full dashboard (1 viewer, 70 s).
# usage: capture-host.sh <lean_sight_data_dir> <users.json> <out.jsonl>
set -u; R=$(cd "$(dirname "$0")/.." && pwd); LD=$1; U=$2; OUT=$3; T=$(mktemp -d "$R/bench/tmp/cap.XXXX")
UB=$R/upstream-bin; NE=$UB/node_exporter-1.12.1.linux-amd64/node_exporter; PB=$UB/prometheus-3.16.0.linux-amd64/prometheus
setsid nohup "$NE" --web.listen-address=127.0.0.1:9100 > "$T/ne.log" 2>&1 & ne=$!
sed 's/9390/9391/' "$R/bench/host.yml" > "$T/p.yml"
read pp _ < <("$R/bench/prom.sh" "$PB" "$T/data" 9391 "$T/p.yml")
node "$R/scripts/record-proxy.mjs" "$OUT" 9390 9391 > "$T/proxy.log" 2>&1 & px=$!
cp -r "$LD" "$T/ls"; read lp _ < <(~/code/grafana-rs/bench/grafana-rs.sh "$T/ls" 3400)
sleep 20   # some data first
node ~/code/grafana-rs/bench/realload.mjs /tmp/claude-1000/-home-kamrot/8a06f3ea-ae5f-4f59-ba85-a3c2acd62392/scratchpad/real/rec-rs.jsonl http://localhost:3400 "$U" 1 1.2 "$T/req.jsonl" 2> "$T/load.log"
kill $lp $px $pp $ne; wait 2>/dev/null; echo "captured $(wc -l < "$OUT") requests; tmp $T"
