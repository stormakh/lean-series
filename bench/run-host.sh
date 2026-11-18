#!/bin/bash
# Workload "host": node_exporter + self scrape; 60 s warm-up, 120 s idle, then <viewers> Node Exporter Full viewers
# for <minutes> (bench/promload.mjs, 10 s refresh), then 120 s after. Samples the Prometheus process every 5 s.
# usage: run-host.sh <label> <prometheus_binary> [viewers=20] [minutes=10]
# Writes bench/results/host-<label>/: samples.csv (epoch,phase,rss_kb,pss_kb,cpu_ticks,procs), req.jsonl, startup.txt
set -u; R=$(cd "$(dirname "$0")/.." && pwd); L=$1; B=$2; V=${3:-20}; M=${4:-10}
O=$R/bench/results/host-$L; D=$R/bench/tmp/host-$L; rm -rf -- "$D"; mkdir -p "$O" "$D"
NE=$R/upstream-bin/node_exporter-1.12.1.linux-amd64/node_exporter
setsid nohup "$NE" --web.listen-address=127.0.0.1:9100 > "$D/ne.log" 2>&1 & ne=$!
read pp ms < <("$R/bench/prom.sh" "$B" "$D/data" 9390 "$R/bench/host.yml")
echo "pid=$pp startup_ms=$ms" | tee "$O/startup.txt"
echo warmup > "$D/phase"
( echo "epoch,phase,rss_kb,pss_kb,cpu_ticks,procs"
  while kill -0 $pp 2>/dev/null; do echo "$(date +%s),$(cat "$D/phase"),$("$R/bench/tree.sh" $pp | tr ' ' ',')"; sleep 5; done ) > "$O/samples.csv" &
smp=$!
sleep 60; echo idle > "$D/phase"; sleep 120
echo load > "$D/phase"
node "$R/bench/promload.mjs" "$R/tests-data/host-requests.jsonl" http://localhost:9390 $V $M "$O/req.jsonl" 2> "$D/load.log"
echo after > "$D/phase"; sleep 120
kill $smp; kill -TERM $pp $ne; wait 2>/dev/null
python3 "$R/bench/summary.py" "$O"; echo HOST_DONE
