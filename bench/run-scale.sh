#!/bin/bash
# Workload "scale": 10 avalanche targets (100k series), 15 s scrape, for <minutes>; then the heavy query set;
# then a restart (shutdown time, WAL replay startup time, memory after replay).
# usage: run-scale.sh <label> <prometheus_binary> [minutes=30] [query_rounds=20]
# Writes bench/results/scale-<label>/: samples.csv (epoch,phase,rss_kb,pss_kb,cpu_ticks,procs), queries.json,
# summary.json. Data dir in bench/tmp (deleted at the start of the next run with the same label).
set -u; R=$(cd "$(dirname "$0")/.." && pwd); L=$1; B=$2; M=${3:-30}; Q=${4:-20}
O=$R/bench/results/scale-$L; D=$R/bench/tmp/scale-$L; rm -rf -- "$D"; mkdir -p "$O" "$D"
av=(); for i in $(seq 1 10); do
  setsid nohup "$R/tools/bin/avalanche" --port=$((9000+i)) --gauge-metric-count=30 --counter-metric-count=30 \
    --histogram-metric-count=4 --series-interval=0 --metric-interval=0 --value-interval=15 > "$D/av$i.log" 2>&1 & av+=($!)
done
sleep 2
read pp ms < <("$R/bench/prom.sh" "$B" "$D/data" 9390 "$R/bench/scale.yml")
echo "start pid=$pp startup_ms=$ms"
echo "epoch,phase,rss_kb,pss_kb,cpu_ticks,procs" > "$O/samples.csv"
sample() { echo "$(date +%s),$1,$("$R/bench/tree.sh" $pp | tr ' ' ',')" >> "$O/samples.csv"; }
end=$(( $(date +%s) + M*60 ))
while [ "$(date +%s)" -lt $end ]; do sample scrape; sleep 10; done
sample before_q; c0=$(awk -F, 'END{print $5}' "$O/samples.csv")
node "$R/bench/scaleq.mjs" "$R/bench/scale-queries.txt" http://localhost:9390 "$Q" "$O/queries.json"
sample after_q; c1=$(awk -F, 'END{print $5}' "$O/samples.csv")
series=$(curl -s 'localhost:9390/api/v1/query?query=prometheus_tsdb_head_series' | grep -o '"value":\[[^]]*\]' | grep -o '"[0-9.]*"$' | tr -d '"')
samples=$(curl -s 'localhost:9390/api/v1/query?query=prometheus_tsdb_head_samples_appended_total' | grep -o '"[0-9.e+]*"\]' | head -1 | tr -d '"]')
t0=$(date +%s%N); kill -TERM $pp; while kill -0 $pp 2>/dev/null; do sleep 0.05; done; stop_ms=$(( ($(date +%s%N)-t0)/1000000 ))
read pp ms2 < <("$R/bench/prom.sh" "$B" "$D/data" 9390 "$R/bench/scale.yml")
sleep 30; sample restarted
kill -TERM $pp; while kill -0 $pp 2>/dev/null; do sleep 0.05; done
kill "${av[@]}"; wait 2>/dev/null
du_kb=$(du -sk "$D/data" | cut -f1)
cat > "$O/summary.json" <<J
{"label":"$L","minutes":$M,"startup_ms":$ms,"head_series":"$series","samples_appended":"$samples","query_rounds":$Q,
 "query_cpu_ticks":$((c1-c0)),"shutdown_ms":$stop_ms,"restart_ms":$ms2,"data_dir_kb":$du_kb}
J
cat "$O/summary.json"; echo SCALE_DONE
