#!/bin/bash
# usage: prom.sh <binary> <data_dir> <port> <config.yml> [extra flags...] -> starts it, prints "pid startup_ms" when /-/ready answers 200
set -e; B=$1; D=$2; P=$3; C=$4; shift 4; mkdir -p "$D"
t0=$(date +%s%N)
setsid nohup "$B" --config.file="$C" --storage.tsdb.path="$D" --web.listen-address=127.0.0.1:$P "$@" > "$D/stdout.log" 2>&1 & pid=$!
until curl -sf localhost:$P/-/ready >/dev/null; do kill -0 $pid 2>/dev/null || { echo "died" >&2; exit 1; }; sleep 0.01; done
echo "$pid $(( ($(date +%s%N)-t0)/1000000 ))"
