#!/bin/bash
# Quick host check for optimization work (about 4 min): 20 s warm-up, 40 s idle, 20 viewers for 2 min, 20 s after.
# usage: quick-host.sh <label> [binary=target/release/prometheus] -> bench/results/host-<label>/summary.json
# Not the frozen benchmark: use bench/run-host.sh (10 min load) for reported numbers.
R=$(cd "$(dirname "$0")/.." && pwd)
WARM=20 IDLE=40 AFTER=20 exec "$R/bench/run-host.sh" "$1" "${2:-$R/target/release/prometheus}" 20 2
