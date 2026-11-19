#!/bin/bash
# Writes crates/server/hot-symbols.txt: the functions that the server runs after startup (idle scrape loop and the
# host query load), in address order. The linker puts them together (crates/server/build.rs), so the steady-state
# code fills a few pages instead of being spread over the whole 13 MB of code.
# Traces with Intel PT (perf, user space only). Regenerate after big code changes; missing symbols are ignored.
# usage: bench/hot-code.sh [binary=target/release/prometheus] [port=9700]
set -eu; R=$(cd "$(dirname "$0")/.." && pwd); B=${1:-$R/target/release/prometheus}; P=${2:-9700}; NP=$((P+1))
T=$(mktemp -d "$R/bench/tmp/hot.XXXX")
NE=$R/upstream-bin/node_exporter-1.12.1.linux-amd64/node_exporter
setsid nohup "$NE" --web.listen-address=127.0.0.1:$NP > "$T/ne.log" 2>&1 & ne=$!
sed "s/9390/$P/; s/9100/$NP/" "$R/bench/host.yml" > "$T/p.yml"
read pp _ < <("$R/bench/prom.sh" "$B" "$T/data" $P "$T/p.yml")
trap 'kill $pp $ne 2>/dev/null' EXIT
base=$(awk 'NR==1{split($1,a,"-"); print a[1]}' /proc/$pp/maps)
sleep 3
timeout 40 perf record -q -e intel_pt//u -p $pp -o "$T/idle.data" >/dev/null 2>&1 || true
node "$R/bench/promload.mjs" "$R/tests-data/host-requests.jsonl" http://localhost:$P 20 1 "$T/req.jsonl" 2> "$T/load.log" &
ld=$!
sleep 15
timeout 25 perf record -q -e intel_pt//u -p $pp -o "$T/load.data" >/dev/null 2>&1 || true
wait $ld
for f in idle load; do
  perf script -i "$T/$f.data" --itrace=b -F ip,addr 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9a-f]+$/) s[$i]=1} END{for(k in s) print k}' > "$T/$f.addrs"
done
python3 - "$B" "$base" "$T/idle.addrs" "$T/load.addrs" > "$R/crates/server/hot-symbols.txt" <<'PY'
import sys, subprocess, bisect
binary, base = sys.argv[1], int(sys.argv[2], 16)
syms = []
for l in subprocess.run(['nm', '-S', '--defined-only', binary], capture_output=True, text=True).stdout.splitlines():
    p = l.split()
    if len(p) == 4 and p[2] in 'tTwW':
        syms.append((int(p[0], 16), int(p[1], 16), p[3]))
syms.sort(); starts = [s[0] for s in syms]; hit = {}
for f in sys.argv[3:]:
    for l in open(f):
        a = int(l, 16) - base
        i = bisect.bisect_right(starts, a) - 1
        if i >= 0 and syms[i][0] <= a < syms[i][0] + max(syms[i][1], 1):
            hit[syms[i][2]] = syms[i][0]
print('\n'.join(sorted(hit, key=hit.get)))
PY
echo "hot functions: $(wc -l < "$R/crates/server/hot-symbols.txt"); trace dir $T"
