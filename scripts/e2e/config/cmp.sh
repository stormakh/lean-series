#!/bin/bash
# Compares Go's and Rust's view of the upstream config testdata files: load result (String() or error text),
# or the scrape configs. Usage: scripts/e2e/config/cmp.sh [load|scrape] [files...]
# Without files: every testdata/*.yml that uses only the SD providers ported so far (file, http, static).
set -e
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
T=$ROOT/bench/tmp/w1-config
mkdir -p "$T"
MODE=${1:-load}
shift || true
(cd "$ROOT/scripts/e2e/config/gocfg" && bash ../../net/go.sh build -o "$T/gocfg" .)
~/.cargo/bin/cargo build -q --manifest-path "$ROOT/Cargo.toml" -p config --example cfgdump
cd "$ROOT/upstream/config"
if [ $# -eq 0 ]; then
  # files whose SD keys are all ported
  set -- $(for f in testdata/*.yml testdata/*/*.yml; do
    keys=$(grep -o '[a-z_]*_sd_configs' "$f" | sort -u | grep -v -x -e file_sd_configs -e http_sd_configs || true)
    [ -z "$keys" ] && echo "$f"
  done)
fi
"$T/gocfg" "$MODE" "$@" > "$T/go_$MODE.txt" 2>&1 || true
"$ROOT/target/debug/examples/cfgdump" "$MODE" "$@" > "$T/rs_$MODE.txt" 2>&1 || true
echo "files: $#"
if diff "$T/go_$MODE.txt" "$T/rs_$MODE.txt" > "$T/diff_$MODE.txt"; then
  echo "SAME"
else
  echo "DIFFERENT: $T/diff_$MODE.txt ($(grep -c '^[<>]' "$T/diff_$MODE.txt") lines)"
fi
