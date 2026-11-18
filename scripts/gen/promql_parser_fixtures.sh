#!/bin/sh
# Regenerates tests-data/promql/parser/*.json from the upstream promql/parser test tables
# (see promql_parser_dump_test.go). Needs the Go toolchain from tools/goenv.sh.
set -e
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
. "$HOME/code/prometheus-rs/tools/goenv.sh"
UP=$(readlink -f "$ROOT/upstream")
OVL=$(mktemp -d "$GOTMPDIR/overlay.XXXXXX")
printf '{"Replace":{"%s":"%s"}}\n' "$UP/promql/parser/zz_rust_dump_test.go" "$ROOT/scripts/gen/promql_parser_dump_test.go" > "$OVL/overlay.json"
mkdir -p "$ROOT/tests-data/promql/parser"
cd "$UP"
DUMP_DIR="$ROOT/tests-data/promql/parser" go test -count=1 -overlay "$OVL/overlay.json" -run '^TestDumpRustFixtures$' ./promql/parser/
rm -r "$OVL"
