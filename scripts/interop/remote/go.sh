#!/usr/bin/env bash
# Runs the project Go toolchain (tools/go) with caches inside the project. Usage: go.sh <go args...>
set -euo pipefail
R="${PROMETHEUS_RS_MAIN:-$HOME/code/prometheus-rs}"
export PATH="$R/tools/go/bin:$PATH" GOPATH="$R/tools/gopath" GOCACHE="$R/tools/gocache" GOTMPDIR="$R/tools/gotmp" GOTOOLCHAIN=local
mkdir -p "$GOTMPDIR"
exec go "$@"
