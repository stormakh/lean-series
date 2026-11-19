#!/bin/bash
# Builds the upstream web UI (web/ui/mantine-ui) as upstream's `build_ui.sh --mantine-ui` does, and copies the
# result to crates/web/ui/static/mantine-ui (the assets the web handler serves).
#
# The upstream tree is read-only, so the sources are copied to a work folder first. node_modules, the pnpm store,
# the npm cache and temporary files stay in that work folder (default bench/tmp/ui-build, gitignored).
#
# Usage: scripts/build-ui.sh [work-dir]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
work=${1:-$root/bench/tmp/ui-build}
out=$root/crates/web/ui/static/mantine-ui

mkdir -p "$work/tmp" "$work/npm-cache" "$work/pnpm-store"
export TMPDIR=$work/tmp npm_config_cache=$work/npm-cache npm_config_store_dir=$work/pnpm-store

# Copy the workspace (without the old react-app and without build output).
mkdir -p "$work/src"
rsync -a --delete --exclude node_modules --exclude dist --exclude react-app --exclude static \
  "$root/upstream/web/ui/" "$work/src/"

cd "$work/src"
pnpm install --frozen-lockfile
for module in lezer-promql codemirror-promql; do
  pnpm --filter "@prometheus-io/${module}" run build
done
pnpm --filter @prometheus-io/mantine-ui run build

rm -rf "$out"
mkdir -p "$(dirname "$out")"
cp -r mantine-ui/dist "$out"
# Upstream's asset filesystem leaves out source maps (ui.go: "map.js").
find "$out" -name '*map.js' -delete
du -sh "$out"
