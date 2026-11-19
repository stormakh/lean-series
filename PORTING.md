# Porting contract

This repo is a Rust rewrite of **Prometheus v3.16.0** (`upstream/`, commit e2e28ade, read-only, not in git).
Reference binaries: `upstream-bin/prometheus-3.16.0.linux-amd64/` (prometheus, promtool) and node_exporter 1.12.1.
Go toolchain for running upstream code and tests: `. tools/goenv.sh` (Go 1.26.7, caches inside `tools/`).

Every agent reads this file first and follows it exactly.

## Goals (frozen before the Rust code; see `bench/results/*-upstream`)

- Behavior: the upstream tests in `tests-data/ledger.json` pass in Rust (`python3 scripts/score.py` prints SCORE),
  including every PromQL `eval` command of `upstream/promql/promqltest/testdata/*.test`.
- On-disk compatibility in both directions: real Prometheus 3.16.0 opens a data dir written by prometheus-rs
  (blocks, WAL, `chunks_head`, WBL) and the reverse, with byte-identical query answers.
- HTTP API: same paths, parameters, status codes, JSON shape, key order and number formatting as upstream.
- At least 20X less memory (PSS) and 30X less CPU than Prometheus 3.16.0 on the frozen workloads
  (`bench/run-host.sh`, `bench/run-scale.sh`). Where a goal is not reachable (for example memory at 100k series,
  where sample data dominates), the report states the measured ratio and the reason.

## Upstream defaults that matter (from cmd/prometheus/main.go, not tsdb.DefaultOptions)

- Labels: the `stringlabels` form (one string per label set). Index: v2 written; v1, v2, v3 read.
- WAL: compression on, Snappy per record (zstd and uncompressed records must still be read); 128 MiB segments,
  32 KiB pages, 7-byte fragment headers. Default float sample records are type 2 (V1); V2 records only with the
  start-timestamp feature. `meta.json` version is 1.
- Memory snapshot on shutdown is off by default. OOO window is 0 by default, but an existing WBL is still replayed.
- Scrape protocols (default list) include OpenMetrics 0.0.1 and 1.0.0, Prometheus text 0.0.4 and 1.0.0, protobuf.
- Regex: Prometheus uses Grafana's fork of Go regexp, anchored as `^(?s:...)$`. Go's `\d \w \s \b` are ASCII-only;
  Rust's are Unicode by default. Translate patterns so that semantics match Go, including `$1`/`${name}`
  expansion in relabeling.
- PromQL test fixtures compare floats with relative error 1e-6 (as upstream); API JSON must match exactly
  (`util/jsonutil/marshal.go`: exponent form below 1e-6 and at 1e21, `"NaN"`, `"+Inf"`, `"-Inf"`, stale NaN bits).
- The UI is `web/ui/mantine-ui` (not the old `react-app`); the server replaces placeholders in `index.html`
  at runtime (`web/web.go`).

## Scope

Close to full Prometheus server: config (YAML, reload), scrape (all exposition formats), service discovery
(static, file, http, dns, kubernetes, consul, and the cloud providers), relabeling, TSDB (head, WAL, WBL, OOO,
compaction, retention, snapshots, exemplars, native histograms), PromQL (everything in promqltest, including
experimental functions behind the same feature flags), HTTP API v1, federation, remote read/write (1.0 and 2.0),
recording and alerting rules, notifier (Alertmanager), the web UI (the upstream React build, served unchanged),
self-instrumentation (`/metrics` with upstream metric names), agent mode, and promtool subcommands that the ledger
covers. Not in scope unless the ledger needs it: tracing export (OTLP), the OTLP receiver.

## Layout

```
crates/json      JSON value with ordered keys, parser, Go-compatible writer (from Lean Sight)   (lust)
crates/crypto    sha256, hmac, md5, sha1, crc32, base64, random (from Lean Sight)                 (lust)
crates/http      blocking HTTP/1.1 server, router, client, gzip (from Lean Sight)                  (lust)
crates/model     labels, matchers, fast regex matcher (Go RE2 compat), histograms, exemplar, metadata, value,
                 timestamp, relabel
crates/chunkenc  chunk encodings 1-6 (XOR, histogram, float histogram, XOR2, start-timestamp variants), iterators
crates/storage   storage interfaces (Queryable, Querier, SeriesSet, Series, iterators, Appender), merge/fanout,
                 series sets. PromQL and TSDB both depend on it; it depends only on model and chunkenc.
crates/textparse exposition parsers: Prometheus text 0.0.4, OpenMetrics 1.0, protobuf; NHCB conversion
crates/promql    lexer, parser, AST printer, engine, functions, promqltest runner
crates/tsdb-format  chunks, index, record, wlog, tombstones, encoding, fileutil (byte formats)
crates/tsdb      head, appenders, isolation, OOO, querier, block, compaction, db, retention, snapshot, exemplars
crates/remote    remote read/write codecs (prompb, protobuf + snappy), queue manager, WAL watcher, write handler
crates/config, scrape, discovery, rules, notifier, web, util, server (the `prometheus` binary), promtool
```

Crate names in `tests-data/ledger.json` (`crate` field) say where each upstream test goes.

## Tests and the ledger

- `tests-data/ledger.json` has one entry per upstream top-level Go test that ran (`upstream`: pass/fail/skip),
  plus one per promqltest `eval` command. Each entry names `crate`, `file` (the Go test file stem) and `rust`
  (the Go name in snake_case). The Rust test is `crates/<crate>/tests/<file>.rs` with `#[test] fn <rust>()`.
  Example: `TestLabels_MatchLabels` in `model/labels/labels_test.go` -> `crates/model/tests/labels_test.rs`,
  `fn test_labels_match_labels()`.
- `python3 scripts/score.py [crate...]` runs `cargo test` and prints `SCORE passed/total` per crate.
- Port every assertion with the same inputs and the same expected values. **Never weaken a test to make it pass**:
  no removed assertions, no looser values, no "does not panic" checks, no special cases in the implementation.
- Port behavior, not tests: port whole modules line by line in behavior, including odd cases.
- A test of a Go-only fact (goroutine leaks, Go benchmarks, reflection, Go GC) gets the closest real Rust check of
  the same behavior with a `// adapted: <why>` comment, or is listed in `tests-data/not-ported.md` with the reason.
  An honest gap beats a fake pass. Table-driven tests: port the whole table.
- Upstream test data files are read from `upstream/...` by relative path from the workspace root
  (`env!("CARGO_MANIFEST_DIR")/../../upstream/...`). Do not copy them.

## Rules

- Keep upstream names (in snake_case), error messages, status codes and JSON field names, because users, tests and
  the UI read them. Error text must be the same string as Go's, including wrapping (`"...: %w"`).
- Numbers: floats print as Go `strconv.FormatFloat(f, 'f', -1, 64)` in the API and as Go `%g`/`'g'` where upstream
  uses it; NaN, +Inf, -Inf as upstream. Integers and timestamps as upstream (ms int64; API time as seconds with
  up to 3 decimals).
- Regex: Go RE2 syntax with Prometheus anchoring (`^(?s:...)$`). Use the `regex` crate; any syntax difference to
  Go's regexp is a bug to handle explicitly, with a test.
- No async runtime. Blocking I/O, plain threads, `std::sync`. No timers that wake when idle.
- Memory and CPU are the goal: no per-sample allocation on hot paths (scrape, append, query), no `format!` on hot
  paths, no needless clones. Prefer compact forms (one byte buffer for labels, `u32` references, delta encoding).
- `unsafe` only in `tsdb`/`tsdb-format` (mmap through `memmap2`) and in `util::runtime` / `server` for libc system
  calls that std does not offer (getrlimit, uname, statfs, malloc tuning), only where needed, with a `// SAFETY:` comment.
- Allowed crates: `regex`, `memmap2`, `snap`, `yaml-rust2`, `rustls` + `rustls-native-certs` + `webpki-roots`,
  `flate2`, `zstd` (only for zstd WAL/remote compression). Any other crate needs a reason in your report.
- Lust: crates marked (lust) must pass `LUST_DAEMON_IDLE=600 lust check <crate>/src/lib.rs --manifest-path
  <crate>/Cargo.toml --lib` (PATH=$HOME/.cargo/bin:$PATH). New pure crates (model, textparse, promql, tsdb-format)
  should pass Lust too where it accepts the code; do not rewrite code only to fit Lust; write down which crates pass.
- Cargo: use `~/.cargo/bin/cargo` (the mise shim fails). Do not run `cargo fmt` on files you do not own.
- Keep each change focused. Commit in your worktree with clear messages; the lead merges.
- Processes: keep pid files and temporary data in your own folder under `bench/tmp/<agent>/`. Stop every process
  you start. Never kill processes you did not start. Ports: each agent uses only its assigned port range.

## Reporting back

What you ported (modules, line counts), the score per crate, every ledger test left out with the reason, every
behavior that differs from upstream, every new crate dependency, and which crates pass Lust.
