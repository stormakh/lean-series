# prometheus-rs plan (draft for review)

Goal from the owner: rewrite Prometheus v3.16.0 (upstream/ at e2e28ade) in Rust, "close to full Prometheus",
reading and writing the real Prometheus on-disk formats so data moves in both directions. Same goals as the
Grafana port (Lean Sight): at least 20X less memory and 30X less CPU than upstream, measured on frozen workloads.

## Phase 0 (now)
- Ledger: `go test -json ./...` of upstream -> tests-data/ledger.json (package, test, subtest). Scorer maps Rust
  `#[test] fn <name>` in `crates/<crate>/tests/<go_file>.rs` to ledger entries and prints SCORE.
- promqltest: all 23 upstream `promql/promqltest/testdata/*.test` files run unchanged through a Rust runner.
- Frozen baseline on real Prometheus 3.16.0 (whole process PSS, CPU ticks):
  - W1 host: node_exporter + self scrape, 20 viewers of Node Exporter Full replayed as the exact Prometheus API
    requests Lean Sight sends (captured with a proxy), 10 s refresh. Idle memory, load memory, CPU, latency.
  - W2 scale: 10 avalanche targets x 10k series (100k series), 15 s scrape; memory after 30 min and 2 h,
    CPU per scraped sample; query CPU on a fixed heavy query set; WAL replay time on restart.
  - W3 formats: Prometheus reads a data dir written by prometheus-rs (blocks, WAL, chunks_head) and the reverse,
    with byte-identical query answers on a query corpus.
- Differential oracle: same data loaded into both (backfilled block or same WAL), same query corpus, JSON compare.

## Architecture
- No async runtime. Blocking threads: thread-per-connection HTTP server (from Lean Sight), a scrape scheduler with
  a fixed worker pool, one compaction thread, WAL writer.
- Crates (lust-checked where possible: no unsafe, no proc macros):
  model (labels as one compact byte string, histograms, relabel, timestamps)  [lust]
  textparse (text 0.0.4, OpenMetrics 1.0, protobuf exposition)               [lust]
  promql (lexer, recursive-descent parser for the yacc grammar, printer, engine, functions) [lust]
  tsdb-format (chunkenc XOR/histogram/float-histogram, index v2, chunks, record, wlog, tombstones, meta.json) [lust]
  tsdb (head, appender, isolation, OOO, querier, compaction, retention, snapshot; mmap via memmap2) [cargo]
  storage (fanout, merge, remote read/write: protobuf + snappy, queue manager, WAL watcher) [cargo]
  config (YAML via yaml-rust2, strict fields, defaults, secrets) ; net (HTTP client, rustls TLS, basic/bearer/
  oauth2/sigv4 auth, proxy) ; scrape ; discovery (manager + providers) ; rules ; notifier ; web (API v1, federation,
  lifecycle, the upstream React UI built from web/ui and served unchanged) ; server binary with kingpin-compatible flags.
- Crates allowed: regex (RE2 semantics, fully anchored like Go), memmap2, rustls (+ native certs), snap, yaml-rust2,
  zstd only if WAL zstd compression is needed. Everything else written in house.

## Agent waves (each agent in its own git worktree; lead merges and runs the scorer)
1. Lead: workspace, model, json/http/crypto from Lean Sight, scorer, promqltest runner skeleton.
2. Wave 1 (parallel): A promql parser+engine+functions; B tsdb-format (byte-compatible with real Prometheus data
   dirs); C textparse + relabel + config; D net (TLS client, auth), protobuf + snappy, remote codecs.
3. Wave 2: E tsdb head/db/compaction/querier; F scrape; G discovery core + static/file/http/dns/kubernetes;
   H web API v1 + UI + federation + /metrics self-instrumentation.
4. Wave 3: rules + notifier; other SD providers; remote write queue manager; promtool subset.
5. Then: end-to-end side-by-side runs, optimization one measured change per commit, Sol review, report.

## Known risks
- At scale most memory is sample and series data, so 20X less memory at 100k series is likely impossible; the
  report will state the measured ratio at both scales.
- Exact float formatting and PromQL corner cases (lookback, staleness markers, native histogram math, @ modifier,
  subqueries, `info()`, delayed name removal) must match bit for bit.
- Cloud SD providers need real accounts to test; they get only unit tests ported from upstream.
