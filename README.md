# Lean Series

A lean, compatible rewrite of the Prometheus 3.16.0 server in Rust. It reads and writes the real Prometheus on-disk format, answers the same HTTP API with the same JSON, and serves the unchanged Prometheus web UI. The binary is called `prometheus` and takes the same flags, so it is a drop-in replacement.

Sister project: [Lean Sight](https://github.com/stormakh/lean-sight), the same approach for Grafana.

## Results

Measured on one Linux machine (12 threads), side by side with the official Prometheus 3.16.0 binary. Memory is PSS. The frozen upstream baselines and every run are in `bench/results/`.

**Dashboard workload** (`bench/run-host.sh`): Prometheus scrapes itself and node_exporter; 20 viewers of the Node Exporter Full dashboard query it every 10 s for 10 minutes (33,600 requests).

| | Prometheus 3.16.0 | Lean Series | Better by |
|---|---|---|---|
| Memory, idle | 108.1 MB | 5.9 MB | 18.4X |
| Memory, average under load | 121.2 MB | 12.0 MB | 10.1X |
| CPU per API request | 1.686 ms | 0.217 ms | 7.8X |
| CPU, idle | 0.21% of a core | 0.05% | 4X |
| Query latency p50 / p99 | 1.46 / 5.52 ms | 0.48 / 1.38 ms | 3–4X |
| Failed requests | 0 | 0 | |

**Scale workload** (`bench/run-scale.sh`): 10 avalanche targets, 100,000 series, 15 s scrape interval, 30 minutes, then 20 rounds of heavy queries.

| | Prometheus 3.16.0 | Lean Series | Better by |
|---|---|---|---|
| Memory at the end of scraping | 396 MB | 148 MB | 2.7X |
| Memory after the queries | 578 MB | 175 MB | 3.3X |
| Scrape CPU, 30 minutes | 29.0 s | 11.3 s | 2.6X |
| Heavy query CPU, 20 rounds | 12.5 s | 9.6 s | 1.3X |
| Data directory size | 86 MB | 68 MB | 1.3X |

At scale, the stored sample data uses most of the memory in both servers, so the gain is smaller than on the dashboard workload.

Note: the optimization work was done in three parallel branches (memory, query CPU, scale). The numbers above come from each branch's own full run: memory rows of the dashboard table from the memory branch (`bench/results/host-o1`), CPU and latency rows from the query branch (`host-o2b`), the scale table from `scale-o3`. All three branches are merged; a single full run of the merged binary has not been done yet.

## Compatibility

- **Upstream tests:** 4,588 of 4,596 upstream Go tests ported and passing (`tests-data/score.txt`), including every PromQL `eval` of the upstream promqltest files. The 8 gaps and their reasons are in `tests-data/not-ported.md`.
- **On-disk format:** blocks, index, chunks, WAL, `chunks_head` and tombstones are byte-compatible. Real Prometheus 3.16.0 opens a data directory written by Lean Series, and Lean Series opens one written by Prometheus, with identical query answers (`scripts/interop/`).
- **HTTP API:** 55 of 55 API checks give the same JSON as real Prometheus (`scripts/web_e2e_compare.py`).
- **Remote read/write:** protobuf messages are byte-identical with the Go encoder.

In scope: config and reload, scraping (all exposition formats), all 45 service discovery providers, relabeling, TSDB (head, WAL, WBL, out-of-order, compaction, retention, snapshots, exemplars, native histograms), PromQL, HTTP API v1, federation, remote read/write 1.0 and 2.0, recording and alerting rules, the Alertmanager notifier, the web UI, self-metrics, agent mode, and `promtool`. Not in scope: tracing export.

## Design

No async runtime: blocking I/O, plain threads, and no timers that wake when idle. Labels in one byte buffer with interned strings, postings packed as varint deltas, scrape lines matched by their position in the previous scrape, and code pages released when the server is idle. See `PORTING.md` for the full contract.

## Build and run

```sh
cargo build --release -p server -p promtool
target/release/prometheus --config.file=prometheus.yml    # same flags as Prometheus, port 9090
```

## Check

```sh
python3 scripts/score.py                    # upstream test score per crate (upstream sources in ./upstream)
bench/run-host.sh <label> <binary>          # dashboard workload
bench/run-scale.sh <label> <binary>         # 100k-series workload
```

The scorer, interop scripts and benchmarks expect the upstream source at `./upstream` (v3.16.0, commit e2e28ade) and the reference binaries in `./upstream-bin`; they are not in this repository.

## License

Apache-2.0, the same as Prometheus. See `LICENSE` and `NOTICE`.

Lean Series is an independent project. It is not affiliated with or endorsed by the Prometheus project, the CNCF or The Linux Foundation. Prometheus is a trademark of The Linux Foundation.
