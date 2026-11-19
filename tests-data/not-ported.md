# Ledger tests that are not ported (or are ported but ignored), with the reason

| crate | file | test | reason |
|---|---|---|---|
| scrape | scrape_test | test_request_traceparent_header | Needs OpenTelemetry tracing (a TracerProvider and the W3C trace context propagator); tracing export is out of scope (PORTING.md). Ported and `#[ignore]`d. |
| web | web_test | test_tlshttp2_alpn | Tests HTTP/2 negotiation over TLS (ALPN `h2`) with exporter-toolkit's web config file. The Rust web server is HTTP/1.1 only and has no TLS or web config file support (`Handler::run` refuses a web config file), so there is no Rust behavior to check. |

## Go-only test facts (not ledger tests)

## discovery/aws

- `ec2_test.go` `TestMain` (`goleak.VerifyTestMain`): a Go goroutine-leak check, not a ledger test. The Rust
  discoverers start no threads of their own outside `run`; the test mock servers are joined on drop.

## promql

- `engine_test` `TestEngine_QueryCostDurationLifecycle`, `query_cost_duration_test` `TestQuery_Exec_QueryCostDurationInput`
  and `TestQuery_Exec_QueryCostDurationCompletion`: they run under Go's `testing/synctest` fake clock and assert exact
  elapsed times (`require.Equal(t, duration, time.Since(start))`) and sleeps of seconds inside storage reads. Rust has
  no fake clock for the engine; with real time the assertions cannot hold. The behavior they cover (query-cost duration
  deadline shared by construction and execution, rejection attribution, stop on interrupted reads) is covered by the
  ported `test_query_cost_max_duration*`, `test_query_cost_max_samples_read_matrix` (cancellation part) and
  `test_query_cost_reject_metric_reason`.
- `estimate_test` `TestEstimateCost_SparseSeries`: needs the real TSDB (head compaction into blocks, multi-block
  sampling). Waits for the TSDB crate.
- `estimate_resource_test` `TestEstimateCost_RemoteSamplingFallback`: needs `storage/remote` (prompb, remote read
  client). Waits for the remote crate.

## tsdb

- `tsdb` `Example` (`tsdb/example_test.go`): a Go godoc example (open a DB, append, query, print). Its ledger entry has
  `file: ""`, so the scorer cannot match a Rust file. The same flow is covered by the db_test ports and by
  `crates/tsdb/examples/tsdb_interop.rs`.
- `tsdb/goversion` `Test`: an empty Go test that only checks that the package builds with Go 1.12 or newer (build
  tags). No Rust equivalent; its ledger entry also has `file: ""`.
