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

## remote (read/write handlers, codec)

- `codec_test` `TestValidateLabelsAndMetricName`: 3 of 9 rows ("label name with \xff", "label value is an invalid
  UTF-8 value", "metric name has invalid utf8") cannot be built: Rust strings are always valid UTF-8, and the prompb
  decoder replaces invalid bytes with U+FFFD. The other 6 rows are ported.
- `read_handler_test` `TestStreamReadEndpoint` "write errors": checks the handler against a failing
  `http.ResponseWriter` (zero and partial writes). The Rust handler builds the body in memory and writes nothing
  itself. "storage errors" runs each case once by calling the handler directly (no HTTP/2, TLS or otelhttp); a
  stream that fails after its first frame aborts the whole response.
- `read_handler_test` `TestRemoteReadGateWaitDurationObserved`: Go uses the synctest fake clock (exact waits); the
  port uses real time and checks each wait within 50ms above the expected value.
