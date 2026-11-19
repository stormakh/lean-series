# Ledger tests that are not ported (or are ported but ignored), with the reason

| crate | file | test | reason |
|---|---|---|---|
| scrape | scrape_test | test_request_traceparent_header | Needs OpenTelemetry tracing (a TracerProvider and the W3C trace context propagator); tracing export is out of scope (PORTING.md). Ported and `#[ignore]`d. |

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

## tsdb

- `tsdb` `Example` (`tsdb/example_test.go`): a Go godoc example (open a DB, append, query, print). Its ledger entry has
  `file: ""`, so the scorer cannot match a Rust file. The same flow is covered by the db_test ports and by
  `crates/tsdb/examples/tsdb_interop.rs`.
- `tsdb/goversion` `Test`: an empty Go test that only checks that the package builds with Go 1.12 or newer (build
  tags). No Rust equivalent; its ledger entry also has `file: ""`.

## util (logging, httputil, netconnlimit, notifications, runtime, compression, junitxml, fuzzing) and tracing

All ledger tests of these packages are ported. Go-only facts and parts left out:

- `util/fuzzing`: `go test` runs each fuzz target once per seed; the Rust tests do the same. Random fuzzing
  (`go test -fuzz`) and `GetDictForFuzzParseExpr` (a libFuzzer dictionary, used by no test) are not ported. The
  `FuzzParseProtobuf` seeds are built by Go code with gogo/protobuf; `scripts/gen/fuzzing` writes them to
  `tests-data/fuzzing/protobuf_corpus.bin`. The chunk fuzz seeds use Go `math/rand` (v1), ported in
  `crates/util/tests/common/gorand.rs` (checked against Go for seeds 0, 42, 1234567890, -5).
- `tracing`: OTLP export is out of scope (PORTING.md). Not ported: the gRPC and HTTP OTLP clients (endpoint,
  compression, headers, timeout), the batcher, the sampler (`sampling_fraction`), the resource attributes, the W3C
  trace-context propagator and the OpenTelemetry error handler of `Run`. Kept: when `ApplyConfig` reinstalls or
  uninstalls, the TLS check on install, the log lines, and `Stop`. Go reads the global `otel.GetTracerProvider()`;
  the Rust manager owns its provider, because Rust tests run in parallel in one process.
- `util/logging` `Deduper`: Go cleans old entries in a goroutine every 10s and has `Stop`; Rust cleans them on the
  next logging call after 10s (no idle timer), so there is nothing to stop. promslog's `go-kit` log style and slog
  groups (`WithGroup`) are not ported (Prometheus uses neither).
- `util/logging` `TestJSONFileLogger_basic`: the `source` field is a `.rs` file, so the regex expects `\w+\.rs`.
- `util/httputil` `BenchmarkNewCompressionHandler_MaliciousAcceptEncoding` and `util/compression` `BenchmarkEncode`,
  `BenchmarkDecode`: Go benchmarks, not tests.

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
