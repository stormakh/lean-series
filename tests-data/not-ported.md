# Ledger tests that are not ported, or wait for other parts

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
