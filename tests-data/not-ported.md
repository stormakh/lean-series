# Ledger tests that are not ported (or are ported but ignored), with the reason

| crate | file | test | reason |
|---|---|---|---|
| scrape | scrape_test | test_request_traceparent_header | Needs OpenTelemetry tracing (a TracerProvider and the W3C trace context propagator); tracing export is out of scope (PORTING.md). Ported and `#[ignore]`d. |
