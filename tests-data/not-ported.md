# Upstream test facts not ported

## discovery/aws

- `ec2_test.go` `TestMain` (`goleak.VerifyTestMain`): a Go goroutine-leak check, not a ledger test. The Rust
  discoverers start no threads of their own outside `run`; the test mock servers are joined on drop.

## tsdb

- `tsdb` `Example` (`tsdb/example_test.go`): a Go godoc example (open a DB, append, query, print). Its ledger entry has
  `file: ""`, so the scorer cannot match a Rust file. The same flow is covered by the db_test ports and by
  `crates/tsdb/examples/tsdb_interop.rs`.
- `tsdb/goversion` `Test`: an empty Go test that only checks that the package builds with Go 1.12 or newer (build
  tags). No Rust equivalent; its ledger entry also has `file: ""`.
