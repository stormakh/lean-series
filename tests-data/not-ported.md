# Upstream test facts not ported

## discovery/aws

- `ec2_test.go` `TestMain` (`goleak.VerifyTestMain`): a Go goroutine-leak check, not a ledger test. The Rust
  discoverers start no threads of their own outside `run`; the test mock servers are joined on drop.
