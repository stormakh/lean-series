//! Port of `cmd/prometheus/main_unix_test.go`.

mod common;

use std::os::unix::process::ExitStatusExt;
use std::time::Duration;

use common::*;

/// As soon as prometheus starts responding to http request it should be able to
/// accept Interrupt signals for a graceful shutdown.
#[test]
fn test_startup_interrupt() {
    let port = format!(":{}", random_port());
    let data = TempDir::new();

    let mut prom = start_with_logging(&[
        format!("--config.file={}", prom_config()),
        format!("--storage.tsdb.path={}", data.path().display()),
        format!("--web.listen-address=0.0.0.0{port}"),
    ]);

    let mut started_ok = false;
    let mut stopped: Option<std::process::ExitStatus> = None;

    let url = format!("http://localhost{port}/graph");

    for _ in 0..10 {
        // error=nil means prometheus has started, so we can send the interrupt
        // signal and wait for the graceful shutdown.
        if get(&url).is_ok() {
            started_ok = true;
            prom.signal(libc::SIGINT);
            stopped = prom.wait_timeout(Duration::from_secs(10));
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    assert!(started_ok, "prometheus didn't start in the specified timeout");
    // adapted: Go checks that `Process.Kill` fails because the process is gone; Rust's `Child::kill` does not fail
    // for a reaped child, so the check is that the process exited within the wait.
    let status = stopped.unwrap_or_else(|| {
        panic!("prometheus didn't shutdown gracefully after sending the Interrupt signal\n{}", prom.logs())
    });
    // Go: a nil error (exit code 0) or "signal: interrupt".
    assert!(
        status.success() || status.signal() == Some(libc::SIGINT),
        "prometheus exit: {status:?}\n{}",
        prom.logs()
    );
}
