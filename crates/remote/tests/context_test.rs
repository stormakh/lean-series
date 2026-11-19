//! Port of `storage/remote/otlptranslator/prometheusremotewrite/context_test.go`.

use remote::otlptranslator::prometheusremotewrite::EveryNTimes;
use storage::{Context, ERR_CANCELED};

#[test]
fn test_every_n_times() {
    const N: i32 = 128;
    let (ctx, cancel) = Context::background().with_cancel();
    let mut e = EveryNTimes { n: N, ..Default::default() };

    for _ in 0..N {
        assert!(e.check_context(&ctx).is_ok());
    }

    cancel.cancel();
    for _ in 0..N - 1 {
        assert!(e.check_context(&ctx).is_ok());
    }
    assert_eq!(e.check_context(&ctx).unwrap_err().to_string(), ERR_CANCELED.to_string());
    // e should remember the error.
    assert_eq!(e.check_context(&ctx).unwrap_err().to_string(), ERR_CANCELED.to_string());
}
