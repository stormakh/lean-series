//! Port of cmd/prometheus/warnings_test.go.

use std::sync::{Arc, Mutex};

use server::helpers::{DedupDeprecationWarningLogger, WarningHandler};

/// Go `fakeWarningLogger`.
struct FakeWarningLogger {
    logs: Arc<Mutex<Vec<String>>>,
}

impl WarningHandler for FakeWarningLogger {
    fn handle_warning_header(&mut self, _code: i32, _agent: &str, message: &str) {
        self.logs.lock().unwrap().push(message.to_string());
    }
}

#[test]
fn test_dedupe_deprecation_warning_logger() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let mut wl = DedupDeprecationWarningLogger::new(Box::new(FakeWarningLogger { logs: logs.clone() }));

    let deprecation_message =
        "v1 Endpoints is deprecated in v1.33+; use [discovery.k8s.io/v1](http://discovery.k8s.io/v1) EndpointSlice";
    for _ in 0..10 {
        wl.handle_warning_header(299, "", deprecation_message);
    }
    assert_eq!(logs.lock().unwrap().len(), 1);
    assert_eq!(wl.logged.lock().unwrap().len(), 1);
    assert_eq!(logs.lock().unwrap()[0], deprecation_message);

    for i in 0..10 {
        wl.handle_warning_header(299, "", &format!("some other warning {}", i + 1));
    }
    assert_eq!(logs.lock().unwrap().len(), 11);
    assert_eq!(wl.logged.lock().unwrap().len(), 1);
    assert_eq!("some other warning 10", logs.lock().unwrap()[10]);
}
