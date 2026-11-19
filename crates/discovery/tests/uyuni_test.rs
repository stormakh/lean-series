//! Port of discovery/uyuni/uyuni_test.go.
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{Req, Resp, TestServer, new_options};
use discovery::uyuni::{Discovery, SdConfig};
use discovery::{CancelToken, Group};

fn new_discovery(ts: &TestServer) -> Discovery {
    let conf = SdConfig { server: ts.url.clone(), ..Default::default() };
    let (mut opts, _reg) = new_options(&conf);
    opts.set_name = "uyuni".to_string();
    Discovery::new(&conf, opts).unwrap()
}

/// Go `testUpdateServices`.
fn test_update_services(handler: impl Fn(&Req) -> Resp + Send + Sync + 'static) -> Result<Vec<Arc<Group>>, String> {
    // Create a test server with mock HTTP handler.
    let ts = TestServer::new(handler);
    let mut md = new_discovery(&ts);
    md.refresh(&CancelToken::new())
}

#[test]
fn test_uyuni_sd_handle_error() {
    let err_testing = "unable to login to Uyuni API: request error: bad status code - 500";
    let r = test_update_services(|_: &Req| Resp::new(500, ""));
    assert_eq!(Err(err_testing.to_string()), r);
}

#[test]
fn test_uyuni_sd_login() {
    let err_testing = "unable to get the managed system groups information of monitored clients: request error: bad status code - 500";
    let call = AtomicUsize::new(0);
    let r = test_update_services(move |_: &Req| {
        let resp = match call.load(Ordering::SeqCst) {
            0 => Resp::new(
                200,
                r#"<?xml version="1.0"?>
<methodResponse>
	<params>
		<param>
			<value>
				a token
			</value>
		</param>
	</params>
</methodResponse>"#,
            )
            .header("Content-Type", "application/xml"),
            _ => Resp::new(500, "").header("Content-Type", "application/xml"),
        };
        call.fetch_add(1, Ordering::SeqCst);
        resp
    });
    assert_eq!(Err(err_testing.to_string()), r);
}

#[test]
fn test_uyuni_sd_skip_login() {
    let err_testing = "unable to get the managed system groups information of monitored clients: request error: bad status code - 500";
    // Create a test server with mock HTTP handler.
    let ts = TestServer::new(|_: &Req| Resp::new(500, ""));
    let mut md = new_discovery(&ts);
    // simulate a cached token
    md.set_token("a token", Instant::now() + Duration::from_secs(60));
    let r = md.refresh(&CancelToken::new());
    assert_eq!(Err(err_testing.to_string()), r);
}
