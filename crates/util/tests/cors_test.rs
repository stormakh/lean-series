//! Port of upstream/util/httputil/cors_test.go.
#[path = "common/rawhttp.rs"]
mod rawhttp;

use rawhttp::{Func, request, serve};
use std::sync::Arc;
use util::httputil::set_cors;

fn get_cors_handler_func() -> Arc<dyn http::Service> {
    let hf = |r: http::Request| {
        let reg = regex::Regex::new(r"^https://foo\.com$").unwrap();
        let mut w = http::Response::new(200);
        set_cors(&mut w, &reg, &r);
        w
    };
    Arc::new(Func(hf))
}

#[test]
fn test_cors_handler() {
    let port = serve(get_cors_handler_func());

    let dummy_origin = "https://foo.com";

    // OPTIONS with legit origin
    let resp = request("OPTIONS", port, "/any_path", &[("Origin", dummy_origin)]).expect("client get failed with unexpected error");

    let vary = resp.header("Vary");
    assert_eq!("Origin", vary);

    let access_control_allow_origin = resp.header("Access-Control-Allow-Origin");
    assert_eq!(dummy_origin, access_control_allow_origin, "expected Access-Control-Allow-Origin header");

    // OPTIONS with bad origin
    let resp = request("OPTIONS", port, "/any_path", &[("Origin", "https://not-foo.com")]).expect("client get failed with unexpected error");

    let access_control_allow_origin = resp.header("Access-Control-Allow-Origin");
    assert!(access_control_allow_origin.is_empty(), "Access-Control-Allow-Origin header should not exist but it was set");

    let vary = resp.header("Vary");
    assert_eq!("Origin", vary);

    // OPTIONS with no origin
    let resp = request("OPTIONS", port, "/any_path", &[]).expect("request");

    let vary = resp.header("Vary");
    assert_eq!("Origin", vary);

    let access_control_allow_origin = resp.header("Access-Control-Allow-Origin");
    assert!(access_control_allow_origin.is_empty());
}
