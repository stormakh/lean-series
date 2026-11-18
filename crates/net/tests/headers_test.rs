//! Ported from prometheus/common v0.72.1 config/headers_test.go.
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use common::*;
use net::config::{Header, Headers, HttpClientConfig, RESERVED_HEADERS};
use net::roundtrip::{HeadersRoundTripper, RoundTripper};
use net::types::canonical_header_key;
use net::{HttpClientOptions, Request, new_client_from_config};

#[test]
fn test_reserved_headers() {
    for k in RESERVED_HEADERS {
        let l = canonical_header_key(k);
        assert_eq!(k, l, "ReservedHeaders keys should be lowercase: got {k:?}, expected {l:?}");
    }
}

fn headers(name: &str, v: &str) -> Headers {
    let mut m = BTreeMap::new();
    m.insert(name.to_string(), Header { values: vec![v.to_string()], ..Default::default() });
    Headers { headers: m }
}

#[test]
fn test_headers_round_tripper_same_host() {
    setup();
    for header in ["Cookie", "X-Custom-Header"] {
        let received = Arc::new(Mutex::new(String::new()));
        let r2 = received.clone();
        let server = new_server(move |r: &SReq| {
            *r2.lock().unwrap() = r.header.get_str(header).to_string();
            SResp::text("ok")
        });
        let rt = HeadersRoundTripper { config: headers(header, "testvalue"), next: Arc::new(net::Transport::default_transport()) };
        let resp = rt.round_trip(Request::get(&server.url).unwrap()).unwrap();
        assert_eq!("ok", String::from_utf8_lossy(&resp.body).trim());
        assert_eq!("testvalue", *received.lock().unwrap(), "header {header:?} must be forwarded on same-host request");
    }
}

#[test]
fn test_headers_round_tripper_reused_request() {
    setup();
    let received = Arc::new(Mutex::new(Vec::<String>::new()));
    let r2 = received.clone();
    let server = new_server(move |r: &SReq| {
        *r2.lock().unwrap() = r.header.values("X-Custom-Header").iter().map(|s| s.to_string()).collect();
        SResp::text("ok")
    });
    let rt = HeadersRoundTripper { config: headers("X-Custom-Header", "testvalue"), next: Arc::new(net::Transport::default_transport()) };
    let req = Request::get(&server.url).unwrap();
    for i in 0..3 {
        rt.round_trip(req.clone()).unwrap();
        assert_eq!(vec!["testvalue".to_string()], *received.lock().unwrap(), "header duplicated on request {}", i + 1);
        assert!(req.header.values("X-Custom-Header").is_empty(), "the caller's request was modified");
    }
}

#[test]
fn test_headers_round_tripper_cross_host_redirect() {
    setup();
    let on_redirect = Arc::new(Mutex::new(String::new()));
    let r2 = on_redirect.clone();
    let target = new_server(move |r: &SReq| {
        *r2.lock().unwrap() = r.header.get_str("Cookie").to_string();
        SResp::text("ok")
    });
    let target_url = format!("http://localhost:{}", target.port);
    let on_origin = Arc::new(Mutex::new(String::new()));
    let o2 = on_origin.clone();
    let origin = new_server(move |r: &SReq| {
        *o2.lock().unwrap() = r.header.get_str("Cookie").to_string();
        SResp::redirect(&target_url)
    });
    let cfg = HttpClientConfig { follow_redirects: true, http_headers: Some(headers("Cookie", "session=abc")), ..Default::default() };
    let client = new_client_from_config(&cfg, &HttpClientOptions::default()).unwrap();
    client.get(&origin.url).unwrap();
    assert_eq!("session=abc", *on_origin.lock().unwrap(), "Cookie must be set on the initial request.");
    assert!(on_redirect.lock().unwrap().is_empty(), "Cookie must not be forwarded on a cross-host redirect.");
}

#[test]
fn test_headers_round_tripper_same_host_redirect() {
    setup();
    let on_redirect = Arc::new(Mutex::new(String::new()));
    let r2 = on_redirect.clone();
    let server = new_server(move |r: &SReq| match r.path.as_str() {
        "/start" => SResp::redirect("/end"),
        _ => {
            *r2.lock().unwrap() = r.header.get_str("Cookie").to_string();
            SResp::text("ok")
        }
    });
    let cfg = HttpClientConfig { follow_redirects: true, http_headers: Some(headers("Cookie", "session=abc")), ..Default::default() };
    let client = new_client_from_config(&cfg, &HttpClientOptions::default()).unwrap();
    client.get(&format!("{}/start", server.url)).unwrap();
    assert_eq!("session=abc", *on_redirect.lock().unwrap(), "Cookie must be forwarded on a same-host redirect.");
}
