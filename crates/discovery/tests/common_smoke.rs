mod common;

#[test]
fn common_server_smoke() {
    let ts = common::TestServer::new(|r: &common::Req| common::Resp::json(format!("{{\"p\":\"{}\",\"q\":\"{}\"}}", r.path, r.query_get("a"))));
    let c = net::Client::new(std::sync::Arc::new(net::Transport::default_transport()));
    let r = discovery::sdutil::get(&c, &format!("{}/x%20y?a=b+c", ts.url), &[], None).unwrap();
    assert_eq!(String::from_utf8(r.body).unwrap(), r#"{"p":"/x y","q":"b c"}"#);
    let r = discovery::sdutil::get(&c, &format!("{}/z", ts.url), &[], None).unwrap();
    assert_eq!(r.status, 200);
}
