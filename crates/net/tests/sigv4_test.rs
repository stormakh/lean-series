//! Ported from github.com/prometheus/sigv4 v0.5.0 sigv4_test.go and sigv4_config_test.go, plus signatures
//! checked against aws-sdk-go-v2's signer (values generated with bench/tmp/w0-net/gosig).
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use common::{SReq, SResp, new_server};
use net::roundtrip::{FnRoundTripper, RoundTripper};
use net::sigv4::{Credentials, EnvFn, SigV4Config, SigV4Options, SigV4RoundTripper, sign_request};
use net::{Request, Response};

const SIGV4_DIR: &str = "../../upstream/../tools/gopath/pkg/mod/github.com/prometheus/sigv4@v0.5.0";

fn load_sigv4_config(f: &str) -> Result<SigV4Config, String> {
    let p = format!("{}/{}/{}", env!("CARGO_MANIFEST_DIR"), SIGV4_DIR, f);
    let content = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    yaml::unmarshal_strict(&content).map_err(|e| e.to_string())
}

#[test]
fn test_good_sig_v4_configs() {
    for f in ["testdata/sigv4_good.yaml", "testdata/sigv4_good.yaml", "testdata/sigv4_good_session.yaml"] {
        load_sigv4_config(f).unwrap_or_else(|e| panic!("Unexpected error parsing {f}: {e}"));
    }
}

#[test]
fn test_bad_sig_v4_config() {
    let cases = [
        ("missing secret key", "testdata/sigv4_bad.yaml", "must provide a AWS SigV4 Access key and Secret Key"),
        ("external_id without role_arn", "testdata/sigv4_bad_external_id.yaml", "external_id can only be used with role_arn"),
        ("session_name too short", "testdata/sigv4_bad_session_too_short.yaml", "session_name must match"),
        ("session_name invalid characters", "testdata/sigv4_bad_session_pattern.yaml", "session_name must match"),
        ("session_name without role_arn", "testdata/sigv4_bad_session_no_role.yaml", "session_name can only be used with role_arn"),
        ("tags without role_arn", "testdata/sigv4_bad_tags_no_role.yaml", "tags can only be used with role_arn"),
    ];
    for (name, f, msg) in cases {
        let e = load_sigv4_config(f).expect_err("Did not receive expected error unmarshaling bad sigv4 config");
        assert!(e.contains(msg), "{name}: Received unexpected error from unmarshal of {f}: {e}");
    }
}

fn env(pairs: &[(&str, &str)]) -> EnvFn {
    let m: BTreeMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    Arc::new(move |k: &str| m.get(k).cloned())
}

// adapted: Go loads the AWS SDK default config; here the round tripper resolves the region from the
// environment the same way.
#[test]
fn test_sig_v4_inferred_region() {
    let o = SigV4Options { env: Some(env(&[("AWS_ACCESS_KEY_ID", "secret"), ("AWS_SECRET_ACCESS_KEY", "token"), ("AWS_REGION", "us-west-2")])), ..Default::default() };
    let rt = SigV4RoundTripper::new(&SigV4Config::default(), Arc::new(net::Transport::default_transport()), o).unwrap();
    assert_eq!("us-west-2", rt.region);
}

#[test]
fn test_new_sig_v4_round_tripper_inferred_region() {
    let o = SigV4Options { env: Some(env(&[("AWS_ACCESS_KEY_ID", "access"), ("AWS_SECRET_ACCESS_KEY", "secret"), ("AWS_REGION", "us-west-2")])), ..Default::default() };
    let rt = SigV4RoundTripper::new(&SigV4Config::default(), Arc::new(net::Transport::default_transport()), o).unwrap();
    assert_eq!("us-west-2", rt.region);
}

// adapted: the "cancelled context" half has no equivalent (no contexts); the live half is kept.
#[test]
fn test_new_sig_v4_round_tripper_with_context() {
    let cfg = SigV4Config { region: "us-east-1".into(), access_key: "access".into(), secret_key: yaml::Secret::new("secret"), ..Default::default() };
    let rt = SigV4RoundTripper::new(&cfg, Arc::new(net::Transport::default_transport()), SigV4Options { env: Some(env(&[])), ..Default::default() });
    assert!(rt.is_ok());
}

fn capture() -> (Arc<dyn RoundTripper>, Arc<Mutex<Option<Request>>>) {
    let got: Arc<Mutex<Option<Request>>> = Arc::new(Mutex::new(None));
    let g2 = got.clone();
    let rt = Arc::new(FnRoundTripper(move |r: Request| {
        *g2.lock().unwrap() = Some(r.clone());
        Ok(Response { status: 200, status_text: "200 OK".into(), header: net::Header::new(), body: Vec::new(), request: r })
    }));
    (rt, got)
}

#[test]
fn test_sig_v4_round_tripper() {
    let (next, got) = capture();
    let cfg = SigV4Config { region: "us-east-2".into(), access_key: "AccessKey".into(), secret_key: yaml::Secret::new("SecretKey"), ..Default::default() };
    let rt = SigV4RoundTripper::new(&cfg, next, SigV4Options { env: Some(env(&[])), ..Default::default() }).unwrap();
    let cli = net::Client::new(Arc::new(rt));

    let req = Request::new("POST", "https://example.com", b"Hello, world!".to_vec()).unwrap();
    cli.do_request(req).unwrap();
    let orig = got.lock().unwrap().take().expect("request");
    assert!(!orig.header.get_str("Authorization").is_empty());
    assert!(!orig.header.get_str("X-Amz-Date").is_empty());

    // Ignored Headers
    let mut req = Request::new("POST", "https://example.com", b"Hello, world!".to_vec()).unwrap();
    req.header.add("Uber-Trace-Id", "some-trace-id");
    cli.do_request(req).unwrap();
    let g = got.lock().unwrap().take().unwrap();
    assert_eq!(b"Hello, world!".to_vec(), g.body);
    // (both requests are signed within the same second in Go's test as well)
    if g.header.get_str("X-Amz-Date") == orig.header.get_str("X-Amz-Date") {
        assert_eq!(orig.header.get_str("Authorization"), g.header.get_str("Authorization"));
    }
    assert_eq!("some-trace-id", g.header.get_str("Uber-Trace-Id"));

    // Escape URL
    let req = Request::new("POST", "https://example.com/test//test", b"Hello, world!".to_vec()).unwrap();
    assert_eq!("/test//test", req.url.path);
    cli.do_request(req).unwrap();
    assert_eq!("/test/test", got.lock().unwrap().take().unwrap().url.path);

    // No body
    let req = Request::get("https://example.com/test/test").unwrap();
    cli.do_request(req.clone()).unwrap();
    assert!(req.body.is_empty());

    // Caller request not mutated
    let req = Request::new("POST", "https://example.com/test//test/", b"Hello, world!".to_vec()).unwrap();
    cli.do_request(req.clone()).unwrap();
    assert_eq!("/test//test/", req.url.path);
    assert_eq!(b"Hello, world!".to_vec(), req.body);

    // Path handling
    for (name, url, want) in [
        ("empty path", "https://example.com", ""),
        ("root path", "https://example.com/", "/"),
        ("trailing slash", "https://example.com/test/", "/test/"),
        ("duplicate slash", "https://example.com/test//test", "/test/test"),
        ("duplicate slash trailing", "https://example.com/test//test/", "/test/test/"),
    ] {
        cli.do_request(Request::get(url).unwrap()).unwrap();
        assert_eq!(want, got.lock().unwrap().take().unwrap().url.path, "{name}");
    }
}

const STS_ASSUME_RESPONSE: &str = r#"<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIA_FAKE</AccessKeyId>
      <SecretAccessKey>FAKE_SECRET</SecretAccessKey>
      <SessionToken>FAKE_TOKEN</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>"#;

fn sts_server() -> (common::TestServer, Arc<Mutex<String>>) {
    let body = Arc::new(Mutex::new(String::new()));
    let b2 = body.clone();
    let srv = new_server(move |r: &SReq| {
        *b2.lock().unwrap() = String::from_utf8_lossy(&r.body).into_owned();
        let mut s = SResp::text(STS_ASSUME_RESPONSE);
        s.header.set("Content-Type", "text/xml");
        s
    });
    (srv, body)
}

#[test]
fn test_assume_role_session_name_and_tags() {
    let (srv, got_body) = sts_server();
    let mut tags = BTreeMap::new();
    tags.insert("team".to_string(), "observability".to_string());
    tags.insert("cost-center".to_string(), "12345".to_string());
    let cfg = SigV4Config { region: "us-east-1".into(), access_key: "AKIA_FAKE".into(), secret_key: yaml::Secret::new("SECRET_FAKE"), role_arn: "arn:aws:iam::123456789012:role/test-role".into(), session_name: "prometheus-1".into(), tags, ..Default::default() };
    let rt = SigV4RoundTripper::new(&cfg, Arc::new(net::Transport::default_transport()), SigV4Options { sts_endpoint: srv.url.clone(), env: Some(env(&[])) }).unwrap();
    let creds = rt.credentials().unwrap();
    assert_eq!("ASIA_FAKE", creds.access_key_id);
    let b = got_body.lock().unwrap().clone();
    assert!(b.contains("RoleSessionName=prometheus-1"), "{b}");
    assert!(b.contains("Tags.member"), "{b}");
}

#[test]
fn test_assume_role_with_external_id_and_session_name() {
    let (srv, got_body) = sts_server();
    let cfg = SigV4Config {
        region: "us-east-1".into(),
        access_key: "AKIA_FAKE".into(),
        secret_key: yaml::Secret::new("SECRET_FAKE"),
        role_arn: "arn:aws:iam::123456789012:role/test-role".into(),
        external_id: "ext-abc-123".into(),
        session_name: "prometheus-prod-1".into(),
        tags: [("env".to_string(), "prod".to_string())].into_iter().collect(),
        ..Default::default()
    };
    let rt = SigV4RoundTripper::new(&cfg, Arc::new(net::Transport::default_transport()), SigV4Options { sts_endpoint: srv.url.clone(), env: Some(env(&[])) }).unwrap();
    rt.credentials().unwrap();
    let b = got_body.lock().unwrap().clone();
    assert!(b.contains("ExternalId=ext-abc-123"), "{b}");
    assert!(b.contains("RoleSessionName=prometheus-prod-1"), "{b}");
}

#[test]
fn test_validate_session_name_and_tags() {
    let base = SigV4Config { role_arn: "arn:aws:iam::123456789012:role/r".into(), ..Default::default() };
    let with = |f: &dyn Fn(&mut SigV4Config)| {
        let mut c = base.clone();
        f(&mut c);
        c
    };
    let cases: Vec<(&str, SigV4Config, &str)> = vec![
        ("valid session name alphanumeric", with(&|c| c.session_name = "Prometheus1".into()), ""),
        ("valid session name special chars", with(&|c| c.session_name = "user+1@acme.org".into()), ""),
        ("valid session name min length", with(&|c| c.session_name = "ab".into()), ""),
        ("session_name too short 1 char", with(&|c| c.session_name = "a".into()), "session_name must match"),
        ("session_name with spaces", with(&|c| c.session_name = "bad name".into()), "session_name must match"),
        ("session_name with disallowed chars", with(&|c| c.session_name = "bad!name".into()), "session_name must match"),
        ("session_name too long 65 chars", with(&|c| c.session_name = "a".repeat(65)), "session_name must match"),
        ("session_name max length 64 chars", with(&|c| c.session_name = "a".repeat(64)), ""),
        ("session_name without role_arn", SigV4Config { session_name: "prometheus-1".into(), ..Default::default() }, "session_name can only be used with role_arn"),
        ("valid tags with role_arn", with(&|c| c.tags = [("team".to_string(), "observability".to_string())].into_iter().collect()), ""),
        ("tags without role_arn", SigV4Config { tags: [("team".to_string(), "a".to_string())].into_iter().collect(), ..Default::default() }, "tags can only be used with role_arn"),
        ("tag value at max length 256", with(&|c| c.tags = [("k".to_string(), "v".repeat(256))].into_iter().collect()), ""),
        ("tag value exceeds 256", with(&|c| c.tags = [("k".to_string(), "v".repeat(257))].into_iter().collect()), "exceeds maximum length of 256"),
    ];
    for (name, cfg, expect) in cases {
        let r = cfg.validate();
        if expect.is_empty() {
            assert!(r.is_ok(), "{name}: {r:?}");
        } else {
            assert!(r.as_ref().unwrap_err().contains(expect), "{name}: {r:?}");
        }
    }
}

/// Signatures from aws-sdk-go-v2 v1.47.3 `signer.SignHTTP` at Unix time 1700000000 (bench/tmp/w0-net/gosig).
#[test]
fn sigv4_matches_aws_sdk_go_v2() {
    let creds = Credentials { access_key_id: "AKIDEXAMPLE".into(), secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(), session_token: String::new() };
    let mut r = Request::new("POST", "https://aps-workspaces.us-west-2.amazonaws.com/workspaces/ws-1/api/v1/remote_write", b"hello snappy".to_vec()).unwrap();
    for (k, v) in [("Content-Type", "application/x-protobuf"), ("Content-Encoding", "snappy"), ("X-Prometheus-Remote-Write-Version", "0.1.0"), ("User-Agent", "Prometheus/3.16.0")] {
        r.header.set(k, v);
    }
    sign_request(&mut r, &creds, "aps", "us-west-2", 1_700_000_000);
    assert_eq!(r.header.get_str("X-Amz-Date"), "20231114T221320Z");
    assert_eq!(
        r.header.get_str("Authorization"),
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-west-2/aps/aws4_request, SignedHeaders=content-encoding;content-length;content-type;host;x-amz-date;x-prometheus-remote-write-version, Signature=babce84662520501b0db32cad4b1be0f592d206f851dec2be684116518c3f24d"
    );

    let mut r = Request::get("https://example.com:443/a%20b/c?b=2&a=1&a=0&z=x+y").unwrap();
    r.header.set("X-Multi", "  a   b  ");
    let c2 = Credentials { session_token: "TOKEN".into(), ..creds.clone() };
    sign_request(&mut r, &c2, "aps", "us-west-2", 1_700_000_000);
    assert_eq!(r.host, "example.com");
    assert_eq!(r.header.get_str("X-Amz-Security-Token"), "TOKEN");
    assert_eq!(
        r.header.get_str("Authorization"),
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-west-2/aps/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token;x-multi, Signature=dfdacae63e6cb774ecc0917e7681b8425676a38723e12ae954c27f98419fd771"
    );

    let mut r = Request::get("http://example.com:8080/").unwrap();
    sign_request(&mut r, &creds, "aps", "us-west-2", 1_700_000_000);
    assert_eq!(
        r.header.get_str("Authorization"),
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-west-2/aps/aws4_request, SignedHeaders=host;x-amz-date, Signature=86a2e7574f3ac3a842c77bc3d7e67abd49fdca86dee3ef4dea7b9b801e7e4da2"
    );
}
