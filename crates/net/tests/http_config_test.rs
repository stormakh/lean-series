//! Ported from prometheus/common v0.72.1 config/http_config_test.go. Tests run from common's config directory
//! (see common::setup) so the relative "testdata/..." paths and error texts are Go's.
//! Not ported: the JSON halves of the URL and HTTPClientConfig marshal tests (Prometheus never JSON-encodes
//! these configs; JSON inputs are decoded with the YAML decoder, a superset).
mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use net::config::*;
use net::roundtrip::*;
use net::secret::{FileSecret, SecretManager, new_file_secret, new_inline_secret};
use net::{Client, HttpClientOptions, Request, Response, new_client_from_config, new_round_tripper_from_config};
use yaml::{Secret, Url};

fn opts() -> HttpClientOptions {
    HttpClientOptions::default()
}

fn tls(ca: &str, cert: &str, key: &str) -> TlsConfig {
    TlsConfig { ca_file: ca.into(), cert_file: cert.into(), key_file: key.into(), ..Default::default() }
}

fn good_tls() -> TlsConfig {
    tls(TLS_CA_CHAIN_PATH, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH)
}

fn body(r: &Response) -> String {
    String::from_utf8_lossy(&r.body).trim().to_string()
}

const INVALID_HTTP_CLIENT_CONFIGS: [(&str, &str); 17] = [
    ("testdata/http.conf.bearer-token-and-file-set.bad.yml", "at most one of bearer_token & bearer_token_file must be configured"),
    ("testdata/http.conf.empty.bad.yml", "at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured"),
    ("testdata/http.conf.basic-auth.too-much.bad.yaml", "at most one of basic_auth password, password_file & password_ref must be configured"),
    ("testdata/http.conf.basic-auth.bad-username.yaml", "at most one of basic_auth username, username_file & username_ref must be configured"),
    ("testdata/http.conf.mix-bearer-and-creds.bad.yaml", "authorization is not compatible with bearer_token & bearer_token_file"),
    ("testdata/http.conf.auth-creds-and-file-set.too-much.bad.yaml", "at most one of authorization credentials & credentials_file must be configured"),
    ("testdata/http.conf.basic-auth-and-auth-creds.too-much.bad.yaml", "at most one of basic_auth, oauth2 & authorization must be configured"),
    ("testdata/http.conf.basic-auth-and-oauth2.too-much.bad.yaml", "at most one of basic_auth, oauth2 & authorization must be configured"),
    ("testdata/http.conf.auth-creds-no-basic.bad.yaml", r#"authorization type cannot be set to "basic", use "basic_auth" instead"#),
    ("testdata/http.conf.oauth2-secret-and-file-set.bad.yml", "at most one of oauth2 client_secret, client_secret_file & client_secret_ref must be configured"),
    ("testdata/http.conf.oauth2-certificate-and-file-set.bad.yml", "at most one of oauth2 client_certificate_key, client_certificate_key_file & client_certificate_key_ref must be configured using grant-type=urn:ietf:params:oauth:grant-type:jwt-beare"),
    ("testdata/http.conf.oauth2-no-client-id.bad.yaml", "oauth2 client_id must be configured"),
    ("testdata/http.conf.oauth2-no-token-url.bad.yaml", "oauth2 token_url must be configured"),
    ("testdata/http.conf.proxy-from-env.bad.yaml", "if proxy_from_environment is configured, proxy_url must not be configured"),
    ("testdata/http.conf.no-proxy.bad.yaml", "if proxy_from_environment is configured, no_proxy must not be configured"),
    ("testdata/http.conf.no-proxy-without-proxy-url.bad.yaml", "if no_proxy is configured, proxy_url must also be configured"),
    ("testdata/http.conf.headers-reserved.bad.yaml", r#"setting header "User-Agent" is not allowed"#),
];

type H = Box<dyn Fn(&SReq) -> SResp + Send + Sync>;

fn bearer_check(expected: String) -> H {
    Box::new(move |r: &SReq| {
        let bearer = r.header.get_str("Authorization");
        if bearer != expected {
            SResp::text(&format!("The expected Bearer Authorization ({expected}) differs from the obtained Bearer Authorization ({bearer})"))
        } else {
            SResp::text(EXPECTED_MESSAGE)
        }
    })
}

fn oauth_handler() -> H {
    Box::new(|r: &SReq| {
        if r.path == "/token" {
            return SResp::json(&format!(r#"{{"access_token":"{EXPECTED_ACCESS_TOKEN}","token_type":"Bearer"}}"#));
        }
        let a = r.header.get_str("Authorization");
        if a != format!("Bearer {EXPECTED_ACCESS_TOKEN}") {
            SResp::text(&format!("Expected Authorization header {:?}, got {:?}", format!("Bearer {EXPECTED_ACCESS_TOKEN}"), a))
        } else {
            SResp::text(EXPECTED_MESSAGE)
        }
    })
}

#[test]
fn test_new_client_from_config() {
    setup();
    let mut cases: Vec<(HttpClientConfig, H)> = Vec::new();
    let msg: fn() -> H = || Box::new(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    cases.push((HttpClientConfig { tls_config: TlsConfig { insecure_skip_verify: true, ..tls("", CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH) }, ..Default::default() }, msg()));
    cases.push((HttpClientConfig { tls_config: good_tls(), ..Default::default() }, msg()));
    cases.push((HttpClientConfig { bearer_token: Secret::new(BEARER_TOKEN), tls_config: good_tls(), ..Default::default() }, bearer_check(expected_bearer())));
    cases.push((HttpClientConfig { bearer_token_file: BEARER_TOKEN_FILE.into(), tls_config: good_tls(), ..Default::default() }, bearer_check(expected_bearer())));
    cases.push((HttpClientConfig { authorization: Some(Authorization { credentials: Secret::new(BEARER_TOKEN), ..Default::default() }), tls_config: good_tls(), ..Default::default() }, bearer_check(expected_bearer())));
    cases.push((
        HttpClientConfig { authorization: Some(Authorization { credentials_file: AUTHORIZATION_CREDENTIALS_FILE.into(), type_: AUTHORIZATION_TYPE.into(), ..Default::default() }), tls_config: good_tls(), ..Default::default() },
        bearer_check(expected_authentication_credentials()),
    ));
    cases.push((
        HttpClientConfig { authorization: Some(Authorization { type_: AUTHORIZATION_TYPE.into(), ..Default::default() }), tls_config: good_tls(), ..Default::default() },
        Box::new(|r: &SReq| {
            let bearer = r.header.get_str("Authorization");
            if bearer.trim() != AUTHORIZATION_TYPE {
                SResp::text(&format!("The expected Bearer Authorization ({AUTHORIZATION_TYPE}) differs from the obtained Bearer Authorization ({bearer})"))
            } else {
                SResp::text(EXPECTED_MESSAGE)
            }
        }),
    ));
    cases.push((
        HttpClientConfig { authorization: Some(Authorization { credentials: Secret::new(AUTHORIZATION_CREDENTIALS), type_: AUTHORIZATION_TYPE.into(), ..Default::default() }), tls_config: good_tls(), ..Default::default() },
        bearer_check(expected_authentication_credentials()),
    ));
    cases.push((HttpClientConfig { authorization: Some(Authorization { credentials_file: BEARER_TOKEN_FILE.into(), ..Default::default() }), tls_config: good_tls(), ..Default::default() }, bearer_check(expected_bearer())));
    cases.push((
        HttpClientConfig { basic_auth: Some(BasicAuth { username: EXPECTED_USERNAME.into(), password: Secret::new(EXPECTED_PASSWORD), ..Default::default() }), tls_config: good_tls(), ..Default::default() },
        Box::new(|r: &SReq| match r.basic_auth() {
            None => SResp::text("The Authorization header wasn't set"),
            Some((u, _)) if u != EXPECTED_USERNAME => SResp::text(&format!("The expected username ({EXPECTED_USERNAME}) differs from the obtained username ({u}).")),
            Some((_, p)) if p != EXPECTED_PASSWORD => SResp::text(&format!("The expected password ({EXPECTED_PASSWORD}) differs from the obtained password ({p}).")),
            _ => SResp::text(EXPECTED_MESSAGE),
        }),
    ));
    cases.push((
        HttpClientConfig { follow_redirects: true, tls_config: good_tls(), ..Default::default() },
        Box::new(|r: &SReq| {
            if r.path == "/redirected" {
                SResp::text(EXPECTED_MESSAGE)
            } else {
                let mut s = SResp::status(302, "It should follow the redirect.");
                s.header.set("Location", "/redirected");
                s
            }
        }),
    ));
    cases.push((
        HttpClientConfig { follow_redirects: false, tls_config: good_tls(), ..Default::default() },
        Box::new(|r: &SReq| {
            if r.path == "/redirected" {
                SResp::text("The redirection was followed.")
            } else {
                let mut s = SResp::status(302, EXPECTED_MESSAGE);
                s.header.set("Location", "/redirected");
                s
            }
        }),
    ));
    cases.push((HttpClientConfig { oauth2: Some(OAuth2 { client_id: "ExpectedUsername".into(), tls_config: good_tls(), ..Default::default() }), tls_config: good_tls(), ..Default::default() }, oauth_handler()));
    cases.push((
        HttpClientConfig { oauth2: Some(OAuth2 { client_id: "ExpectedUsername".into(), client_secret: Secret::new("ExpectedPassword"), tls_config: good_tls(), ..Default::default() }), tls_config: good_tls(), ..Default::default() },
        oauth_handler(),
    ));

    for (mut cfg, handler) in cases {
        let ts = new_tls_server(handler);
        if let Some(o) = cfg.oauth2.as_mut() {
            o.token_url = format!("{}/token", ts.url);
        }
        cfg.validate().unwrap();
        let client = new_client_from_config(&cfg, &opts()).unwrap_or_else(|e| panic!("Can't create a client from this config: {cfg:?}: {e}"));
        let resp = client.get(&ts.url).unwrap_or_else(|e| panic!("Can't connect to the test server using this config: {cfg:?}: {e}"));
        assert_eq!(EXPECTED_MESSAGE, body(&resp), "The expected message differs from the obtained message using this config: {cfg:?}");
    }
}

/// adapted: Go decodes the JSON files with encoding/json; here the YAML decoder reads them (JSON is YAML).
fn load_http_config_json_file(f: &str) -> Result<HttpClientConfig, String> {
    let b = std::fs::read_to_string(f).map_err(|e| e.to_string())?;
    yaml::unmarshal::<HttpClientConfig>(&b).map_err(|e| e.to_string())
}

#[test]
fn test_proxy_configuration() {
    setup();
    let cases: [(&str, bool, bool); 4] = [
        ("testdata/http.conf.proxy-headers.good.yml", false, true),
        ("testdata/http.conf.proxy-headers.bad.yml", false, false),
        ("testdata/http.conf.proxy-headers.good.json", true, true),
        ("testdata/http.conf.proxy-headers.bad.json", true, false),
    ];
    for (f, json, valid) in cases {
        let r = if json { load_http_config_json_file(f) } else { load_http_config_file(f).map(|(c, _)| c).map_err(|e| e.to_string()) };
        assert_eq!(r.is_ok(), valid, "{f}: {r:?}");
    }
}

#[test]
fn test_new_client_from_invalid_config() {
    setup();
    let cases = [
        (HttpClientConfig { tls_config: TlsConfig { ca_file: MISSING_CA.into(), insecure_skip_verify: true, ..Default::default() }, ..Default::default() }, format!("unable to read CA cert: unable to read file {MISSING_CA}")),
        (HttpClientConfig { tls_config: TlsConfig { ca_file: INVALID_CA.into(), insecure_skip_verify: true, ..Default::default() }, ..Default::default() }, format!("unable to use specified CA cert file {INVALID_CA}")),
    ];
    for (cfg, msg) in cases {
        match new_client_from_config(&cfg, &opts()) {
            Ok(_) => panic!("A client instance was returned instead of nil using this config: {cfg:?}"),
            Err(e) => assert!(e.msg.contains(&msg), "Expected error {:?} does not contain {:?}", e.msg, msg),
        }
    }
}

#[test]
fn test_custom_dial_context_func() {
    setup();
    let dial: net::transport::DialFunc = Arc::new(|_, _, _| Err(std::io::Error::other(EXPECTED_ERROR)));
    let cfg = HttpClientConfig::default();
    let client = new_client_from_config(&cfg, &opts().with_dial(dial)).unwrap();
    let err = client.get("http://localhost").unwrap_err();
    assert!(err.msg.contains(EXPECTED_ERROR), "Expected error {EXPECTED_ERROR:?} but got {:?}", err.msg);
}

#[test]
fn test_custom_idle_conn_timeout() {
    setup();
    let timeout = Duration::from_secs(5);
    let cfg = HttpClientConfig::default();
    let rt = new_round_tripper_from_config(&cfg, &opts().with_idle_conn_timeout(timeout)).unwrap();
    let t = downcast::<net::Transport>(rt.as_ref()).expect("Unexpected transport");
    assert_eq!(t.opts.idle_conn_timeout, timeout);
}

#[test]
fn test_missing_bearer_auth_file() {
    setup();
    let cfg = HttpClientConfig { bearer_token_file: MISSING_BEARER_TOKEN_FILE.into(), tls_config: good_tls(), ..Default::default() };
    let ts = new_tls_server(bearer_check(expected_bearer()));
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let err = client.get(&ts.url).expect_err("No error is returned here");
    assert!(err.msg.contains("unable to read authorization credentials: unable to read file missing/bearer.token: open missing/bearer.token: no such file or directory"), "wrong error message being returned: {}", err.msg);
}

fn check_rt(check: impl Fn(&Request) + Send + Sync + 'static) -> Arc<dyn RoundTripper> {
    Arc::new(FnRoundTripper(move |r: Request| {
        check(&r);
        Ok(Response { status: 200, status_text: "200 OK".into(), header: net::Header::new(), body: Vec::new(), request: r })
    }))
}

#[test]
fn test_bearer_auth_round_tripper() {
    setup();
    let fake = check_rt(|req| {
        let bearer = req.header.get_str("Authorization");
        assert_eq!(bearer, expected_bearer(), "The expected Bearer Authorization differs from the obtained Bearer Authorization");
    });
    let rt = AuthorizationCredentialsRoundTripper { auth_type: "Bearer".into(), credentials: Some(new_inline_secret(BEARER_TOKEN)), next: fake.clone() };
    let mut req = Request::new("GET", "/hitchhiker", Vec::new()).unwrap();
    req.header.set("User-Agent", "Douglas Adams mind");
    rt.round_trip(req).unwrap();
    // Should honor already Authorization header set.
    let rt = AuthorizationCredentialsRoundTripper { auth_type: "Bearer".into(), credentials: Some(new_inline_secret("goodbyeandthankyouforthefish")), next: fake };
    let mut req = Request::new("GET", "/hitchhiker", Vec::new()).unwrap();
    req.header.set("Authorization", &expected_bearer());
    rt.round_trip(req).unwrap();
}

#[test]
fn test_bearer_auth_file_round_tripper() {
    setup();
    let fake = check_rt(|req| {
        assert_eq!(req.header.get_str("Authorization"), expected_bearer());
    });
    let rt = AuthorizationCredentialsRoundTripper { auth_type: "Bearer".into(), credentials: Some(Arc::new(FileSecret(BEARER_TOKEN_FILE.into()))), next: fake.clone() };
    let mut req = Request::new("GET", "/hitchhiker", Vec::new()).unwrap();
    req.header.set("User-Agent", "Douglas Adams mind");
    rt.round_trip(req).unwrap();
    let rt = AuthorizationCredentialsRoundTripper { auth_type: "Bearer".into(), credentials: Some(Arc::new(FileSecret(MISSING_BEARER_TOKEN_FILE.into()))), next: fake };
    let mut req = Request::new("GET", "/hitchhiker", Vec::new()).unwrap();
    req.header.set("Authorization", &expected_bearer());
    rt.round_trip(req).unwrap();
}

fn pem_certs(p: &str) -> Vec<Vec<u8>> {
    net::tls::parse_ca_pem(read_file(p).as_bytes()).unwrap().into_iter().map(|c| c.to_vec()).collect()
}

#[test]
fn test_tls_config() {
    setup();
    let cfg = TlsConfig { server_name: "localhost".into(), ..good_tls() };
    let t = net::tls::new_tls_config(&cfg, None).unwrap();
    let expected = net::tls::x509_key_pair(read_file(CLIENT_CERTIFICATE_PATH).as_bytes(), read_file(CLIENT_KEY_NO_PASS_PATH).as_bytes()).unwrap();
    let got = t.client_cert.as_ref().unwrap().get().unwrap();
    assert_eq!(got.cert, expected.cert, "Unexpected client certificate result");
    assert_eq!(got.key.public_key().map(|k| k.to_vec()), expected.key.public_key().map(|k| k.to_vec()));
    let roots: Vec<Vec<u8>> = t.root_cas.as_ref().unwrap().iter().map(|c| c.to_vec()).collect();
    assert_eq!(roots, pem_certs(TLS_CA_CHAIN_PATH), "Unexpected RootCAs result");
    assert_eq!(t.server_name, "localhost");
    assert!(!t.insecure_skip_verify);
    assert_eq!((t.min_version, t.max_version), (0, 0));
}

#[test]
fn test_tls_config_empty() {
    setup();
    let t = net::tls::new_tls_config(&TlsConfig { insecure_skip_verify: true, ..Default::default() }, None).unwrap();
    assert!(t.insecure_skip_verify);
    assert!(t.root_cas.is_none() && t.server_name.is_empty() && t.client_cert.is_none() && t.min_version == 0 && t.max_version == 0);
}

#[test]
fn test_tls_config_invalid_ca() {
    setup();
    let cases = [
        (TlsConfig { ca_file: MISSING_CA.into(), ..Default::default() }, format!("unable to read CA cert: unable to read file {MISSING_CA}")),
        (tls("", MISSING_CERT, CLIENT_KEY_NO_PASS_PATH), format!("unable to read specified client cert: unable to read file {MISSING_CERT}")),
        (tls("", CLIENT_CERTIFICATE_PATH, MISSING_KEY), format!("unable to read specified client key: unable to read file {MISSING_KEY}")),
        (TlsConfig { cert: read_file(CLIENT_CERTIFICATE_PATH), ..tls("", CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH) }, "at most one of cert, cert_file & cert_ref must be configured".to_string()),
        (TlsConfig { key: Secret::new(read_file(CLIENT_KEY_NO_PASS_PATH)), ..tls("", CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH) }, "at most one of key and key_file must be configured".to_string()),
    ];
    for (cfg, msg) in cases {
        match net::tls::new_tls_config(&cfg, None) {
            Ok(_) => panic!("The TLS Config could be created even with this {cfg:?}"),
            Err(e) => assert!(e.contains(&msg), "The expected error should contain {msg}, but got {e}"),
        }
    }
}

fn basic_rt(client: &Client) -> &BasicAuthRoundTripper {
    downcast::<BasicAuthRoundTripper>(client.transport.as_ref()).expect("Error casting to basic auth transport")
}

#[test]
fn test_basic_auth_no_password() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.no-password.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let rt = basic_rt(&client);
    assert_eq!(rt.username.as_ref().unwrap().fetch().unwrap(), "user", "Bad HTTP client username");
    assert!(rt.password.is_none(), "Expected empty HTTP client password");
}

#[test]
fn test_basic_auth_no_username() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.no-username.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let rt = basic_rt(&client);
    assert!(rt.username.is_none(), "Got unexpected username");
    assert_eq!(rt.password.as_ref().unwrap().fetch().unwrap(), "secret", "Unexpected HTTP client password");
}

#[test]
fn test_basic_auth_password_file() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.good.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let rt = basic_rt(&client);
    assert_eq!(rt.username.as_ref().unwrap().fetch().unwrap(), "user", "Bad HTTP client username");
    assert_eq!(rt.password.as_ref().unwrap().fetch().unwrap(), "foobar", "Bad HTTP client password");
}

struct TestSecretManager(BTreeMap<String, String>);
impl SecretManager for TestSecretManager {
    fn fetch(&self, r: &str) -> Result<String, String> {
        self.0.get(r).cloned().ok_or_else(|| format!("unknown secret {r}"))
    }
}

#[test]
fn test_basic_auth_secret_manager() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.ref.yaml").unwrap();
    let m = TestSecretManager([("admin".to_string(), "user".to_string()), ("pass".to_string(), "foobar".to_string())].into_iter().collect());
    let client = new_client_from_config(&cfg, &opts().with_secret_manager(Arc::new(m))).unwrap();
    let rt = basic_rt(&client);
    assert_eq!(rt.username.as_ref().unwrap().fetch().unwrap(), "user", "Bad HTTP client username");
    assert_eq!(rt.password.as_ref().unwrap().fetch().unwrap(), "foobar", "Bad HTTP client password");
}

#[test]
fn test_basic_auth_secret_manager_not_found() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.ref.yaml").unwrap();
    let m = TestSecretManager([("admin1".to_string(), "user".to_string()), ("foobar".to_string(), "pass".to_string())].into_iter().collect());
    let client = new_client_from_config(&cfg, &opts().with_secret_manager(Arc::new(m))).unwrap();
    let rt = basic_rt(&client);
    assert!(rt.username.as_ref().unwrap().fetch().unwrap_err().contains("unknown secret admin"));
    assert!(rt.password.as_ref().unwrap().fetch().unwrap_err().contains("unknown secret pass"));
}

#[test]
fn test_basic_username_file() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.basic-auth.username-file.good.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let rt = basic_rt(&client);
    assert_eq!(rt.username.as_ref().unwrap().fetch().unwrap(), "testuser", "Bad HTTP client username");
    assert_eq!(rt.password.as_ref().unwrap().fetch().unwrap(), "foobar", "Bad HTTP client passwordFile");
}

fn write_certificate(src: &str, dst: &str) {
    std::fs::write(dst, std::fs::read(src).unwrap()).unwrap();
}

#[test]
fn test_tls_round_tripper() {
    setup();
    let tmp = temp_dir("tlsroundtripper");
    let (ca, cert, key) = (format!("{tmp}/ca"), format!("{tmp}/cert"), format!("{tmp}/key"));
    let ts = new_tls_server(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    let cases: [(&str, &str, &str, &str); 7] = [
        (TLS_CA_CHAIN_PATH, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH, ""),
        (CLIENT_CERTIFICATE_PATH, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH, "certificate signed by unknown authority"),
        (TLS_CA_CHAIN_PATH, WRONG_CLIENT_CERT_PATH, WRONG_CLIENT_KEY_PATH, "remote error: tls"),
        (EMPTY_FILE, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH, "unable to use specified CA cert"),
        (TLS_CA_CHAIN_PATH, EMPTY_FILE, CLIENT_KEY_NO_PASS_PATH, "failed to find any PEM data in certificate input"),
        (TLS_CA_CHAIN_PATH, CLIENT_CERTIFICATE_PATH, EMPTY_FILE, "failed to find any PEM data in key input"),
        (TLS_CA_CHAIN_PATH, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH, ""),
    ];
    let cfg = HttpClientConfig { tls_config: tls(&ca, &cert, &key), ..Default::default() };
    let mut c: Option<Client> = None;
    for (i, (tca, tcert, tkey, err_msg)) in cases.iter().enumerate() {
        write_certificate(tca, &ca);
        write_certificate(tcert, &cert);
        write_certificate(tkey, &key);
        if c.is_none() {
            c = Some(new_client_from_config(&cfg, &opts()).unwrap_or_else(|e| panic!("Error creating HTTP Client: {e}")));
        }
        let r = c.as_ref().unwrap().do_request(Request::get(&ts.url).unwrap());
        if !err_msg.is_empty() {
            let e = r.expect_err("Could connect to the test server.");
            assert!(e.msg.contains(err_msg), "case {i}: Expected error message to contain {err_msg:?}, got {:?}", e.msg);
            continue;
        }
        let r = r.unwrap_or_else(|e| panic!("case {i}: Can't connect to the test server: {e}"));
        assert_eq!(EXPECTED_MESSAGE, body(&r));
    }
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_tls_round_tripper_inline() {
    setup();
    let ts = new_tls_server(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    let ca_t = read_file(TLS_CA_CHAIN_PATH);
    let cert_t = read_file(CLIENT_CERTIFICATE_PATH);
    let key_t = read_file(CLIENT_KEY_NO_PASS_PATH);
    // (ca text, ca file, cert text, cert file, key text, key file, error)
    let cases: Vec<(String, &str, String, &str, String, &str, &str)> = vec![
        (String::new(), TLS_CA_CHAIN_PATH, String::new(), CLIENT_CERTIFICATE_PATH, String::new(), CLIENT_KEY_NO_PASS_PATH, ""),
        (ca_t.clone(), "", String::new(), CLIENT_CERTIFICATE_PATH, String::new(), CLIENT_KEY_NO_PASS_PATH, ""),
        (String::new(), TLS_CA_CHAIN_PATH, cert_t.clone(), "", String::new(), CLIENT_KEY_NO_PASS_PATH, ""),
        (String::new(), TLS_CA_CHAIN_PATH, String::new(), CLIENT_CERTIFICATE_PATH, key_t.clone(), "", ""),
        (ca_t.clone(), "", cert_t.clone(), "", key_t.clone(), "", ""),
        ("badca".into(), "", cert_t.clone(), "", key_t.clone(), "", "unable to use specified CA cert inline"),
        (ca_t.clone(), "", "badcert".into(), "", key_t.clone(), "", "failed to find any PEM data in certificate input"),
        (ca_t.clone(), "", cert_t.clone(), "", "badkey".into(), "", "failed to find any PEM data in key input"),
    ];
    for (i, (cat, caf, certt, certf, keyt, keyf, err_msg)) in cases.into_iter().enumerate() {
        let cfg = HttpClientConfig { tls_config: TlsConfig { ca: cat, ca_file: caf.into(), cert: certt, cert_file: certf.into(), key: Secret::new(keyt), key_file: keyf.into(), ..Default::default() }, ..Default::default() };
        let c = new_client_from_config(&cfg, &opts());
        if !err_msg.is_empty() {
            let e = c.err().expect("expected an error");
            assert!(e.msg.contains(err_msg), "case {i}: Expected error message to contain {err_msg:?}, got {:?}", e.msg);
            continue;
        }
        let c = c.unwrap_or_else(|e| panic!("Error creating HTTP Client: {e}"));
        let r = c.do_request(Request::get(&ts.url).unwrap()).unwrap_or_else(|e| panic!("case {i}: Can't connect to the test server: {e}"));
        assert_eq!(EXPECTED_MESSAGE, body(&r));
    }
}

#[test]
fn test_tls_round_tripper_races() {
    setup();
    let tmp = temp_dir("tlsroundtripper");
    let (ca, cert, key) = (format!("{tmp}/ca"), format!("{tmp}/cert"), format!("{tmp}/key"));
    let ts = new_tls_server(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    let cfg = HttpClientConfig { tls_config: tls(&ca, &cert, &key), ..Default::default() };
    write_certificate(TLS_CA_CHAIN_PATH, &ca);
    write_certificate(CLIENT_CERTIFICATE_PATH, &cert);
    write_certificate(CLIENT_KEY_NO_PASS_PATH, &key);
    let c = Arc::new(new_client_from_config(&cfg, &opts()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicI64::new(0));
    let ok = Arc::new(AtomicI64::new(0));
    let mut hs = Vec::new();
    for _ in 0..10 {
        let (c, stop, total, ok, url) = (c.clone(), stop.clone(), total.clone(), ok.clone(), ts.url.clone());
        hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                total.fetch_add(1, Ordering::SeqCst);
                if c.get(&url).is_ok() {
                    ok.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for i in 0..=100 {
        std::thread::sleep(Duration::from_millis(10));
        if i % 2 == 0 {
            write_certificate(CLIENT_CERTIFICATE_PATH, &ca);
        } else {
            write_certificate(TLS_CA_CHAIN_PATH, &ca);
        }
    }
    stop.store(true, Ordering::SeqCst);
    for h in hs {
        h.join().unwrap();
    }
    let (ok, total) = (ok.load(Ordering::SeqCst), total.load(Ordering::SeqCst));
    assert_ne!(ok, total, "Expecting some requests to fail but got {ok}/{total} successful requests");
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_hide_http_client_config_secrets() {
    setup();
    let (c, _) = load_http_config_file("testdata/http.conf.good.yml").unwrap();
    let s = c.to_yaml_string();
    assert!(!s.contains("mysecret"), "http client config's String method reveals authentication credentials.");
}

#[test]
fn test_default_follow_redirect() {
    setup();
    let (cfg, _) = load_http_config_file("testdata/http.conf.good.yml").unwrap();
    assert!(cfg.follow_redirects, "follow_redirects should be true");
}

fn leak_check() -> impl Fn(&SReq) -> SResp {
    |r: &SReq| {
        if !r.header.get_str("Authorization").is_empty() {
            return SResp::status(403, "credentials leaked to cross-host redirect target\n");
        }
        SResp::text(EXPECTED_MESSAGE)
    }
}

#[test]
fn test_cross_host_redirect_drops_credentials() {
    setup();
    let cases = [
        HttpClientConfig { follow_redirects: true, authorization: Some(Authorization { type_: "Bearer".into(), credentials: Secret::new("secret-token"), ..Default::default() }), ..Default::default() },
        HttpClientConfig { follow_redirects: true, basic_auth: Some(BasicAuth { username: "user".into(), password: Secret::new("pass"), ..Default::default() }), ..Default::default() },
    ];
    for cfg in cases {
        let target = new_server(leak_check());
        let target_localhost = format!("http://localhost:{}", target.port);
        let origin = new_server(move |r: &SReq| SResp::redirect(&format!("{}{}", target_localhost, r.path)));
        let client = new_client_from_config(&cfg, &opts()).unwrap();
        let resp = client.get(&origin.url).unwrap();
        assert_eq!(EXPECTED_MESSAGE, body(&resp));
    }
}

#[test]
fn test_is_domain_or_subdomain() {
    let cases = [
        ("example.com", "example.com", true),
        ("sub.example.com", "example.com", true),
        ("deep.sub.example.com", "example.com", true),
        ("notexample.com", "example.com", false),
        ("example.com", "sub.example.com", false),
        ("bar.com", "foo.com", false),
        ("127.0.0.1", "127.0.0.1", true),
        ("localhost", "127.0.0.1", false),
        ("127.0.0.1", "localhost", false),
        ("::1", "::1", true),
        ("::2", "::1", false),
        ("::1", "example.com", false),
        ("::1%.www.example.com", "example.com", false),
        ("fe80::1%eth0", "eth0", false),
        ("example.com", "", false),
        ("", "", false),
        ("sub.example.com.", "example.com.", true),
        ("sub.example.com.", "example.com", false),
        ("Sub.Example.Com", "example.com", false),
    ];
    for (sub, parent, want) in cases {
        assert_eq!(want, is_domain_or_subdomain(sub, parent), "{sub}→{parent}");
    }
}

fn bearer_cfg() -> HttpClientConfig {
    HttpClientConfig { follow_redirects: true, authorization: Some(Authorization { type_: "Bearer".into(), credentials: Secret::new("secret-token"), ..Default::default() }), ..Default::default() }
}

#[test]
fn test_same_host_redirect_keeps_credentials() {
    setup();
    let seen = Arc::new(AtomicBool::new(false));
    let s2 = seen.clone();
    let server = new_server(move |r: &SReq| match r.path.as_str() {
        "/start" => SResp::redirect("/end"),
        _ => {
            if !r.header.get_str("Authorization").is_empty() {
                s2.store(true, Ordering::SeqCst);
            }
            SResp::text(EXPECTED_MESSAGE)
        }
    });
    let client = new_client_from_config(&bearer_cfg(), &opts()).unwrap();
    let resp = client.get(&format!("{}/start", server.url)).unwrap();
    assert_eq!(EXPECTED_MESSAGE, body(&resp));
    assert!(seen.load(Ordering::SeqCst), "credentials should be forwarded on same-host redirect");
}

#[test]
fn test_redirect_back_to_original_host_keeps_credentials_stripped() {
    setup();
    let seen = Arc::new(AtomicBool::new(false));
    let s2 = seen.clone();
    let fin = new_server(move |r: &SReq| {
        if !r.header.get_str("Authorization").is_empty() {
            s2.store(true, Ordering::SeqCst);
        }
        SResp::text(EXPECTED_MESSAGE)
    });
    let fin_url = fin.url.clone();
    let middle = new_server(move |_: &SReq| SResp::redirect(&fin_url));
    let middle_localhost = format!("http://localhost:{}", middle.port);
    let origin = new_server(move |_: &SReq| SResp::redirect(&middle_localhost));
    let client = new_client_from_config(&bearer_cfg(), &opts()).unwrap();
    client.get(&origin.url).unwrap();
    assert!(!seen.load(Ordering::SeqCst), "credentials must stay stripped after leaving the original host, even when redirected back to it");
}

#[test]
fn test_round_tripper_cross_host_redirect_drops_credentials() {
    setup();
    let target = new_server(leak_check());
    let target_localhost = format!("http://localhost:{}", target.port);
    let origin = new_server(move |r: &SReq| SResp::redirect(&format!("{}{}", target_localhost, r.path)));
    let rt = new_round_tripper_from_config(&bearer_cfg(), &opts()).unwrap();
    let client = Client::new(rt);
    let resp = client.get(&origin.url).unwrap();
    assert_eq!(EXPECTED_MESSAGE, body(&resp));
}

#[test]
fn test_o_auth2_cross_host_redirect_drops_credentials() {
    setup();
    let target = new_server(leak_check());
    let target_localhost = format!("http://localhost:{}", target.port);
    let origin_auth = Arc::new(Mutex::new(String::new()));
    let oa = origin_auth.clone();
    let origin = new_server(move |r: &SReq| {
        *oa.lock().unwrap() = r.header.get_str("Authorization").to_string();
        SResp::redirect(&format!("{}{}", target_localhost, r.path))
    });
    let token = new_server(|_: &SReq| SResp::json(r#"{"access_token":"oauth2-secret-token","token_type":"Bearer"}"#));
    let cfg = HttpClientConfig { follow_redirects: true, oauth2: Some(OAuth2 { client_id: "testclient".into(), token_url: format!("{}/token", token.url), ..Default::default() }), ..Default::default() };
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    let resp = client.get(&origin.url).unwrap();
    assert_eq!(EXPECTED_MESSAGE, body(&resp));
    assert!(!origin_auth.lock().unwrap().is_empty(), "OAuth2 Bearer token must be present on the initial request to origin");
}

#[test]
fn test_multi_hop_cross_host_redirect_drops_credentials() {
    setup();
    let final_auth = Arc::new(Mutex::new(String::new()));
    let fa = final_auth.clone();
    let fin = new_server(move |r: &SReq| {
        *fa.lock().unwrap() = r.header.get_str("Authorization").to_string();
        SResp::text(EXPECTED_MESSAGE)
    });
    let final_url = format!("http://localhost:{}/", fin.port);
    let hop_auth = Arc::new(Mutex::new(String::new()));
    let ha = hop_auth.clone();
    let hop = new_server(move |r: &SReq| {
        *ha.lock().unwrap() = r.header.get_str("Authorization").to_string();
        SResp::redirect(&final_url)
    });
    let hop_url = format!("http://localhost:{}/", hop.port);
    let origin = new_server(move |_: &SReq| SResp::redirect(&hop_url));
    let client = new_client_from_config(&bearer_cfg(), &opts()).unwrap();
    let resp = client.get(&origin.url).unwrap();
    assert_eq!(EXPECTED_MESSAGE, body(&resp));
    assert!(hop_auth.lock().unwrap().is_empty(), "credentials must not reach hop: original host 127.0.0.1 differs from localhost");
    assert!(final_auth.lock().unwrap().is_empty(), "credentials must not reach final: original host 127.0.0.1 still differs from localhost");
}

#[test]
fn test_port_change_redirect_keeps_credentials() {
    setup();
    let seen = Arc::new(AtomicBool::new(false));
    let s2 = seen.clone();
    let target = new_server(move |r: &SReq| {
        if !r.header.get_str("Authorization").is_empty() {
            s2.store(true, Ordering::SeqCst);
        }
        SResp::text(EXPECTED_MESSAGE)
    });
    let turl = target.url.clone();
    let origin = new_server(move |_: &SReq| SResp::redirect(&turl));
    let client = new_client_from_config(&bearer_cfg(), &opts()).unwrap();
    let resp = client.get(&origin.url).unwrap();
    assert_eq!(EXPECTED_MESSAGE, body(&resp));
    assert!(seen.load(Ordering::SeqCst), "credentials must be forwarded when only the port changes");
}

#[test]
fn test_validate_http_config() {
    setup();
    let (mut cfg, _) = load_http_config_file("testdata/http.conf.good.yml").unwrap();
    cfg.validate().unwrap();
}

#[test]
fn test_invalid_http_configs() {
    setup();
    for (f, msg) in INVALID_HTTP_CLIENT_CONFIGS {
        match load_http_config_file(f) {
            Ok(_) => panic!("Expected error with config {f:?} but got none"),
            Err(e) => assert!(e.to_string().contains(msg), "Expected error for invalid HTTP client configuration to contain {msg:?} but got: {e}"),
        }
    }
}

struct OAuthServer {
    token: TestServer,
    ts: TestServer,
    calls: Arc<AtomicI64>,
}

/// Go `newTestOAuthServer`: the token endpoint checks the client auth (Basic header or JWT assertion).
fn new_test_oauth_server(expected_auth: impl Fn(&str) + Send + Sync + 'static) -> OAuthServer {
    let previous = Arc::new(Mutex::new(String::new()));
    let calls = Arc::new(AtomicI64::new(0));
    let c2 = calls.clone();
    let token = new_server(move |r: &SReq| {
        c2.fetch_add(1, Ordering::SeqCst);
        let mut auth = r.header.get_str("Authorization").to_string();
        if auth.is_empty() {
            auth = r.form_value("assertion");
        }
        expected_auth(&auth);
        let mut p = previous.lock().unwrap();
        assert_ne!(auth, *p, "token endpoint called twice");
        *p = auth;
        SResp::json(r#"{"access_token":"12345","token_type":"Bearer"}"#)
    });
    let ts = new_server(|r: &SReq| {
        let auth = r.header.get_str("Authorization");
        assert_eq!("Bearer 12345", auth, "bad auth, expected Bearer 12345, got {auth}");
        SResp::text("Hello, client\n")
    });
    OAuthServer { token, ts, calls }
}

fn default_transport() -> Arc<dyn RoundTripper> {
    Arc::new(net::Transport::default_transport())
}

#[test]
fn test_o_auth2() {
    setup();
    let expected_auth = Arc::new(Mutex::new(String::new()));
    let ea = expected_auth.clone();
    let srv = new_test_oauth_server(move |auth| {
        let e = ea.lock().unwrap().clone();
        assert_eq!(e, auth, "bad auth, expected {e}, got {auth}");
    });
    let yaml_config = format!("\nclient_id: 1\nclient_secret: 2\nscopes:\n - A\n - B\ntoken_url: {}\nendpoint_params:\n hi: hello\n", srv.token.url);
    let expected = OAuth2 {
        client_id: "1".into(),
        client_secret: Secret::new("2"),
        scopes: vec!["A".into(), "B".into()],
        endpoint_params: [("hi".to_string(), "hello".to_string())].into_iter().collect(),
        token_url: srv.token.url.clone(),
        ..Default::default()
    };
    let unmarshalled: OAuth2 = yaml::unmarshal(&yaml_config).unwrap();
    assert_eq!(unmarshalled, expected);

    let secret = new_inline_secret(expected.client_secret.as_str());
    let rt = Arc::new(net::oauth2::OAuth2RoundTripper::new(Some(secret), &expected, default_transport(), opts()));
    let client = Client::new(rt);

    // Default secret.
    *expected_auth.lock().unwrap() = "Basic MToy".into();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    // Making a second request with the same secret should not re-call the token API.
    client.get(&srv.ts.url).unwrap();

    // Empty secret (the round tripper keeps its own copy of the config and secret, as in Go the token is cached).
    *expected_auth.lock().unwrap() = "Basic MTo=".into();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    client.get(&srv.ts.url).unwrap();

    // Update secret.
    *expected_auth.lock().unwrap() = "Basic MToxMjM0NTY3".into();
    client.get(&srv.ts.url).unwrap();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    assert_eq!(srv.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_o_auth2_user_agent() {
    setup();
    let ts = new_server(|r: &SReq| {
        assert_eq!("myuseragent", r.header.get_str("User-Agent"), "Expected User-Agent header in oauth request to be 'myuseragent'");
        SResp::json(r#"{"access_token":"12345","token_type":"Bearer"}"#)
    });
    let mut config = HttpClientConfig::default_config();
    config.oauth2 = Some(OAuth2 {
        client_id: "1".into(),
        client_secret: Secret::new("2"),
        scopes: vec!["A".into(), "B".into()],
        endpoint_params: [("hi".to_string(), "hello".to_string())].into_iter().collect(),
        token_url: format!("{}/token", ts.url),
        ..Default::default()
    });
    let rt = new_round_tripper_from_config(&config, &opts().with_user_agent("myuseragent")).unwrap();
    let client = Client::new(rt);
    let resp = client.get(&ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
}

#[test]
fn test_o_auth2_dial_context_func() {
    setup();
    let invoked = Arc::new(AtomicBool::new(false));
    let inv = invoked.clone();
    let token = new_server(move |_: &SReq| {
        inv.store(true, Ordering::SeqCst);
        SResp::json(r#"{"access_token":"12345","token_type":"Bearer"}"#)
    });
    let mut config = HttpClientConfig::default_config();
    config.oauth2 = Some(OAuth2 { client_id: "1".into(), client_secret: Secret::new("2"), token_url: format!("{}/token", token.url), ..Default::default() });
    let dial: net::transport::DialFunc = Arc::new(|_, _, _| Err(std::io::Error::other(EXPECTED_ERROR)));
    let rt = new_round_tripper_from_config(&config, &opts().with_dial(dial)).unwrap();
    let client = Client::new(rt);
    let err = client.get(&token.url).unwrap_err();
    assert!(err.msg.contains(EXPECTED_ERROR), "expected error from DialContextFunc, got: {}", err.msg);
    assert!(!invoked.load(Ordering::SeqCst), "OAuth2 token endpoint must not be reached when DialContextFunc blocks");
}

#[test]
fn test_host() {
    setup();
    let ts = new_server(|r: &SReq| {
        assert_eq!("localhost.localdomain", r.host, "Expected Host header in request to be 'localhost.localdomain'");
        let mut s = SResp::text("");
        s.header.add("Content-Type", "application/json");
        s
    });
    let rt = new_round_tripper_from_config(&HttpClientConfig::default_config(), &opts().with_host("localhost.localdomain")).unwrap();
    Client::new(rt).get(&ts.url).unwrap();
}

#[test]
fn test_o_auth2_with_file() {
    setup();
    let expected_auth = Arc::new(Mutex::new(String::new()));
    let ea = expected_auth.clone();
    let srv = new_test_oauth_server(move |auth| {
        let e = ea.lock().unwrap().clone();
        assert_eq!(e, auth, "bad auth, expected {e}, got {auth}");
    });
    let tmp = temp_dir("oauth2_secret");
    let secret_file = format!("{tmp}/oauth2_secret");
    std::fs::write(&secret_file, "").unwrap();
    let yaml_config = format!("\nclient_id: 1\nclient_secret_file: {secret_file}\nscopes:\n - A\n - B\ntoken_url: {}\nendpoint_params:\n hi: hello\n", srv.token.url);
    let expected = OAuth2 {
        client_id: "1".into(),
        client_secret_file: secret_file.clone(),
        scopes: vec!["A".into(), "B".into()],
        endpoint_params: [("hi".to_string(), "hello".to_string())].into_iter().collect(),
        token_url: srv.token.url.clone(),
        ..Default::default()
    };
    let unmarshalled: OAuth2 = yaml::unmarshal(&yaml_config).unwrap();
    assert_eq!(unmarshalled, expected);
    let rt = Arc::new(net::oauth2::OAuth2RoundTripper::new(Some(new_file_secret(&secret_file)), &expected, default_transport(), opts()));
    let client = Client::new(rt);

    // Empty secret file.
    *expected_auth.lock().unwrap() = "Basic MTo=".into();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    client.get(&srv.ts.url).unwrap();

    // File populated.
    *expected_auth.lock().unwrap() = "Basic MToxMjM0NTY=".into();
    std::fs::write(&secret_file, "123456").unwrap();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    client.get(&srv.ts.url).unwrap();

    // Update file.
    *expected_auth.lock().unwrap() = "Basic MToxMjM0NTY3".into();
    std::fs::write(&secret_file, "1234567").unwrap();
    client.get(&srv.ts.url).unwrap();
    let resp = client.get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
    assert_eq!(srv.calls.load(Ordering::SeqCst), 3);
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_o_auth2_with_jwt_auth() {
    setup();
    let srv = new_test_oauth_server(|auth| {
        let parts: Vec<&str> = auth.split('.').collect();
        assert_eq!(parts.len(), 3, "Expected JWT to have 3 parts, got {}", parts.len());
        let mut p = parts[1].replace('-', "+").replace('_', "/");
        while p.len() % 4 != 0 {
            p.push('=');
        }
        let payload = crypto::base64_decode(&p).expect("Failed to decode JWT payload");
        let jwt = json::Json::parse(&String::from_utf8(payload).unwrap()).expect("Failed to unmarshal JWT payload");
        assert_eq!("common-test", jwt.str_of("aud"));
        assert_eq!("A B", jwt.str_of("scope"));
        assert_eq!("common", jwt.str_of("sub"));
        assert_eq!("https://example.com", jwt.str_of("iss"));
        assert_eq!(1, jwt.i64_of("integer"));
    });
    let yaml_config = format!(
        "\ngrant_type: urn:ietf:params:oauth:grant-type:jwt-bearer\nclient_id: 1\nclient_certificate_key_file: {CLIENT_KEY_NO_PASS_PATH}\nscopes:\n - A\n - B\nclaims:\n  iss: \"https://example.com\"\n  aud: common-test\n  sub: common\n  integer: 1\ntoken_url: {}\nendpoint_params:\n  hi: hello\n",
        srv.token.url
    );
    let mut claims = BTreeMap::new();
    claims.insert("iss".to_string(), yaml::Value::Str("https://example.com".into()));
    claims.insert("aud".to_string(), yaml::Value::Str("common-test".into()));
    claims.insert("sub".to_string(), yaml::Value::Str("common".into()));
    claims.insert("integer".to_string(), yaml::Value::Int(1));
    let expected = OAuth2 {
        grant_type: GRANT_TYPE_JWT_BEARER.into(),
        client_id: "1".into(),
        client_certificate_key_file: CLIENT_KEY_NO_PASS_PATH.into(),
        scopes: vec!["A".into(), "B".into()],
        token_url: srv.token.url.clone(),
        endpoint_params: [("hi".to_string(), "hello".to_string())].into_iter().collect(),
        claims,
        ..Default::default()
    };
    let unmarshalled: OAuth2 = yaml::unmarshal(&yaml_config).unwrap();
    assert_eq!(unmarshalled, expected);
    let rt = Arc::new(net::oauth2::OAuth2RoundTripper::new(Some(new_file_secret(&expected.client_certificate_key_file)), &expected, default_transport(), opts()));
    let resp = Client::new(rt).get(&srv.ts.url).unwrap();
    assert_eq!("Bearer 12345", resp.request.header.get_str("Authorization"));
}

#[test]
fn test_marshal_url() {
    let u = Url::parse("http://example.com/").unwrap();
    // JSON half not ported (see the file comment)
    assert_eq!(yaml::marshal(&u).unwrap(), "http://example.com/\n", "URL not properly marshaled in YAML");
}

#[test]
fn test_marshal_url_wrapper_with_nil_value() {
    let u = Url::default();
    assert_eq!(yaml::marshal(&u).unwrap(), "null\n");
}

#[test]
fn test_unmarshal_null_url() {
    let u: Url = yaml::unmarshal("null").unwrap();
    // UnmarshalYAML is not called when parsing null literal.
    assert!(u.0.is_none(), "`null` literal not properly unmarshaled from YAML as URL, got {:?}", u.0);
}

#[test]
fn test_unmarshal_empty_url() {
    let u: Url = yaml::unmarshal(r#""""#).unwrap();
    assert_eq!(u.0, Some(yaml::url::GoUrl::default()), "empty string not properly unmarshaled from YAML as URL");
}

#[test]
fn test_unmarshal_url() {
    let u: Url = yaml::unmarshal(r#""http://example.com/a b""#).unwrap();
    assert_eq!("http://example.com/a%20b", u.to_url_string(), "URL not properly unmarshaled in YAML");
}

#[test]
fn test_marshal_url_with_secret() {
    let u: Url = yaml::unmarshal("http://foo:bar@example.com").unwrap();
    let b = yaml::marshal(&u).unwrap();
    assert_eq!("http://foo:xxxxx@example.com", b.trim(), "URL not properly marshaled in YAML");
}

/// require.YAMLEq: both documents decode to the same value.
fn yaml_eq(expected: &str, actual: &str) {
    let e: yaml::Value = yaml::unmarshal(expected).unwrap();
    let a: yaml::Value = yaml::unmarshal(actual).unwrap();
    assert_eq!(e, a, "YAML differs:\n{expected}\n---\n{actual}");
}

#[test]
fn test_http_client_config_marshal() {
    let proxy_url = Url::parse("http://localhost:8080").unwrap();
    // without HTTP headers
    let config = HttpClientConfig { proxy_config: ProxyConfig { proxy_url: proxy_url.clone(), ..Default::default() }, ..Default::default() };
    let actual_yaml = yaml::marshal(&config).unwrap();
    yaml_eq("\nproxy_url: \"http://localhost:8080\"\nfollow_redirects: false\nenable_http2: false\n", &actual_yaml);
    // Unmarshalling the YAML should get the same struct in input.
    let actual: HttpClientConfig = yaml::unmarshal(&actual_yaml).unwrap();
    assert_eq!(config, actual);
    // with HTTP headers
    let mut headers = BTreeMap::new();
    headers.insert("X-Test".to_string(), Header { values: vec!["Value-1".into(), "Value-2".into()], ..Default::default() });
    let config = HttpClientConfig { proxy_config: ProxyConfig { proxy_url, ..Default::default() }, http_headers: Some(Headers { headers }), ..Default::default() };
    let actual_yaml = yaml::marshal(&config).unwrap();
    yaml_eq("\nproxy_url: \"http://localhost:8080\"\nfollow_redirects: false\nenable_http2: false\nhttp_headers:\n  X-Test:\n    values:\n    - Value-1\n    - Value-2\n", &actual_yaml);
}

#[test]
fn test_o_auth2_proxy() {
    setup();
    load_http_config_file("testdata/http.conf.oauth2-proxy.good.yml").unwrap_or_else(|e| panic!("Error loading OAuth2 client config: {e}"));
}

#[test]
fn test_modify_tls_certificates() {
    setup();
    let tmp = temp_dir("modifytlscertificates");
    let (ca, cert, key) = (format!("{tmp}/ca"), format!("{tmp}/cert"), format!("{tmp}/key"));
    let ts = new_tls_server(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    // (ca, cert, key, error, modification (src, dst))
    let cases = [
        (CLIENT_CERTIFICATE_PATH, CLIENT_CERTIFICATE_PATH, CLIENT_KEY_NO_PASS_PATH, "certificate signed by unknown authority", (TLS_CA_CHAIN_PATH, ca.clone())),
        (TLS_CA_CHAIN_PATH, WRONG_CLIENT_CERT_PATH, CLIENT_KEY_NO_PASS_PATH, "private key does not match public key", (CLIENT_CERTIFICATE_PATH, cert.clone())),
        (TLS_CA_CHAIN_PATH, CLIENT_CERTIFICATE_PATH, WRONG_CLIENT_CERT_PATH, "found a certificate rather than a key in the PEM for the private key", (CLIENT_KEY_NO_PASS_PATH, key.clone())),
    ];
    let cfg = HttpClientConfig { tls_config: tls(&ca, &cert, &key), ..Default::default() };
    let mut c: Option<Client> = None;
    for (i, (tca, tcert, tkey, err_msg, (msrc, mdst))) in cases.iter().enumerate() {
        write_certificate(tca, &ca);
        write_certificate(tcert, &cert);
        write_certificate(tkey, &key);
        if c.is_none() {
            c = Some(new_client_from_config(&cfg, &opts()).unwrap_or_else(|e| panic!("Error creating HTTP Client: {e}")));
        }
        let c = c.as_ref().unwrap();
        let e = c.do_request(Request::get(&ts.url).unwrap()).expect_err("Could connect to the test server.");
        assert!(e.msg.contains(err_msg), "case {i}: Expected error message to contain {err_msg:?}, got {:?}", e.msg);
        write_certificate(msrc, mdst);
        let r = c.do_request(Request::get(&ts.url).unwrap()).unwrap_or_else(|e| panic!("case {i}: Expected no error, got {e}"));
        assert_eq!(EXPECTED_MESSAGE, body(&r));
    }
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_tls_round_tripper_no_ca_configured() {
    setup();
    let tmp = temp_dir("tlsnocacert");
    let (cert, key) = (format!("{tmp}/cert"), format!("{tmp}/key"));
    let ts = new_tls_server(|_: &SReq| SResp::text(EXPECTED_MESSAGE));
    let cfg = HttpClientConfig { tls_config: TlsConfig { cert_file: cert.clone(), key_file: key.clone(), insecure_skip_verify: true, ..Default::default() }, ..Default::default() };
    write_certificate(CLIENT_CERTIFICATE_PATH, &cert);
    write_certificate(CLIENT_KEY_NO_PASS_PATH, &key);
    let c = new_client_from_config(&cfg, &opts()).unwrap();
    c.do_request(Request::get(&ts.url).unwrap()).expect("request should succeed before cert rotation");
    // Rotate the cert/key files to different (but still valid) certs.
    write_certificate(SERVER_CERTIFICATE_PATH, &cert);
    write_certificate(SERVER_KEY_PATH, &key);
    c.do_request(Request::get(&ts.url).unwrap()).expect("request should succeed after cert rotation without ca_file");
    std::fs::remove_dir_all(&tmp).ok();
}

#[test]
fn test_proxy_config_proxy() {
    setup();
    let proxy_server = new_server(|r: &SReq| SResp::text(&format!("Hello, {}", r.path)));
    let purl = proxy_server.url.clone();
    // (name, config, expected proxy, target, HTTP_PROXY, NO_PROXY)
    let cases: Vec<(&str, String, String, &str, String, &str)> = vec![
        ("proxy from environment", "proxy_from_environment: true".into(), purl.clone(), "http://prometheus.io/", purl.clone(), ""),
        ("proxy_from_environment with no_proxy", "proxy_from_environment: true".into(), String::new(), "http://prometheus.io/", purl.clone(), "prometheus.io"),
        ("proxy_from_environment and localhost", "proxy_from_environment: true".into(), String::new(), "http://localhost/", purl.clone(), ""),
        ("valid proxy_url and localhost", format!("proxy_url: {purl}"), purl.clone(), "http://localhost/", String::new(), ""),
        ("valid proxy_url and no_proxy and localhost", format!("proxy_url: {purl}\nno_proxy: prometheus.io"), String::new(), "http://localhost/", String::new(), ""),
        ("valid proxy_url", format!("proxy_url: {purl}"), purl.clone(), "http://prometheus.io/", String::new(), ""),
        ("valid proxy url and no_proxy", format!("proxy_url: {purl}\nno_proxy: prometheus.io"), String::new(), "http://prometheus.io/", String::new(), ""),
        ("valid proxy url and no_proxies", format!("proxy_url: {purl}\nno_proxy: promcon.io,prometheus.io,cncf.io"), String::new(), "http://prometheus.io/", String::new(), ""),
        ("valid proxy url and no_proxies that do not include target", format!("proxy_url: {purl}\nno_proxy: promcon.io,cncf.io"), purl.clone(), "http://prometheus.io/", String::new(), ""),
    ];
    for (name, cfg, expected, target, proxy_env, no_proxy_env) in cases {
        let pc: ProxyConfig = yaml::unmarshal(&cfg).unwrap_or_else(|e| panic!("{name}: failed to unmarshal proxy config: {e}"));
        // SAFETY of the env changes: only this test reads the proxy variables; the old values are restored.
        let old = (std::env::var("HTTP_PROXY").ok(), std::env::var("NO_PROXY").ok());
        unsafe {
            if !proxy_env.is_empty() {
                std::env::set_var("HTTP_PROXY", &proxy_env);
            }
            if !no_proxy_env.is_empty() {
                std::env::set_var("NO_PROXY", no_proxy_env);
            }
        }
        let f = pc.proxy().expect("proxy function");
        let result = f(&yaml::url::parse(target).unwrap()).unwrap_or_else(|e| panic!("{name}: expected no error, but got: {e}"));
        unsafe {
            match &old.0 {
                Some(v) => std::env::set_var("HTTP_PROXY", v),
                None => std::env::remove_var("HTTP_PROXY"),
            }
            match &old.1 {
                Some(v) => std::env::set_var("NO_PROXY", v),
                None => std::env::remove_var("NO_PROXY"),
            }
        }
        if expected.is_empty() {
            assert!(result.is_none(), "{name}: expected no result URL, but got: {:?}", result.map(|u| u.to_url_string()));
        } else {
            let r = result.unwrap_or_else(|| panic!("{name}: expected result URL, but got nil"));
            assert_eq!(r.to_url_string(), expected, "{name}");
        }
    }
}

#[test]
fn test_headers() {
    setup();
    let ts = new_server(|r: &SReq| {
        for (k, v) in [("One", "value1"), ("Two", "value2"), ("Three", "value3")] {
            assert_eq!(r.header.get_str(k), v, "expected {v:?}, got {:?}", r.header.get_str(k));
        }
        SResp::status(204, "")
    });
    let (cfg, _) = load_http_config_file("testdata/http.conf.headers.good.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    client.get(&ts.url).unwrap_or_else(|e| panic!("can't fetch URL: {e}"));
}

#[test]
fn test_multiple_headers() {
    setup();
    let ts = new_server(|r: &SReq| {
        for (k, v) in [("One", ["value1a", "value1b", "value1c"]), ("Two", ["value2a", "value2b", "value2c"]), ("Three", ["value3a", "value3b", "value3c"])] {
            assert_eq!(r.header.values(k), v.to_vec(), "expected {v:?}, got {:?}", r.header.values(k));
        }
        SResp::status(204, "")
    });
    let (cfg, _) = load_http_config_file("testdata/http.conf.headers-multiple.good.yaml").unwrap();
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    client.get(&ts.url).unwrap_or_else(|e| panic!("can't fetch URL: {e}"));
}

#[test]
fn test_load_http_config_file_resolves_paths_relative_to_config_file() {
    setup();
    let ts = new_server(|r: &SReq| {
        assert_eq!("header-value", r.header.get_str("X-Test"));
        SResp::status(204, "")
    });
    let dir = temp_dir("relpaths");
    std::fs::write(format!("{dir}/header-value"), "header-value\n").unwrap();
    std::fs::write(format!("{dir}/http.yml"), "http_headers:\n  X-Test:\n    files: [header-value]\n").unwrap();
    let (cfg, _) = load_http_config_file(&format!("{dir}/http.yml")).unwrap();
    assert_eq!(format!("{dir}/header-value"), cfg.http_headers.as_ref().unwrap().headers["X-Test"].files[0]);
    let client = new_client_from_config(&cfg, &opts()).unwrap();
    client.get(&ts.url).unwrap_or_else(|e| panic!("can't fetch URL: {e}"));
    std::fs::remove_dir_all(&dir).ok();
}

// adapted: Go's cloneRequest guards against shared header maps; Rust round trippers take the request by value,
// so the check is that a layer adding headers never changes the caller's copy.
#[test]
fn test_clone_request() {
    let mut r = Request::new("GET", "http://example.com", Vec::new()).unwrap();
    r.header.set("X-Original", "value");
    let mut r2 = r.clone();
    r2.header.add("X-Added", "value");
    assert_eq!("value", r2.header.get_str("X-Original"), "existing headers must be carried over");
    assert!(r.header.values("X-Added").is_empty(), "the original request must not be modified");
    // clone of a request without a header is usable
    let mut r = Request::new("GET", "http://example.com", Vec::new()).unwrap();
    r.header = net::Header::new();
    let mut r2 = r.clone();
    r2.header.set("X-Added", "value");
    assert!(r.header.is_empty(), "the original request must not be modified");
}
