//! Port of discovery/triton/triton_test.go. Go's `httptest.NewTLSServer` is a small HTTPS server on an ephemeral
//! local port here, with the certificate and key of upstream/scrape/testdata (server.cer, server.key: CN
//! localhost, IP SAN 127.0.0.1); the client skips verification as in Go (`InsecureSkipVerify: true`).
mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{new_options, read_upstream};
use discovery::CancelToken;
use discovery::triton::{Discovery, SdConfig};
use model::labelset::LabelSet;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn tls_config(insecure_skip_verify: bool) -> net::TlsConfig {
    net::TlsConfig { insecure_skip_verify, ..Default::default() }
}

fn conf() -> SdConfig {
    SdConfig {
        account: "testAccount".into(),
        role: "container".into(),
        dns_suffix: "triton.example.com".into(),
        endpoint: "127.0.0.1".into(),
        port: 443,
        version: 1,
        refresh_interval: yaml::Duration(1),
        tls_config: tls_config(true),
        ..Default::default()
    }
}

fn badconf() -> SdConfig {
    SdConfig {
        account: "badTestAccount".into(),
        role: "container".into(),
        dns_suffix: "bad.triton.example.com".into(),
        endpoint: "127.0.0.1".into(),
        port: 443,
        version: 1,
        refresh_interval: yaml::Duration(1),
        tls_config: net::TlsConfig { insecure_skip_verify: false, key_file: "shouldnotexist.key".into(), ca_file: "shouldnotexist.ca".into(), cert_file: "shouldnotexist.cert".into(), ..Default::default() },
        ..Default::default()
    }
}

fn groupsconf() -> SdConfig {
    SdConfig { groups: vec!["foo".into(), "bar".into()], ..conf() }
}

fn cnconf() -> SdConfig {
    SdConfig { role: "cn".into(), ..conf() }
}

/// Go `newTritonDiscovery` (logger nil, set name "triton").
fn new_triton_discovery(c: &SdConfig) -> Result<Discovery, String> {
    let (mut opts, _reg) = new_options(c);
    opts.set_name = "triton".to_string();
    Discovery::new(c, opts)
}

#[test]
fn test_triton_sd_new() {
    let conf = conf();
    let mut td = new_triton_discovery(&conf).unwrap();
    let st = td.state();
    // td.client: not optional in Rust.
    let _client: &net::Client = &st.client;
    assert!(!st.interval.is_zero());
    assert_eq!(conf.account, st.sd_config.account);
    assert_eq!(conf.dns_suffix, st.sd_config.dns_suffix);
    assert_eq!(conf.endpoint, st.sd_config.endpoint);
    assert_eq!(conf.port, st.sd_config.port);
}

#[test]
fn test_triton_sd_new_bad_config() {
    let td = new_triton_discovery(&badconf());
    assert!(td.is_err());
}

#[test]
fn test_triton_sd_new_groups_config() {
    let groupsconf = groupsconf();
    let mut td = new_triton_discovery(&groupsconf).unwrap();
    let st = td.state();
    let _client: &net::Client = &st.client;
    assert!(!st.interval.is_zero());
    assert_eq!(groupsconf.account, st.sd_config.account);
    assert_eq!(groupsconf.dns_suffix, st.sd_config.dns_suffix);
    assert_eq!(groupsconf.endpoint, st.sd_config.endpoint);
    assert_eq!(groupsconf.groups, st.sd_config.groups);
    assert_eq!(groupsconf.port, st.sd_config.port);
}

#[test]
fn test_triton_sd_new_cn_config() {
    let cnconf = cnconf();
    let mut td = new_triton_discovery(&cnconf).unwrap();
    let st = td.state();
    let _client: &net::Client = &st.client;
    assert!(!st.interval.is_zero());
    assert_ne!(st.sd_config, SdConfig::default());
    assert_eq!(cnconf.role, st.sd_config.role);
    assert_eq!(cnconf.account, st.sd_config.account);
    assert_eq!(cnconf.dns_suffix, st.sd_config.dns_suffix);
    assert_eq!(cnconf.endpoint, st.sd_config.endpoint);
    assert_eq!(cnconf.port, st.sd_config.port);
}

#[test]
fn test_triton_sd_refresh_no_targets() {
    let tgts = test_triton_sd_refresh(&conf(), "{\"containers\":[]}");
    // Go: require.Nil (a nil slice): no targets.
    assert!(tgts.is_empty());
}

#[test]
fn test_triton_sd_refresh_multiple_targets() {
    let dstr = r#"{"containers":[
		 	{
                                "groups":["foo","bar","baz"],
				"server_uuid":"44454c4c-5000-104d-8037-b7c04f5a5131",
				"vm_alias":"server01",
				"vm_brand":"lx",
				"vm_image_uuid":"7b27a514-89d7-11e6-bee6-3f96f367bee7",
				"vm_uuid":"ad466fbf-46a2-4027-9b64-8d3cdb7e9072"
			},
			{
				"server_uuid":"a5894692-bd32-4ca1-908a-e2dda3c3a5e6",
				"vm_alias":"server02",
				"vm_brand":"kvm",
				"vm_image_uuid":"a5894692-bd32-4ca1-908a-e2dda3c3a5e6",
				"vm_uuid":"7b27a514-89d7-11e6-bee6-3f96f367bee7"
			}]
		}"#;
    let tgts = test_triton_sd_refresh(&conf(), dstr);
    assert_eq!(tgts.len(), 2);
}

#[test]
fn test_triton_sd_refresh_no_server() {
    let mut td = new_triton_discovery(&conf()).unwrap();
    let err = td.refresh(&CancelToken::new()).unwrap_err();
    assert!(err.contains("an error occurred when requesting targets from the discovery endpoint"), "{err}");
}

#[test]
fn test_triton_sd_refresh_cancelled() {
    let mut td = new_triton_discovery(&conf()).unwrap();
    let ctx = CancelToken::new();
    ctx.cancel();
    let err = td.refresh(&ctx).unwrap_err();
    assert!(err.contains("context canceled"), "{err}");
}

#[test]
fn test_triton_sd_refresh_c_ns_uuid_only() {
    let dstr = r#"{"cns":[
		 	{
				"server_uuid":"44454c4c-5000-104d-8037-b7c04f5a5131"
			},
			{
				"server_uuid":"a5894692-bd32-4ca1-908a-e2dda3c3a5e6"
			}]
		}"#;
    let tgts = test_triton_sd_refresh(&cnconf(), dstr);
    assert_eq!(tgts.len(), 2);
}

#[test]
fn test_triton_sd_refresh_c_ns_with_hostname() {
    let dstr = r#"{"cns":[
		 	{
				"server_uuid":"44454c4c-5000-104d-8037-b7c04f5a5131",
				"server_hostname": "server01"
			},
			{
				"server_uuid":"a5894692-bd32-4ca1-908a-e2dda3c3a5e6",
				"server_hostname": "server02"
			}]
		}"#;
    let tgts = test_triton_sd_refresh(&cnconf(), dstr);
    assert_eq!(tgts.len(), 2);
}

/// Go `httptest.NewTLSServer`: answers every request with `body` until dropped.
struct TlsServer {
    url: String,
    addr: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TlsServer {
    fn new(body: String) -> TlsServer {
        let certs: Vec<CertificateDer<'static>> = net::tls::pem_blocks(&read_upstream("scrape/testdata/server.cer")).into_iter().filter(|b| b.type_ == "CERTIFICATE").map(|b| CertificateDer::from(b.der)).collect();
        let key = PrivateKeyDer::try_from(net::tls::pem_blocks(&read_upstream("scrape/testdata/server.key")).remove(0).der).unwrap();
        let cfg = Arc::new(rustls::ServerConfig::builder_with_provider(net::tls::provider()).with_safe_default_protocol_versions().unwrap().with_no_client_auth().with_single_cert(certs, key).unwrap());
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let st = stop.clone();
        let thread = std::thread::spawn(move || {
            for c in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(c) = c else { continue };
                let (cfg, body) = (cfg.clone(), body.clone());
                std::thread::spawn(move || {
                    let _ = c.set_read_timeout(Some(std::time::Duration::from_secs(5)));
                    let Ok(sc) = rustls::ServerConnection::new(cfg) else { return };
                    let mut r = BufReader::new(rustls::StreamOwned::new(sc, c));
                    loop {
                        let mut line = String::new();
                        if r.read_line(&mut line).unwrap_or(0) == 0 || line.trim_end().is_empty() {
                            break;
                        }
                    }
                    let s = r.get_mut();
                    let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = s.write_all(resp.as_bytes());
                    let _ = s.flush();
                    s.conn.send_close_notify();
                    let _ = s.flush();
                });
            }
        });
        TlsServer { url: format!("https://{addr}"), addr, stop, thread: Some(thread) }
    }
}

impl Drop for TlsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.addr);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Go `testTritonSDRefresh`.
fn test_triton_sd_refresh(c: &SdConfig, dstr: &str) -> Vec<LabelSet> {
    let mut td = new_triton_discovery(c).unwrap();
    // fmt.Fprintln(w, dstr)
    let s = TlsServer::new(format!("{dstr}\n"));

    let u = yaml::url::parse(&s.url).unwrap();
    let (host, strport) = (u.hostname().to_string(), u.port().to_string());
    assert!(!host.is_empty());
    assert!(!strport.is_empty());

    let port: i64 = strport.parse().unwrap();
    assert_ne!(port, 0);

    td.state().sd_config.port = port;

    let tgs = td.refresh(&CancelToken::new()).unwrap();
    assert_eq!(tgs.len(), 1);
    let tg = &tgs[0];

    tg.targets.clone()
}
