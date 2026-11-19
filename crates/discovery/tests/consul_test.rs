//! Port of discovery/consul/consul_test.go. Go's httptest servers are the shared `common::TestServer`; Go's
//! `t.Errorf` in a handler becomes an error list that the test checks at its end.
//!
//! adapted: Go's `TestMain` runs `goleak.VerifyTestMain`; there are no goroutines here. Each test joins the
//! discoverer thread after cancelling it instead (`Run` must return).
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use common::{Req, Resp, TestServer};
use discovery::consul::{Discovery, SdConfig, default_sd_config};
use discovery::{CancelToken, Config, DiscovererMetrics, Discoverer, Group, Receiver, Recv, Registry, channel_with_cancel};

/// Go `NewTestMetrics`: refresh metrics on `reg`, the consul metrics on a new registry.
fn new_test_metrics(conf: &SdConfig, reg: &Arc<Registry>) -> Arc<dyn DiscovererMetrics> {
    let refresh_metrics = discovery::new_refresh_metrics(reg);
    refresh_metrics.register().unwrap();
    let metrics = conf.new_discoverer_metrics(&Registry::new(), refresh_metrics);
    metrics.register().unwrap();
    metrics
}

fn new_discovery_nil_logger(conf: &SdConfig) -> Discovery {
    let metrics = new_test_metrics(conf, &Registry::new());
    Discovery::new(conf, discovery::Logger::nop(), Some(metrics)).expect("when initializing discovery")
}

fn tags(t: &[&str]) -> Vec<String> {
    t.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_configured_service() {
    let conf = SdConfig { services: tags(&["configuredServiceName"]), ..Default::default() };
    let d = new_discovery_nil_logger(&conf);
    assert!(d.should_watch("configuredServiceName", &tags(&[""])), "Expected service configuredServiceName to be watched");
    assert!(!d.should_watch("nonConfiguredServiceName", &tags(&[""])), "Expected service nonConfiguredServiceName to not be watched");
}

#[test]
fn test_configured_service_with_tag() {
    let conf = SdConfig { services: tags(&["configuredServiceName"]), service_tags: tags(&["http"]), ..Default::default() };
    let d = new_discovery_nil_logger(&conf);
    assert!(!d.should_watch("configuredServiceName", &tags(&[""])), "Expected service configuredServiceName to not be watched without tag");
    assert!(d.should_watch("configuredServiceName", &tags(&["http"])), "Expected service configuredServiceName to be watched with tag http");
    assert!(!d.should_watch("nonConfiguredServiceName", &tags(&[""])), "Expected service nonConfiguredServiceName to not be watched without tag");
    assert!(!d.should_watch("nonConfiguredServiceName", &tags(&["http"])), "Expected service nonConfiguredServiceName to not be watched with tag http");
}

#[test]
fn test_configured_service_with_tags() {
    struct Case {
        conf: SdConfig,
        service_name: &'static str,
        service_tags: Vec<String>,
        should_watch: bool,
    }
    let conf = |services: &[&str], t: &[&str]| SdConfig { services: tags(services), service_tags: tags(t), ..Default::default() };
    let cases = vec![
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "configuredServiceName", service_tags: tags(&[""]), should_watch: false },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "configuredServiceName", service_tags: tags(&["http", "v1"]), should_watch: true },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "nonConfiguredServiceName", service_tags: tags(&[""]), should_watch: false },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "nonConfiguredServiceName", service_tags: tags(&["http, v1"]), should_watch: false },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "configuredServiceName", service_tags: tags(&["http", "v1", "foo"]), should_watch: true },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1", "foo"]), service_name: "configuredServiceName", service_tags: tags(&["http", "v1", "foo"]), should_watch: true },
        Case { conf: conf(&["configuredServiceName"], &["http", "v1"]), service_name: "configuredServiceName", service_tags: tags(&["http", "v1", "v1"]), should_watch: true },
    ];
    for tc in cases {
        let d = new_discovery_nil_logger(&tc.conf);
        let ret = d.should_watch(tc.service_name, &tc.service_tags);
        assert_eq!(
            tc.should_watch, ret,
            "Watched service and tags: {:?} {:?}, input was {} {:?}",
            tc.conf.services, tc.conf.service_tags, tc.service_name, tc.service_tags
        );
    }
}

#[test]
fn test_non_configured_service() {
    let conf = SdConfig::default();
    let d = new_discovery_nil_logger(&conf);
    assert!(d.should_watch("nonConfiguredServiceName", &tags(&[""])), "Expected service nonConfiguredServiceName to be watched");
}

const AGENT_ANSWER: &str = r#"{"Config": {"Datacenter": "test-dc"}}"#;
const SERVICE_TEST_ANSWER: &str = r#"
[{
	"Node": {
		"ID": "b78c2e48-5ef3-1814-31b8-0d880f50471e",
		"Node": "node1",
		"Address": "1.1.1.1",
		"Datacenter": "test-dc",
		"TaggedAddresses": {
			"lan": "192.168.10.10",
			"wan": "10.0.10.10"
		},
		"Meta": {"rack_name": "2304"},
		"CreateIndex": 1,
		"ModifyIndex": 1
	},
	"Service": {
		"ID": "test",
		"Service": "test",
		"Tags": ["tag1"],
		"Address": "",
		"Meta": {"version":"1.0.0","environment":"staging"},
		"Port": 3341,
		"Weights": {
			"Passing": 1,
			"Warning": 1
		},
		"EnableTagOverride": false,
		"ProxyDestination": "",
		"Proxy": {},
		"Connect": {},
		"CreateIndex": 1,
		"ModifyIndex": 1
	},
	"Checks": [{
		"Node": "node1",
		"CheckID": "serfHealth",
		"Name": "Serf Health Status",
		"Status": "passing"
	}]
}]"#;
const SERVICES_TEST_ANSWER: &str = r#"{"test": ["tag1"], "other": ["tag2"]}"#;

/// Go `t.Errorf` calls made by a handler.
type Errors = Arc<Mutex<Vec<String>>>;

fn check_no_errors(errs: &Errors) {
    let e = errs.lock().unwrap();
    assert!(e.is_empty(), "{:?}", *e);
}

/// Go `newServer`.
fn new_server() -> (TestServer, SdConfig, Errors) {
    let errs: Errors = Arc::default();
    let e2 = errs.clone();
    let stub = TestServer::new(move |r: &Req| {
        let mut delay = None;
        let response = match r.uri.as_str() {
            "/v1/agent/self" => AGENT_ANSWER,
            "/v1/health/service/test?node-meta=rack_name%3A2304&stale=&tag=tag1&wait=120000ms" => SERVICE_TEST_ANSWER,
            "/v1/health/service/test?wait=120000ms" => SERVICE_TEST_ANSWER,
            "/v1/health/service/other?wait=120000ms" => "[]",
            "/v1/catalog/services?node-meta=rack_name%3A2304&stale=&wait=120000ms" => SERVICES_TEST_ANSWER,
            "/v1/catalog/services?wait=120000ms" => SERVICES_TEST_ANSWER,
            "/v1/catalog/services?index=1&node-meta=rack_name%3A2304&stale=&wait=120000ms" => {
                delay = Some(Duration::from_secs(5));
                SERVICES_TEST_ANSWER
            }
            "/v1/catalog/services?index=1&wait=120000ms" => {
                delay = Some(Duration::from_secs(5));
                SERVICES_TEST_ANSWER
            }
            "/v1/catalog/services?filter=NodeMeta.rack_name+%3D%3D+%222304%22&index=1&wait=120000ms" => SERVICES_TEST_ANSWER,
            _ => {
                e2.lock().unwrap().push(format!("Unhandled consul call: {}", r.uri));
                ""
            }
        };
        let resp = Resp::new(200, response).header("X-Consul-Index", "1");
        match delay {
            Some(d) => resp.with_delay(d),
            None => resp,
        }
    });
    let config = SdConfig { server: stub.addr.clone(), token: "fake-token".into(), refresh_interval: yaml::Duration::from_secs(1), ..Default::default() };
    (stub, config, errs)
}

/// Go `newDiscovery`.
fn new_discovery(config: &SdConfig) -> Discovery {
    let metrics = new_test_metrics(config, &Registry::new());
    Discovery::new(config, discovery::Logger::nop(), Some(metrics)).unwrap()
}

/// Go `go func() { d.Run(ctx, ch); close(ch) }()`.
fn run(mut d: Discovery, ctx: &CancelToken) -> (Receiver, JoinHandle<()>) {
    let (tx, rx) = channel_with_cancel(ctx);
    let c = ctx.clone();
    let h = std::thread::spawn(move || d.run(&c, tx));
    (rx, h)
}

/// Go `<-ch` (bounded so that a broken discoverer fails the test instead of hanging it).
fn recv(rx: &Receiver) -> Vec<Arc<Group>> {
    match rx.recv_timeout(Duration::from_secs(30)) {
        Recv::Update(u) => u,
        other => panic!("expected an update, got {other:?}"),
    }
}

fn check_one_target(tg: Vec<Arc<Group>>) {
    assert_eq!(1, tg.len());
    let target = &tg[0];
    let label = |k: &str| target.labels.get(k).cloned().unwrap_or_default();
    assert_eq!("test-dc", label("__meta_consul_dc"));
    assert_eq!(target.source, label("__meta_consul_service"));
    if target.source == "test" {
        // test service should have one node.
        assert!(!target.targets.is_empty(), "Test service should have one node");
    }
}

/// Watch all the services in the catalog.
#[test]
fn test_all_services() {
    let (stub, config, errs) = new_server();
    let d = new_discovery(&config);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    check_one_target(recv(&rx));
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// targetgroup with no targets is emitted if no services were discovered.
#[test]
fn test_no_targets() {
    let (stub, mut config, errs) = new_server();
    config.service_tags = tags(&["missing"]);
    let d = new_discovery(&config);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    let targets = recv(&rx)[0].targets.clone();
    assert!(targets.is_empty());
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// Watch only the test service.
#[test]
fn test_one_service() {
    let (stub, mut config, errs) = new_server();
    config.services = tags(&["test"]);
    let d = new_discovery(&config);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// Watch the test service with a specific tag and node-meta.
#[test]
fn test_all_options() {
    let (stub, mut config, errs) = new_server();
    config.services = tags(&["test"]);
    config.node_meta = BTreeMap::from([("rack_name".to_string(), "2304".to_string())]);
    config.service_tags = tags(&["tag1"]);
    config.allow_stale = true;
    config.token = "fake-token".into();
    let d = new_discovery(&config);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// What the filter tests' handler saw (Go: the captured variables).
#[derive(Default)]
struct Seen {
    catalog_called: bool,
    catalog_filter: String,
    health_called: bool,
    health_filter: String,
}

/// The handler shared by the filter tests.
fn filter_server() -> (TestServer, Arc<Mutex<Seen>>, Errors) {
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let errs: Errors = Arc::default();
    let (s2, e2) = (seen.clone(), errs.clone());
    let stub = TestServer::new(move |r: &Req| {
        let body = match r.path.as_str() {
            "/v1/agent/self" => AGENT_ANSWER,
            "/v1/catalog/services" => {
                let mut s = s2.lock().unwrap();
                s.catalog_called = true;
                s.catalog_filter = r.query_get("filter").to_string();
                r#"{"test": []}"#
            }
            "/v1/health/service/test" => {
                let mut s = s2.lock().unwrap();
                s.health_called = true;
                s.health_filter = r.query_get("filter").to_string();
                SERVICE_TEST_ANSWER
            }
            _ => {
                e2.lock().unwrap().push(format!("Unhandled consul call: {}", r.uri));
                ""
            }
        };
        Resp::new(200, body).header("X-Consul-Index", "1")
    });
    (stub, seen, errs)
}

/// When services and filter are both configured, the Catalog API is still called and receives the filter
/// parameter, while the Health API does not.
#[test]
fn test_filter_option() {
    let (stub, seen, errs) = filter_server();
    let cfg = SdConfig {
        server: stub.addr.clone(),
        services: tags(&["test"]),
        filter: r#"NodeMeta.rack_name == "2304""#.to_string(),
        refresh_interval: yaml::Duration::from_secs(1),
        ..Default::default()
    };
    let d = new_discovery(&cfg);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    {
        // All handler writes happened-before the channel receive above.
        let s = seen.lock().unwrap();
        assert!(s.catalog_called, "Catalog endpoint should be called when filter is set alongside services.");
        assert_eq!(r#"NodeMeta.rack_name == "2304""#, s.catalog_filter, "Catalog should receive the filter parameter.");
        assert!(s.health_called, "Health endpoint should be called.");
        assert!(s.health_filter.is_empty(), "Health endpoint should not receive the catalog filter.");
    }
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// health_filter is passed to the Health API and not to the Catalog API.
#[test]
fn test_health_filter_option() {
    let (stub, seen, errs) = filter_server();
    // No services configured: catalog path is always used, allowing us to assert
    // that health_filter is not forwarded to the Catalog API.
    let cfg = SdConfig {
        server: stub.addr.clone(),
        health_filter: r#"Service.Tags contains "canary""#.to_string(),
        refresh_interval: yaml::Duration::from_secs(1),
        ..Default::default()
    };
    let d = new_discovery(&cfg);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    {
        // All handler writes happened-before the channel receive above.
        let s = seen.lock().unwrap();
        assert!(s.catalog_called, "Catalog endpoint should be called.");
        assert!(s.catalog_filter.is_empty(), "Catalog should not receive the health_filter parameter.");
        assert!(s.health_called, "Health endpoint should be called.");
        assert_eq!(r#"Service.Tags contains "canary""#, s.health_filter, "Health endpoint should receive the health_filter parameter.");
    }
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

/// When both filter and health_filter are configured, each filter is sent only to its own API endpoint.
#[test]
fn test_both_filters_option() {
    let (stub, seen, errs) = filter_server();
    let cfg = SdConfig {
        server: stub.addr.clone(),
        services: tags(&["test"]),
        filter: r#"NodeMeta.rack_name == "2304""#.to_string(),
        health_filter: r#"Service.Tags contains "canary""#.to_string(),
        refresh_interval: yaml::Duration::from_secs(1),
        ..Default::default()
    };
    let d = new_discovery(&cfg);
    let ctx = CancelToken::new();
    let (rx, h) = run(d, &ctx);
    check_one_target(recv(&rx));
    {
        // All handler writes happened-before the channel receive above.
        let s = seen.lock().unwrap();
        assert!(s.catalog_called, "Catalog endpoint should be called when filter is set.");
        assert_eq!(r#"NodeMeta.rack_name == "2304""#, s.catalog_filter, "Catalog should receive only the catalog filter.");
        assert!(s.health_called, "Health endpoint should be called.");
        assert_eq!(r#"Service.Tags contains "canary""#, s.health_filter, "Health endpoint should receive only the health_filter.");
    }
    ctx.cancel();
    h.join().unwrap();
    check_no_errors(&errs);
    drop(stub);
}

#[test]
fn test_get_datacenter_should_return_error() {
    type Handler = Box<dyn Fn(&Req) -> Resp + Send + Sync>;
    let cases: Vec<(Handler, &str)> = vec![
        // A handler that returns status 500.
        (Box::new(|_: &Req| Resp::new(500, "")), "Unexpected response code: 500 ()"),
        // A handler that returns an incorrect response.
        (Box::new(|_: &Req| Resp::new(200, r#"{"Config": {"Not-Datacenter": "test-dc"}}"#)), "invalid value '<nil>' for Config.Datacenter"),
    ];
    for (handler, err_message) in cases {
        let stub = TestServer::new(move |r: &Req| handler(r));
        let config = SdConfig { server: stub.addr.clone(), token: "fake-token".into(), refresh_interval: yaml::Duration::from_secs(1), ..Default::default() };
        let mut d = new_discovery(&config);

        // Should be empty if not initialized.
        assert!(d.client_datacenter().is_empty());

        let err = d.get_datacenter();

        // An error should be returned.
        assert_eq!(Err(err_message.to_string()), err);
        // Should still be empty.
        assert!(d.client_datacenter().is_empty());
    }
}

#[test]
fn test_unmarshal_config() {
    let mut good_config = default_sd_config();
    good_config.username = "123".to_string();
    good_config.password = "1234".into();
    good_config.http_client_config = net::HttpClientConfig {
        basic_auth: Some(net::BasicAuth { username: "123".to_string(), password: "1234".into(), ..Default::default() }),
        follow_redirects: true,
        enable_http2: true,
        ..Default::default()
    };

    struct Case {
        name: &'static str,
        config: &'static str,
        expected: SdConfig,
        err_message: &'static str,
    }
    let cases = vec![
        Case {
            name: "good",
            config: "
server: localhost:8500
username: 123
password: 1234
",
            expected: good_config,
            err_message: "",
        },
        Case {
            name: "username and password and basic auth configured",
            config: "
server: localhost:8500
username: 123
password: 1234
basic_auth:
  username: 12345
  password: 123456
",
            expected: SdConfig::default(),
            err_message: "at most one of consul SD configuration username and password and basic auth can be configured",
        },
        Case {
            name: "token and authorization configured",
            config: "
server: localhost:8500
token: 1234567
authorization:
  credentials: 12345678
",
            expected: SdConfig::default(),
            err_message: "at most one of consul SD token, authorization, or oauth2 can be configured",
        },
        Case {
            name: "token and oauth2 configured",
            config: "
server: localhost:8500
token: 1234567
oauth2:
  client_id: 10
  client_secret: 11
  token_url: http://example.com
",
            expected: SdConfig::default(),
            err_message: "at most one of consul SD token, authorization, or oauth2 can be configured",
        },
    ];

    for test in cases {
        let r: yaml::Result<SdConfig> = yaml::unmarshal(test.config);
        match r {
            Err(err) => assert_eq!(test.err_message, err.to_string(), "{}", test.name),
            Ok(config) => {
                assert!(test.err_message.is_empty(), "{}: Expected error.", test.name);
                assert_eq!(test.expected, config, "{}", test.name);
            }
        }
    }
}

