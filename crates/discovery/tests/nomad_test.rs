//! Port of discovery/nomad/nomad_test.go. Go's `httptest` server with an `http.ServeMux` is the shared
//! `common::TestServer` with a handler per path (404 otherwise, as the mux).
mod common;

use common::{Req, Resp, TestServer};
use discovery::nomad::{Discovery, SdConfig, default_sd_config};
use discovery::{CancelToken, Config, DiscovererMetrics, DiscovererOptions, Registry};

/// Go `SDMock` with `HandleServicesList` and `HandleServiceHashiCupsGet` (Go `SetupTest`).
fn new_sd_mock() -> TestServer {
    TestServer::new(|r: &Req| match r.path.as_str() {
        "/v1/services" => Resp::new(
            200,
            r#"
		[
			{
				"Namespace": "default",
				"Services": [
				{
					"ServiceName": "hashicups",
					"Tags": [
					"metrics"
					]
				}
				]
			}
		]"#,
        )
        .header("content-type", "application/json; charset=utf-8"),
        "/v1/service/hashicups" => Resp::new(
            200,
            r#"
		[
			{
				"ID": "_nomad-task-6a1d5f0a-7362-3f5d-9baf-5ed438918e50-group-hashicups-hashicups-hashicups_ui",
				"ServiceName": "hashicups",
				"Namespace": "default",
				"NodeID": "d92fdc3c-9c2b-298a-e8f4-c33f3a449f09",
				"Datacenter": "dc1",
				"JobID": "dashboard",
				"AllocID": "6a1d5f0a-7362-3f5d-9baf-5ed438918e50",
				"Tags": [
				"metrics"
				],
				"Address": "127.0.0.1",
				"Port": 30456,
				"CreateIndex": 226,
				"ModifyIndex": 226
			}
		]"#,
        )
        .header("content-type", "application/json; charset=utf-8"),
        _ => Resp::not_found(),
    })
}

#[test]
fn test_configured_service() {
    let test_cases = [
        ("invalid hostname URL", "http://foo.bar:4646", true),
        ("invalid even though accepted by parsing", "foo.bar:4646", true),
        ("valid address URL", "http://172.30.29.23:4646", true),
        ("invalid URL", "172.30.29.23:4646", false),
    ];
    for (name, server, accepted_url) in test_cases {
        let conf = SdConfig { server: server.to_string(), ..Default::default() };
        let reg = Registry::new();
        let refresh_metrics = discovery::new_refresh_metrics(&reg);
        let metrics = conf.new_discoverer_metrics(&reg, refresh_metrics);
        metrics.register().unwrap();

        let r = Discovery::new(&conf, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics.clone()), set_name: "nomad".to_string(), ..Default::default() });
        if accepted_url {
            assert!(r.is_ok(), "{name}: {:?}", r.err());
        } else {
            assert!(r.is_err(), "{name}");
        }
        metrics.unregister();
    }
}

#[test]
fn test_nomad_sd_refresh() {
    let mock = new_sd_mock();
    // Go `SDMock.Endpoint()`.
    let endpoint = yaml::url::parse(&format!("{}/", mock.url)).unwrap();

    let mut cfg = default_sd_config();
    cfg.server = endpoint.to_url_string();

    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics.clone());
    metrics.register().unwrap();

    let mut d = Discovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics.clone()), set_name: "nomad".to_string(), ..Default::default() }).unwrap();

    let tgs = d.refresh(&CancelToken::new()).unwrap();

    assert_eq!(1, tgs.len());

    let tg = &tgs[0];
    assert_eq!(1, tg.targets.len());

    let lbls = common::ls(&[
        ("__address__", "127.0.0.1:30456"),
        ("__meta_nomad_address", "127.0.0.1"),
        ("__meta_nomad_dc", "dc1"),
        ("__meta_nomad_namespace", "default"),
        ("__meta_nomad_node_id", "d92fdc3c-9c2b-298a-e8f4-c33f3a449f09"),
        ("__meta_nomad_service", "hashicups"),
        ("__meta_nomad_service_address", "127.0.0.1"),
        ("__meta_nomad_service_id", "_nomad-task-6a1d5f0a-7362-3f5d-9baf-5ed438918e50-group-hashicups-hashicups-hashicups_ui"),
        ("__meta_nomad_service_port", "30456"),
        ("__meta_nomad_tags", ",metrics,"),
    ]);
    assert_eq!(lbls, tg.targets[0]);

    metrics.unregister();
    refresh_metrics.unregister();
}
