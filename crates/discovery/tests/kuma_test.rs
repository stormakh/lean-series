//! Port of discovery/xds/kuma_test.go.
mod xds_common;

use std::collections::BTreeMap;
use std::time::Duration;

use discovery::xds::{
    Any, ResourceClient as _, DiscoveryResponse, FetchDiscovery, HttpResourceClient, KUMA_MADS_V1_RESOURCE_TYPE, KUMA_MADS_V1_RESOURCE_TYPE_URL, KumaSdConfig, Message, MonitoringAssignment, MonitoringAssignmentTarget,
    kuma_mads_v1_resource_parser, new_kuma_http_discovery,
};
use discovery::{CancelToken, Config, DiscovererMetrics, Logger, Recv, Registry};
use model::labelset::LabelSet;
use xds_common::{create_test_http_server, sd_conf};

fn kuma_conf() -> KumaSdConfig {
    sd_conf()
}

fn labels(p: &[(&str, &str)]) -> BTreeMap<String, String> {
    p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn target(name: &str, scheme: &str, address: &str, metrics_path: &str, l: &[(&str, &str)]) -> MonitoringAssignmentTarget {
    MonitoringAssignmentTarget { name: name.to_string(), scheme: scheme.to_string(), address: address.to_string(), metrics_path: metrics_path.to_string(), labels: labels(l) }
}

/// Go `testKumaMadsV1Resources`.
fn test_kuma_mads_v1_resources() -> Vec<MonitoringAssignment> {
    vec![
        MonitoringAssignment {
            mesh: "metrics".to_string(),
            service: "prometheus".to_string(),
            targets: vec![
                target("prometheus-01", "http", "10.1.4.32:9090", "/custom-metrics", &[("commit_hash", "620506a88")]),
                target("prometheus-02", "http", "10.1.4.33:9090", "", &[("commit_hash", "3513bba00")]),
            ],
            labels: labels(&[("kuma.io/zone", "us-east-1"), ("team", "infra")]),
        },
        MonitoringAssignment { mesh: "metrics".to_string(), service: "grafana".to_string(), targets: vec![], labels: labels(&[("kuma.io/zone", "us-east-1"), ("team", "infra")]) },
        MonitoringAssignment {
            mesh: "data".to_string(),
            service: "elasticsearch".to_string(),
            targets: vec![target("elasticsearch-01", "http", "10.1.1.1", "", &[("role", "ml")])],
            labels: BTreeMap::new(),
        },
    ]
}

/// Go `getKumaMadsV1DiscoveryResponse`.
fn get_kuma_mads_v1_discovery_response(resources: &[MonitoringAssignment]) -> DiscoveryResponse {
    let serialized = resources.iter().map(|r| Any { type_url: KUMA_MADS_V1_RESOURCE_TYPE_URL.to_string(), value: r.encode_to_vec() }).collect();
    DiscoveryResponse { type_url: KUMA_MADS_V1_RESOURCE_TYPE_URL.to_string(), resources: serialized, ..Default::default() }
}

/// Go `newKumaTestHTTPDiscovery`.
fn new_kuma_test_http_discovery(c: KumaSdConfig) -> Result<FetchDiscovery, String> {
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let metrics = c.new_discoverer_metrics(&reg, refresh_metrics);
    metrics.register()?;
    new_kuma_http_discovery(&c, Logger::nop(), &Some(metrics))
}

fn expected_targets() -> Vec<LabelSet> {
    vec![
        LabelSet::from_pairs(&[
            ("__address__", "10.1.4.32:9090"),
            ("__metrics_path__", "/custom-metrics"),
            ("__scheme__", "http"),
            ("instance", "prometheus-01"),
            ("__meta_kuma_mesh", "metrics"),
            ("__meta_kuma_service", "prometheus"),
            ("__meta_kuma_label_team", "infra"),
            ("__meta_kuma_label_kuma_io_zone", "us-east-1"),
            ("__meta_kuma_label_commit_hash", "620506a88"),
            ("__meta_kuma_dataplane", "prometheus-01"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.1.4.33:9090"),
            ("__metrics_path__", ""),
            ("__scheme__", "http"),
            ("instance", "prometheus-02"),
            ("__meta_kuma_mesh", "metrics"),
            ("__meta_kuma_service", "prometheus"),
            ("__meta_kuma_label_team", "infra"),
            ("__meta_kuma_label_kuma_io_zone", "us-east-1"),
            ("__meta_kuma_label_commit_hash", "3513bba00"),
            ("__meta_kuma_dataplane", "prometheus-02"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.1.1.1"),
            ("__metrics_path__", ""),
            ("__scheme__", "http"),
            ("instance", "elasticsearch-01"),
            ("__meta_kuma_mesh", "data"),
            ("__meta_kuma_service", "elasticsearch"),
            ("__meta_kuma_label_role", "ml"),
            ("__meta_kuma_dataplane", "elasticsearch-01"),
        ]),
    ]
}

#[test]
fn test_kuma_mads_v1_resource_parser_invalid_type_url() {
    let resources: Vec<Any> = Vec::new();
    let r = kuma_mads_v1_resource_parser(&resources, "type.googleapis.com/some.api.v1.Monitoring");
    assert!(r.is_err(), "{r:?}");
}

#[test]
fn test_kuma_mads_v1_resource_parser_empty_slice() {
    let resources: Vec<Any> = Vec::new();
    let groups = kuma_mads_v1_resource_parser(&resources, KUMA_MADS_V1_RESOURCE_TYPE_URL).unwrap();
    assert!(groups.is_empty());
}

#[test]
fn test_kuma_mads_v1_resource_parser_valid_resources() {
    let res = get_kuma_mads_v1_discovery_response(&test_kuma_mads_v1_resources());
    let targets = kuma_mads_v1_resource_parser(&res.resources, KUMA_MADS_V1_RESOURCE_TYPE_URL).unwrap();
    assert_eq!(targets.len(), 3);
    assert_eq!(expected_targets(), targets);
}

#[test]
fn test_kuma_mads_v1_resource_parser_invalid_resources() {
    let data = MonitoringAssignmentTarget::default().to_proto_json().unwrap();
    let resources = vec![Any { type_url: KUMA_MADS_V1_RESOURCE_TYPE_URL.to_string(), value: data.into_bytes() }];
    let r = kuma_mads_v1_resource_parser(&resources, KUMA_MADS_V1_RESOURCE_TYPE_URL);
    let err = r.expect_err("expected an error");
    assert!(err.contains("cannot parse"), "{err}");
}

#[test]
fn test_new_kuma_http_discovery() {
    let kd = new_kuma_test_http_discovery(kuma_conf()).unwrap();
    let res_client = kd.client.as_any().downcast_ref::<HttpResourceClient>().expect("not an HttpResourceClient");
    assert_eq!(kuma_conf().server, res_client.server());
    assert_eq!(KUMA_MADS_V1_RESOURCE_TYPE_URL, res_client.resource_type_url());
    assert_eq!(kuma_conf().client_id, res_client.id());
    assert_eq!(KUMA_MADS_V1_RESOURCE_TYPE, res_client.config.resource_type);
    kd.metrics.unregister();
}

#[test]
fn test_kuma_http_discovery_refresh() {
    let s = create_test_http_server(|request| {
        if request.version_info == "1" {
            return Ok(None);
        }
        let mut res = get_kuma_mads_v1_discovery_response(&test_kuma_mads_v1_resources());
        res.version_info = "1".to_string();
        res.nonce = "abc".to_string();
        Ok(Some(res))
    });

    let cfg_string = format!(
        "
---
server: {}
refresh_interval: 10s
tls_config:
  insecure_skip_verify: true
",
        s.url
    );
    let cfg: KumaSdConfig = yaml::unmarshal(&cfg_string).unwrap();

    let mut kd = new_kuma_test_http_discovery(cfg).unwrap();

    // Go uses a channel with a buffer of 1; the rendezvous channel needs the receiver on another thread.
    let ctx = CancelToken::new();
    let (tx, rx) = discovery::channel();
    let groups = std::thread::scope(|sc| {
        let (kd, ctx) = (&mut kd, &ctx);
        sc.spawn(move || {
            kd.poll(ctx, &tx);
            drop(tx);
        });
        match rx.recv() {
            Recv::Update(g) => g,
            other => panic!("expected an update, got {other:?}"),
        }
    });
    assert_eq!(groups.len(), 1);
    let targets = &groups[0].targets;
    assert_eq!(targets.len(), 3);
    assert_eq!(&expected_targets(), targets);

    // Should skip the next update.
    let ctx = CancelToken::new();
    let c2 = ctx.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        c2.cancel();
    });
    let (tx, rx) = discovery::channel_with_cancel(&ctx);
    kd.poll(&ctx, &tx);
    match rx.recv() {
        Recv::Cancelled => {}
        Recv::Update(_) => panic!("no update expected"),
        other => panic!("unexpected {other:?}"),
    }
    canceller.join().unwrap();

    kd.metrics.unregister();
    s.close();
}
