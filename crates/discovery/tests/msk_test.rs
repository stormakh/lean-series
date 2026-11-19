//! Port of discovery/aws/msk_test.go. Go mocks the Kafka SDK client; here the same data is served as real
//! Kafka (restJson1) responses by a local server, and the discovery is built from a config whose endpoint is
//! that server.
//!
//! adapted: Go sets the mock client on the struct and calls the helper methods directly; here the client is
//! built by `init_msk_client` (which also makes the empty ListClustersV2 credential test call that the mock
//! answers) before the helper methods are called.
mod cloud_mock;

use std::collections::BTreeMap;
use std::sync::Arc;

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::msk::{BrokerNodeInfo, BrokerSoftwareInfo, ControllerNodeInfo, JmxExporterInfo, MskCluster, MskDiscovery, MskMetrics, MskSdConfig, NodeExporterInfo, NodeInfo, OpenMonitoringInfo, PrometheusInfo, Provisioned, node_type};
use discovery::{CancelToken, DiscovererOptions, Group, Registry};
use json::Json;
use model::labelset::LabelSet;

/// Go `mskDataStore`.
#[derive(Clone, Default)]
struct MskDataStore {
    region: String,
    clusters: Vec<MskCluster>,
    /// Keyed by cluster ARN.
    nodes: BTreeMap<String, Vec<NodeInfo>>,
    /// ARNs DescribeClusterV2 answers without any cluster information.
    clusters_without_info: Vec<String>,
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn strs<S: AsRef<str>>(v: &[S]) -> Vec<String> {
    v.iter().map(|x| x.as_ref().to_string()).collect()
}

fn tags(p: &[(&str, &str)]) -> BTreeMap<String, String> {
    p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

// ---------------- the mock server (Go mockMSKClient) ----------------

fn set_opt(j: &mut Json, k: &str, v: &Option<String>) {
    if let Some(v) = v {
        j.set(k, v.as_str());
    }
}

fn enabled(v: Option<bool>) -> Json {
    let mut j = Json::obj();
    if let Some(b) = v {
        j.set("enabledInBroker", b);
    }
    j
}

fn cluster_json(c: &MskCluster) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "clusterArn", &c.cluster_arn);
    set_opt(&mut j, "clusterName", &c.cluster_name);
    if !c.state.is_empty() {
        j.set("state", c.state.as_str());
    }
    if !c.cluster_type.is_empty() {
        j.set("clusterType", c.cluster_type.as_str());
    }
    set_opt(&mut j, "currentVersion", &c.current_version);
    if !c.tags.is_empty() {
        j.set("tags", Json::Obj(c.tags.iter().map(|(k, v)| (k.clone(), Json::from(v.as_str()))).collect()));
    }
    if let Some(p) = &c.provisioned {
        let mut pj = Json::obj();
        if let Some(b) = &p.current_broker_software_info {
            let mut bj = Json::obj();
            set_opt(&mut bj, "configurationArn", &b.configuration_arn);
            if let Some(r) = b.configuration_revision {
                bj.set("configurationRevision", r);
            }
            set_opt(&mut bj, "kafkaVersion", &b.kafka_version);
            pj.set("currentBrokerSoftwareInfo", bj);
        }
        if let Some(om) = &p.open_monitoring {
            let mut omj = Json::obj();
            if let Some(pr) = &om.prometheus {
                let mut prj = Json::obj();
                if let Some(e) = &pr.jmx_exporter {
                    prj.set("jmxExporter", enabled(e.enabled_in_broker));
                }
                if let Some(e) = &pr.node_exporter {
                    prj.set("nodeExporter", enabled(e.enabled_in_broker));
                }
                omj.set("prometheus", prj);
            }
            pj.set("openMonitoring", omj);
        }
        j.set("provisioned", pj);
    }
    j
}

fn node_json(n: &NodeInfo) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "nodeARN", &n.node_arn);
    set_opt(&mut j, "addedToClusterTime", &n.added_to_cluster_time);
    set_opt(&mut j, "instanceType", &n.instance_type);
    if let Some(b) = &n.broker_node_info {
        let mut bj = Json::obj();
        set_opt(&mut bj, "attachedENIId", &b.attached_eni_id);
        if let Some(id) = b.broker_id {
            bj.set("brokerId", id);
        }
        set_opt(&mut bj, "clientSubnet", &b.client_subnet);
        set_opt(&mut bj, "clientVpcIpAddress", &b.client_vpc_ip_address);
        if !b.endpoints.is_empty() {
            bj.set("endpoints", b.endpoints.clone());
        }
        j.set("brokerNodeInfo", bj);
        j.set("nodeType", "BROKER");
    }
    if let Some(c) = &n.controller_node_info {
        let mut cj = Json::obj();
        if !c.endpoints.is_empty() {
            cj.set("endpoints", c.endpoints.clone());
        }
        j.set("controllerNodeInfo", cj);
    }
    j
}

fn unescape_path(p: &str) -> String {
    cloud_mock::parse_form(&format!("x={}", p.replace('+', "%2B"))).remove(0).1
}

/// Appends the request and response to `$MSK_SD_RECORD_FILE` (to record a response set for end-to-end runs).
fn record(req: &MockRequest, body: &str) {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    if let Ok(f) = std::env::var("MSK_SD_RECORD_FILE") {
        use std::io::Write;
        let _g = LOCK.lock().unwrap();
        let entry = format!("### {} {}?{}\n<<< {}\n\n", req.method, req.path, req.query, body);
        std::fs::OpenOptions::new().create(true).append(true).open(f).unwrap().write_all(entry.as_bytes()).unwrap();
    }
}

fn kafka_response(data: &MskDataStore, req: &MockRequest) -> MockResponse {
    let param = |k: &str| req.query_params().into_iter().find(|(n, _)| n == k).map(|(_, v)| v);
    let body = if req.path == "/api/v2/clusters" {
        let name_filter = param("clusterNameFilter").unwrap_or_default();
        let type_filter = param("clusterTypeFilter").unwrap_or_default();
        let out: Vec<Json> = data
            .clusters
            .iter()
            .filter(|c| name_filter.is_empty() || c.cluster_name.as_ref().is_none_or(|n| *n == name_filter))
            .filter(|c| type_filter.is_empty() || c.cluster_type == type_filter)
            .map(cluster_json)
            .collect();
        Json::obj().with("clusterInfoList", Json::Arr(out))
    } else if let Some(rest) = req.path.strip_prefix("/api/v2/clusters/") {
        let arn = unescape_path(rest);
        if data.clusters_without_info.contains(&arn) {
            Json::obj()
        } else {
            match data.clusters.iter().find(|c| c.cluster_arn.as_deref() == Some(&arn)) {
                Some(c) => Json::obj().with("clusterInfo", cluster_json(c)),
                None => {
                    let b = Json::obj().with("message", format!("cluster not found: {arn}")).stringify();
                    return MockResponse::new(404, "application/json", b).header("X-Amzn-Errortype", "NotFoundException");
                }
            }
        }
    } else if let Some(rest) = req.path.strip_prefix("/v1/clusters/").and_then(|r| r.strip_suffix("/nodes")) {
        let arn = unescape_path(rest);
        let nodes: Vec<Json> = data.nodes.get(&arn).map(|n| n.iter().map(node_json).collect()).unwrap_or_default();
        Json::obj().with("nodeInfoList", Json::Arr(nodes))
    } else {
        panic!("unexpected Kafka request {} {}", req.method, req.path);
    };
    let b = body.stringify();
    record(req, &b);
    MockResponse::json(b)
}

fn discovery_for(data: &MskDataStore, port: i64, request_concurrency: i64, clusters: Vec<String>) -> (MockServer, MskDiscovery) {
    let d2 = data.clone();
    let server = MockServer::from_fn(move |req: &MockRequest| kafka_response(&d2, req));
    let cfg = MskSdConfig {
        region: data.region.clone(),
        endpoint: server.url.clone(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: yaml::Secret::new("secret"),
        port,
        request_concurrency,
        clusters,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    let reg = Registry::new();
    let metrics = MskMetrics::new(discovery::new_refresh_metrics(&reg));
    let d = MskDiscovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), set_name: "msk".to_string(), ..Default::default() }).unwrap();
    (server, d)
}

fn initialized(data: &MskDataStore, request_concurrency: i64) -> (MockServer, MskDiscovery) {
    let (server, mut d) = discovery_for(data, 80, request_concurrency, vec![]);
    d.refresher().init_msk_client(&CancelToken::new()).unwrap();
    (server, d)
}

fn cluster(name: &str, arn: &str, cluster_type: &str, version: Option<&str>) -> MskCluster {
    MskCluster { cluster_name: s(name), cluster_arn: s(arn), state: "ACTIVE".into(), cluster_type: cluster_type.into(), current_version: version.map(str::to_string), ..Default::default() }
}

#[test]
fn test_msk_discovery_list_clusters() {
    let c1 = cluster("test-cluster", "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc-123", "PROVISIONED", None);
    let c2 = cluster("prod-cluster", "arn:aws:kafka:us-west-2:123456789012:cluster/prod-cluster/def-456", "PROVISIONED", None);
    let c3 = cluster("single-cluster", "arn:aws:kafka:us-east-1:123456789012:cluster/single-cluster/xyz-789", "PROVISIONED", None);
    let cases = vec![
        ("MultipleClusters", MskDataStore { region: "us-west-2".into(), clusters: vec![c1.clone(), c2.clone()], ..Default::default() }, vec![c1.clone(), c2.clone()]),
        ("SingleCluster", MskDataStore { region: "us-east-1".into(), clusters: vec![c3.clone()], ..Default::default() }, vec![c3.clone()]),
        ("NoClusters", MskDataStore { region: "us-east-1".into(), ..Default::default() }, vec![]),
    ];
    for (name, data, expected) in cases {
        let (_srv, mut d) = initialized(&data, 10);
        let clusters = d.refresher().list_clusters(&CancelToken::new()).unwrap();
        assert_eq!(expected, clusters, "{name}");
    }
}

#[test]
fn test_msk_discovery_describe_clusters() {
    let prov = cluster("provisioned-cluster", "arn:aws:kafka:us-west-2:123456789012:cluster/provisioned-cluster/abc-123", "PROVISIONED", Some("1.2.3"));
    let serverless = cluster("serverless-cluster", "arn:aws:kafka:us-west-2:123456789012:cluster/serverless-cluster/def-456", "SERVERLESS", Some("1.0.0"));
    let mut single = cluster("test-cluster", "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc-123", "PROVISIONED", Some("1.2.3"));
    single.tags = tags(&[("Environment", "production"), ("Team", "platform")]);
    let m1 = cluster("cluster-1", "arn:aws:kafka:us-east-1:123456789012:cluster/cluster-1/xyz-789", "PROVISIONED", None);
    let mut m2 = cluster("cluster-2", "arn:aws:kafka:us-east-1:123456789012:cluster/cluster-2/def-456", "PROVISIONED", None);
    m2.tags = tags(&[("Stage", "prod")]);
    let cases = vec![
        (
            "ServerlessClusterIsSkipped",
            MskDataStore { region: "us-west-2".into(), clusters: vec![prov.clone(), serverless.clone()], ..Default::default() },
            strs(&["arn:aws:kafka:us-west-2:123456789012:cluster/provisioned-cluster/abc-123", "arn:aws:kafka:us-west-2:123456789012:cluster/serverless-cluster/def-456"]),
            vec![prov.clone()],
        ),
        ("SingleCluster", MskDataStore { region: "us-west-2".into(), clusters: vec![single.clone()], ..Default::default() }, strs(&["arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc-123"]), vec![single.clone()]),
        (
            "MultipleClusters",
            MskDataStore { region: "us-east-1".into(), clusters: vec![m1.clone(), m2.clone()], ..Default::default() },
            strs(&["arn:aws:kafka:us-east-1:123456789012:cluster/cluster-1/xyz-789", "arn:aws:kafka:us-east-1:123456789012:cluster/cluster-2/def-456"]),
            vec![m1.clone(), m2.clone()],
        ),
    ];
    for (name, data, arns, mut expected) in cases {
        let (_srv, mut d) = initialized(&data, 10);
        let mut clusters = d.refresher().describe_clusters(&CancelToken::new(), &arns).unwrap();
        // Sort by ARN: the order of concurrent calls is not part of the contract.
        clusters.sort_by(|a, b| a.cluster_arn.cmp(&b.cluster_arn));
        expected.sort_by(|a, b| a.cluster_arn.cmp(&b.cluster_arn));
        assert_eq!(expected, clusters, "{name}");
    }
}

#[allow(clippy::too_many_arguments)]
fn broker(arn: &str, time: Option<&str>, itype: &str, id: f64, subnet: Option<&str>, ip: Option<&str>, endpoints: &[&str], eni: Option<&str>) -> NodeInfo {
    NodeInfo {
        node_arn: s(arn),
        added_to_cluster_time: time.map(str::to_string),
        instance_type: s(itype),
        broker_node_info: Some(BrokerNodeInfo { broker_id: Some(id), client_subnet: subnet.map(str::to_string), client_vpc_ip_address: ip.map(str::to_string), endpoints: strs(endpoints), attached_eni_id: eni.map(str::to_string) }),
        controller_node_info: None,
    }
}

fn controller(arn: &str, time: &str, itype: &str, endpoints: &[&str]) -> NodeInfo {
    NodeInfo { node_arn: s(arn), added_to_cluster_time: s(time), instance_type: s(itype), broker_node_info: None, controller_node_info: Some(ControllerNodeInfo { endpoints: strs(endpoints) }) }
}

fn arn_cluster(arn: &str) -> MskCluster {
    MskCluster { cluster_arn: s(arn), ..Default::default() }
}

#[test]
fn test_msk_discovery_list_nodes() {
    let tc = "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc-123";
    let b1 = broker("arn:aws:kafka:us-west-2:123456789012:node/broker-1", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 1.0, Some("subnet-12345"), Some("10.0.1.100"), &["b-1.test-cluster.abc123.kafka.us-west-2.amazonaws.com"], Some("eni-12345"));
    let b2 = broker("arn:aws:kafka:us-west-2:123456789012:node/broker-2", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 2.0, Some("subnet-67890"), Some("10.0.1.101"), &["b-2.test-cluster.abc123.kafka.us-west-2.amazonaws.com"], Some("eni-67890"));
    let empty = "arn:aws:kafka:us-west-2:123456789012:cluster/empty-cluster/xyz-789";
    let mc1 = "arn:aws:kafka:us-west-2:123456789012:cluster/cluster-1/abc-123";
    let mc2 = "arn:aws:kafka:us-west-2:123456789012:cluster/cluster-2/def-456";
    let n1 = broker("arn:aws:kafka:us-west-2:123456789012:node/broker-1", None, "kafka.m5.large", 1.0, None, None, &[], None);
    let n2 = broker("arn:aws:kafka:us-west-2:123456789012:node/broker-2", None, "kafka.m5.xlarge", 2.0, None, None, &[], None);
    let cases = vec![
        (
            "ClusterWithBrokers",
            BTreeMap::from([(tc.to_string(), vec![b1.clone(), b2.clone()])]),
            vec![arn_cluster(tc)],
            BTreeMap::from([(tc.to_string(), vec![b1.clone(), b2.clone()])]),
        ),
        ("ClusterWithNoNodes", BTreeMap::from([(empty.to_string(), vec![])]), vec![arn_cluster(empty)], BTreeMap::from([(empty.to_string(), vec![])])),
        (
            "MultipleClusters",
            BTreeMap::from([(mc1.to_string(), vec![n1.clone()]), (mc2.to_string(), vec![n2.clone()])]),
            vec![arn_cluster(mc1), arn_cluster(mc2)],
            BTreeMap::from([(mc1.to_string(), vec![n1.clone()]), (mc2.to_string(), vec![n2.clone()])]),
        ),
    ];
    for (name, nodes, clusters, expected) in cases {
        let data = MskDataStore { region: "us-west-2".into(), nodes, ..Default::default() };
        let (_srv, mut d) = initialized(&data, 10);
        let m = d.refresher().list_nodes(&CancelToken::new(), &clusters).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

fn provisioned(cfg_arn: Option<&str>, rev: Option<i64>, kafka: &str, jmx: Option<bool>, node: Option<bool>) -> Option<Provisioned> {
    Some(Provisioned {
        current_broker_software_info: Some(BrokerSoftwareInfo { configuration_arn: cfg_arn.map(str::to_string), configuration_revision: rev, kafka_version: s(kafka) }),
        open_monitoring: match (jmx, node) {
            (None, None) => None,
            _ => Some(OpenMonitoringInfo { prometheus: Some(PrometheusInfo { jmx_exporter: Some(JmxExporterInfo { enabled_in_broker: jmx }), node_exporter: Some(NodeExporterInfo { enabled_in_broker: node }) }) }),
        },
    })
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

fn sorted(mut groups: Vec<Arc<Group>>) -> Vec<Group> {
    groups
        .iter_mut()
        .map(|g| {
            let mut g = (**g).clone();
            g.targets.sort_by_key(|l| l.get("__address__").cloned());
            g
        })
        .collect()
}

#[test]
fn test_msk_discovery_refresh() {
    struct Case {
        name: &'static str,
        data: MskDataStore,
        clusters: Vec<String>,
        region: &'static str,
        expected: Vec<LabelSet>,
    }
    let mut cases = Vec::new();

    // ClusterWithBrokersUsingClustersConfig
    {
        let arn = "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc-123";
        let mut c = cluster("test-cluster", arn, "PROVISIONED", Some("1.2.3"));
        c.tags = tags(&[("Environment", "production"), ("Team", "platform")]);
        c.provisioned = provisioned(Some("arn:aws:kafka:us-west-2:123456789012:configuration/my-config/abc-123"), Some(1), "2.8.1", Some(true), Some(true));
        cases.push(Case {
            name: "ClusterWithBrokersUsingClustersConfig",
            data: MskDataStore {
                region: "us-west-2".into(),
                clusters: vec![c],
                nodes: BTreeMap::from([(arn.to_string(), vec![broker("arn:aws:kafka:us-west-2:123456789012:node/broker-1", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 1.0, Some("subnet-12345"), Some("10.0.1.100"), &["b-1.test-cluster.abc123.kafka.us-west-2.amazonaws.com"], Some("eni-12345"))])]),
                ..Default::default()
            },
            clusters: strs(&[arn]),
            region: "us-west-2",
            expected: vec![ls(&[
                ("__address__", "b-1.test-cluster.abc123.kafka.us-west-2.amazonaws.com:80"),
                ("__meta_msk_cluster_name", "test-cluster"),
                ("__meta_msk_cluster_arn", arn),
                ("__meta_msk_cluster_state", "ACTIVE"),
                ("__meta_msk_cluster_type", "PROVISIONED"),
                ("__meta_msk_cluster_version", "1.2.3"),
                ("__meta_msk_cluster_jmx_exporter_enabled", "true"),
                ("__meta_msk_cluster_configuration_arn", "arn:aws:kafka:us-west-2:123456789012:configuration/my-config/abc-123"),
                ("__meta_msk_cluster_configuration_revision", "1"),
                ("__meta_msk_cluster_kafka_version", "2.8.1"),
                ("__meta_msk_cluster_tag_Environment", "production"),
                ("__meta_msk_cluster_tag_Team", "platform"),
                ("__meta_msk_node_type", "BROKER"),
                ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/broker-1"),
                ("__meta_msk_node_added_time", "2023-01-01T00:00:00Z"),
                ("__meta_msk_node_instance_type", "kafka.m5.large"),
                ("__meta_msk_node_attached_eni", "eni-12345"),
                ("__meta_msk_broker_id", "1"),
                ("__meta_msk_broker_client_subnet", "subnet-12345"),
                ("__meta_msk_broker_client_vpc_ip", "10.0.1.100"),
                ("__meta_msk_broker_node_exporter_enabled", "true"),
                ("__meta_msk_broker_endpoint_index", "0"),
            ])],
        });
    }

    // ServerlessClusterProducesNoTargets
    {
        let arn = "arn:aws:kafka:us-west-2:123456789012:cluster/serverless-cluster/def-456";
        cases.push(Case {
            name: "ServerlessClusterProducesNoTargets",
            data: MskDataStore {
                region: "us-west-2".into(),
                clusters: vec![cluster("serverless-cluster", arn, "SERVERLESS", Some("1.0.0"))],
                nodes: BTreeMap::from([(arn.to_string(), vec![broker("arn:aws:kafka:us-west-2:123456789012:node/broker-2", Some("2023-01-01T00:00:00Z"), "kafka.m5.xlarge", 2.0, Some("subnet-67890"), Some("10.0.2.100"), &["b-2.serverless-cluster.def456.kafka.us-west-2.amazonaws.com"], Some("eni-67890"))])]),
                ..Default::default()
            },
            clusters: strs(&[arn]),
            region: "us-west-2",
            expected: vec![],
        });
    }

    // ProvisionedClusterWithoutOpenMonitoring and ProvisionedClusterWithoutCustomConfiguration
    {
        let arn = "arn:aws:kafka:us-west-2:123456789012:cluster/no-monitoring-cluster/ghi-789";
        let nodes = BTreeMap::from([(arn.to_string(), vec![broker("arn:aws:kafka:us-west-2:123456789012:node/broker-3", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 3.0, Some("subnet-12345"), Some("10.0.3.100"), &["b-3.no-monitoring-cluster.ghi789.kafka.us-west-2.amazonaws.com"], Some("eni-12345"))])]);
        let base = [
            ("__address__", "b-3.no-monitoring-cluster.ghi789.kafka.us-west-2.amazonaws.com:80"),
            ("__meta_msk_cluster_name", "no-monitoring-cluster"),
            ("__meta_msk_cluster_arn", arn),
            ("__meta_msk_cluster_state", "ACTIVE"),
            ("__meta_msk_cluster_type", "PROVISIONED"),
            ("__meta_msk_cluster_version", "1.2.3"),
            ("__meta_msk_cluster_kafka_version", "2.8.1"),
            ("__meta_msk_node_type", "BROKER"),
            ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/broker-3"),
            ("__meta_msk_node_added_time", "2023-01-01T00:00:00Z"),
            ("__meta_msk_node_instance_type", "kafka.m5.large"),
            ("__meta_msk_node_attached_eni", "eni-12345"),
            ("__meta_msk_broker_id", "3"),
            ("__meta_msk_broker_client_subnet", "subnet-12345"),
            ("__meta_msk_broker_client_vpc_ip", "10.0.3.100"),
            ("__meta_msk_broker_endpoint_index", "0"),
        ];
        let mut c = cluster("no-monitoring-cluster", arn, "PROVISIONED", Some("1.2.3"));
        c.provisioned = provisioned(Some("arn:aws:kafka:us-west-2:123456789012:configuration/my-config/ghi-789"), Some(2), "2.8.1", None, None);
        let mut l1 = ls(&base);
        l1.insert("__meta_msk_cluster_configuration_arn".into(), "arn:aws:kafka:us-west-2:123456789012:configuration/my-config/ghi-789".into());
        l1.insert("__meta_msk_cluster_configuration_revision".into(), "2".into());
        cases.push(Case { name: "ProvisionedClusterWithoutOpenMonitoring", data: MskDataStore { region: "us-west-2".into(), clusters: vec![c], nodes: nodes.clone(), ..Default::default() }, clusters: strs(&[arn]), region: "us-west-2", expected: vec![l1] });

        let mut c = cluster("no-monitoring-cluster", arn, "PROVISIONED", Some("1.2.3"));
        c.provisioned = provisioned(None, None, "2.8.1", Some(true), Some(true));
        let mut l2 = ls(&base);
        l2.insert("__meta_msk_cluster_jmx_exporter_enabled".into(), "true".into());
        l2.insert("__meta_msk_broker_node_exporter_enabled".into(), "true".into());
        cases.push(Case { name: "ProvisionedClusterWithoutCustomConfiguration", data: MskDataStore { region: "us-west-2".into(), clusters: vec![c], nodes, ..Default::default() }, clusters: strs(&[arn]), region: "us-west-2", expected: vec![l2] });
    }

    // NoClustersWithEmptyClustersConfig
    cases.push(Case { name: "NoClustersWithEmptyClustersConfig", data: MskDataStore { region: "us-east-1".into(), ..Default::default() }, clusters: vec![], region: "us-east-1", expected: vec![] });

    // ClusterWithBrokersUsingListClusters
    {
        let arn = "arn:aws:kafka:us-west-2:123456789012:cluster/auto-discovered-cluster/xyz-123";
        let mut c = cluster("auto-discovered-cluster", arn, "PROVISIONED", Some("1.0.0"));
        c.provisioned = provisioned(Some("arn:aws:kafka:us-west-2:123456789012:configuration/config/xyz"), Some(1), "3.3.1", Some(true), Some(true));
        cases.push(Case {
            name: "ClusterWithBrokersUsingListClusters",
            data: MskDataStore {
                region: "us-west-2".into(),
                clusters: vec![c],
                nodes: BTreeMap::from([(arn.to_string(), vec![broker("arn:aws:kafka:us-west-2:123456789012:node/broker-auto", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 1.0, Some("subnet-auto"), Some("10.0.1.200"), &["b-auto.cluster.kafka.us-west-2.amazonaws.com"], Some("eni-auto"))])]),
                ..Default::default()
            },
            clusters: vec![],
            region: "us-west-2",
            expected: vec![ls(&[
                ("__address__", "b-auto.cluster.kafka.us-west-2.amazonaws.com:80"),
                ("__meta_msk_cluster_name", "auto-discovered-cluster"),
                ("__meta_msk_cluster_arn", arn),
                ("__meta_msk_cluster_state", "ACTIVE"),
                ("__meta_msk_cluster_type", "PROVISIONED"),
                ("__meta_msk_cluster_version", "1.0.0"),
                ("__meta_msk_cluster_jmx_exporter_enabled", "true"),
                ("__meta_msk_cluster_configuration_arn", "arn:aws:kafka:us-west-2:123456789012:configuration/config/xyz"),
                ("__meta_msk_cluster_configuration_revision", "1"),
                ("__meta_msk_cluster_kafka_version", "3.3.1"),
                ("__meta_msk_node_type", "BROKER"),
                ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/broker-auto"),
                ("__meta_msk_node_added_time", "2023-01-01T00:00:00Z"),
                ("__meta_msk_node_instance_type", "kafka.m5.large"),
                ("__meta_msk_node_attached_eni", "eni-auto"),
                ("__meta_msk_broker_id", "1"),
                ("__meta_msk_broker_client_subnet", "subnet-auto"),
                ("__meta_msk_broker_client_vpc_ip", "10.0.1.200"),
                ("__meta_msk_broker_node_exporter_enabled", "true"),
                ("__meta_msk_broker_endpoint_index", "0"),
            ])],
        });
    }

    // ClusterWithBrokersAndControllersUsingClustersConfig
    {
        let arn = "arn:aws:kafka:us-west-2:123456789012:cluster/kraft-cluster/xyz-789";
        let mut c = cluster("kraft-cluster", arn, "PROVISIONED", Some("1.0.0"));
        c.tags = tags(&[("Type", "kraft")]);
        c.provisioned = provisioned(Some("arn:aws:kafka:us-west-2:123456789012:configuration/config/xyz"), Some(2), "3.3.1", Some(true), Some(false));
        let common = [
            ("__meta_msk_cluster_name", "kraft-cluster"),
            ("__meta_msk_cluster_arn", arn),
            ("__meta_msk_cluster_state", "ACTIVE"),
            ("__meta_msk_cluster_type", "PROVISIONED"),
            ("__meta_msk_cluster_version", "1.0.0"),
            ("__meta_msk_cluster_jmx_exporter_enabled", "true"),
            ("__meta_msk_cluster_configuration_arn", "arn:aws:kafka:us-west-2:123456789012:configuration/config/xyz"),
            ("__meta_msk_cluster_configuration_revision", "2"),
            ("__meta_msk_cluster_kafka_version", "3.3.1"),
            ("__meta_msk_cluster_tag_Type", "kraft"),
            ("__meta_msk_node_added_time", "2023-06-01T00:00:00Z"),
            ("__meta_msk_node_instance_type", "kafka.m5.large"),
        ];
        let with = |extra: &[(&str, &str)]| {
            let mut l = ls(&common);
            for (k, v) in extra {
                l.insert(k.to_string(), v.to_string());
            }
            l
        };
        cases.push(Case {
            name: "ClusterWithBrokersAndControllersUsingClustersConfig",
            data: MskDataStore {
                region: "us-west-2".into(),
                clusters: vec![c],
                nodes: BTreeMap::from([(
                    arn.to_string(),
                    vec![
                        broker("arn:aws:kafka:us-west-2:123456789012:node/broker-1", Some("2023-06-01T00:00:00Z"), "kafka.m5.large", 1.0, Some("subnet-abc123"), Some("10.0.2.100"), &["b-1.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com"], Some("eni-broker-1")),
                        broker("arn:aws:kafka:us-west-2:123456789012:node/broker-2", Some("2023-06-01T00:00:00Z"), "kafka.m5.large", 2.0, Some("subnet-abc124"), Some("10.0.2.101"), &["b-2.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com"], Some("eni-broker-2")),
                        controller("arn:aws:kafka:us-west-2:123456789012:node/controller-1", "2023-06-01T00:00:00Z", "kafka.m5.large", &["c-1.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com"]),
                        controller("arn:aws:kafka:us-west-2:123456789012:node/controller-2", "2023-06-01T00:00:00Z", "kafka.m5.large", &["c-2.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com"]),
                    ],
                )]),
                ..Default::default()
            },
            clusters: strs(&[arn]),
            region: "us-west-2",
            expected: vec![
                with(&[
                    ("__address__", "b-1.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com:80"),
                    ("__meta_msk_node_type", "BROKER"),
                    ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/broker-1"),
                    ("__meta_msk_node_attached_eni", "eni-broker-1"),
                    ("__meta_msk_broker_id", "1"),
                    ("__meta_msk_broker_client_subnet", "subnet-abc123"),
                    ("__meta_msk_broker_client_vpc_ip", "10.0.2.100"),
                    ("__meta_msk_broker_node_exporter_enabled", "false"),
                    ("__meta_msk_broker_endpoint_index", "0"),
                ]),
                with(&[
                    ("__address__", "b-2.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com:80"),
                    ("__meta_msk_node_type", "BROKER"),
                    ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/broker-2"),
                    ("__meta_msk_node_attached_eni", "eni-broker-2"),
                    ("__meta_msk_broker_id", "2"),
                    ("__meta_msk_broker_client_subnet", "subnet-abc124"),
                    ("__meta_msk_broker_client_vpc_ip", "10.0.2.101"),
                    ("__meta_msk_broker_node_exporter_enabled", "false"),
                    ("__meta_msk_broker_endpoint_index", "0"),
                ]),
                with(&[
                    ("__address__", "c-1.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com:80"),
                    ("__meta_msk_node_type", "CONTROLLER"),
                    ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/controller-1"),
                    ("__meta_msk_controller_endpoint_index", "0"),
                ]),
                with(&[
                    ("__address__", "c-2.kraft-cluster.xyz789.kafka.us-west-2.amazonaws.com:80"),
                    ("__meta_msk_node_type", "CONTROLLER"),
                    ("__meta_msk_node_arn", "arn:aws:kafka:us-west-2:123456789012:node/controller-2"),
                    ("__meta_msk_controller_endpoint_index", "0"),
                ]),
            ],
        });
    }

    // NodesWithMultipleEndpointsUsingClustersConfig
    {
        let arn = "arn:aws:kafka:us-east-1:123456789012:cluster/multi-endpoint-cluster/abc-999";
        let mut c = cluster("multi-endpoint-cluster", arn, "PROVISIONED", Some("2.0.0"));
        c.provisioned = provisioned(Some("arn:aws:kafka:us-east-1:123456789012:configuration/config/abc"), Some(1), "3.4.0", Some(true), Some(true));
        let common = [
            ("__meta_msk_cluster_name", "multi-endpoint-cluster"),
            ("__meta_msk_cluster_arn", arn),
            ("__meta_msk_cluster_state", "ACTIVE"),
            ("__meta_msk_cluster_type", "PROVISIONED"),
            ("__meta_msk_cluster_version", "2.0.0"),
            ("__meta_msk_cluster_jmx_exporter_enabled", "true"),
            ("__meta_msk_cluster_configuration_arn", "arn:aws:kafka:us-east-1:123456789012:configuration/config/abc"),
            ("__meta_msk_cluster_configuration_revision", "1"),
            ("__meta_msk_cluster_kafka_version", "3.4.0"),
            ("__meta_msk_node_added_time", "2023-08-01T00:00:00Z"),
        ];
        let mut expected = Vec::new();
        for (i, ep) in ["b-3-1.cluster.kafka.us-east-1.amazonaws.com", "b-3-2.cluster.kafka.us-east-1.amazonaws.com", "b-3-3.cluster.kafka.us-east-1.amazonaws.com"].iter().enumerate() {
            let mut l = ls(&common);
            for (k, v) in [
                ("__address__", format!("{ep}:80")),
                ("__meta_msk_node_type", "BROKER".into()),
                ("__meta_msk_node_arn", "arn:aws:kafka:us-east-1:123456789012:node/broker-multi".into()),
                ("__meta_msk_node_instance_type", "kafka.m5.xlarge".into()),
                ("__meta_msk_node_attached_eni", "eni-multi-broker".into()),
                ("__meta_msk_broker_id", "3".into()),
                ("__meta_msk_broker_client_subnet", "subnet-multi-1".into()),
                ("__meta_msk_broker_client_vpc_ip", "10.0.3.50".into()),
                ("__meta_msk_broker_node_exporter_enabled", "true".into()),
                ("__meta_msk_broker_endpoint_index", i.to_string()),
            ] {
                l.insert(k.to_string(), v);
            }
            expected.push(l);
        }
        let cends = ["c-1-1.cluster.kafka.us-east-1.amazonaws.com", "c-1-2.cluster.kafka.us-east-1.amazonaws.com", "c-1-3.cluster.kafka.us-east-1.amazonaws.com", "c-1-4.cluster.kafka.us-east-1.amazonaws.com"];
        for (i, ep) in cends.iter().enumerate() {
            let mut l = ls(&common);
            for (k, v) in [
                ("__address__", format!("{ep}:80")),
                ("__meta_msk_node_type", "CONTROLLER".into()),
                ("__meta_msk_node_arn", "arn:aws:kafka:us-east-1:123456789012:node/controller-multi".into()),
                ("__meta_msk_node_instance_type", "kafka.m5.large".into()),
                ("__meta_msk_controller_endpoint_index", i.to_string()),
            ] {
                l.insert(k.to_string(), v);
            }
            expected.push(l);
        }
        cases.push(Case {
            name: "NodesWithMultipleEndpointsUsingClustersConfig",
            data: MskDataStore {
                region: "us-east-1".into(),
                clusters: vec![c],
                nodes: BTreeMap::from([(
                    arn.to_string(),
                    vec![
                        broker(
                            "arn:aws:kafka:us-east-1:123456789012:node/broker-multi",
                            Some("2023-08-01T00:00:00Z"),
                            "kafka.m5.xlarge",
                            3.0,
                            Some("subnet-multi-1"),
                            Some("10.0.3.50"),
                            &["b-3-1.cluster.kafka.us-east-1.amazonaws.com", "b-3-2.cluster.kafka.us-east-1.amazonaws.com", "b-3-3.cluster.kafka.us-east-1.amazonaws.com"],
                            Some("eni-multi-broker"),
                        ),
                        controller("arn:aws:kafka:us-east-1:123456789012:node/controller-multi", "2023-08-01T00:00:00Z", "kafka.m5.large", &cends),
                    ],
                )]),
                ..Default::default()
            },
            clusters: strs(&[arn]),
            region: "us-east-1",
            expected,
        });
    }

    for c in cases {
        let (_srv, mut d) = discovery_for(&c.data, 80, 10, c.clusters.clone());
        let groups = d.refresh(&CancelToken::new()).unwrap();
        let expected = sorted(vec![Arc::new(Group { targets: c.expected, labels: LabelSet::new(), source: c.region.to_string() })]);
        assert_eq!(expected, sorted(groups), "{}", c.name);
    }
}

#[test]
fn test_node_type() {
    assert_eq!("BROKER", node_type(&NodeInfo { broker_node_info: Some(BrokerNodeInfo::default()), ..Default::default() }), "BrokerNode");
    assert_eq!("CONTROLLER", node_type(&NodeInfo { controller_node_info: Some(ControllerNodeInfo::default()), ..Default::default() }), "ControllerNode");
    assert_eq!("", node_type(&NodeInfo::default()), "UnknownNode");
}

const FULLY_POPULATED_MSK_CLUSTER_ARN: &str = "arn:aws:kafka:region-full:123456789012:cluster/cluster-full/abc-123";

/// Go `mskTestDiscovery`: configured to describe the clusters held by data rather than list them.
fn msk_test_discovery(data: &MskDataStore) -> (MockServer, MskDiscovery) {
    let arns = data.clusters.iter().map(|c| c.cluster_arn.clone().unwrap_or_default()).collect();
    discovery_for(data, 4242, 10, arns)
}

fn fully_populated_msk_cluster() -> MskCluster {
    MskCluster {
        cluster_name: s("cluster-full"),
        cluster_arn: s(FULLY_POPULATED_MSK_CLUSTER_ARN),
        state: "ACTIVE".into(),
        cluster_type: "PROVISIONED".into(),
        current_version: s("1.2.3"),
        provisioned: provisioned(Some("arn:aws:kafka:region-full:123456789012:configuration/config-full/def-456"), Some(1), "3.4.0", Some(true), Some(true)),
        ..Default::default()
    }
}

fn fully_populated_msk_data_store() -> MskDataStore {
    MskDataStore {
        region: "region-full".into(),
        clusters: vec![fully_populated_msk_cluster()],
        nodes: BTreeMap::from([(
            FULLY_POPULATED_MSK_CLUSTER_ARN.to_string(),
            vec![broker("arn:aws:kafka:region-full:123456789012:node/broker-full", Some("2023-01-01T00:00:00Z"), "kafka.m5.large", 1.0, Some("subnet-full"), Some("10.0.0.1"), &["b-1.cluster-full.kafka.region-full.amazonaws.com"], Some("eni-full"))],
        )]),
        ..Default::default()
    }
}

#[test]
fn test_msk_discovery_refresh_fully_populated() {
    let (_srv, mut d) = msk_test_discovery(&fully_populated_msk_data_store());
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    let expected = vec![Arc::new(Group {
        targets: vec![ls(&[
            ("__address__", "b-1.cluster-full.kafka.region-full.amazonaws.com:4242"),
            ("__meta_msk_cluster_name", "cluster-full"),
            ("__meta_msk_cluster_arn", FULLY_POPULATED_MSK_CLUSTER_ARN),
            ("__meta_msk_cluster_state", "ACTIVE"),
            ("__meta_msk_cluster_type", "PROVISIONED"),
            ("__meta_msk_cluster_version", "1.2.3"),
            ("__meta_msk_cluster_kafka_version", "3.4.0"),
            ("__meta_msk_cluster_configuration_arn", "arn:aws:kafka:region-full:123456789012:configuration/config-full/def-456"),
            ("__meta_msk_cluster_configuration_revision", "1"),
            ("__meta_msk_cluster_jmx_exporter_enabled", "true"),
            ("__meta_msk_node_type", "BROKER"),
            ("__meta_msk_node_arn", "arn:aws:kafka:region-full:123456789012:node/broker-full"),
            ("__meta_msk_node_added_time", "2023-01-01T00:00:00Z"),
            ("__meta_msk_node_instance_type", "kafka.m5.large"),
            ("__meta_msk_node_attached_eni", "eni-full"),
            ("__meta_msk_broker_id", "1"),
            ("__meta_msk_broker_client_subnet", "subnet-full"),
            ("__meta_msk_broker_client_vpc_ip", "10.0.0.1"),
            ("__meta_msk_broker_node_exporter_enabled", "true"),
            ("__meta_msk_broker_endpoint_index", "0"),
        ])],
        labels: LabelSet::new(),
        source: "region-full".to_string(),
    })];
    assert_eq!(expected, tgs);
}

#[test]
fn test_msk_discovery_refresh_cluster_nil_optional_fields() {
    type Mutate = fn(&mut MskCluster);
    let cases: Vec<(&str, Mutate, Vec<&str>)> = vec![
        (
            "NilProvisioned",
            |c| c.provisioned = None,
            vec!["__meta_msk_cluster_kafka_version", "__meta_msk_cluster_configuration_arn", "__meta_msk_cluster_configuration_revision", "__meta_msk_cluster_jmx_exporter_enabled", "__meta_msk_broker_node_exporter_enabled"],
        ),
        ("NilCurrentBrokerSoftwareInfo", |c| c.provisioned.as_mut().unwrap().current_broker_software_info = None, vec!["__meta_msk_cluster_kafka_version", "__meta_msk_cluster_configuration_arn", "__meta_msk_cluster_configuration_revision"]),
    ];
    for (name, mutate, absent) in cases {
        let mut data = fully_populated_msk_data_store();
        mutate(&mut data.clusters[0]);
        let (_srv, mut d) = msk_test_discovery(&data);
        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(1, tgs.len(), "{name}");
        assert_eq!(1, tgs[0].targets.len(), "{name}: broker must still be discovered when an optional cluster field is absent");
        let target = &tgs[0].targets[0];
        for label in absent {
            assert!(!target.contains_key(label), "{name}: label {label} sourced from the absent field should be omitted");
        }
        assert_eq!(Some(&"b-1.cluster-full.kafka.region-full.amazonaws.com:4242".to_string()), target.get("__address__"), "{name}");
    }
}

#[test]
fn test_msk_discovery_describe_clusters_nil_cluster_info() {
    let mut data = fully_populated_msk_data_store();
    data.clusters_without_info = vec![FULLY_POPULATED_MSK_CLUSTER_ARN.to_string()];
    let (_srv, mut d) = msk_test_discovery(&data);
    d.refresher().init_msk_client(&CancelToken::new()).unwrap();
    let clusters = d.refresher().describe_clusters(&CancelToken::new(), &strs(&[FULLY_POPULATED_MSK_CLUSTER_ARN])).unwrap();
    assert!(clusters.is_empty(), "a cluster described without information cannot be turned into targets");
}
