//! Port of discovery/aws/elasticache_test.go. The Go tests mock the ElastiCache client with SDK structs; here the
//! same data is served as ElastiCache API (awsQuery XML) responses by a local server, and the SDK structs passed
//! to `addServerlessCacheTargets` / `addCacheClusterTargets` are their XML wire form.
mod cloud_mock;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::client::{Client, ELASTICACHE};
use discovery::aws::elasticache::{ElasticacheDiscovery, ElasticacheSdConfig, add_cache_cluster_targets, add_serverless_cache_targets, split_cache_deployment_options};
use discovery::aws::xml::{self, Element, escape};
use discovery::{CancelToken, Config, DiscovererMetrics, DiscovererOptions, Group, Registry};
use model::labelset::LabelSet;

/// An XML element under construction (the wire form of an SDK struct).
#[derive(Clone, Default)]
struct X(String);

impl X {
    fn new() -> X {
        X(String::new())
    }
    fn f(mut self, k: &str, v: impl ToString) -> X {
        self.0.push_str(&format!("<{k}>{}</{k}>", escape(&v.to_string())));
        self
    }
    fn raw(mut self, k: &str, inner: &X) -> X {
        self.0.push_str(&format!("<{k}>{}</{k}>", inner.0));
        self
    }
    fn list(self, k: &str, member: &str, items: &[X]) -> X {
        let inner: String = items.iter().map(|i| format!("<{member}>{}</{member}>", i.0)).collect();
        self.raw(k, &X(inner))
    }
    fn strings(self, k: &str, member: &str, items: &[&str]) -> X {
        let xs: Vec<X> = items.iter().map(|s| X(escape(s))).collect();
        self.list(k, member, &xs)
    }
    /// Drops the field `k` (Go: setting it to nil).
    fn without(self, k: &str) -> X {
        let doc = format!("<r>{}</r>", self.0);
        let root = xml::parse(&doc).unwrap();
        let mut out = X::new();
        for c in &root.children {
            if c.name != k {
                out.0.push_str(&element_xml(c));
            }
        }
        out
    }
    fn element(&self, name: &str) -> Element {
        xml::parse(&format!("<{name}>{}</{name}>", self.0)).unwrap()
    }
}

fn element_xml(e: &Element) -> String {
    let inner: String = if e.children.is_empty() { escape(&e.text) } else { e.children.iter().map(element_xml).collect() };
    format!("<{0}>{inner}</{0}>", e.name)
}

fn endpoint(addr: &str, port: i32) -> X {
    X::new().f("Address", addr).f("Port", port)
}

fn tag(k: &str, v: &str) -> X {
    X::new().f("Key", k).f("Value", v)
}

fn tag_elements(t: &[(&str, &str)]) -> Vec<Element> {
    t.iter().map(|(k, v)| tag(k, v).element("Tag")).collect()
}

const NOW: &str = "2025-06-01T12:00:00Z";
const TEST_TIME: &str = "2024-01-01T00:00:00Z";

#[derive(Clone)]
struct Serverless {
    name: Option<String>,
    xml: X,
}

#[derive(Clone)]
struct Cluster {
    id: Option<String>,
    replication_group: bool,
    xml: X,
}

/// Go `elasticacheDataStore`.
#[derive(Clone, Default)]
struct Data {
    region: String,
    serverless: Vec<Serverless>,
    clusters: Vec<Cluster>,
    tags: HashMap<String, Vec<X>>,
}

fn serverless(xml: X) -> Serverless {
    let name = xml.element("S").text_at("ServerlessCacheName").map(|s| s.to_string());
    Serverless { name, xml }
}

fn cluster(xml: X) -> Cluster {
    let e = xml.element("C");
    Cluster { id: e.text_at("CacheClusterId").map(|s| s.to_string()), replication_group: e.child("ReplicationGroupId").is_some(), xml }
}

fn response(op: &str, inner: &str) -> MockResponse {
    MockResponse::xml(format!(
        "<{op}Response xmlns=\"http://elasticache.amazonaws.com/doc/2015-02-02/\"><{op}Result>{inner}</{op}Result><ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata></{op}Response>"
    ))
}

/// Go `mockElasticacheClient`: serves the data and records one entry per describe call.
#[derive(Clone)]
struct Mock {
    data: Arc<Data>,
    describe_requests: Arc<Mutex<Vec<String>>>,
}

impl Mock {
    fn new(data: Data) -> Mock {
        Mock { data: Arc::new(data), describe_requests: Arc::new(Mutex::new(Vec::new())) }
    }

    fn handle(&self, req: &MockRequest) -> MockResponse {
        let param = |k: &str| req.param(k);
        match param("Action").as_deref() {
            Some("DescribeServerlessCaches") => {
                self.describe_requests.lock().unwrap().push(format!("DescribeServerlessCaches name={} token={}", param("ServerlessCacheName").unwrap_or_default(), param("NextToken").unwrap_or_default()));
                let items: Vec<&Serverless> = match param("ServerlessCacheName") {
                    Some(n) => self.data.serverless.iter().filter(|c| c.name.as_deref() == Some(n.as_str())).take(1).collect(),
                    None => self.data.serverless.iter().collect(),
                };
                let inner: String = items.iter().map(|c| format!("<member>{}</member>", c.xml.0)).collect();
                response("DescribeServerlessCaches", &format!("<ServerlessCaches>{inner}</ServerlessCaches>"))
            }
            Some("DescribeCacheClusters") => {
                let not_in_rg = param("ShowCacheClustersNotInReplicationGroups").as_deref() == Some("true");
                self.describe_requests.lock().unwrap().push(format!("DescribeCacheClusters id={} marker={} notInReplicationGroups={}", param("CacheClusterId").unwrap_or_default(), param("Marker").unwrap_or_default(), not_in_rg));
                let id = param("CacheClusterId");
                let inner: String = self
                    .data
                    .clusters
                    .iter()
                    .filter(|c| id.is_none() || c.id == id)
                    .filter(|c| !(not_in_rg && c.replication_group))
                    .map(|c| format!("<CacheCluster>{}</CacheCluster>", c.xml.0))
                    .collect();
                response("DescribeCacheClusters", &format!("<CacheClusters>{inner}</CacheClusters>"))
            }
            Some("ListTagsForResource") => {
                let tags = param("ResourceName").and_then(|n| self.data.tags.get(&n).cloned()).unwrap_or_default();
                let inner: String = tags.iter().map(|t| format!("<Tag>{}</Tag>", t.0)).collect();
                response("ListTagsForResource", &format!("<TagList>{inner}</TagList>"))
            }
            _ => MockResponse::new(400, "text/xml", "<ErrorResponse><Error><Type>Sender</Type><Code>InvalidAction</Code><Message>unknown action</Message></Error><RequestId>req-1</RequestId></ErrorResponse>"),
        }
    }
}

/// Go `elasticacheTestDiscovery`: a discovery whose client is already set (no credential test call), backed by
/// the mock data.
fn test_discovery(mock: &Mock) -> (ElasticacheDiscovery, MockServer) {
    let m = mock.clone();
    let srv = MockServer::from_fn(move |r| m.handle(r));
    let cfg = ElasticacheSdConfig {
        region: mock.data.region.clone(),
        endpoint: srv.url.clone(),
        access_key: "AKIDTEST".to_string(),
        secret_key: yaml::Secret("secret".to_string()),
        request_concurrency: 10,
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    let reg = Registry::new();
    let rm = discovery::new_refresh_metrics(&reg);
    rm.register().unwrap();
    let metrics = cfg.new_discoverer_metrics(&reg, rm);
    let mut d = ElasticacheDiscovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), ..Default::default() }).unwrap();
    d.set_client(Client::new(&cfg.client_config(), ELASTICACHE, "elasticache_sd").unwrap());
    (d, srv)
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_elasticache_discovery_describe_serverless_caches() {
    let cases = vec![
        (
            "MultipleCaches",
            Data {
                region: "us-west-2".to_string(),
                serverless: vec![
                    serverless(
                        X::new()
                            .f("ServerlessCacheName", "test-cache")
                            .f("ARN", "arn:aws:elasticache:us-west-2:123456789012:serverlesscache:test-cache")
                            .f("Status", "available")
                            .f("Engine", "redis")
                            .f("FullEngineVersion", "7.1")
                            .f("CreateTime", NOW)
                            .raw("Endpoint", &endpoint("test-cache.serverless.use1.cache.amazonaws.com", 6379)),
                    ),
                    serverless(
                        X::new()
                            .f("ServerlessCacheName", "prod-cache")
                            .f("ARN", "arn:aws:elasticache:us-west-2:123456789012:serverlesscache:prod-cache")
                            .f("Status", "available")
                            .f("Engine", "valkey")
                            .f("FullEngineVersion", "7.2")
                            .f("CreateTime", NOW)
                            .raw("Endpoint", &endpoint("prod-cache.serverless.use1.cache.amazonaws.com", 6379)),
                    ),
                ],
                ..Default::default()
            },
            vec![],
            2,
        ),
        (
            "SingleCache",
            Data {
                region: "us-east-1".to_string(),
                serverless: vec![serverless(
                    X::new()
                        .f("ServerlessCacheName", "single-cache")
                        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:single-cache")
                        .f("Status", "available")
                        .f("Engine", "redis")
                        .f("FullEngineVersion", "7.1")
                        .f("CreateTime", NOW),
                )],
                ..Default::default()
            },
            strings(&["single-cache"]),
            1,
        ),
        ("NoCaches", Data { region: "us-east-1".to_string(), ..Default::default() }, vec![], 0),
    ];
    for (name, data, names, expected) in cases {
        let (mut d, _srv) = test_discovery(&Mock::new(data));
        let caches = d.describe_serverless_caches(&CancelToken::new(), &names).unwrap();
        assert_eq!(caches.len(), expected, "{name}");
    }
}

#[test]
fn test_elasticache_discovery_describe_cache_clusters() {
    // adapted: the Go test only checks that its mock returns the stored clusters; here the discovery's
    // describeCacheClusters reads them from the mock server.
    let cases = vec![
        (
            "MockValidation",
            Data {
                region: "us-west-2".to_string(),
                clusters: vec![cluster(
                    X::new()
                        .f("CacheClusterId", "test-cluster-001")
                        .f("ARN", "arn:aws:elasticache:us-west-2:123456789012:cluster:test-cluster-001")
                        .f("CacheClusterStatus", "available")
                        .f("Engine", "redis")
                        .f("EngineVersion", "7.1")
                        .f("CacheNodeType", "cache.t3.micro")
                        .f("NumCacheNodes", 1)
                        .raw("ConfigurationEndpoint", &endpoint("test-cluster.abc123.cfg.use1.cache.amazonaws.com", 6379)),
                )],
                ..Default::default()
            },
            1,
        ),
        ("NoClusters", Data { region: "us-east-1".to_string(), ..Default::default() }, 0),
    ];
    for (name, data, expected) in cases {
        let (mut d, _srv) = test_discovery(&Mock::new(data));
        let clusters = d.describe_cache_clusters(&CancelToken::new(), &[]).unwrap();
        assert_eq!(clusters.len(), expected, "{name}");
    }
}

fn assert_contains_all(name: &str, expected: &[(&str, &str)], got: &LabelSet) {
    for (k, v) in expected {
        let actual = got.get(*k).unwrap_or_else(|| panic!("{name}: label {k} should exist"));
        assert_eq!(v, actual, "{name}: label {k} mismatch");
    }
}

#[test]
fn test_add_serverless_cache_targets() {
    let cache = X::new()
        .f("ServerlessCacheName", "my-cache")
        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-cache")
        .f("Status", "available")
        .f("Engine", "redis")
        .f("FullEngineVersion", "7.1")
        .f("MajorEngineVersion", "7")
        .f("CreateTime", TEST_TIME)
        .raw("Endpoint", &endpoint("my-cache.serverless.use1.cache.amazonaws.com", 6379))
        .raw("ReaderEndpoint", &endpoint("my-cache-ro.serverless.use1.cache.amazonaws.com", 6379))
        .strings("SecurityGroupIds", "SecurityGroupId", &["sg-12345"])
        .strings("SubnetIds", "SubnetId", &["subnet-abcdef"])
        .raw("CacheUsageLimits", &X::new().raw("DataStorage", &X::new().f("Maximum", 10).f("Minimum", 1).f("Unit", "GB")).raw("ECPUPerSecond", &X::new().f("Maximum", 5000).f("Minimum", 1000)));
    let mut tg = Group { source: "test".to_string(), ..Default::default() };
    add_serverless_cache_targets(&mut tg, &cache.element("ServerlessCache"), &tag_elements(&[("Environment", "test")])).unwrap();
    assert_eq!(tg.targets.len(), 1);
    assert_contains_all(
        "ServerlessCacheWithEndpoint",
        &[
            ("__meta_elasticache_deployment_option", "serverless"),
            ("__meta_elasticache_serverless_cache_arn", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-cache"),
            ("__meta_elasticache_serverless_cache_name", "my-cache"),
            ("__meta_elasticache_serverless_cache_status", "available"),
            ("__meta_elasticache_serverless_cache_engine", "redis"),
            ("__meta_elasticache_serverless_cache_full_engine_version", "7.1"),
            ("__meta_elasticache_serverless_cache_major_engine_version", "7"),
            ("__meta_elasticache_serverless_cache_create_time", "2024-01-01T00:00:00Z"),
            ("__meta_elasticache_serverless_cache_endpoint_address", "my-cache.serverless.use1.cache.amazonaws.com"),
            ("__meta_elasticache_serverless_cache_endpoint_port", "6379"),
            ("__meta_elasticache_serverless_cache_security_group_id_0", "sg-12345"),
            ("__meta_elasticache_serverless_cache_subnet_id_0", "subnet-abcdef"),
            ("__address__", "my-cache.serverless.use1.cache.amazonaws.com:6379"),
        ],
        &tg.targets[0],
    );
}

fn cache_node(id: &str, az: &str, addr: &str) -> X {
    X::new().f("CacheNodeId", id).f("CacheNodeStatus", "available").f("CacheNodeCreateTime", TEST_TIME).f("CustomerAvailabilityZone", az).raw("Endpoint", &endpoint(addr, 6379))
}

#[test]
fn test_add_cache_cluster_targets() {
    let multi = X::new()
        .f("CacheClusterId", "my-cluster-001")
        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:cluster:my-cluster-001")
        .f("CacheClusterStatus", "available")
        .f("Engine", "redis")
        .f("EngineVersion", "7.1")
        .f("CacheNodeType", "cache.t3.micro")
        .f("NumCacheNodes", 2)
        .f("CacheClusterCreateTime", TEST_TIME)
        .raw("ConfigurationEndpoint", &endpoint("my-cluster.abc123.cfg.use1.cache.amazonaws.com", 6379))
        .f("AtRestEncryptionEnabled", true)
        .f("TransitEncryptionEnabled", true)
        .f("AuthTokenEnabled", true)
        .f("AutoMinorVersionUpgrade", true)
        .f("CacheSubnetGroupName", "my-subnet-group")
        .f("PreferredAvailabilityZone", "us-east-1a")
        .list("SecurityGroups", "member", &[X::new().f("SecurityGroupId", "sg-12345").f("Status", "active")])
        .list(
            "CacheNodes",
            "CacheNode",
            &[cache_node("0001", "us-east-1a", "my-cluster-001.abc123.0001.use1.cache.amazonaws.com"), cache_node("0002", "us-east-1b", "my-cluster-001.abc123.0002.use1.cache.amazonaws.com")],
        );
    let common: Vec<(&str, &str)> = vec![
        ("__meta_elasticache_deployment_option", "node"),
        ("__meta_elasticache_cache_cluster_arn", "arn:aws:elasticache:us-east-1:123456789012:cluster:my-cluster-001"),
        ("__meta_elasticache_cache_cluster_cache_cluster_id", "my-cluster-001"),
        ("__meta_elasticache_cache_cluster_cache_cluster_status", "available"),
        ("__meta_elasticache_cache_cluster_engine", "redis"),
        ("__meta_elasticache_cache_cluster_engine_version", "7.1"),
        ("__meta_elasticache_cache_cluster_cache_node_type", "cache.t3.micro"),
        ("__meta_elasticache_cache_cluster_num_cache_nodes", "2"),
        ("__meta_elasticache_cache_cluster_cache_cluster_create_time", "2024-01-01T00:00:00Z"),
        ("__meta_elasticache_cache_cluster_configuration_endpoint_address", "my-cluster.abc123.cfg.use1.cache.amazonaws.com"),
        ("__meta_elasticache_cache_cluster_configuration_endpoint_port", "6379"),
        ("__meta_elasticache_cache_cluster_at_rest_encryption_enabled", "true"),
        ("__meta_elasticache_cache_cluster_transit_encryption_enabled", "true"),
        ("__meta_elasticache_cache_cluster_auth_token_enabled", "true"),
        ("__meta_elasticache_cache_cluster_auto_minor_version_upgrade", "true"),
        ("__meta_elasticache_cache_cluster_cache_subnet_group_name", "my-subnet-group"),
        ("__meta_elasticache_cache_cluster_preferred_availability_zone", "us-east-1a"),
        ("__meta_elasticache_cache_cluster_security_group_membership_id_0", "sg-12345"),
        ("__meta_elasticache_cache_cluster_security_group_membership_status_0", "active"),
        ("__meta_elasticache_cache_cluster_tag_Environment", "production"),
        ("__meta_elasticache_cache_cluster_tag_Application", "web-app"),
    ];
    let node = |id: &'static str, az: &'static str, addr: &'static str, a: &'static str| -> Vec<(&'static str, &'static str)> {
        vec![
            ("__meta_elasticache_cache_cluster_node_id", id),
            ("__meta_elasticache_cache_cluster_node_status", "available"),
            ("__meta_elasticache_cache_cluster_node_create_time", "2024-01-01T00:00:00Z"),
            ("__meta_elasticache_cache_cluster_node_availability_zone", az),
            ("__meta_elasticache_cache_cluster_node_endpoint_address", addr),
            ("__meta_elasticache_cache_cluster_node_endpoint_port", "6379"),
            ("__address__", a),
        ]
    };
    let e1: Vec<(&str, &str)> = common.iter().cloned().chain(node("0001", "us-east-1a", "my-cluster-001.abc123.0001.use1.cache.amazonaws.com", "my-cluster-001.abc123.0001.use1.cache.amazonaws.com:6379")).collect();
    let e2: Vec<(&str, &str)> = common.iter().cloned().chain(node("0002", "us-east-1b", "my-cluster-001.abc123.0002.use1.cache.amazonaws.com", "my-cluster-001.abc123.0002.use1.cache.amazonaws.com:6379")).collect();

    let single = X::new()
        .f("CacheClusterId", "node-cluster-001")
        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:cluster:node-cluster-001")
        .f("CacheClusterStatus", "available")
        .f("Engine", "redis")
        .f("EngineVersion", "6.2")
        .f("CacheNodeType", "cache.r6g.large")
        .f("NumCacheNodes", 1)
        .list("CacheNodes", "CacheNode", &[cache_node("0001", "us-east-1a", "node-cluster-001.abc123.0001.use1.cache.amazonaws.com")]);
    let s1: Vec<(&str, &str)> = vec![
        ("__meta_elasticache_deployment_option", "node"),
        ("__meta_elasticache_cache_cluster_arn", "arn:aws:elasticache:us-east-1:123456789012:cluster:node-cluster-001"),
        ("__meta_elasticache_cache_cluster_cache_cluster_id", "node-cluster-001"),
        ("__meta_elasticache_cache_cluster_cache_cluster_status", "available"),
        ("__meta_elasticache_cache_cluster_engine", "redis"),
        ("__meta_elasticache_cache_cluster_engine_version", "6.2"),
        ("__meta_elasticache_cache_cluster_cache_node_type", "cache.r6g.large"),
        ("__meta_elasticache_cache_cluster_num_cache_nodes", "1"),
        ("__meta_elasticache_cache_cluster_node_id", "0001"),
        ("__meta_elasticache_cache_cluster_node_status", "available"),
        ("__meta_elasticache_cache_cluster_node_create_time", "2024-01-01T00:00:00Z"),
        ("__meta_elasticache_cache_cluster_node_availability_zone", "us-east-1a"),
        ("__meta_elasticache_cache_cluster_node_endpoint_address", "node-cluster-001.abc123.0001.use1.cache.amazonaws.com"),
        ("__meta_elasticache_cache_cluster_node_endpoint_port", "6379"),
        ("__address__", "node-cluster-001.abc123.0001.use1.cache.amazonaws.com:6379"),
    ];

    let cases = vec![
        ("CacheClusterWithMultipleNodes", multi, tag_elements(&[("Environment", "production"), ("Application", "web-app")]), vec![e1, e2]),
        ("CacheClusterWithSingleNode", single, vec![], vec![s1]),
    ];
    for (name, c, tags, expected) in cases {
        let mut tg = Group { source: "test".to_string(), ..Default::default() };
        add_cache_cluster_targets(&mut tg, &c.element("CacheCluster"), &tags).unwrap();
        assert_eq!(tg.targets.len(), expected.len(), "{name}");
        for (i, exp) in expected.iter().enumerate() {
            assert_contains_all(&format!("{name} target {i}"), exp, &tg.targets[i]);
        }
    }
}

#[test]
fn test_split_cache_deployment_options() {
    let cases: Vec<(&str, Vec<&str>, Vec<&str>, Vec<&str>)> = vec![
        (
            "MixedARNs",
            vec![
                "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-serverless-cache",
                "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:my-replication-group",
                "arn:aws:elasticache:us-west-2:123456789012:serverlesscache:prod-cache",
            ],
            vec!["my-serverless-cache", "prod-cache"],
            vec!["my-replication-group"],
        ),
        ("OnlyServerlessCaches", vec!["arn:aws:elasticache:us-east-1:123456789012:serverlesscache:cache-1", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:cache-2"], vec!["cache-1", "cache-2"], vec![]),
        ("OnlyReplicationGroups", vec!["arn:aws:elasticache:us-east-1:123456789012:replicationgroup:cluster-1", "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:cluster-2"], vec![], vec!["cluster-1", "cluster-2"]),
        ("EmptyInput", vec![], vec![], vec![]),
        ("InvalidARNs", vec!["not-an-arn", "arn:aws:elasticache:us-east-1", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache", ""], vec![], vec![]),
        ("UnknownResourceType", vec!["arn:aws:elasticache:us-east-1:123456789012:unknown:resource-id"], vec![], vec![]),
        (
            "MixedWithInvalidARNs",
            vec![
                "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:valid-cache",
                "invalid-arn",
                "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:valid-cluster",
                "",
                "arn:aws:elasticache:us-east-1:123456789012:unknown:ignored",
            ],
            vec!["valid-cache"],
            vec!["valid-cluster"],
        ),
    ];
    for (name, caches, want_s, want_c) in cases {
        let (s, c) = split_cache_deployment_options(&strings(&caches), &discovery::Logger::nop());
        assert_eq!(strings(&want_s), s, "{name}: serverless cache IDs mismatch");
        assert_eq!(strings(&want_c), c, "{name}: cache cluster IDs mismatch");
    }
}

/// Go `fullyPopulatedServerlessCache`.
fn fully_populated_serverless_cache() -> X {
    X::new()
        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-cache")
        .f("ServerlessCacheName", "my-cache")
        .f("Status", "available")
        .f("Engine", "redis")
        .f("FullEngineVersion", "7.1")
        .f("MajorEngineVersion", "7")
        .raw("Endpoint", &endpoint("my-cache.serverless.use1.cache.amazonaws.com", 6379))
}

/// Go `fullyPopulatedCacheCluster`.
fn fully_populated_cache_cluster() -> X {
    X::new()
        .f("ARN", "arn:aws:elasticache:us-east-1:123456789012:cluster:my-cluster-001")
        .f("CacheClusterId", "my-cluster-001")
        .f("CacheClusterStatus", "available")
        .list("CacheNodes", "CacheNode", &[X::new().f("CacheNodeId", "0001").raw("Endpoint", &endpoint("my-cluster-001.abc123.0001.use1.cache.amazonaws.com", 6379))])
}

#[test]
fn test_elasticache_refresh_fully_populated() {
    let data = Data {
        region: "us-east-1".to_string(),
        serverless: vec![serverless(fully_populated_serverless_cache())],
        clusters: vec![cluster(fully_populated_cache_cluster())],
        tags: HashMap::from([
            ("arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-cache".to_string(), vec![tag("Environment", "test")]),
            ("arn:aws:elasticache:us-east-1:123456789012:cluster:my-cluster-001".to_string(), vec![tag("Environment", "prod")]),
        ]),
    };
    let (mut d, _srv) = test_discovery(&Mock::new(data));
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(tgs.len(), 1);
    assert_eq!(tgs[0].targets.len(), 2, "one target per deployment option, and no duplicates");
    let by_option: BTreeMap<&str, &LabelSet> = tgs[0].targets.iter().map(|t| (t.get("__meta_elasticache_deployment_option").map(|s| s.as_str()).unwrap_or(""), t)).collect();
    assert_eq!(by_option.len(), 2, "both deployment options must be discovered");

    let s = by_option["serverless"];
    let g = |l: &LabelSet, k: &str| l.get(k).cloned().unwrap_or_default();
    assert_eq!("my-cache.serverless.use1.cache.amazonaws.com:6379", g(s, "__address__"));
    assert_eq!("arn:aws:elasticache:us-east-1:123456789012:serverlesscache:my-cache", g(s, "__meta_elasticache_serverless_cache_arn"));
    assert_eq!("my-cache", g(s, "__meta_elasticache_serverless_cache_name"));
    assert_eq!("available", g(s, "__meta_elasticache_serverless_cache_status"));
    assert_eq!("redis", g(s, "__meta_elasticache_serverless_cache_engine"));
    assert_eq!("7.1", g(s, "__meta_elasticache_serverless_cache_full_engine_version"));
    assert_eq!("7", g(s, "__meta_elasticache_serverless_cache_major_engine_version"));
    assert_eq!("test", g(s, "__meta_elasticache_serverless_cache_tag_Environment"));

    let n = by_option["node"];
    assert_eq!("my-cluster-001.abc123.0001.use1.cache.amazonaws.com:6379", g(n, "__address__"));
    assert_eq!("arn:aws:elasticache:us-east-1:123456789012:cluster:my-cluster-001", g(n, "__meta_elasticache_cache_cluster_arn"));
    assert_eq!("my-cluster-001", g(n, "__meta_elasticache_cache_cluster_cache_cluster_id"));
    assert_eq!("available", g(n, "__meta_elasticache_cache_cluster_cache_cluster_status"));
    assert_eq!("prod", g(n, "__meta_elasticache_cache_cluster_tag_Environment"));
}

#[test]
fn test_elasticache_refresh_serverless_cache_nil_optional_fields() {
    for (name, field, absent) in [
        ("NilARN", "ARN", "__meta_elasticache_serverless_cache_arn"),
        ("NilServerlessCacheName", "ServerlessCacheName", "__meta_elasticache_serverless_cache_name"),
        ("NilStatus", "Status", "__meta_elasticache_serverless_cache_status"),
        ("NilEngine", "Engine", "__meta_elasticache_serverless_cache_engine"),
        ("NilFullEngineVersion", "FullEngineVersion", "__meta_elasticache_serverless_cache_full_engine_version"),
        ("NilMajorEngineVersion", "MajorEngineVersion", "__meta_elasticache_serverless_cache_major_engine_version"),
    ] {
        let data = Data { region: "us-east-1".to_string(), serverless: vec![serverless(fully_populated_serverless_cache().without(field))], ..Default::default() };
        let (mut d, _srv) = test_discovery(&Mock::new(data));
        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(tgs.len(), 1, "{name}");
        assert_eq!(tgs[0].targets.len(), 1, "{name}: cache must still be discovered when an optional field is absent");
        let t = &tgs[0].targets[0];
        assert!(!t.contains_key(absent), "{name}: label sourced from the absent field should be omitted");
        assert_eq!(Some(&"my-cache.serverless.use1.cache.amazonaws.com:6379".to_string()), t.get("__address__"), "{name}");
    }
}

#[test]
fn test_elasticache_refresh_cache_cluster_nil_optional_fields() {
    for (name, field, absent) in [
        ("NilARN", "ARN", "__meta_elasticache_cache_cluster_arn"),
        ("NilCacheClusterId", "CacheClusterId", "__meta_elasticache_cache_cluster_cache_cluster_id"),
        ("NilCacheClusterStatus", "CacheClusterStatus", "__meta_elasticache_cache_cluster_cache_cluster_status"),
    ] {
        let data = Data { region: "us-east-1".to_string(), clusters: vec![cluster(fully_populated_cache_cluster().without(field))], ..Default::default() };
        let (mut d, _srv) = test_discovery(&Mock::new(data));
        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(tgs.len(), 1, "{name}");
        assert_eq!(tgs[0].targets.len(), 1, "{name}: cluster node must still be discovered when an optional field is absent");
        let t = &tgs[0].targets[0];
        assert!(!t.contains_key(absent), "{name}: label sourced from the absent field should be omitted");
        assert_eq!(Some(&"my-cluster-001.abc123.0001.use1.cache.amazonaws.com:6379".to_string()), t.get("__address__"), "{name}");
    }
}

/// Go `elasticacheFixture`.
fn elasticache_fixture(serverless_count: usize, cluster_count: usize, node_count: usize) -> Data {
    let mut data = Data { region: "us-east-1".to_string(), ..Default::default() };
    for s in 0..serverless_count {
        let name = format!("serverless-{s}");
        let arn = format!("arn:aws:elasticache:us-east-1:123456789012:serverlesscache:{name}");
        let x = fully_populated_serverless_cache().without("ARN").without("ServerlessCacheName").without("Endpoint").f("ARN", &arn).f("ServerlessCacheName", &name).raw("Endpoint", &endpoint(&format!("{name}.serverless.use1.cache.amazonaws.com"), 6379));
        data.serverless.push(serverless(x));
        data.tags.insert(arn, vec![tag("Environment", "bench")]);
    }
    for c in 0..cluster_count {
        let id = format!("cluster-{c}");
        let arn = format!("arn:aws:elasticache:us-east-1:123456789012:cluster:{id}");
        let nodes: Vec<X> = (0..node_count).map(|n| X::new().f("CacheNodeId", format!("{:04}", n + 1)).raw("Endpoint", &endpoint(&format!("{id}-{:04}.use1.cache.amazonaws.com", n + 1), 6379))).collect();
        let x = fully_populated_cache_cluster().without("ARN").without("CacheClusterId").without("CacheNodes").f("ARN", &arn).f("CacheClusterId", &id).list("CacheNodes", "CacheNode", &nodes);
        data.clusters.push(cluster(x));
        data.tags.insert(arn, vec![tag("Environment", "bench")]);
    }
    data
}

#[test]
fn test_elasticache_refresh_describes_each_resource_once() {
    let mock = Mock::new(elasticache_fixture(2, 2, 1));
    let (mut d, _srv) = test_discovery(&mock);
    d.refresh(&CancelToken::new()).unwrap();
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for r in mock.describe_requests.lock().unwrap().iter() {
        *counts.entry(r.clone()).or_default() += 1;
    }
    let repeated: Vec<String> = counts.iter().filter(|(_, c)| **c > 1).map(|(r, c)| format!("{r}: {c} times")).collect();
    assert!(repeated.is_empty(), "a refresh must not issue the same describe request more than once: {repeated:?}");
}

#[test]
fn test_elasticache_describe_cache_clusters_returns_each_cluster_once() {
    for (name, ids, expected) in [("AllClusters", vec![], vec!["standalone-001", "member-001"]), ("StandaloneClusterByID", vec!["standalone-001"], vec!["standalone-001"]), ("ReplicationGroupMemberByID", vec!["member-001"], vec!["member-001"])] {
        let data = Data {
            region: "us-east-1".to_string(),
            clusters: vec![
                // Memcached and single node clusters are not members of a replication group, so both API
                // responses carry them.
                cluster(X::new().f("CacheClusterId", "standalone-001")),
                cluster(X::new().f("CacheClusterId", "member-001").f("ReplicationGroupId", "my-replication-group")),
            ],
            ..Default::default()
        };
        let (mut d, _srv) = test_discovery(&Mock::new(data));
        let clusters = d.describe_cache_clusters(&CancelToken::new(), &strings(&ids)).unwrap();
        let mut got: Vec<String> = clusters.iter().map(|c| c.text_at("CacheClusterId").unwrap_or("").to_string()).collect();
        let mut want = strings(&expected);
        got.sort();
        want.sort();
        assert_eq!(want, got, "{name}: each cluster must be described exactly once");
    }
}
