//! Port of discovery/aws/rds_test.go. The Go tests mock the RDS client; here the same clusters and instances are
//! served as RDS API (awsQuery XML) responses by a local server, with the same filtering as the Go mock.
mod cloud_mock;

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::Filter;
use discovery::aws::rds::{RdsDiscovery, RdsSdConfig};
use discovery::aws::xml::escape;
use discovery::{CancelToken, Config, DiscovererMetrics, DiscovererOptions, Registry};
use model::labelset::LabelSet;

const TEST_TIME: &str = "2024-01-01T00:00:00Z";

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
    fn raw(mut self, k: &str, inner: &str) -> X {
        self.0.push_str(&format!("<{k}>{inner}</{k}>"));
        self
    }
    fn list(self, k: &str, member: &str, items: &[X]) -> X {
        let inner: String = items.iter().map(|i| format!("<{member}>{}</{member}>", i.0)).collect();
        self.raw(k, &inner)
    }
    fn tags(self, t: &[(&str, &str)]) -> X {
        let items: Vec<X> = t.iter().map(|(k, v)| X::new().f("Key", k).f("Value", v)).collect();
        self.list("TagList", "Tag", &items)
    }
}

fn member(id: &str, writer: bool) -> X {
    X::new().f("DBInstanceIdentifier", id).f("IsClusterWriter", writer)
}

fn endpoint(addr: &str, port: i32, zone: Option<&str>) -> X {
    let x = X::new().f("Address", addr).f("Port", port);
    match zone {
        Some(z) => x.f("HostedZoneId", z),
        None => x,
    }
}

/// An instance: its engine (for the mock's engine filter) and its XML.
#[derive(Clone)]
struct Inst {
    engine: Option<String>,
    xml: X,
}

fn inst(engine: Option<&str>, xml: X) -> Inst {
    let xml = match engine {
        Some(e) => xml.f("Engine", e),
        None => xml,
    };
    Inst { engine: engine.map(|s| s.to_string()), xml }
}

/// Go `mockRDSClient`.
#[derive(Clone, Default)]
struct MockRds {
    clusters: BTreeMap<String, X>,
    instances: BTreeMap<String, Vec<Inst>>,
}

fn filters_of(req: &MockRequest) -> Vec<(String, Vec<String>)> {
    let p = req.form_params();
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for i in 1.. {
        let Some((_, name)) = p.iter().find(|(k, _)| *k == format!("Filters.Filter.{i}.Name")) else { break };
        let mut vals = Vec::new();
        for j in 1.. {
            match p.iter().find(|(k, _)| *k == format!("Filters.Filter.{i}.Values.Value.{j}")) {
                Some((_, v)) => vals.push(v.clone()),
                None => break,
            }
        }
        out.push((name.clone(), vals));
    }
    out
}

fn response(op: &str, list: &str, member: &str, items: &[X]) -> MockResponse {
    let inner: String = items.iter().map(|i| format!("<{member}>{}</{member}>", i.0)).collect();
    MockResponse::xml(format!(
        "<{op}Response xmlns=\"http://rds.amazonaws.com/doc/2014-10-31/\"><{op}Result><{list}>{inner}</{list}></{op}Result><ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata></{op}Response>"
    ))
}

impl MockRds {
    fn handle(&self, req: &MockRequest) -> MockResponse {
        match req.param("Action").as_deref() {
            Some("DescribeDBClusters") => {
                let clusters: Vec<X> = match req.param("DBClusterIdentifier") {
                    Some(id) => self.clusters.get(&id).cloned().into_iter().collect(),
                    None => self.clusters.values().cloned().collect(),
                };
                response("DescribeDBClusters", "DBClusters", "DBCluster", &clusters)
            }
            Some("DescribeDBInstances") => {
                let filters = filters_of(req);
                let mut instances: Vec<Inst> = Vec::new();
                if !filters.is_empty() {
                    for (name, values) in &filters {
                        if name == "db-cluster-id" {
                            for c in values {
                                if let Some(ci) = self.instances.get(c) {
                                    instances.extend(ci.iter().cloned());
                                }
                            }
                        }
                    }
                } else {
                    for ci in self.instances.values() {
                        instances.extend(ci.iter().cloned());
                    }
                }
                for (name, values) in &filters {
                    if name != "engine" {
                        continue;
                    }
                    instances.retain(|i| i.engine.as_ref().is_some_and(|e| values.contains(e)));
                }
                let xs: Vec<X> = instances.into_iter().map(|i| i.xml).collect();
                response("DescribeDBInstances", "DBInstances", "DBInstance", &xs)
            }
            _ => MockResponse::new(400, "text/xml", "<ErrorResponse><Error><Type>Sender</Type><Code>InvalidAction</Code><Message>unknown action</Message></Error><RequestId>req-1</RequestId></ErrorResponse>"),
        }
    }
}

fn config(url: &str, port: i64, concurrency: i64, filters: Vec<Filter>) -> RdsSdConfig {
    RdsSdConfig {
        region: "us-east-1".to_string(),
        endpoint: url.to_string(),
        access_key: "AKIDTEST".to_string(),
        secret_key: yaml::Secret("secret".to_string()),
        port,
        request_concurrency: concurrency,
        filters,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    }
}

fn new_discovery(cfg: &RdsSdConfig) -> RdsDiscovery {
    let reg = Registry::new();
    let rm = discovery::new_refresh_metrics(&reg);
    rm.register().unwrap();
    let metrics = cfg.new_discoverer_metrics(&reg, rm);
    RdsDiscovery::new(cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), ..Default::default() }).unwrap()
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

struct Case {
    name: &'static str,
    mock: MockRds,
    filters: Vec<Filter>,
    expected: Vec<LabelSet>,
}

#[test]
fn test_rds_discovery_refresh() {
    let c1 = "arn:aws:rds:us-east-1:123456789012:cluster:test-cluster";
    let c2 = "arn:aws:rds:us-west-2:123456789012:cluster:prod-cluster";
    let c3 = "arn:aws:rds:us-east-1:123456789012:cluster:filter-cluster";
    let cases = vec![
        Case {
            name: "SingleClusterWithInstance",
            mock: MockRds {
                clusters: BTreeMap::from([(
                    c1.to_string(),
                    X::new()
                        .f("DBClusterArn", c1)
                        .f("DBClusterIdentifier", "test-cluster")
                        .f("Engine", "aurora-postgresql")
                        .f("EngineVersion", "15.4")
                        .f("Status", "available")
                        .f("Endpoint", "test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com")
                        .f("Port", 5432)
                        .f("MasterUsername", "admin")
                        .f("MultiAZ", true)
                        .f("ClusterCreateTime", TEST_TIME)
                        .list("DBClusterMembers", "DBClusterMember", &[member("test-instance-1", true)])
                        .tags(&[("Environment", "test")]),
                )]),
                instances: BTreeMap::from([(
                    c1.to_string(),
                    vec![inst(
                        Some("aurora-postgresql"),
                        X::new()
                            .f("DBInstanceArn", "arn:aws:rds:us-east-1:123456789012:db:test-instance-1")
                            .f("DBInstanceIdentifier", "test-instance-1")
                            .f("DBInstanceClass", "db.r5.large")
                            .f("DBInstanceStatus", "available")
                            .f("EngineVersion", "15.4")
                            .f("AvailabilityZone", "us-east-1a")
                            .f("DBClusterIdentifier", "test-cluster")
                            .f("PubliclyAccessible", false)
                            .f("InstanceCreateTime", TEST_TIME)
                            .raw("Endpoint", &endpoint("test-instance-1.xyz.us-east-1.rds.amazonaws.com", 5432, Some("Z2R2ITUGPM61AM")).0)
                            .tags(&[("Name", "test-instance")]),
                    )],
                )]),
            },
            filters: vec![],
            expected: vec![ls(&[
                ("__address__", "test-instance-1.xyz.us-east-1.rds.amazonaws.com:9187"),
                ("__meta_rds_cluster_arn", c1),
                ("__meta_rds_cluster_identifier", "test-cluster"),
                ("__meta_rds_cluster_engine", "aurora-postgresql"),
                ("__meta_rds_cluster_engine_version", "15.4"),
                ("__meta_rds_cluster_status", "available"),
                ("__meta_rds_cluster_endpoint", "test-cluster.cluster-xyz.us-east-1.rds.amazonaws.com"),
                ("__meta_rds_cluster_port", "5432"),
                ("__meta_rds_cluster_master_username", "admin"),
                ("__meta_rds_cluster_multi_az", "true"),
                ("__meta_rds_cluster_cluster_create_time", TEST_TIME),
                ("__meta_rds_cluster_tag_Environment", "test"),
                ("__meta_rds_instance_arn", "arn:aws:rds:us-east-1:123456789012:db:test-instance-1"),
                ("__meta_rds_instance_identifier", "test-instance-1"),
                ("__meta_rds_instance_is_cluster_writer", "true"),
                ("__meta_rds_instance_class", "db.r5.large"),
                ("__meta_rds_instance_status", "available"),
                ("__meta_rds_instance_engine", "aurora-postgresql"),
                ("__meta_rds_instance_engine_version", "15.4"),
                ("__meta_rds_instance_availability_zone", "us-east-1a"),
                ("__meta_rds_instance_db_cluster_identifier", "test-cluster"),
                ("__meta_rds_instance_publicly_accessible", "false"),
                ("__meta_rds_instance_instance_create_time", TEST_TIME),
                ("__meta_rds_instance_endpoint_address", "test-instance-1.xyz.us-east-1.rds.amazonaws.com"),
                ("__meta_rds_instance_endpoint_port", "5432"),
                ("__meta_rds_instance_endpoint_hosted_zone_id", "Z2R2ITUGPM61AM"),
                ("__meta_rds_instance_tag_Name", "test-instance"),
            ])],
        },
        Case {
            name: "MultipleInstancesInCluster",
            mock: MockRds {
                clusters: BTreeMap::from([(
                    c2.to_string(),
                    X::new()
                        .f("DBClusterArn", c2)
                        .f("DBClusterIdentifier", "prod-cluster")
                        .f("Engine", "aurora-mysql")
                        .f("EngineVersion", "8.0.mysql_aurora.3.04.0")
                        .f("Status", "available")
                        .list("DBClusterMembers", "DBClusterMember", &[member("prod-instance-1", true), member("prod-instance-2", false)]),
                )]),
                instances: BTreeMap::from([(
                    c2.to_string(),
                    vec![
                        inst(
                            None,
                            X::new()
                                .f("DBInstanceArn", "arn:aws:rds:us-west-2:123456789012:db:prod-instance-1")
                                .f("DBInstanceIdentifier", "prod-instance-1")
                                .f("DBInstanceClass", "db.r6g.xlarge")
                                .f("DBInstanceStatus", "available")
                                .raw("Endpoint", &endpoint("prod-instance-1.xyz.us-west-2.rds.amazonaws.com", 3306, None).0),
                        ),
                        inst(
                            None,
                            X::new()
                                .f("DBInstanceArn", "arn:aws:rds:us-west-2:123456789012:db:prod-instance-2")
                                .f("DBInstanceIdentifier", "prod-instance-2")
                                .f("DBInstanceClass", "db.r6g.xlarge")
                                .f("DBInstanceStatus", "available")
                                .raw("Endpoint", &endpoint("prod-instance-2.xyz.us-west-2.rds.amazonaws.com", 3306, None).0),
                        ),
                    ],
                )]),
            },
            filters: vec![],
            expected: vec![
                ls(&[
                    ("__address__", "prod-instance-1.xyz.us-west-2.rds.amazonaws.com:9187"),
                    ("__meta_rds_cluster_arn", c2),
                    ("__meta_rds_cluster_identifier", "prod-cluster"),
                    ("__meta_rds_cluster_engine", "aurora-mysql"),
                    ("__meta_rds_cluster_engine_version", "8.0.mysql_aurora.3.04.0"),
                    ("__meta_rds_cluster_status", "available"),
                    ("__meta_rds_instance_arn", "arn:aws:rds:us-west-2:123456789012:db:prod-instance-1"),
                    ("__meta_rds_instance_identifier", "prod-instance-1"),
                    ("__meta_rds_instance_is_cluster_writer", "true"),
                    ("__meta_rds_instance_class", "db.r6g.xlarge"),
                    ("__meta_rds_instance_status", "available"),
                    ("__meta_rds_instance_endpoint_address", "prod-instance-1.xyz.us-west-2.rds.amazonaws.com"),
                    ("__meta_rds_instance_endpoint_port", "3306"),
                ]),
                ls(&[
                    ("__address__", "prod-instance-2.xyz.us-west-2.rds.amazonaws.com:9187"),
                    ("__meta_rds_cluster_arn", c2),
                    ("__meta_rds_cluster_identifier", "prod-cluster"),
                    ("__meta_rds_cluster_engine", "aurora-mysql"),
                    ("__meta_rds_cluster_engine_version", "8.0.mysql_aurora.3.04.0"),
                    ("__meta_rds_cluster_status", "available"),
                    ("__meta_rds_instance_arn", "arn:aws:rds:us-west-2:123456789012:db:prod-instance-2"),
                    ("__meta_rds_instance_identifier", "prod-instance-2"),
                    ("__meta_rds_instance_is_cluster_writer", "false"),
                    ("__meta_rds_instance_class", "db.r6g.xlarge"),
                    ("__meta_rds_instance_status", "available"),
                    ("__meta_rds_instance_endpoint_address", "prod-instance-2.xyz.us-west-2.rds.amazonaws.com"),
                    ("__meta_rds_instance_endpoint_port", "3306"),
                ]),
            ],
        },
        Case {
            name: "NoInstancesInCluster",
            mock: MockRds {
                clusters: BTreeMap::from([(
                    c2.to_string(),
                    X::new()
                        .f("DBClusterArn", c2)
                        .f("DBClusterIdentifier", "prod-cluster")
                        .f("Engine", "aurora-mysql")
                        .f("EngineVersion", "8.0.mysql_aurora.3.04.0")
                        .f("Status", "available")
                        .list("DBClusterMembers", "DBClusterMember", &[]),
                )]),
                instances: BTreeMap::new(),
            },
            filters: vec![],
            expected: vec![],
        },
        Case {
            name: "FiltersMatchSingleInstance",
            mock: MockRds {
                clusters: BTreeMap::from([(
                    c3.to_string(),
                    X::new()
                        .f("DBClusterArn", c3)
                        .f("DBClusterIdentifier", "filter-cluster")
                        .f("Engine", "aurora-postgresql")
                        .f("Status", "available")
                        .list("DBClusterMembers", "DBClusterMember", &[member("filter-instance-1", true), member("filter-instance-2", false)]),
                )]),
                instances: BTreeMap::from([(
                    c3.to_string(),
                    vec![
                        inst(
                            Some("aurora-postgresql"),
                            X::new()
                                .f("DBInstanceArn", "arn:aws:rds:us-east-1:123456789012:db:filter-instance-1")
                                .f("DBInstanceIdentifier", "filter-instance-1")
                                .f("DBInstanceClass", "db.r6g.large")
                                .f("DBInstanceStatus", "available")
                                .raw("Endpoint", &endpoint("filter-instance-1.rds.amazonaws.com", 5432, None).0),
                        ),
                        inst(
                            Some("mysql"),
                            X::new()
                                .f("DBInstanceArn", "arn:aws:rds:us-east-1:123456789012:db:filter-instance-2")
                                .f("DBInstanceIdentifier", "filter-instance-2")
                                .f("DBInstanceClass", "db.r6g.large")
                                .f("DBInstanceStatus", "available")
                                .raw("Endpoint", &endpoint("filter-instance-2.rds.amazonaws.com", 3306, None).0),
                        ),
                    ],
                )]),
            },
            filters: vec![Filter { name: "engine".to_string(), values: vec!["aurora-postgresql".to_string()] }],
            expected: vec![ls(&[
                ("__address__", "filter-instance-1.rds.amazonaws.com:9187"),
                ("__meta_rds_cluster_arn", c3),
                ("__meta_rds_cluster_identifier", "filter-cluster"),
                ("__meta_rds_cluster_engine", "aurora-postgresql"),
                ("__meta_rds_cluster_status", "available"),
                ("__meta_rds_instance_arn", "arn:aws:rds:us-east-1:123456789012:db:filter-instance-1"),
                ("__meta_rds_instance_identifier", "filter-instance-1"),
                ("__meta_rds_instance_is_cluster_writer", "true"),
                ("__meta_rds_instance_class", "db.r6g.large"),
                ("__meta_rds_instance_status", "available"),
                ("__meta_rds_instance_engine", "aurora-postgresql"),
                ("__meta_rds_instance_endpoint_address", "filter-instance-1.rds.amazonaws.com"),
                ("__meta_rds_instance_endpoint_port", "5432"),
            ])],
        },
        Case {
            name: "FiltersMatchNoInstances",
            mock: MockRds {
                clusters: BTreeMap::from([(
                    c3.to_string(),
                    X::new().f("DBClusterArn", c3).f("DBClusterIdentifier", "filter-cluster").f("Engine", "aurora-postgresql").f("Status", "available"),
                )]),
                instances: BTreeMap::from([(
                    c3.to_string(),
                    vec![
                        inst(Some("aurora-postgresql"), X::new().f("DBInstanceIdentifier", "filter-instance-1")),
                        inst(Some("mysql"), X::new().f("DBInstanceIdentifier", "filter-instance-2")),
                    ],
                )]),
            },
            filters: vec![Filter { name: "engine".to_string(), values: vec!["sqlserver-ee".to_string()] }],
            expected: vec![],
        },
    ];

    for tt in cases {
        let mock = tt.mock.clone();
        let srv = MockServer::from_fn(move |r| mock.handle(r));
        let mut d = new_discovery(&config(&srv.url, 9187, 10, tt.filters.clone()));
        let tgs = d.refresh(&CancelToken::new()).unwrap_or_else(|e| panic!("{}: {e}", tt.name));
        assert_eq!(tgs.len(), 1, "{}", tt.name);
        let tg = &tgs[0];
        assert_eq!(tg.targets.len(), tt.expected.len(), "{}", tt.name);
        let by_addr: BTreeMap<&str, &LabelSet> = tg.targets.iter().map(|t| (t.get("__address__").map(|s| s.as_str()).unwrap_or(""), t)).collect();
        for exp in &tt.expected {
            let addr = exp.get("__address__").unwrap().as_str();
            let got = by_addr.get(addr).unwrap_or_else(|| panic!("{}: Expected target with address {addr} not found", tt.name));
            assert_eq!(exp, *got, "{}", tt.name);
        }
    }
}

#[test]
fn test_describe_all_db_clusters() {
    let a1 = "arn:aws:rds:us-east-1:123456789012:cluster:cluster-1";
    let a2 = "arn:aws:rds:us-east-1:123456789012:cluster:cluster-2";
    let mock = MockRds {
        clusters: BTreeMap::from([
            (a1.to_string(), X::new().f("DBClusterArn", a1).f("DBClusterIdentifier", "cluster-1")),
            (a2.to_string(), X::new().f("DBClusterArn", a2).f("DBClusterIdentifier", "cluster-2")),
        ]),
        instances: BTreeMap::new(),
    };
    let srv = MockServer::from_fn(move |r| mock.handle(r));
    let mut d = new_discovery(&config(&srv.url, 0, 10, vec![]));
    let clusters = d.describe_all_db_clusters(&CancelToken::new()).unwrap();
    assert_eq!(clusters.len(), 2);
    assert!(clusters.contains_key(a1));
    assert!(clusters.contains_key(a2));
}

/// Go `rdsFixture`: clusters populated the way the RDS API populates a provisioned Aurora PostgreSQL cluster,
/// each with `instance_count` instances.
fn rds_fixture(cluster_count: usize, instance_count: usize) -> MockRds {
    let mut m = MockRds::default();
    for c in 0..cluster_count {
        let cluster_id = format!("aurora-cluster-{c}");
        let cluster_arn = format!("arn:aws:rds:us-east-1:123456789012:cluster:{cluster_id}");
        let mut members = Vec::new();
        let mut instances = Vec::new();
        for i in 0..instance_count {
            let instance_id = format!("{cluster_id}-instance-{i}");
            members.push(member(&instance_id, i == 0).f("PromotionTier", i));
            instances.push(inst(
                Some("aurora-postgresql"),
                X::new()
                    .f("AutoMinorVersionUpgrade", true)
                    .f("AvailabilityZone", "us-east-1a")
                    .f("BackupRetentionPeriod", 7)
                    .f("CopyTagsToSnapshot", true)
                    .f("DBInstanceArn", format!("arn:aws:rds:us-east-1:123456789012:db:{instance_id}"))
                    .f("DBInstanceClass", "db.r6g.xlarge")
                    .f("DBInstanceIdentifier", &instance_id)
                    .f("DBInstanceStatus", "available")
                    .f("DBClusterIdentifier", &cluster_id)
                    .f("DbiResourceId", "db-ABCDEFGHIJKLMNOPQRSTUVWXYZ")
                    .f("DeletionProtection", false)
                    .raw("Endpoint", &endpoint(&format!("{instance_id}.xyz.us-east-1.rds.amazonaws.com"), 5432, Some("Z2R2ITUGPM61AM")).0)
                    .f("EngineVersion", "15.4")
                    .f("InstanceCreateTime", TEST_TIME)
                    .f("KmsKeyId", "arn:aws:kms:us-east-1:123456789012:key/abcd1234")
                    .f("MonitoringInterval", 60)
                    .f("MonitoringRoleArn", "arn:aws:iam::123456789012:role/rds-monitoring-role")
                    .f("MultiAZ", false)
                    .f("NetworkType", "IPV4")
                    .f("PerformanceInsightsEnabled", true)
                    .f("PreferredBackupWindow", "07:00-07:30")
                    .f("PreferredMaintenanceWindow", "sun:09:00-sun:09:30")
                    .f("PromotionTier", i)
                    .f("PubliclyAccessible", false)
                    .f("StorageEncrypted", true)
                    .f("StorageType", "aurora")
                    .tags(&[("Name", &instance_id), ("Environment", "production")]),
            ));
        }
        m.clusters.insert(
            cluster_arn.clone(),
            X::new()
                .f("ActivityStreamStatus", "stopped")
                .f("AllocatedStorage", 1)
                .f("AutoMinorVersionUpgrade", true)
                .f("BackupRetentionPeriod", 7)
                .f("ClusterCreateTime", TEST_TIME)
                .f("CopyTagsToSnapshot", true)
                .f("CrossAccountClone", false)
                .f("DatabaseName", "appdb")
                .f("DBClusterArn", &cluster_arn)
                .f("DBClusterIdentifier", &cluster_id)
                .list("DBClusterMembers", "DBClusterMember", &members)
                .f("DBClusterParameterGroup", "default.aurora-postgresql15")
                .f("DbClusterResourceId", "cluster-ABCDEFGHIJKLMNOPQRSTUVWXYZ")
                .f("DBSubnetGroup", "default-vpc-0123456789abcdef0")
                .f("DeletionProtection", false)
                .f("EarliestRestorableTime", TEST_TIME)
                .f("Endpoint", format!("{cluster_id}.cluster-xyz.us-east-1.rds.amazonaws.com"))
                .f("Engine", "aurora-postgresql")
                .f("EngineLifecycleSupport", "open-source-rds-extended-support")
                .f("EngineMode", "provisioned")
                .f("EngineVersion", "15.4")
                .f("HostedZoneId", "Z2R2ITUGPM61AM")
                .f("HttpEndpointEnabled", false)
                .f("IAMDatabaseAuthenticationEnabled", false)
                .f("KmsKeyId", "arn:aws:kms:us-east-1:123456789012:key/abcd1234")
                .f("LatestRestorableTime", TEST_TIME)
                .f("LocalWriteForwardingStatus", "disabled")
                .f("MasterUsername", "postgres")
                .f("MonitoringInterval", 60)
                .f("MonitoringRoleArn", "arn:aws:iam::123456789012:role/rds-monitoring-role")
                .f("MultiAZ", true)
                .f("NetworkType", "IPV4")
                .f("PercentProgress", "100")
                .f("PerformanceInsightsEnabled", true)
                .f("PerformanceInsightsRetentionPeriod", 7)
                .f("Port", 5432)
                .f("PreferredBackupWindow", "07:00-07:30")
                .f("PreferredMaintenanceWindow", "sun:09:00-sun:09:30")
                .f("PubliclyAccessible", false)
                .f("ReaderEndpoint", format!("{cluster_id}.cluster-ro-xyz.us-east-1.rds.amazonaws.com"))
                .f("Status", "available")
                .f("StorageEncrypted", true)
                .f("StorageType", "aurora")
                .tags(&[("Name", &cluster_id), ("Environment", "production"), ("Team", "platform")]),
        );
        m.instances.insert(cluster_arn, instances);
    }
    m
}

#[test]
fn test_rds_discovery_describes_instances_concurrently() {
    const CLUSTER_COUNT: usize = 8;
    const CONCURRENCY: usize = 4;
    let mock = rds_fixture(CLUSTER_COUNT, 1);

    // (in flight, max in flight, all in flight once)
    let state = Arc::new((Mutex::new((0usize, 0usize, false)), Condvar::new()));
    let st = state.clone();
    let srv = MockServer::from_fn(move |r| {
        if r.param("Action").as_deref() == Some("DescribeDBInstances") {
            let (m, cv) = &*st;
            let mut g = m.lock().unwrap();
            g.0 += 1;
            g.1 = g.1.max(g.0);
            if g.0 == CONCURRENCY {
                g.2 = true;
                cv.notify_all();
            }
            // Hold every call open until CONCURRENCY of them are in flight. A serial implementation never gets
            // past the first one and falls back on the timeout, leaving the maximum at 1.
            let deadline = Instant::now() + Duration::from_secs(2);
            while !g.2 {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                g = cv.wait_timeout(g, deadline - now).unwrap().0;
            }
            g.0 -= 1;
        }
        mock.handle(r)
    });
    let mut d = new_discovery(&config(&srv.url, 9187, CONCURRENCY as i64, vec![]));
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(tgs.len(), 1);
    assert_eq!(tgs[0].targets.len(), CLUSTER_COUNT);
    assert_eq!(state.0.lock().unwrap().1, CONCURRENCY, "DescribeDBInstances was not called concurrently");
}
