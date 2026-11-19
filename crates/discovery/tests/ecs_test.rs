//! Port of discovery/aws/ecs_test.go. Go mocks the ECS and EC2 SDK clients; here the same data is served as
//! real ECS (awsJson1.1) and EC2 (Query/XML) responses by a local server, and the discovery is built from a
//! config whose endpoint is that server.
//!
//! adapted: Go sets the mock clients on the struct and calls the helper methods directly; here the clients
//! are built by `init_ecs_client` (which also makes the empty DescribeClusters credential test call that the
//! mock answers) before the helper methods are called.
mod cloud_mock;

use std::collections::BTreeMap;
use std::sync::Arc;

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::ecs::{Attachment, ContainerInstance, Ec2InstanceInfo, EcsCluster, EcsDiscovery, EcsMetrics, EcsSdConfig, EcsService, EcsTask, KeyValuePair, Tag};
use discovery::{CancelToken, DiscovererOptions, Group, Registry};
use json::Json;
use model::labelset::LabelSet;

/// Go `ecsDataStore`.
#[derive(Clone, Default)]
struct EcsDataStore {
    region: String,
    clusters: Vec<EcsCluster>,
    services: Vec<EcsService>,
    tasks: Vec<EcsTask>,
    container_instances: Vec<ContainerInstance>,
    ec2_instances: BTreeMap<String, Ec2InstanceInfo>,
    eni_public_ips: BTreeMap<String, String>,
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn tag(k: &str, v: &str) -> Tag {
    Tag { key: s(k), value: s(v) }
}

fn kv(n: &str, v: &str) -> KeyValuePair {
    KeyValuePair { name: s(n), value: s(v) }
}

fn eni(details: Vec<KeyValuePair>) -> Vec<Attachment> {
    vec![Attachment { r#type: s("ElasticNetworkInterface"), details, ..Default::default() }]
}

fn ec2_info(private_ip: &str, public_ip: &str, subnet_id: &str, instance_type: &str, tags: &[(&str, &str)]) -> Ec2InstanceInfo {
    Ec2InstanceInfo { private_ip: private_ip.into(), public_ip: public_ip.into(), subnet_id: subnet_id.into(), instance_type: instance_type.into(), tags: tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect() }
}

fn strs<S: AsRef<str>>(v: &[S]) -> Vec<String> {
    v.iter().map(|x| x.as_ref().to_string()).collect()
}

// ---------------- the mock server (Go mockECSClient and mockECSEC2Client) ----------------

fn set_opt(j: &mut Json, k: &str, v: &Option<String>) {
    if let Some(v) = v {
        j.set(k, v.as_str());
    }
}

fn tags_json(tags: &[Tag]) -> Json {
    Json::Arr(
        tags.iter()
            .map(|t| {
                let mut j = Json::obj();
                set_opt(&mut j, "key", &t.key);
                set_opt(&mut j, "value", &t.value);
                j
            })
            .collect(),
    )
}

fn cluster_json(c: &EcsCluster) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "clusterArn", &c.cluster_arn);
    set_opt(&mut j, "clusterName", &c.cluster_name);
    set_opt(&mut j, "status", &c.status);
    if !c.tags.is_empty() {
        j.set("tags", tags_json(&c.tags));
    }
    j
}

fn service_json(sv: &EcsService) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "serviceArn", &sv.service_arn);
    set_opt(&mut j, "serviceName", &sv.service_name);
    set_opt(&mut j, "clusterArn", &sv.cluster_arn);
    set_opt(&mut j, "status", &sv.status);
    if !sv.tags.is_empty() {
        j.set("tags", tags_json(&sv.tags));
    }
    j
}

fn task_json(t: &EcsTask) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "taskArn", &t.task_arn);
    set_opt(&mut j, "clusterArn", &t.cluster_arn);
    set_opt(&mut j, "taskDefinitionArn", &t.task_definition_arn);
    set_opt(&mut j, "group", &t.group);
    if !t.launch_type.is_empty() {
        j.set("launchType", t.launch_type.as_str());
    }
    set_opt(&mut j, "lastStatus", &t.last_status);
    set_opt(&mut j, "desiredStatus", &t.desired_status);
    if !t.health_status.is_empty() {
        j.set("healthStatus", t.health_status.as_str());
    }
    set_opt(&mut j, "availabilityZone", &t.availability_zone);
    set_opt(&mut j, "platformFamily", &t.platform_family);
    set_opt(&mut j, "platformVersion", &t.platform_version);
    set_opt(&mut j, "containerInstanceArn", &t.container_instance_arn);
    let atts: Vec<Json> = t
        .attachments
        .iter()
        .map(|a| {
            let mut aj = Json::obj();
            set_opt(&mut aj, "id", &a.id);
            set_opt(&mut aj, "type", &a.r#type);
            set_opt(&mut aj, "status", &a.status);
            let details: Vec<Json> = a
                .details
                .iter()
                .map(|d| {
                    let mut dj = Json::obj();
                    set_opt(&mut dj, "name", &d.name);
                    set_opt(&mut dj, "value", &d.value);
                    dj
                })
                .collect();
            aj.set("details", Json::Arr(details));
            aj
        })
        .collect();
    j.set("attachments", Json::Arr(atts));
    if !t.tags.is_empty() {
        j.set("tags", tags_json(&t.tags));
    }
    j
}

fn ci_json(c: &ContainerInstance) -> Json {
    let mut j = Json::obj();
    set_opt(&mut j, "containerInstanceArn", &c.container_instance_arn);
    set_opt(&mut j, "ec2InstanceId", &c.ec2_instance_id);
    set_opt(&mut j, "status", &c.status);
    j
}

fn list(j: &Json, k: &str) -> Vec<String> {
    j.get(k).and_then(|v| v.as_arr()).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

fn ecs_response(data: &EcsDataStore, op: &str, input: &Json) -> Json {
    let cluster = input.get("cluster").and_then(|v| v.as_str()).unwrap_or("").to_string();
    match op {
        "ListClusters" => Json::obj().with("clusterArns", Json::Arr(data.clusters.iter().map(|c| Json::from(c.cluster_arn.clone().unwrap())).collect())),
        "DescribeClusters" => {
            let out: Vec<Json> = list(input, "clusters").iter().filter_map(|arn| data.clusters.iter().find(|c| c.cluster_arn.as_ref() == Some(arn))).map(cluster_json).collect();
            Json::obj().with("clusters", Json::Arr(out)).with("failures", Json::arr())
        }
        "ListServices" => Json::obj().with("serviceArns", Json::Arr(data.services.iter().filter(|sv| sv.cluster_arn.as_deref() == Some(&cluster)).map(|sv| Json::from(sv.service_arn.clone().unwrap())).collect())),
        "DescribeServices" => {
            let out: Vec<Json> = list(input, "services").iter().filter_map(|arn| data.services.iter().find(|sv| sv.service_arn.as_ref() == Some(arn) && sv.cluster_arn.as_deref() == Some(&cluster))).map(service_json).collect();
            Json::obj().with("services", Json::Arr(out)).with("failures", Json::arr())
        }
        "ListTasks" => {
            let service = input.get("serviceName").and_then(|v| v.as_str());
            let arns: Vec<Json> = data
                .tasks
                .iter()
                .filter(|t| t.cluster_arn.as_deref() == Some(&cluster))
                .filter(|t| match service {
                    Some(sn) => t.group.as_deref() == Some(&format!("service:{sn}")),
                    None => true,
                })
                .map(|t| Json::from(t.task_arn.clone().unwrap()))
                .collect();
            Json::obj().with("taskArns", Json::Arr(arns))
        }
        "DescribeTasks" => {
            let out: Vec<Json> = list(input, "tasks").iter().filter_map(|arn| data.tasks.iter().find(|t| t.task_arn.as_ref() == Some(arn) && t.cluster_arn.as_deref() == Some(&cluster))).map(task_json).collect();
            Json::obj().with("tasks", Json::Arr(out)).with("failures", Json::arr())
        }
        "DescribeContainerInstances" => {
            let out: Vec<Json> = list(input, "containerInstances").iter().filter_map(|arn| data.container_instances.iter().find(|c| c.container_instance_arn.as_ref() == Some(arn))).map(ci_json).collect();
            Json::obj().with("containerInstances", Json::Arr(out)).with("failures", Json::arr())
        }
        _ => panic!("unexpected ECS operation {op}"),
    }
}

fn x(v: &str) -> String {
    discovery::aws::xml::escape(v)
}

fn indexed(params: &[(String, String)], prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1.. {
        match params.iter().find(|(k, _)| *k == format!("{prefix}.{i}")) {
            Some((_, v)) => out.push(v.clone()),
            None => break,
        }
    }
    out
}

fn ec2_response(data: &EcsDataStore, op: &str, params: &[(String, String)]) -> String {
    const NS: &str = "http://ec2.amazonaws.com/doc/2016-11-15/";
    match op {
        "DescribeInstances" => {
            let mut r = String::new();
            for id in indexed(params, "InstanceId") {
                let Some(info) = data.ec2_instances.get(&id) else { continue };
                let mut inst = format!("<instanceId>{}</instanceId><privateIpAddress>{}</privateIpAddress>", x(&id), x(&info.private_ip));
                if !info.public_ip.is_empty() {
                    inst.push_str(&format!("<ipAddress>{}</ipAddress>", x(&info.public_ip)));
                }
                if !info.subnet_id.is_empty() {
                    inst.push_str(&format!("<subnetId>{}</subnetId>", x(&info.subnet_id)));
                }
                if !info.instance_type.is_empty() {
                    inst.push_str(&format!("<instanceType>{}</instanceType>", x(&info.instance_type)));
                }
                if !info.tags.is_empty() {
                    inst.push_str("<tagSet>");
                    for (k, v) in &info.tags {
                        inst.push_str(&format!("<item><key>{}</key><value>{}</value></item>", x(k), x(v)));
                    }
                    inst.push_str("</tagSet>");
                }
                r.push_str(&format!("<item><instancesSet><item>{inst}</item></instancesSet></item>"));
            }
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DescribeInstancesResponse xmlns=\"{NS}\"><requestId>req-1</requestId><reservationSet>{r}</reservationSet></DescribeInstancesResponse>")
        }
        "DescribeNetworkInterfaces" => {
            let mut r = String::new();
            for id in indexed(params, "NetworkInterfaceId") {
                let Some(ip) = data.eni_public_ips.get(&id) else { continue };
                let assoc = if ip.is_empty() { String::new() } else { format!("<association><publicIp>{}</publicIp></association>", x(ip)) };
                r.push_str(&format!("<item><networkInterfaceId>{}</networkInterfaceId>{assoc}</item>", x(&id)));
            }
            format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DescribeNetworkInterfacesResponse xmlns=\"{NS}\"><requestId>req-2</requestId><networkInterfaceSet>{r}</networkInterfaceSet></DescribeNetworkInterfacesResponse>")
        }
        _ => panic!("unexpected EC2 operation {op}"),
    }
}

/// The refresh case being run (for `record`).
static RECORD_CASE: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

/// Appends the request and response to `$ECS_SD_RECORD_FILE` while the refresh case `$ECS_SD_RECORD_CASE`
/// runs (to record a response set for end-to-end runs against real Prometheus).
fn record(req: &MockRequest, body: &str) {
    let (Ok(f), Ok(case)) = (std::env::var("ECS_SD_RECORD_FILE"), std::env::var("ECS_SD_RECORD_CASE")) else { return };
    let guard = RECORD_CASE.lock().unwrap();
    if *guard != case {
        return;
    }
    use std::io::Write;
    let entry = format!("### {} {} X-Amz-Target: {}\n>>> {}\n<<< {}\n\n", req.method, req.path, req.header("x-amz-target"), req.body_str(), body);
    std::fs::OpenOptions::new().create(true).append(true).open(f).unwrap().write_all(entry.as_bytes()).unwrap();
}

fn mock_server(data: &EcsDataStore) -> MockServer {
    let data = data.clone();
    MockServer::from_fn(move |req: &MockRequest| {
        let target = req.header("x-amz-target");
        if let Some(op) = target.strip_prefix("AmazonEC2ContainerServiceV20141113.") {
            let input = Json::parse(&req.body_str()).unwrap();
            let body = ecs_response(&data, op, &input).stringify();
            record(req, &body);
            return MockResponse::new(200, "application/x-amz-json-1.1", body);
        }
        let params = req.form_params();
        let op = params.iter().find(|(k, _)| k == "Action").map(|(_, v)| v.clone()).unwrap_or_default();
        let body = ec2_response(&data, &op, &params);
        record(req, &body);
        MockResponse::xml(body)
    })
}

/// Builds the discovery against the mock (Go: `&ECSDiscovery{ecs: mock, ec2: mock, cfg: ...}`).
fn discovery_for(data: &EcsDataStore, request_concurrency: i64) -> (MockServer, EcsDiscovery) {
    let server = mock_server(data);
    let cfg = EcsSdConfig {
        region: data.region.clone(),
        endpoint: server.url.clone(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: yaml::Secret::new("secret"),
        port: 80,
        request_concurrency,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    let reg = Registry::new();
    let rm = discovery::new_refresh_metrics(&reg);
    let metrics = EcsMetrics::new(rm);
    let mut d = EcsDiscovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics), set_name: "ecs".to_string(), ..Default::default() }).unwrap();
    d.refresher().ec2_endpoint = Some(server.url.clone());
    (server, d)
}

fn initialized(data: &EcsDataStore, request_concurrency: i64) -> (MockServer, EcsDiscovery) {
    let (server, mut d) = discovery_for(data, request_concurrency);
    d.refresher().init_ecs_client(&CancelToken::new()).unwrap();
    (server, d)
}

#[test]
fn test_ecs_discovery_list_cluster_ar_ns() {
    let cases = vec![
        (
            "MultipleClusters",
            EcsDataStore {
                region: "us-west-2".into(),
                clusters: vec![
                    EcsCluster { cluster_name: s("test-cluster"), cluster_arn: s("arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster"), status: s("ACTIVE"), ..Default::default() },
                    EcsCluster { cluster_name: s("prod-cluster"), cluster_arn: s("arn:aws:ecs:us-west-2:123456789012:cluster/prod-cluster"), status: s("ACTIVE"), ..Default::default() },
                ],
                ..Default::default()
            },
            strs(&["arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster", "arn:aws:ecs:us-west-2:123456789012:cluster/prod-cluster"]),
        ),
        (
            "SingleCluster",
            EcsDataStore {
                region: "us-east-1".into(),
                clusters: vec![EcsCluster { cluster_name: s("single-cluster"), cluster_arn: s("arn:aws:ecs:us-east-1:123456789012:cluster/single-cluster"), status: s("ACTIVE"), ..Default::default() }],
                ..Default::default()
            },
            strs(&["arn:aws:ecs:us-east-1:123456789012:cluster/single-cluster"]),
        ),
        ("NoClusters", EcsDataStore { region: "us-east-1".into(), ..Default::default() }, vec![]),
    ];
    for (name, data, expected) in cases {
        let (_srv, mut d) = initialized(&data, 10);
        let clusters = d.refresher().list_cluster_arns(&CancelToken::new()).unwrap();
        assert_eq!(expected, clusters, "{name}");
    }
}

#[test]
fn test_ecs_discovery_describe_clusters() {
    let c1 = EcsCluster { cluster_name: s("test-cluster"), cluster_arn: s("arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster"), status: s("ACTIVE"), tags: vec![tag("Environment", "test"), tag("Team", "backend")] };
    let m1 = EcsCluster { cluster_name: s("cluster-1"), cluster_arn: s("arn:aws:ecs:us-east-1:123456789012:cluster/cluster-1"), status: s("ACTIVE"), ..Default::default() };
    let m2 = EcsCluster { cluster_name: s("cluster-2"), cluster_arn: s("arn:aws:ecs:us-east-1:123456789012:cluster/cluster-2"), status: s("DRAINING"), tags: vec![tag("Stage", "prod")] };
    let cases = vec![
        (
            "SingleClusterWithTags",
            EcsDataStore { region: "us-west-2".into(), clusters: vec![c1.clone()], ..Default::default() },
            strs(&["arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster"]),
            BTreeMap::from([("arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster".to_string(), c1.clone())]),
        ),
        (
            "MultipleClusters",
            EcsDataStore { region: "us-east-1".into(), clusters: vec![m1.clone(), m2.clone()], ..Default::default() },
            strs(&["arn:aws:ecs:us-east-1:123456789012:cluster/cluster-1", "arn:aws:ecs:us-east-1:123456789012:cluster/cluster-2"]),
            BTreeMap::from([("arn:aws:ecs:us-east-1:123456789012:cluster/cluster-1".to_string(), m1.clone()), ("arn:aws:ecs:us-east-1:123456789012:cluster/cluster-2".to_string(), m2.clone())]),
        ),
    ];
    for (name, data, arns, expected) in cases {
        let (_srv, mut d) = initialized(&data, 10);
        let m = d.refresher().describe_clusters(&CancelToken::new(), &arns).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

fn svc(name: &str, arn: &str, cluster: &str, tags: Vec<Tag>) -> EcsService {
    EcsService { service_name: s(name), service_arn: s(arn), cluster_arn: s(cluster), status: s("ACTIVE"), tags }
}

#[test]
fn test_ecs_discovery_list_service_ar_ns() {
    let tc = "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster";
    let c1 = "arn:aws:ecs:us-west-2:123456789012:cluster/cluster-1";
    let c2 = "arn:aws:ecs:us-west-2:123456789012:cluster/cluster-2";
    let cases = vec![
        (
            "SingleClusterWithServices",
            EcsDataStore {
                region: "us-west-2".into(),
                services: vec![
                    svc("web-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service", tc, vec![]),
                    svc("api-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/api-service", tc, vec![]),
                ],
                ..Default::default()
            },
            strs(&[tc]),
            BTreeMap::from([(tc.to_string(), strs(&["arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/api-service"]))]),
        ),
        (
            "MultipleClusters",
            EcsDataStore {
                region: "us-west-2".into(),
                services: vec![
                    svc("web-service", "arn:aws:ecs:us-west-2:123456789012:service/cluster-1/web-service", c1, vec![]),
                    svc("api-service", "arn:aws:ecs:us-west-2:123456789012:service/cluster-2/api-service", c2, vec![]),
                ],
                ..Default::default()
            },
            strs(&[c1, c2]),
            BTreeMap::from([
                (c1.to_string(), strs(&["arn:aws:ecs:us-west-2:123456789012:service/cluster-1/web-service"])),
                (c2.to_string(), strs(&["arn:aws:ecs:us-west-2:123456789012:service/cluster-2/api-service"])),
            ]),
        ),
        ("EmptyCluster", EcsDataStore { region: "us-west-2".into(), ..Default::default() }, strs(&[tc]), BTreeMap::from([(tc.to_string(), vec![])])),
    ];
    for (name, data, arns, expected) in cases {
        let (_srv, mut d) = initialized(&data, 2);
        let m = d.refresher().list_service_arns(&CancelToken::new(), &arns).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

#[test]
fn test_ecs_discovery_describe_services() {
    let tc = "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster";
    let web = svc("web-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service", tc, vec![tag("Environment", "production"), tag("Team", "platform")]);
    let api = svc("api-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/api-service", tc, vec![tag("Environment", "staging")]);
    let cases = vec![
        (
            "ServicesWithTags",
            EcsDataStore { region: "us-west-2".into(), services: vec![web.clone(), api.clone()], ..Default::default() },
            strs(&["arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/api-service"]),
            BTreeMap::from([("web-service".to_string(), web.clone()), ("api-service".to_string(), api.clone())]),
        ),
        ("EmptyServiceList", EcsDataStore { region: "us-west-2".into(), ..Default::default() }, vec![], BTreeMap::new()),
    ];
    for (name, data, arns, expected) in cases {
        let (_srv, mut d) = initialized(&data, 2);
        let m = d.refresher().describe_services(&CancelToken::new(), tc, &arns).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

fn ci(arn: &str, id: &str, status: Option<String>) -> ContainerInstance {
    ContainerInstance { container_instance_arn: s(arn), ec2_instance_id: s(id), status }
}

#[test]
fn test_ecs_discovery_describe_container_instances() {
    let tc = "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster";
    let abc = "arn:aws:ecs:us-west-2:123456789012:container-instance/test-cluster/abc123";
    let xyz = "arn:aws:ecs:us-west-2:123456789012:container-instance/test-cluster/xyz789";
    let task = |arn: &str, ci: Option<&str>, lt: &str| EcsTask { task_arn: s(arn), container_instance_arn: ci.map(str::to_string), launch_type: lt.into(), ..Default::default() };
    let cases = vec![
        (
            "EC2Tasks",
            EcsDataStore { region: "us-west-2".into(), container_instances: vec![ci(abc, "i-1234567890abcdef0", None), ci(xyz, "i-0987654321fedcba0", None)], ..Default::default() },
            vec![task("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", Some(abc), "EC2"), task("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-2", Some(xyz), "EC2")],
            BTreeMap::from([(abc.to_string(), "i-1234567890abcdef0".to_string()), (xyz.to_string(), "i-0987654321fedcba0".to_string())]),
        ),
        ("FargateTasks", EcsDataStore { region: "us-west-2".into(), ..Default::default() }, vec![task("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", None, "FARGATE")], BTreeMap::new()),
        (
            "MixedTasks",
            EcsDataStore { region: "us-west-2".into(), container_instances: vec![ci(abc, "i-1234567890abcdef0", None)], ..Default::default() },
            vec![task("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-ec2", Some(abc), "EC2"), task("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-fargate", None, "FARGATE")],
            BTreeMap::from([(abc.to_string(), "i-1234567890abcdef0".to_string())]),
        ),
    ];
    for (name, data, tasks, expected) in cases {
        let (_srv, mut d) = initialized(&data, 2);
        let m = d.refresher().describe_container_instances(&CancelToken::new(), tc, &tasks).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

#[test]
fn test_ecs_discovery_describe_ec2_instances() {
    let i1 = ec2_info("10.0.1.50", "54.1.2.3", "subnet-12345", "t3.medium", &[("Name", "ecs-host-1"), ("Environment", "production")]);
    let i2 = ec2_info("10.0.1.75", "54.2.3.4", "subnet-67890", "t3.large", &[("Name", "ecs-host-2"), ("Team", "platform")]);
    let ip = ec2_info("10.0.1.100", "", "subnet-private", "t3.micro", &[]);
    let cases = vec![
        (
            "InstancesWithTags",
            BTreeMap::from([("i-1234567890abcdef0".to_string(), i1.clone()), ("i-0987654321fedcba0".to_string(), i2.clone())]),
            strs(&["i-1234567890abcdef0", "i-0987654321fedcba0"]),
            BTreeMap::from([("i-1234567890abcdef0".to_string(), i1.clone()), ("i-0987654321fedcba0".to_string(), i2.clone())]),
        ),
        ("EmptyList", BTreeMap::new(), vec![], BTreeMap::new()),
        ("InstanceWithoutPublicIP", BTreeMap::from([("i-privateonly".to_string(), ip.clone())]), strs(&["i-privateonly"]), BTreeMap::from([("i-privateonly".to_string(), ip.clone())])),
    ];
    for (name, instances, ids, expected) in cases {
        let data = EcsDataStore { region: "us-west-2".into(), ec2_instances: instances, ..Default::default() };
        let (_srv, mut d) = initialized(&data, 2);
        let m = d.refresher().describe_ec2_instances(&CancelToken::new(), &ids).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

#[test]
fn test_ecs_discovery_describe_network_interfaces() {
    let fargate = |arn: &str, eni_id: &str, ip: &str| EcsTask { task_arn: s(arn), launch_type: "FARGATE".into(), attachments: eni(vec![kv("networkInterfaceId", eni_id), kv("privateIPv4Address", ip)]), ..Default::default() };
    let bridge = |arn: &str| EcsTask { task_arn: s(arn), launch_type: "EC2".into(), ..Default::default() };
    let cases = vec![
        (
            "AwsvpcTasksWithPublicIPs",
            BTreeMap::from([("eni-12345".to_string(), "52.1.2.3".to_string()), ("eni-67890".to_string(), "52.2.3.4".to_string())]),
            vec![fargate("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", "eni-12345", "10.0.1.100"), fargate("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-2", "eni-67890", "10.0.1.200")],
            BTreeMap::from([("eni-12345".to_string(), "52.1.2.3".to_string()), ("eni-67890".to_string(), "52.2.3.4".to_string())]),
        ),
        ("AwsvpcTasksWithoutPublicIPs", BTreeMap::new(), vec![fargate("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", "eni-private", "10.0.1.100")], BTreeMap::new()),
        ("BridgeTasksNoENI", BTreeMap::new(), vec![bridge("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1")], BTreeMap::new()),
        (
            "MixedTasks",
            BTreeMap::from([("eni-fargate".to_string(), "52.1.2.3".to_string())]),
            vec![fargate("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-fargate", "eni-fargate", "10.0.1.100"), bridge("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-bridge")],
            BTreeMap::from([("eni-fargate".to_string(), "52.1.2.3".to_string())]),
        ),
    ];
    for (name, ips, tasks, expected) in cases {
        let data = EcsDataStore { region: "us-west-2".into(), eni_public_ips: ips, ..Default::default() };
        let (_srv, mut d) = initialized(&data, 2);
        let m = d.refresher().describe_network_interfaces(&CancelToken::new(), &tasks).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

#[test]
fn test_ecs_discovery_list_task_ar_ns() {
    let tc = "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster";
    let t = |n: &str, g: &str, def: &str| EcsTask { task_arn: s(&format!("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/{n}")), cluster_arn: s(tc), group: s(g), task_definition_arn: s(def), ..Default::default() };
    let cases = vec![
        (
            "TasksInCluster",
            EcsDataStore {
                region: "us-west-2".into(),
                tasks: vec![
                    t("task-1", "service:web-service", "arn:aws:ecs:us-west-2:123456789012:task-definition/web-task:1"),
                    t("task-2", "service:web-service", "arn:aws:ecs:us-west-2:123456789012:task-definition/web-task:1"),
                    t("task-3", "service:api-service", "arn:aws:ecs:us-west-2:123456789012:task-definition/api-task:2"),
                ],
                ..Default::default()
            },
            BTreeMap::from([(tc.to_string(), strs(&["arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-2", "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-3"]))]),
        ),
        ("EmptyCluster", EcsDataStore { region: "us-west-2".into(), ..Default::default() }, BTreeMap::from([(tc.to_string(), vec![])])),
    ];
    for (name, data, expected) in cases {
        let (_srv, mut d) = initialized(&data, 1);
        let m = d.refresher().list_task_arns(&CancelToken::new(), &strs(&[tc])).unwrap();
        assert_eq!(expected, m, "{name}");
    }
}

#[test]
fn test_ecs_discovery_describe_tasks() {
    let tc = "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster";
    let t1 = EcsTask {
        task_arn: s("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1"),
        cluster_arn: s(tc),
        group: s("service:web-service"),
        task_definition_arn: s("arn:aws:ecs:us-west-2:123456789012:task-definition/web-task:1"),
        last_status: s("RUNNING"),
        tags: vec![tag("Environment", "production")],
        ..Default::default()
    };
    let t2 = EcsTask {
        task_arn: s("arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-2"),
        cluster_arn: s(tc),
        group: s("service:api-service"),
        task_definition_arn: s("arn:aws:ecs:us-west-2:123456789012:task-definition/api-task:2"),
        last_status: s("RUNNING"),
        ..Default::default()
    };
    let cases = vec![
        (
            "TasksInCluster",
            EcsDataStore { region: "us-west-2".into(), tasks: vec![t1.clone(), t2.clone()], ..Default::default() },
            strs(&["arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1", "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-2"]),
            vec![t1.clone(), t2.clone()],
        ),
        ("EmptyTaskList", EcsDataStore { region: "us-west-2".into(), ..Default::default() }, vec![], vec![]),
    ];
    for (name, data, arns, expected) in cases {
        let (_srv, mut d) = initialized(&data, 1);
        let tasks = d.refresher().describe_tasks(&CancelToken::new(), tc, &arns).unwrap();
        assert_eq!(expected, tasks, "{name}");
    }
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

fn group(source: &str, targets: Vec<LabelSet>) -> Vec<Arc<Group>> {
    vec![Arc::new(Group { targets, labels: LabelSet::new(), source: source.to_string() })]
}

fn cluster(name: &str, region: &str) -> EcsCluster {
    EcsCluster { cluster_name: s(name), cluster_arn: s(&format!("arn:aws:ecs:{region}:123456789012:cluster/{name}")), status: s("ACTIVE"), ..Default::default() }
}

#[allow(clippy::too_many_arguments)]
fn run_task(cluster: &str, name: &str, def: &str, group: &str, lt: &str, az: &str, ci: Option<&str>, attachments: Vec<Attachment>) -> EcsTask {
    EcsTask {
        task_arn: s(&format!("arn:aws:ecs:us-west-2:123456789012:task/{cluster}/{name}")),
        cluster_arn: s(&format!("arn:aws:ecs:us-west-2:123456789012:cluster/{cluster}")),
        task_definition_arn: s(&format!("arn:aws:ecs:us-west-2:123456789012:task-definition/{def}")),
        group: s(group),
        launch_type: lt.into(),
        last_status: s("RUNNING"),
        desired_status: s("RUNNING"),
        health_status: "HEALTHY".into(),
        availability_zone: s(az),
        container_instance_arn: ci.map(str::to_string),
        attachments,
        ..Default::default()
    }
}

#[test]
fn test_ecs_discovery_refresh() {
    let mut cases: Vec<(&str, EcsDataStore, Vec<Arc<Group>>)> = Vec::new();

    // SingleClusterWithTasks
    {
        let mut c = cluster("test-cluster", "us-west-2");
        c.tags = vec![tag("Environment", "test")];
        let mut t = run_task("test-cluster", "task-1", "web-task:1", "service:web-service", "FARGATE", "us-west-2a", None, eni(vec![kv("subnetId", "subnet-12345"), kv("privateIPv4Address", "10.0.1.100"), kv("networkInterfaceId", "eni-fargate-123")]));
        t.platform_family = s("Linux");
        t.platform_version = s("1.4.0");
        t.tags = vec![tag("Version", "v1.0")];
        cases.push((
            "SingleClusterWithTasks",
            EcsDataStore {
                region: "us-west-2".into(),
                clusters: vec![c],
                services: vec![svc("web-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service", "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster", vec![tag("App", "web")])],
                tasks: vec![t],
                eni_public_ips: BTreeMap::from([("eni-fargate-123".to_string(), "52.1.2.3".to_string())]),
                ..Default::default()
            },
            group(
                "us-west-2",
                vec![ls(&[
                    ("__address__", "10.0.1.100:80"),
                    ("__meta_ecs_cluster", "test-cluster"),
                    ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster"),
                    ("__meta_ecs_service", "web-service"),
                    ("__meta_ecs_service_arn", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/web-service"),
                    ("__meta_ecs_service_status", "ACTIVE"),
                    ("__meta_ecs_task_group", "service:web-service"),
                    ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-1"),
                    ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/web-task:1"),
                    ("__meta_ecs_region", "us-west-2"),
                    ("__meta_ecs_availability_zone", "us-west-2a"),
                    ("__meta_ecs_subnet_id", "subnet-12345"),
                    ("__meta_ecs_ip_address", "10.0.1.100"),
                    ("__meta_ecs_launch_type", "FARGATE"),
                    ("__meta_ecs_desired_status", "RUNNING"),
                    ("__meta_ecs_last_status", "RUNNING"),
                    ("__meta_ecs_health_status", "HEALTHY"),
                    ("__meta_ecs_platform_family", "Linux"),
                    ("__meta_ecs_platform_version", "1.4.0"),
                    ("__meta_ecs_network_mode", "awsvpc"),
                    ("__meta_ecs_public_ip", "52.1.2.3"),
                    ("__meta_ecs_tag_cluster_Environment", "test"),
                    ("__meta_ecs_tag_service_App", "web"),
                    ("__meta_ecs_tag_task_Version", "v1.0"),
                ])],
            ),
        ));
    }

    // NoTasks
    cases.push((
        "NoTasks",
        EcsDataStore {
            region: "us-east-1".into(),
            clusters: vec![cluster("empty-cluster", "us-east-1")],
            services: vec![svc("empty-service", "arn:aws:ecs:us-east-1:123456789012:service/empty-cluster/empty-service", "arn:aws:ecs:us-east-1:123456789012:cluster/empty-cluster", vec![])],
            ..Default::default()
        },
        group("us-east-1", vec![]),
    ));

    // TaskWithoutENI
    cases.push((
        "TaskWithoutENI",
        EcsDataStore {
            region: "us-west-2".into(),
            clusters: vec![cluster("test-cluster", "us-west-2")],
            services: vec![svc("service-1", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/service-1", "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster", vec![])],
            tasks: vec![run_task("test-cluster", "task-1", "task-def:1", "service:service-1", "EC2", "us-west-2a", None, vec![])],
            ..Default::default()
        },
        group("us-west-2", vec![]),
    ));

    // StandaloneTaskWithCustomGroup
    {
        let mut t = run_task("standalone-cluster", "task-standalone", "standalone-task:1", "batch-jobs", "FARGATE", "us-west-2a", None, eni(vec![kv("subnetId", "subnet-standalone-1"), kv("privateIPv4Address", "10.0.4.10"), kv("networkInterfaceId", "eni-standalone-123")]));
        t.tags = vec![tag("Role", "batch")];
        cases.push((
            "StandaloneTaskWithCustomGroup",
            EcsDataStore {
                region: "us-west-2".into(),
                clusters: vec![cluster("standalone-cluster", "us-west-2")],
                tasks: vec![t],
                eni_public_ips: BTreeMap::from([("eni-standalone-123".to_string(), "52.4.5.6".to_string())]),
                ..Default::default()
            },
            group(
                "us-west-2",
                vec![ls(&[
                    ("__address__", "10.0.4.10:80"),
                    ("__meta_ecs_cluster", "standalone-cluster"),
                    ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/standalone-cluster"),
                    ("__meta_ecs_task_group", "batch-jobs"),
                    ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/standalone-cluster/task-standalone"),
                    ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/standalone-task:1"),
                    ("__meta_ecs_region", "us-west-2"),
                    ("__meta_ecs_availability_zone", "us-west-2a"),
                    ("__meta_ecs_subnet_id", "subnet-standalone-1"),
                    ("__meta_ecs_ip_address", "10.0.4.10"),
                    ("__meta_ecs_launch_type", "FARGATE"),
                    ("__meta_ecs_desired_status", "RUNNING"),
                    ("__meta_ecs_last_status", "RUNNING"),
                    ("__meta_ecs_health_status", "HEALTHY"),
                    ("__meta_ecs_network_mode", "awsvpc"),
                    ("__meta_ecs_public_ip", "52.4.5.6"),
                    ("__meta_ecs_tag_task_Role", "batch"),
                ])],
            ),
        ));
    }

    // TaskWithBridgeNetworking
    cases.push((
        "TaskWithBridgeNetworking",
        EcsDataStore {
            region: "us-west-2".into(),
            clusters: vec![cluster("test-cluster", "us-west-2")],
            services: vec![svc("bridge-service", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/bridge-service", "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster", vec![])],
            tasks: vec![run_task("test-cluster", "task-bridge", "bridge-task:1", "service:bridge-service", "EC2", "us-west-2a", Some("arn:aws:ecs:us-west-2:123456789012:container-instance/test-cluster/abc123"), vec![])],
            container_instances: vec![ci("arn:aws:ecs:us-west-2:123456789012:container-instance/test-cluster/abc123", "i-1234567890abcdef0", s("ACTIVE"))],
            ec2_instances: BTreeMap::from([("i-1234567890abcdef0".to_string(), ec2_info("10.0.1.50", "54.1.2.3", "subnet-bridge-1", "t3.medium", &[("Name", "ecs-host-1"), ("Environment", "production")]))]),
            ..Default::default()
        },
        group(
            "us-west-2",
            vec![ls(&[
                ("__address__", "10.0.1.50:80"),
                ("__meta_ecs_cluster", "test-cluster"),
                ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/test-cluster"),
                ("__meta_ecs_service", "bridge-service"),
                ("__meta_ecs_service_arn", "arn:aws:ecs:us-west-2:123456789012:service/test-cluster/bridge-service"),
                ("__meta_ecs_service_status", "ACTIVE"),
                ("__meta_ecs_task_group", "service:bridge-service"),
                ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/test-cluster/task-bridge"),
                ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/bridge-task:1"),
                ("__meta_ecs_region", "us-west-2"),
                ("__meta_ecs_availability_zone", "us-west-2a"),
                ("__meta_ecs_ip_address", "10.0.1.50"),
                ("__meta_ecs_subnet_id", "subnet-bridge-1"),
                ("__meta_ecs_launch_type", "EC2"),
                ("__meta_ecs_desired_status", "RUNNING"),
                ("__meta_ecs_last_status", "RUNNING"),
                ("__meta_ecs_health_status", "HEALTHY"),
                ("__meta_ecs_network_mode", "bridge"),
                ("__meta_ecs_container_instance_arn", "arn:aws:ecs:us-west-2:123456789012:container-instance/test-cluster/abc123"),
                ("__meta_ecs_ec2_instance_id", "i-1234567890abcdef0"),
                ("__meta_ecs_ec2_instance_type", "t3.medium"),
                ("__meta_ecs_ec2_instance_private_ip", "10.0.1.50"),
                ("__meta_ecs_ec2_instance_public_ip", "54.1.2.3"),
                ("__meta_ecs_public_ip", "54.1.2.3"),
                ("__meta_ecs_tag_ec2_Name", "ecs-host-1"),
                ("__meta_ecs_tag_ec2_Environment", "production"),
            ])],
        ),
    ));

    // MixedNetworkingModes
    cases.push((
        "MixedNetworkingModes",
        EcsDataStore {
            region: "us-west-2".into(),
            clusters: vec![cluster("mixed-cluster", "us-west-2")],
            services: vec![svc("mixed-service", "arn:aws:ecs:us-west-2:123456789012:service/mixed-cluster/mixed-service", "arn:aws:ecs:us-west-2:123456789012:cluster/mixed-cluster", vec![])],
            tasks: vec![
                run_task("mixed-cluster", "task-awsvpc", "awsvpc-task:1", "service:mixed-service", "FARGATE", "us-west-2a", None, eni(vec![kv("subnetId", "subnet-12345"), kv("privateIPv4Address", "10.0.2.100"), kv("networkInterfaceId", "eni-mixed-awsvpc")])),
                run_task("mixed-cluster", "task-bridge", "bridge-task:1", "service:mixed-service", "EC2", "us-west-2b", Some("arn:aws:ecs:us-west-2:123456789012:container-instance/mixed-cluster/xyz789"), vec![]),
            ],
            container_instances: vec![ci("arn:aws:ecs:us-west-2:123456789012:container-instance/mixed-cluster/xyz789", "i-0987654321fedcba0", s("ACTIVE"))],
            ec2_instances: BTreeMap::from([("i-0987654321fedcba0".to_string(), ec2_info("10.0.1.75", "54.2.3.4", "subnet-bridge-2", "t3.large", &[("Name", "mixed-host"), ("Team", "platform")]))]),
            eni_public_ips: BTreeMap::from([("eni-mixed-awsvpc".to_string(), "52.2.3.4".to_string())]),
        },
        group(
            "us-west-2",
            vec![
                ls(&[
                    ("__address__", "10.0.2.100:80"),
                    ("__meta_ecs_cluster", "mixed-cluster"),
                    ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/mixed-cluster"),
                    ("__meta_ecs_service", "mixed-service"),
                    ("__meta_ecs_service_arn", "arn:aws:ecs:us-west-2:123456789012:service/mixed-cluster/mixed-service"),
                    ("__meta_ecs_service_status", "ACTIVE"),
                    ("__meta_ecs_task_group", "service:mixed-service"),
                    ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/mixed-cluster/task-awsvpc"),
                    ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/awsvpc-task:1"),
                    ("__meta_ecs_region", "us-west-2"),
                    ("__meta_ecs_availability_zone", "us-west-2a"),
                    ("__meta_ecs_ip_address", "10.0.2.100"),
                    ("__meta_ecs_subnet_id", "subnet-12345"),
                    ("__meta_ecs_launch_type", "FARGATE"),
                    ("__meta_ecs_desired_status", "RUNNING"),
                    ("__meta_ecs_last_status", "RUNNING"),
                    ("__meta_ecs_health_status", "HEALTHY"),
                    ("__meta_ecs_network_mode", "awsvpc"),
                    ("__meta_ecs_public_ip", "52.2.3.4"),
                ]),
                ls(&[
                    ("__address__", "10.0.1.75:80"),
                    ("__meta_ecs_cluster", "mixed-cluster"),
                    ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/mixed-cluster"),
                    ("__meta_ecs_service", "mixed-service"),
                    ("__meta_ecs_service_arn", "arn:aws:ecs:us-west-2:123456789012:service/mixed-cluster/mixed-service"),
                    ("__meta_ecs_service_status", "ACTIVE"),
                    ("__meta_ecs_task_group", "service:mixed-service"),
                    ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/mixed-cluster/task-bridge"),
                    ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/bridge-task:1"),
                    ("__meta_ecs_region", "us-west-2"),
                    ("__meta_ecs_availability_zone", "us-west-2b"),
                    ("__meta_ecs_ip_address", "10.0.1.75"),
                    ("__meta_ecs_subnet_id", "subnet-bridge-2"),
                    ("__meta_ecs_launch_type", "EC2"),
                    ("__meta_ecs_desired_status", "RUNNING"),
                    ("__meta_ecs_last_status", "RUNNING"),
                    ("__meta_ecs_health_status", "HEALTHY"),
                    ("__meta_ecs_network_mode", "bridge"),
                    ("__meta_ecs_container_instance_arn", "arn:aws:ecs:us-west-2:123456789012:container-instance/mixed-cluster/xyz789"),
                    ("__meta_ecs_ec2_instance_id", "i-0987654321fedcba0"),
                    ("__meta_ecs_ec2_instance_type", "t3.large"),
                    ("__meta_ecs_ec2_instance_private_ip", "10.0.1.75"),
                    ("__meta_ecs_ec2_instance_public_ip", "54.2.3.4"),
                    ("__meta_ecs_public_ip", "54.2.3.4"),
                    ("__meta_ecs_tag_ec2_Name", "mixed-host"),
                    ("__meta_ecs_tag_ec2_Team", "platform"),
                ]),
            ],
        ),
    ));

    // EC2WithAwsvpcNetworking
    cases.push((
        "EC2WithAwsvpcNetworking",
        EcsDataStore {
            region: "us-west-2".into(),
            clusters: vec![cluster("ec2-awsvpc-cluster", "us-west-2")],
            services: vec![svc("ec2-awsvpc-service", "arn:aws:ecs:us-west-2:123456789012:service/ec2-awsvpc-cluster/ec2-awsvpc-service", "arn:aws:ecs:us-west-2:123456789012:cluster/ec2-awsvpc-cluster", vec![])],
            tasks: vec![run_task(
                "ec2-awsvpc-cluster",
                "task-ec2-awsvpc",
                "ec2-awsvpc-task:1",
                "service:ec2-awsvpc-service",
                "EC2",
                "us-west-2c",
                Some("arn:aws:ecs:us-west-2:123456789012:container-instance/ec2-awsvpc-cluster/def456"),
                eni(vec![kv("subnetId", "subnet-99999"), kv("privateIPv4Address", "10.0.3.200"), kv("networkInterfaceId", "eni-ec2-awsvpc")]),
            )],
            eni_public_ips: BTreeMap::from([("eni-ec2-awsvpc".to_string(), "52.3.4.5".to_string())]),
            container_instances: vec![ci("arn:aws:ecs:us-west-2:123456789012:container-instance/ec2-awsvpc-cluster/def456", "i-ec2awsvpcinstance", s("ACTIVE"))],
            ec2_instances: BTreeMap::from([("i-ec2awsvpcinstance".to_string(), ec2_info("10.0.9.99", "54.3.4.5", "subnet-wrong", "c5.2xlarge", &[("Name", "ec2-awsvpc-host"), ("Owner", "team-a")]))]),
        },
        group(
            "us-west-2",
            vec![ls(&[
                ("__address__", "10.0.3.200:80"),
                ("__meta_ecs_cluster", "ec2-awsvpc-cluster"),
                ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/ec2-awsvpc-cluster"),
                ("__meta_ecs_service", "ec2-awsvpc-service"),
                ("__meta_ecs_service_arn", "arn:aws:ecs:us-west-2:123456789012:service/ec2-awsvpc-cluster/ec2-awsvpc-service"),
                ("__meta_ecs_service_status", "ACTIVE"),
                ("__meta_ecs_task_group", "service:ec2-awsvpc-service"),
                ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/ec2-awsvpc-cluster/task-ec2-awsvpc"),
                ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/ec2-awsvpc-task:1"),
                ("__meta_ecs_region", "us-west-2"),
                ("__meta_ecs_availability_zone", "us-west-2c"),
                ("__meta_ecs_ip_address", "10.0.3.200"),
                ("__meta_ecs_subnet_id", "subnet-99999"),
                ("__meta_ecs_launch_type", "EC2"),
                ("__meta_ecs_desired_status", "RUNNING"),
                ("__meta_ecs_last_status", "RUNNING"),
                ("__meta_ecs_health_status", "HEALTHY"),
                ("__meta_ecs_network_mode", "awsvpc"),
                ("__meta_ecs_container_instance_arn", "arn:aws:ecs:us-west-2:123456789012:container-instance/ec2-awsvpc-cluster/def456"),
                ("__meta_ecs_ec2_instance_id", "i-ec2awsvpcinstance"),
                ("__meta_ecs_ec2_instance_type", "c5.2xlarge"),
                ("__meta_ecs_ec2_instance_private_ip", "10.0.9.99"),
                ("__meta_ecs_ec2_instance_public_ip", "54.3.4.5"),
                ("__meta_ecs_public_ip", "52.3.4.5"),
                ("__meta_ecs_tag_ec2_Name", "ec2-awsvpc-host"),
                ("__meta_ecs_tag_ec2_Owner", "team-a"),
            ])],
        ),
    ));

    // TaskWithNilOptionalFields
    cases.push((
        "TaskWithNilOptionalFields",
        EcsDataStore {
            region: "us-west-2".into(),
            clusters: vec![cluster("nil-cluster", "us-west-2")],
            tasks: vec![EcsTask {
                task_arn: s("arn:aws:ecs:us-west-2:123456789012:task/nil-cluster/task-nil"),
                cluster_arn: s("arn:aws:ecs:us-west-2:123456789012:cluster/nil-cluster"),
                task_definition_arn: s("arn:aws:ecs:us-west-2:123456789012:task-definition/nil-task:1"),
                launch_type: "FARGATE".into(),
                // A detail with no name at all, a known key whose value is absent, then the IP.
                attachments: eni(vec![KeyValuePair::default(), KeyValuePair { name: s("subnetId"), value: None }, kv("privateIPv4Address", "10.0.4.10")]),
                ..Default::default()
            }],
            ..Default::default()
        },
        group(
            "us-west-2",
            vec![ls(&[
                ("__address__", "10.0.4.10:80"),
                ("__meta_ecs_cluster", "nil-cluster"),
                ("__meta_ecs_cluster_arn", "arn:aws:ecs:us-west-2:123456789012:cluster/nil-cluster"),
                ("__meta_ecs_task_arn", "arn:aws:ecs:us-west-2:123456789012:task/nil-cluster/task-nil"),
                ("__meta_ecs_task_definition", "arn:aws:ecs:us-west-2:123456789012:task-definition/nil-task:1"),
                ("__meta_ecs_region", "us-west-2"),
                ("__meta_ecs_ip_address", "10.0.4.10"),
                ("__meta_ecs_launch_type", "FARGATE"),
                ("__meta_ecs_health_status", ""),
                ("__meta_ecs_network_mode", "awsvpc"),
            ])],
        ),
    ));

    for (name, data, expected) in cases {
        *RECORD_CASE.lock().unwrap() = name.to_string();
        let (_srv, mut d) = discovery_for(&data, 1);
        let groups = d.refresh(&CancelToken::new()).unwrap();
        if name == "MixedNetworkingModes" {
            // ElementsMatch: the order of targets is not part of the contract.
            assert_eq!(expected.len(), groups.len(), "{name}");
            assert_eq!(expected[0].source, groups[0].source, "{name}");
            let mut got = groups[0].targets.clone();
            let mut want = expected[0].targets.clone();
            got.sort_by_key(|l| l.get("__address__").cloned());
            want.sort_by_key(|l| l.get("__address__").cloned());
            assert_eq!(want, got, "{name}");
        } else {
            assert_eq!(expected, groups, "{name}");
        }
    }
}
