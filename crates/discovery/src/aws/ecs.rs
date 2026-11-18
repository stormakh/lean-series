//! Port of `discovery/aws/ecs.go` and `metrics_ecs.go`: ECS tasks as targets (`ecs_sd_configs`).
//!
//! The ECS calls use awsJson1.1 (`client::ECS`); the EC2 calls of the role (DescribeInstances,
//! DescribeNetworkInterfaces) use a second client for EC2 (EC2 Query protocol). As in Go, the configured
//! `endpoint` applies to the ECS client only; the EC2 client resolves its endpoint from the environment or
//! the region (`EcsRefresher::ec2_endpoint` overrides it, for tests).
//!
//! Differs from Go: the per-cluster work runs on at most `request_concurrency` threads (Go: one goroutine per
//! cluster), and targets come out in cluster-list and task order (Go: completion order). A cluster that
//! ListTasks returned but DescribeClusters did not, or a described service without a name, panics in Go; here
//! the cluster is skipped (with an error log) and the service is left out of the name map.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use json::Json;
use model::labelset::{LabelSet, META_LABEL_PREFIX};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::client::{self, Client, ClientConfig};
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::util::{join_host_port, sanitize_label_name};
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const ECS_LABEL: &str = "__meta_ecs_";

fn l(name: &str) -> String {
    format!("{META_LABEL_PREFIX}ecs_{name}")
}

/// Go `DefaultECSSDConfig`.
pub fn default_ecs_sd_config() -> EcsSdConfig {
    EcsSdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), request_concurrency: 20, http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

/// Go `ECSSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EcsSdConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: yaml::Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub clusters: Vec<String>,
    pub port: i64,
    pub refresh_interval: yaml::Duration,
    /// The maximum number of concurrent ECS API requests (per batch group).
    pub request_concurrency: i64,
    pub http_client_config: HttpClientConfig,
}

yaml::yaml_struct!(EcsSdConfig, "aws.plain", [
    (region: "region")
    (endpoint: "endpoint")
    (access_key: "access_key", omitempty)
    (secret_key: "secret_key", omitempty)
    (profile: "profile", omitempty)
    (role_arn: "role_arn", omitempty)
    (external_id: "external_id", omitempty)
    (clusters: "clusters", omitempty)
    (port: "port")
    (refresh_interval: "refresh_interval", omitempty)
    (request_concurrency: "request_concurrency", omitempty)
    (inline http_client_config)
]);

impl Unmarshal for EcsSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_ecs_sd_config();
            d.inner_struct(n, self)?;
            if self.request_concurrency <= 0 {
                return Err(YError::Msg(format!("ecs_sd: request_concurrency must be positive, got {}", self.request_concurrency)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.ECSSDConfig".to_string()
    }
}

impl yaml::Marshal for EcsSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for EcsSdConfig {
    const GO_TYPE: &'static str = "aws.ECSSDConfig";
}

impl Config for EcsSdConfig {
    fn name(&self) -> &'static str {
        "ecs"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn crate::Discoverer>, String> {
        Ok(Box::new(EcsDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        EcsMetrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl EcsSdConfig {
    /// The connection settings for the shared AWS client.
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            profile: self.profile.clone(),
            role_arn: self.role_arn.clone(),
            external_id: self.external_id.clone(),
            http_client_config: self.http_client_config.clone(),
        }
    }
}

/// Go `ecsMetrics`.
pub struct EcsMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl EcsMetrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<EcsMetrics> {
        Arc::new(EcsMetrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for EcsMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// ---------------- API types (the fields discovery reads; `Option` = Go pointer) ----------------

/// ECS `Tag`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tag {
    pub key: Option<String>,
    pub value: Option<String>,
}

/// ECS `Cluster`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EcsCluster {
    pub cluster_arn: Option<String>,
    pub cluster_name: Option<String>,
    pub status: Option<String>,
    pub tags: Vec<Tag>,
}

/// ECS `Service`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EcsService {
    pub service_arn: Option<String>,
    pub service_name: Option<String>,
    pub cluster_arn: Option<String>,
    pub status: Option<String>,
    pub tags: Vec<Tag>,
}

/// ECS `KeyValuePair`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KeyValuePair {
    pub name: Option<String>,
    pub value: Option<String>,
}

/// ECS `Attachment`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Attachment {
    pub id: Option<String>,
    pub r#type: Option<String>,
    pub status: Option<String>,
    pub details: Vec<KeyValuePair>,
}

/// ECS `Task`. `launch_type` and `health_status` are Go string enums ("" when absent).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EcsTask {
    pub task_arn: Option<String>,
    pub cluster_arn: Option<String>,
    pub task_definition_arn: Option<String>,
    pub group: Option<String>,
    pub launch_type: String,
    pub last_status: Option<String>,
    pub desired_status: Option<String>,
    pub health_status: String,
    pub availability_zone: Option<String>,
    pub platform_family: Option<String>,
    pub platform_version: Option<String>,
    pub container_instance_arn: Option<String>,
    pub attachments: Vec<Attachment>,
    pub tags: Vec<Tag>,
}

/// ECS `ContainerInstance`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContainerInstance {
    pub container_instance_arn: Option<String>,
    pub ec2_instance_id: Option<String>,
    pub status: Option<String>,
}

/// Go `ec2InstanceInfo`: what DescribeInstances gives for an instance.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ec2InstanceInfo {
    pub private_ip: String,
    pub public_ip: String,
    pub subnet_id: String,
    pub instance_type: String,
    pub tags: BTreeMap<String, String>,
}

fn opt_str(j: &Json, k: &str) -> Option<String> {
    j.get(k).and_then(|v| v.as_str()).map(str::to_string)
}

fn str_list(j: &Json, k: &str) -> Vec<String> {
    j.get(k).and_then(|v| v.as_arr()).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

fn objs<'a>(j: &'a Json, k: &str) -> &'a [Json] {
    j.get(k).and_then(|v| v.as_arr()).map(|a| a.as_slice()).unwrap_or(&[])
}

fn tags_from(j: &Json) -> Vec<Tag> {
    objs(j, "tags").iter().map(|t| Tag { key: opt_str(t, "key"), value: opt_str(t, "value") }).collect()
}

impl EcsCluster {
    pub fn from_json(j: &Json) -> EcsCluster {
        EcsCluster { cluster_arn: opt_str(j, "clusterArn"), cluster_name: opt_str(j, "clusterName"), status: opt_str(j, "status"), tags: tags_from(j) }
    }
}

impl EcsService {
    pub fn from_json(j: &Json) -> EcsService {
        EcsService { service_arn: opt_str(j, "serviceArn"), service_name: opt_str(j, "serviceName"), cluster_arn: opt_str(j, "clusterArn"), status: opt_str(j, "status"), tags: tags_from(j) }
    }
}

impl EcsTask {
    pub fn from_json(j: &Json) -> EcsTask {
        EcsTask {
            task_arn: opt_str(j, "taskArn"),
            cluster_arn: opt_str(j, "clusterArn"),
            task_definition_arn: opt_str(j, "taskDefinitionArn"),
            group: opt_str(j, "group"),
            launch_type: opt_str(j, "launchType").unwrap_or_default(),
            last_status: opt_str(j, "lastStatus"),
            desired_status: opt_str(j, "desiredStatus"),
            health_status: opt_str(j, "healthStatus").unwrap_or_default(),
            availability_zone: opt_str(j, "availabilityZone"),
            platform_family: opt_str(j, "platformFamily"),
            platform_version: opt_str(j, "platformVersion"),
            container_instance_arn: opt_str(j, "containerInstanceArn"),
            attachments: objs(j, "attachments")
                .iter()
                .map(|a| Attachment {
                    id: opt_str(a, "id"),
                    r#type: opt_str(a, "type"),
                    status: opt_str(a, "status"),
                    details: objs(a, "details").iter().map(|d| KeyValuePair { name: opt_str(d, "name"), value: opt_str(d, "value") }).collect(),
                })
                .collect(),
            tags: tags_from(j),
        }
    }
}

impl ContainerInstance {
    pub fn from_json(j: &Json) -> ContainerInstance {
        ContainerInstance { container_instance_arn: opt_str(j, "containerInstanceArn"), ec2_instance_id: opt_str(j, "ec2InstanceId"), status: opt_str(j, "status") }
    }
}

/// Go `fmt.Sprintf("%v", []string)`.
pub(crate) fn go_list(v: &[String]) -> String {
    format!("[{}]", v.join(" "))
}

/// Go `errgroup.WithContext` + `SetLimit(limit)`: runs `f` for every item on at most `limit` threads and
/// returns the results in item order, or the first error. After an error the group context is cancelled and
/// no new item starts (Go starts them with the cancelled context, where they fail at once).
pub(crate) fn run_limited<T: Sync, R: Send>(ctx: &CancelToken, limit: usize, items: &[T], f: impl Fn(&CancelToken, &T) -> Result<R, String> + Sync) -> Result<Vec<R>, String> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let gctx = ctx.child();
    let next = Mutex::new(0usize);
    let results: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    let first_err: Mutex<Option<String>> = Mutex::new(None);
    let workers = limit.max(1).min(items.len());
    let work = || loop {
        if first_err.lock().unwrap_or_else(|e| e.into_inner()).is_some() {
            return;
        }
        let i = {
            let mut n = next.lock().unwrap_or_else(|e| e.into_inner());
            if *n >= items.len() {
                return;
            }
            *n += 1;
            *n - 1
        };
        match f(&gctx, &items[i]) {
            Ok(r) => results.lock().unwrap_or_else(|e| e.into_inner())[i] = Some(r),
            Err(e) => {
                let mut fe = first_err.lock().unwrap_or_else(|e| e.into_inner());
                if fe.is_none() {
                    *fe = Some(e);
                    gctx.cancel();
                }
                return;
            }
        }
    };
    if workers == 1 {
        work();
    } else {
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(work);
            }
        });
    }
    gctx.cancel();
    if let Some(e) = first_err.into_inner().unwrap_or_else(|e| e.into_inner()) {
        return Err(e);
    }
    Ok(results.into_inner().unwrap_or_else(|e| e.into_inner()).into_iter().flatten().collect())
}

/// Go `slices.Chunk`.
fn chunks(v: &[String], n: usize) -> Vec<Vec<String>> {
    v.chunks(n).map(|c| c.to_vec()).collect()
}

fn strs(v: &[String]) -> Json {
    Json::Arr(v.iter().map(|s| Json::Str(s.clone())).collect())
}

// ---------------- discovery ----------------

/// The refresh state of the ECS discoverer (Go `ECSDiscovery` without the embedded refresh loop).
pub struct EcsRefresher {
    logger: Logger,
    cfg: EcsSdConfig,
    ecs: Option<Client>,
    ec2: Option<Client>,
    /// The resolved region (Source and `__meta_ecs_region`).
    region: String,
    /// The base endpoint of the EC2 client (Go uses the environment or regional endpoint; for tests).
    pub ec2_endpoint: Option<String>,
}

impl EcsRefresher {
    fn limit(&self) -> usize {
        self.cfg.request_concurrency.max(1) as usize
    }

    fn ecs(&self) -> Result<&Client, String> {
        self.ecs.as_ref().ok_or_else(|| "ECS client is not initialized".to_string())
    }

    fn ec2(&self) -> Result<&Client, String> {
        self.ec2.as_ref().ok_or_else(|| "EC2 client is not initialized".to_string())
    }

    /// The resolved region (empty before the first refresh).
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Go `initEcsClient`: builds the ECS and EC2 clients once and tests the credentials with an empty
    /// DescribeClusters call. As in Go, the clients are kept even when the test fails, so the next refresh
    /// does not test again.
    pub fn init_ecs_client(&mut self, ctx: &CancelToken) -> Result<(), String> {
        if self.ecs.is_some() && self.ec2.is_some() {
            return Ok(());
        }
        let cc = self.cfg.client_config();
        let ecs = Client::new(&cc, client::ECS, "ecs_sd").inspect_err(|e| {
            if e.starts_with("could not create aws config") {
                self.logger.error("Failed to create AWS config", &[("error", e)]);
            }
        })?;
        self.region = ecs.region().to_string();
        let ec2_cc = ClientConfig { region: self.region.clone(), endpoint: self.ec2_endpoint.clone().unwrap_or_default(), ..cc };
        let ec2 = Client::new(&ec2_cc, client::EC2, "ecs_sd")?;
        self.ecs = Some(ecs);
        self.ec2 = Some(ec2);

        // Test credentials by making a simple API call (Go: with a 10s timeout).
        let test_ctx = ctx.child();
        let tc = test_ctx.clone();
        let timer = std::thread::spawn(move || {
            tc.wait_timeout(Duration::from_secs(10));
            tc.cancel();
        });
        let r = self.ecs()?.json_rpc(&test_ctx, "DescribeClusters", &Json::obj());
        test_ctx.cancel();
        let _ = timer.join();
        if let Err(e) = r {
            self.logger.error("Failed to test ECS credentials", &[("error", &e)]);
            return Err(format!("ECS credential test failed: {e}"));
        }
        Ok(())
    }

    /// Go `listClusterARNs`.
    pub fn list_cluster_arns(&self, ctx: &CancelToken) -> Result<Vec<String>, String> {
        let mut arns = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut body = Json::obj();
            if let Some(t) = &next_token {
                body.set("nextToken", t.as_str());
            }
            body.set("maxResults", 100i64);
            let resp = self.ecs()?.json_rpc(ctx, "ListClusters", &body).map_err(|e| format!("could not list clusters: {e}"))?;
            arns.extend(str_list(&resp, "clusterArns"));
            match opt_str(&resp, "nextToken") {
                None => break,
                t => next_token = t,
            }
        }
        Ok(arns)
    }

    /// Go `describeClusters`: cluster ARN -> cluster, in batches of 100.
    pub fn describe_clusters(&self, ctx: &CancelToken, clusters: &[String]) -> Result<BTreeMap<String, EcsCluster>, String> {
        let ecs = self.ecs()?;
        let batches = chunks(clusters, 100);
        let res = run_limited(ctx, self.limit(), &batches, |c, batch| {
            let body = Json::obj().with("clusters", strs(batch)).with("include", strs(&["TAGS".to_string()]));
            let resp = ecs.json_rpc(c, "DescribeClusters", &body).map_err(|e| {
                self.logger.error("Failed to describe clusters", &[("clusters", &go_list(batch)), ("error", &e)]);
                format!("could not describe clusters {}: {e}", go_list(batch))
            })?;
            Ok(objs(&resp, "clusters").iter().map(EcsCluster::from_json).collect::<Vec<_>>())
        })?;
        let mut m = BTreeMap::new();
        for c in res.into_iter().flatten() {
            if let Some(arn) = c.cluster_arn.clone() {
                m.insert(arn, c);
            }
        }
        Ok(m)
    }

    fn list_paged(&self, ctx: &CancelToken, op: &str, cluster: &str, key: &str) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut body = Json::obj().with("cluster", cluster);
            if let Some(t) = &next_token {
                body.set("nextToken", t.as_str());
            }
            body.set("maxResults", 100i64);
            let resp = self.ecs()?.json_rpc(ctx, op, &body)?;
            out.extend(str_list(&resp, key));
            match opt_str(&resp, "nextToken") {
                None => break,
                t => next_token = t,
            }
        }
        Ok(out)
    }

    /// Go `listServiceARNs`: cluster ARN -> service ARNs.
    pub fn list_service_arns(&self, ctx: &CancelToken, clusters: &[String]) -> Result<BTreeMap<String, Vec<String>>, String> {
        let res = run_limited(ctx, self.limit(), clusters, |c, cluster| {
            self.list_paged(c, "ListServices", cluster, "serviceArns").map_err(|e| format!("could not list services for cluster {}: {e}", yaml::go_quote(cluster)))
        })?;
        Ok(clusters.iter().cloned().zip(res).collect())
    }

    /// Go `describeServices`: service name -> service, in batches of 10.
    pub fn describe_services(&self, ctx: &CancelToken, cluster_arn: &str, service_arns: &[String]) -> Result<BTreeMap<String, EcsService>, String> {
        let ecs = self.ecs()?;
        let batches = chunks(service_arns, 10);
        let res = run_limited(ctx, self.limit(), &batches, |c, batch| {
            let body = Json::obj().with("cluster", cluster_arn).with("services", strs(batch)).with("include", strs(&["TAGS".to_string()]));
            let resp = ecs.json_rpc(c, "DescribeServices", &body).map_err(|e| {
                self.logger.error("Failed to describe services", &[("cluster", cluster_arn), ("batch", &go_list(batch)), ("error", &e)]);
                format!("could not describe services for cluster {}: batch {}: {e}", yaml::go_quote(cluster_arn), go_list(batch))
            })?;
            Ok(objs(&resp, "services").iter().map(EcsService::from_json).collect::<Vec<_>>())
        })?;
        let mut m = BTreeMap::new();
        for s in res.into_iter().flatten() {
            if s.service_arn.is_some()
                && let Some(name) = s.service_name.clone() {
                    m.insert(name, s);
                }
        }
        Ok(m)
    }

    /// Go `listTaskARNs`: cluster ARN -> task ARNs.
    pub fn list_task_arns(&self, ctx: &CancelToken, clusters: &[String]) -> Result<BTreeMap<String, Vec<String>>, String> {
        let res = run_limited(ctx, self.limit(), clusters, |c, cluster| {
            self.list_paged(c, "ListTasks", cluster, "taskArns").map_err(|e| format!("could not list tasks for cluster {}: {e}", yaml::go_quote(cluster)))
        })?;
        Ok(clusters.iter().cloned().zip(res).collect())
    }

    /// Go `describeTasks`, in batches of 100.
    pub fn describe_tasks(&self, ctx: &CancelToken, cluster_arn: &str, task_arns: &[String]) -> Result<Vec<EcsTask>, String> {
        let ecs = self.ecs()?;
        let batches = chunks(task_arns, 100);
        let res = run_limited(ctx, self.limit(), &batches, |c, batch| {
            let body = Json::obj().with("cluster", cluster_arn).with("tasks", strs(batch)).with("include", strs(&["TAGS".to_string()]));
            let resp = ecs.json_rpc(c, "DescribeTasks", &body).map_err(|e| {
                self.logger.error("Failed to describe tasks", &[("cluster", cluster_arn), ("batch", &go_list(batch)), ("error", &e)]);
                format!("could not describe tasks in cluster {}: batch {}: {e}", yaml::go_quote(cluster_arn), go_list(batch))
            })?;
            Ok(objs(&resp, "tasks").iter().map(EcsTask::from_json).collect::<Vec<_>>())
        })?;
        Ok(res.into_iter().flatten().collect())
    }

    /// Go `describeContainerInstances`: container instance ARN -> EC2 instance ID, in batches of 100.
    pub fn describe_container_instances(&self, ctx: &CancelToken, cluster_arn: &str, tasks: &[EcsTask]) -> Result<BTreeMap<String, String>, String> {
        let arns: Vec<String> = tasks.iter().filter_map(|t| t.container_instance_arn.clone()).collect();
        if arns.is_empty() {
            return Ok(BTreeMap::new());
        }
        let ecs = self.ecs()?;
        let batches = chunks(&arns, 100);
        let res = run_limited(ctx, self.limit(), &batches, |c, batch| {
            let body = Json::obj().with("cluster", cluster_arn).with("containerInstances", strs(batch));
            let resp = ecs.json_rpc(c, "DescribeContainerInstances", &body).map_err(|e| format!("could not describe container instances: {e}"))?;
            Ok(objs(&resp, "containerInstances").iter().map(ContainerInstance::from_json).collect::<Vec<_>>())
        })?;
        let mut m = BTreeMap::new();
        for ci in res.into_iter().flatten() {
            if let (Some(arn), Some(id)) = (ci.container_instance_arn, ci.ec2_instance_id) {
                m.insert(arn, id);
            }
        }
        Ok(m)
    }

    /// Go `describeEC2Instances`: EC2 instance ID -> instance info (paginated, not concurrent).
    pub fn describe_ec2_instances(&self, ctx: &CancelToken, instance_ids: &[String]) -> Result<BTreeMap<String, Ec2InstanceInfo>, String> {
        let mut m = BTreeMap::new();
        if instance_ids.is_empty() {
            return Ok(m);
        }
        let ec2 = self.ec2()?;
        let mut next_token: Option<String> = None;
        loop {
            let mut params: Vec<(String, String)> = instance_ids.iter().enumerate().map(|(i, id)| (format!("InstanceId.{}", i + 1), id.clone())).collect();
            if let Some(t) = &next_token {
                params.push(("NextToken".to_string(), t.clone()));
            }
            let root = ec2.query(ctx, "DescribeInstances", &params).map_err(|e| format!("could not describe EC2 instances: {e}"))?;
            for r in root.list("reservationSet") {
                for inst in r.list("instancesSet") {
                    let (Some(id), Some(private_ip)) = (inst.text_at("instanceId"), inst.text_at("privateIpAddress")) else { continue };
                    let mut info = Ec2InstanceInfo { private_ip: private_ip.to_string(), ..Default::default() };
                    if let Some(p) = inst.text_at("ipAddress") {
                        info.public_ip = p.to_string();
                    }
                    if let Some(s) = inst.text_at("subnetId") {
                        info.subnet_id = s.to_string();
                    }
                    info.instance_type = inst.text_at("instanceType").unwrap_or("").to_string();
                    for t in inst.list("tagSet") {
                        if let (Some(k), Some(v)) = (t.text_at("key"), t.text_at("value")) {
                            info.tags.insert(k.to_string(), v.to_string());
                        }
                    }
                    m.insert(id.to_string(), info);
                }
            }
            match root.text_at("nextToken") {
                None => break,
                Some(t) => next_token = Some(t.to_string()),
            }
        }
        Ok(m)
    }

    /// Go `describeNetworkInterfaces`: ENI ID -> public IP for the tasks' ENI attachments (paginated).
    pub fn describe_network_interfaces(&self, ctx: &CancelToken, tasks: &[EcsTask]) -> Result<BTreeMap<String, String>, String> {
        let mut eni_ids = Vec::new();
        for t in tasks {
            if let Some(a) = t.attachments.iter().find(|a| a.r#type.as_deref() == Some("ElasticNetworkInterface"))
                && let Some(v) = a.details.iter().find(|d| d.name.as_deref() == Some("networkInterfaceId") && d.value.is_some()).and_then(|d| d.value.clone()) {
                    eni_ids.push(v);
                }
        }
        let mut m = BTreeMap::new();
        if eni_ids.is_empty() {
            return Ok(m);
        }
        let ec2 = self.ec2()?;
        let mut next_token: Option<String> = None;
        loop {
            let mut params: Vec<(String, String)> = eni_ids.iter().enumerate().map(|(i, id)| (format!("NetworkInterfaceId.{}", i + 1), id.clone())).collect();
            if let Some(t) = &next_token {
                params.push(("NextToken".to_string(), t.clone()));
            }
            let root = ec2.query(ctx, "DescribeNetworkInterfaces", &params).map_err(|e| format!("could not describe network interfaces: {e}"))?;
            for eni in root.list("networkInterfaceSet") {
                if let (Some(id), Some(ip)) = (eni.text_at("networkInterfaceId"), eni.text_at("association/publicIp")) {
                    m.insert(id.to_string(), ip.to_string());
                }
            }
            match root.text_at("nextToken") {
                None => break,
                Some(t) => next_token = Some(t.to_string()),
            }
        }
        Ok(m)
    }

    /// The targets of one cluster (the Go per-cluster goroutine). Errors are logged and give no targets.
    fn cluster_targets(&self, ctx: &CancelToken, cluster: &EcsCluster, service_arns: &[String], task_arns: &[String]) -> Vec<LabelSet> {
        let Some(cluster_arn) = cluster.cluster_arn.as_deref() else {
            self.logger.error("Cluster not described, skipping it", &[]);
            return Vec::new();
        };
        // Services and tasks in parallel.
        let rctx = ctx.child();
        let (services, tasks) = std::thread::scope(|s| {
            let h = s.spawn(|| {
                let r = self.describe_services(&rctx, cluster_arn, service_arns);
                if let Err(e) = &r {
                    self.logger.error("Failed to describe services for cluster", &[("cluster", cluster_arn), ("error", e)]);
                    rctx.cancel();
                }
                r
            });
            let t = self.describe_tasks(&rctx, cluster_arn, task_arns);
            if let Err(e) = &t {
                self.logger.error("Failed to describe tasks for cluster", &[("cluster", cluster_arn), ("error", e)]);
                rctx.cancel();
            }
            (h.join().unwrap_or_else(|_| Err("panic".to_string())), t)
        });
        rctx.cancel();
        let (Ok(services), Ok(tasks)) = (services, tasks) else { return Vec::new() };

        // Container instances and network interfaces in parallel.
        let ictx = ctx.child();
        let (container_instances, eni_to_public_ip) = std::thread::scope(|s| {
            let h = s.spawn(|| {
                let r = self.describe_container_instances(&ictx, cluster_arn, &tasks);
                if let Err(e) = &r {
                    self.logger.error("Failed to describe container instances for cluster", &[("cluster", cluster_arn), ("error", e)]);
                    ictx.cancel();
                }
                r
            });
            let n = self.describe_network_interfaces(&ictx, &tasks);
            if let Err(e) = &n {
                self.logger.error("Failed to describe network interfaces for cluster", &[("cluster", cluster_arn), ("error", e)]);
                ictx.cancel();
            }
            (h.join().unwrap_or_else(|_| Err("panic".to_string())), n)
        });
        ictx.cancel();
        let (Ok(container_instances), Ok(eni_to_public_ip)) = (container_instances, eni_to_public_ip) else { return Vec::new() };

        let mut ec2_instances = BTreeMap::new();
        if !container_instances.is_empty() {
            let ids: Vec<String> = container_instances.values().cloned().collect::<BTreeSet<_>>().into_iter().collect();
            match self.describe_ec2_instances(ctx, &ids) {
                Ok(m) => ec2_instances = m,
                Err(e) => {
                    self.logger.error("Failed to describe EC2 instances for cluster", &[("cluster", cluster_arn), ("error", &e)]);
                    return Vec::new();
                }
            }
        }

        tasks.iter().filter_map(|t| self.task_target(cluster, &services, t, &container_instances, &ec2_instances, &eni_to_public_ip)).collect()
    }

    /// The target of one task (the Go per-task goroutine), or None without an IP address.
    fn task_target(&self, cluster: &EcsCluster, services: &BTreeMap<String, EcsService>, task: &EcsTask, container_instances: &BTreeMap<String, String>, ec2_instances: &BTreeMap<String, Ec2InstanceInfo>, eni_to_public_ip: &BTreeMap<String, String>) -> Option<LabelSet> {
        let (mut ip_address, mut subnet_id, mut public_ip, mut network_mode) = (String::new(), String::new(), String::new(), "");
        let (mut ec2_instance_id, mut ec2_instance_type, mut ec2_private_ip, mut ec2_public_ip) = (String::new(), String::new(), String::new(), String::new());
        let task_arn = task.task_arn.as_deref().unwrap_or("");

        if let Some(eni) = task.attachments.iter().find(|a| a.r#type.as_deref() == Some("ElasticNetworkInterface")) {
            network_mode = "awsvpc";
            let mut eni_id = String::new();
            for d in &eni.details {
                let (Some(name), Some(value)) = (d.name.as_deref(), d.value.as_deref()) else { continue };
                match name {
                    "privateIPv4Address" => ip_address = value.to_string(),
                    "subnetId" => subnet_id = value.to_string(),
                    "networkInterfaceId" => eni_id = value.to_string(),
                    _ => {}
                }
            }
            if !eni_id.is_empty()
                && let Some(p) = eni_to_public_ip.get(&eni_id) {
                    public_ip = p.clone();
                }
        } else if let Some(ci) = &task.container_instance_arn {
            network_mode = "bridge";
            match container_instances.get(ci) {
                Some(id) => {
                    ec2_instance_id = id.clone();
                    match ec2_instances.get(id) {
                        Some(info) => {
                            ip_address = info.private_ip.clone();
                            public_ip = info.public_ip.clone();
                            subnet_id = info.subnet_id.clone();
                            ec2_instance_type = info.instance_type.clone();
                            ec2_private_ip = info.private_ip.clone();
                            ec2_public_ip = info.public_ip.clone();
                        }
                        None => self.logger.debug("EC2 instance info not found", &[("instance", id), ("task", task_arn)]),
                    }
                }
                None => self.logger.debug("Container instance not found in map", &[("arn", ci), ("task", task_arn)]),
            }
        }

        // EC2 instance metadata for awsvpc tasks running on EC2.
        if network_mode == "awsvpc"
            && let Some(ci) = &task.container_instance_arn
                && let Some(id) = container_instances.get(ci) {
                    ec2_instance_id = id.clone();
                    if let Some(info) = ec2_instances.get(id) {
                        ec2_instance_type = info.instance_type.clone();
                        ec2_private_ip = info.private_ip.clone();
                        ec2_public_ip = info.public_ip.clone();
                    }
                }

        if ip_address.is_empty() {
            return None;
        }

        let mut labels = LabelSet::new();
        labels.insert(l("ip_address"), ip_address.clone());
        labels.insert(l("region"), self.region.clone());
        labels.insert(l("launch_type"), task.launch_type.clone());
        labels.insert(l("health_status"), task.health_status.clone());
        labels.insert(l("network_mode"), network_mode.to_string());
        let mut opt = |name: &str, v: &Option<String>| {
            if let Some(v) = v {
                labels.insert(l(name), v.clone());
            }
        };
        opt("cluster_arn", &cluster.cluster_arn);
        opt("cluster", &cluster.cluster_name);
        opt("task_group", &task.group);
        opt("task_arn", &task.task_arn);
        opt("task_definition", &task.task_definition_arn);
        opt("availability_zone", &task.availability_zone);
        opt("desired_status", &task.desired_status);
        opt("last_status", &task.last_status);
        opt("container_instance_arn", &task.container_instance_arn);
        opt("platform_family", &task.platform_family);
        opt("platform_version", &task.platform_version);
        for (name, v) in [("subnet_id", &subnet_id), ("ec2_instance_id", &ec2_instance_id), ("ec2_instance_type", &ec2_instance_type), ("ec2_instance_private_ip", &ec2_private_ip), ("ec2_instance_public_ip", &ec2_public_ip), ("public_ip", &public_ip)] {
            if !v.is_empty() {
                labels.insert(l(name), v.clone());
            }
        }
        labels.insert(model::labelset::ADDRESS_LABEL.to_string(), join_host_port(&ip_address, &self.cfg.port.to_string()));

        for t in &cluster.tags {
            if let (Some(k), Some(v)) = (&t.key, &t.value) {
                labels.insert(format!("{ECS_LABEL}tag_cluster_{}", sanitize_label_name(k)), v.clone());
            }
        }
        if let Some(service_name) = task.group.as_deref().unwrap_or("").strip_prefix("service:") {
            let service = services.get(service_name);
            if service.is_none() {
                self.logger.debug("Service not found for task", &[("task", task_arn), ("service", service_name)]);
            }
            if let Some(svc) = service {
                if let Some(v) = &svc.service_name {
                    labels.insert(l("service"), v.clone());
                }
                if let Some(v) = &svc.service_arn {
                    labels.insert(l("service_arn"), v.clone());
                }
                if let Some(v) = &svc.status {
                    labels.insert(l("service_status"), v.clone());
                }
                for t in &svc.tags {
                    if let (Some(k), Some(v)) = (&t.key, &t.value) {
                        labels.insert(format!("{ECS_LABEL}tag_service_{}", sanitize_label_name(k)), v.clone());
                    }
                }
            }
        }
        for t in &task.tags {
            if let (Some(k), Some(v)) = (&t.key, &t.value) {
                labels.insert(format!("{ECS_LABEL}tag_task_{}", sanitize_label_name(k)), v.clone());
            }
        }
        if !ec2_instance_id.is_empty()
            && let Some(info) = ec2_instances.get(&ec2_instance_id) {
                for (k, v) in &info.tags {
                    labels.insert(format!("{ECS_LABEL}tag_ec2_{}", sanitize_label_name(k)), v.clone());
                }
            }
        Some(labels)
    }
}

impl Refresher for EcsRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.init_ecs_client(ctx)?;
        let clusters = if self.cfg.clusters.is_empty() { self.list_cluster_arns(ctx)? } else { self.cfg.clusters.clone() };
        if clusters.is_empty() {
            return Ok(vec![Arc::new(Group { source: self.region.clone(), ..Default::default() })]);
        }

        // Cluster details, service ARNs and task ARNs in parallel (one errgroup).
        let this = &*self;
        let gctx = ctx.child();
        let fail = |e: &String, first: &Mutex<Option<String>>| {
            let mut f = first.lock().unwrap_or_else(|e| e.into_inner());
            if f.is_none() {
                *f = Some(e.clone());
                gctx.cancel();
            }
        };
        let first: Mutex<Option<String>> = Mutex::new(None);
        let (cluster_map, service_map, task_map) = std::thread::scope(|s| {
            let hc = s.spawn(|| this.describe_clusters(&gctx, &clusters).inspect_err(|e| fail(e, &first)));
            let hs = s.spawn(|| this.list_service_arns(&gctx, &clusters).inspect_err(|e| fail(e, &first)));
            let t = this.list_task_arns(&gctx, &clusters).inspect_err(|e| fail(e, &first));
            (hc.join().unwrap_or_else(|_| Err("panic".to_string())), hs.join().unwrap_or_else(|_| Err("panic".to_string())), t)
        });
        gctx.cancel();
        if let Some(e) = first.into_inner().unwrap_or_else(|e| e.into_inner()) {
            return Err(e);
        }
        let (cluster_map, service_map, task_map) = (cluster_map?, service_map?, task_map?);

        // Clusters with tasks, in the order of the cluster list.
        let mut work: Vec<(EcsCluster, Vec<String>, Vec<String>)> = Vec::new();
        let mut seen = BTreeSet::new();
        for arn in &clusters {
            if !seen.insert(arn.clone()) {
                continue;
            }
            let Some(task_arns) = task_map.get(arn).filter(|t| !t.is_empty()) else { continue };
            work.push((cluster_map.get(arn).cloned().unwrap_or_default(), service_map.get(arn).cloned().unwrap_or_default(), task_arns.clone()));
        }
        let per_cluster: Vec<Vec<LabelSet>> = if work.len() <= 1 {
            work.iter().map(|(c, s, t)| this.cluster_targets(ctx, c, s, t)).collect()
        } else {
            run_limited(ctx, this.limit(), &work, |_, (c, s, t)| Ok(this.cluster_targets(ctx, c, s, t)))?
        };
        let targets: Vec<LabelSet> = per_cluster.into_iter().flatten().collect();
        Ok(vec![Arc::new(Group { targets, labels: LabelSet::new(), source: self.region.clone() })])
    }
}

/// Go `ECSDiscovery`.
pub struct EcsDiscovery {
    inner: refresh::Discovery<EcsRefresher>,
}

impl EcsDiscovery {
    /// Go `NewECSDiscovery`.
    pub fn new(conf: &EcsSdConfig, opts: DiscovererOptions) -> Result<EcsDiscovery, String> {
        let m = crate::downcast_metrics::<EcsMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = EcsRefresher { logger: opts.logger.clone(), cfg: conf.clone(), ecs: None, ec2: None, region: String::new(), ec2_endpoint: None };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "ecs",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(EcsDiscovery { inner })
    }

    /// The refresh state (the Go discovery's fields and helper methods).
    pub fn refresher(&mut self) -> &mut EcsRefresher {
        self.inner.refresher()
    }

    /// Go `ECSDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for EcsDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
