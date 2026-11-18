//! Port of `discovery/aws/msk.go` and `metrics_msk.go`: Amazon MSK (Kafka) broker and controller nodes as
//! targets (`msk_sd_configs`). The Kafka API is restJson1 (`client::KAFKA`).
//!
//! Differs from Go: targets come out in cluster and node order (Go: goroutine completion order).
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::client::{self, Client, ClientConfig};
use super::ecs::run_limited;
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::util::{join_host_port, sanitize_label_name};
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// Go `NodeTypeBroker`.
pub const NODE_TYPE_BROKER: &str = "BROKER";
/// Go `NodeTypeController`.
pub const NODE_TYPE_CONTROLLER: &str = "CONTROLLER";

const MSK_LABEL_CLUSTER_NAME: &str = "__meta_msk_cluster_name";
const MSK_LABEL_CLUSTER_ARN: &str = "__meta_msk_cluster_arn";
const MSK_LABEL_CLUSTER_STATE: &str = "__meta_msk_cluster_state";
const MSK_LABEL_CLUSTER_TYPE: &str = "__meta_msk_cluster_type";
const MSK_LABEL_CLUSTER_VERSION: &str = "__meta_msk_cluster_version";
const MSK_LABEL_CLUSTER_JMX_EXPORTER_ENABLED: &str = "__meta_msk_cluster_jmx_exporter_enabled";
const MSK_LABEL_CLUSTER_CONFIGURATION_ARN: &str = "__meta_msk_cluster_configuration_arn";
const MSK_LABEL_CLUSTER_CONFIGURATION_REVISION: &str = "__meta_msk_cluster_configuration_revision";
const MSK_LABEL_CLUSTER_KAFKA_VERSION: &str = "__meta_msk_cluster_kafka_version";
const MSK_LABEL_CLUSTER_TAGS: &str = "__meta_msk_cluster_tag_";
const MSK_LABEL_NODE_TYPE: &str = "__meta_msk_node_type";
const MSK_LABEL_NODE_ARN: &str = "__meta_msk_node_arn";
const MSK_LABEL_NODE_ADDED_TIME: &str = "__meta_msk_node_added_time";
const MSK_LABEL_NODE_INSTANCE_TYPE: &str = "__meta_msk_node_instance_type";
const MSK_LABEL_NODE_ATTACHED_ENI: &str = "__meta_msk_node_attached_eni";
const MSK_LABEL_BROKER_ENDPOINT_INDEX: &str = "__meta_msk_broker_endpoint_index";
const MSK_LABEL_BROKER_ID: &str = "__meta_msk_broker_id";
const MSK_LABEL_BROKER_CLIENT_SUBNET: &str = "__meta_msk_broker_client_subnet";
const MSK_LABEL_BROKER_CLIENT_VPC_IP: &str = "__meta_msk_broker_client_vpc_ip";
const MSK_LABEL_BROKER_NODE_EXPORTER_ENABLED: &str = "__meta_msk_broker_node_exporter_enabled";
const MSK_LABEL_CONTROLLER_ENDPOINT_INDEX: &str = "__meta_msk_controller_endpoint_index";

/// Go `DefaultMSKSDConfig`.
pub fn default_msk_sd_config() -> MskSdConfig {
    MskSdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), request_concurrency: 10, http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

/// Go `MSKSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MskSdConfig {
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
    pub request_concurrency: i64,
    pub http_client_config: HttpClientConfig,
}

yaml::yaml_struct!(MskSdConfig, "aws.plain", [
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

impl Unmarshal for MskSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_msk_sd_config();
            d.inner_struct(n, self)?;
            if self.request_concurrency <= 0 {
                return Err(YError::Msg(format!("msk_sd: request_concurrency must be positive, got {}", self.request_concurrency)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.MSKSDConfig".to_string()
    }
}

impl yaml::Marshal for MskSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for MskSdConfig {
    const GO_TYPE: &'static str = "aws.MSKSDConfig";
}

impl Config for MskSdConfig {
    fn name(&self) -> &'static str {
        "msk"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn crate::Discoverer>, String> {
        Ok(Box::new(MskDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        MskMetrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl MskSdConfig {
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

/// Go `mskMetrics`.
pub struct MskMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl MskMetrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<MskMetrics> {
        Arc::new(MskMetrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for MskMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// ---------------- API types (the fields discovery reads; `Option` = Go pointer) ----------------

/// Kafka `Cluster`. `state` and `cluster_type` are Go string enums ("" when absent).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MskCluster {
    pub cluster_arn: Option<String>,
    pub cluster_name: Option<String>,
    pub state: String,
    pub cluster_type: String,
    pub current_version: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub provisioned: Option<Provisioned>,
}

/// Kafka `Provisioned` (the fields discovery reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Provisioned {
    pub current_broker_software_info: Option<BrokerSoftwareInfo>,
    pub open_monitoring: Option<OpenMonitoringInfo>,
}

/// Kafka `BrokerSoftwareInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerSoftwareInfo {
    pub configuration_arn: Option<String>,
    pub configuration_revision: Option<i64>,
    pub kafka_version: Option<String>,
}

/// Kafka `OpenMonitoringInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OpenMonitoringInfo {
    pub prometheus: Option<PrometheusInfo>,
}

/// Kafka `PrometheusInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PrometheusInfo {
    pub jmx_exporter: Option<JmxExporterInfo>,
    pub node_exporter: Option<NodeExporterInfo>,
}

/// Kafka `JmxExporterInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JmxExporterInfo {
    pub enabled_in_broker: Option<bool>,
}

/// Kafka `NodeExporterInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeExporterInfo {
    pub enabled_in_broker: Option<bool>,
}

/// Kafka `NodeInfo` (the fields discovery reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeInfo {
    pub node_arn: Option<String>,
    pub added_to_cluster_time: Option<String>,
    pub instance_type: Option<String>,
    pub broker_node_info: Option<BrokerNodeInfo>,
    pub controller_node_info: Option<ControllerNodeInfo>,
}

/// Kafka `BrokerNodeInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerNodeInfo {
    pub attached_eni_id: Option<String>,
    pub broker_id: Option<f64>,
    pub client_subnet: Option<String>,
    pub client_vpc_ip_address: Option<String>,
    pub endpoints: Vec<String>,
}

/// Kafka `ControllerNodeInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControllerNodeInfo {
    pub endpoints: Vec<String>,
}

fn opt_str(j: &Json, k: &str) -> Option<String> {
    j.get(k).and_then(|v| v.as_str()).map(str::to_string)
}

fn obj<'a>(j: &'a Json, k: &str) -> Option<&'a Json> {
    j.get(k).filter(|v| v.as_obj().is_some())
}

fn str_list(j: &Json, k: &str) -> Vec<String> {
    j.get(k).and_then(|v| v.as_arr()).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

/// A smithy JSON double: a number, or "NaN", "Infinity", "-Infinity".
fn double(j: &Json, k: &str) -> Option<f64> {
    match j.get(k)? {
        Json::Str(s) => match s.as_str() {
            "NaN" => Some(f64::NAN),
            "Infinity" => Some(f64::INFINITY),
            "-Infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        v => v.as_f64(),
    }
}

impl MskCluster {
    pub fn from_json(j: &Json) -> MskCluster {
        let tags = j.get("tags").and_then(|t| t.as_obj()).map(|o| o.iter().filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string()))).collect()).unwrap_or_default();
        MskCluster {
            cluster_arn: opt_str(j, "clusterArn"),
            cluster_name: opt_str(j, "clusterName"),
            state: opt_str(j, "state").unwrap_or_default(),
            cluster_type: opt_str(j, "clusterType").unwrap_or_default(),
            current_version: opt_str(j, "currentVersion"),
            tags,
            provisioned: obj(j, "provisioned").map(|p| Provisioned {
                current_broker_software_info: obj(p, "currentBrokerSoftwareInfo").map(|b| BrokerSoftwareInfo {
                    configuration_arn: opt_str(b, "configurationArn"),
                    configuration_revision: b.get("configurationRevision").and_then(|v| v.as_i64()),
                    kafka_version: opt_str(b, "kafkaVersion"),
                }),
                open_monitoring: obj(p, "openMonitoring").map(|om| OpenMonitoringInfo {
                    prometheus: obj(om, "prometheus").map(|pr| PrometheusInfo {
                        jmx_exporter: obj(pr, "jmxExporter").map(|e| JmxExporterInfo { enabled_in_broker: e.get("enabledInBroker").and_then(|v| v.as_bool()) }),
                        node_exporter: obj(pr, "nodeExporter").map(|e| NodeExporterInfo { enabled_in_broker: e.get("enabledInBroker").and_then(|v| v.as_bool()) }),
                    }),
                }),
            }),
        }
    }
}

impl NodeInfo {
    pub fn from_json(j: &Json) -> NodeInfo {
        NodeInfo {
            node_arn: opt_str(j, "nodeARN"),
            added_to_cluster_time: opt_str(j, "addedToClusterTime"),
            instance_type: opt_str(j, "instanceType"),
            broker_node_info: obj(j, "brokerNodeInfo").map(|b| BrokerNodeInfo {
                attached_eni_id: opt_str(b, "attachedENIId"),
                broker_id: double(b, "brokerId"),
                client_subnet: opt_str(b, "clientSubnet"),
                client_vpc_ip_address: opt_str(b, "clientVpcIpAddress"),
                endpoints: str_list(b, "endpoints"),
            }),
            controller_node_info: obj(j, "controllerNodeInfo").map(|c| ControllerNodeInfo { endpoints: str_list(c, "endpoints") }),
        }
    }
}

/// Go `nodeType`: "BROKER", "CONTROLLER" or "".
pub fn node_type(node: &NodeInfo) -> &'static str {
    if node.broker_node_info.is_some() {
        NODE_TYPE_BROKER
    } else if node.controller_node_info.is_some() {
        NODE_TYPE_CONTROLLER
    } else {
        ""
    }
}

/// smithy `httpbinding.EscapePath(v, true)`: an HTTP label value, every byte but unreserved ones escaped.
fn escape_label(v: &str) -> String {
    let mut o = String::with_capacity(v.len());
    for b in v.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}

/// Go `fmt.Sprintf("%.0f", f)`.
fn format_broker_id(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else {
        format!("{f:.0}")
    }
}

// ---------------- discovery ----------------

/// The refresh state of the MSK discoverer (Go `MSKDiscovery` without the embedded refresh loop).
pub struct MskRefresher {
    logger: Logger,
    cfg: MskSdConfig,
    msk: Option<Client>,
    /// The resolved region (the Source of the target group).
    region: String,
}

impl MskRefresher {
    fn limit(&self) -> usize {
        self.cfg.request_concurrency.max(1) as usize
    }

    fn msk(&self) -> Result<&Client, String> {
        self.msk.as_ref().ok_or_else(|| "MSK client is not initialized".to_string())
    }

    /// The resolved region (empty before the first refresh).
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Go `initMskClient`: builds the Kafka client once and tests the credentials with an empty
    /// ListClustersV2 call. As in Go, the client is kept even when the test fails.
    pub fn init_msk_client(&mut self, ctx: &CancelToken) -> Result<(), String> {
        if self.msk.is_some() {
            return Ok(());
        }
        let c = Client::new(&self.cfg.client_config(), client::KAFKA, "msk_sd").inspect_err(|e| {
            if e.starts_with("could not create aws config") {
                self.logger.error("Failed to create AWS config", &[("error", e)]);
            }
        })?;
        self.region = c.region().to_string();
        self.msk = Some(c);

        // Test credentials by making a simple API call (Go: with a 10s timeout).
        let test_ctx = ctx.child();
        let tc = test_ctx.clone();
        let timer = std::thread::spawn(move || {
            tc.wait_timeout(Duration::from_secs(10));
            tc.cancel();
        });
        let r = self.msk()?.rest_json(&test_ctx, "ListClustersV2", "GET", "/api/v2/clusters", &[], None);
        test_ctx.cancel();
        let _ = timer.join();
        if let Err(e) = r {
            self.logger.error("Failed to test MSK credentials", &[("error", &e)]);
            return Err(format!("MSK credential test failed: {e}"));
        }
        Ok(())
    }

    /// Go `describeClusters`: the provisioned clusters among `cluster_arns` (DescribeClusterV2 each).
    pub fn describe_clusters(&self, ctx: &CancelToken, cluster_arns: &[String]) -> Result<Vec<MskCluster>, String> {
        let msk = self.msk()?;
        let res = run_limited(ctx, self.limit(), cluster_arns, |c, arn| {
            let path = format!("/api/v2/clusters/{}", escape_label(arn));
            let resp = msk.rest_json(c, "DescribeClusterV2", "GET", &path, &[], None).map_err(|e| format!("could not describe cluster {arn}: {e}"))?;
            let Some(info) = obj(&resp, "clusterInfo") else {
                self.logger.warn("Skipping MSK cluster described without cluster information", &[("cluster", arn)]);
                return Ok(None);
            };
            let cluster = MskCluster::from_json(info);
            if cluster.cluster_type != "PROVISIONED" {
                self.logger.warn("Skipping non-provisioned MSK cluster, only provisioned clusters are supported", &[("cluster", arn), ("type", &cluster.cluster_type)]);
                return Ok(None);
            }
            Ok(Some(cluster))
        })?;
        Ok(res.into_iter().flatten().collect())
    }

    /// Go `listClusters`: every provisioned cluster in the region (ListClustersV2, paginated).
    pub fn list_clusters(&self, ctx: &CancelToken) -> Result<Vec<MskCluster>, String> {
        let mut clusters = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut q = vec![("clusterTypeFilter".to_string(), "PROVISIONED".to_string()), ("maxResults".to_string(), "100".to_string())];
            if let Some(t) = &next_token {
                q.push(("nextToken".to_string(), t.clone()));
            }
            let resp = self.msk()?.rest_json(ctx, "ListClustersV2", "GET", "/api/v2/clusters", &q, None).map_err(|e| format!("could not list clusters: {e}"))?;
            if let Some(a) = resp.get("clusterInfoList").and_then(|v| v.as_arr()) {
                clusters.extend(a.iter().map(MskCluster::from_json));
            }
            match opt_str(&resp, "nextToken") {
                None => break,
                t => next_token = t,
            }
        }
        Ok(clusters)
    }

    /// Go `listNodes`: cluster ARN -> nodes (ListNodes, paginated).
    pub fn list_nodes(&self, ctx: &CancelToken, clusters: &[MskCluster]) -> Result<BTreeMap<String, Vec<NodeInfo>>, String> {
        let msk = self.msk()?;
        let arns: Vec<String> = clusters.iter().map(|c| c.cluster_arn.clone().unwrap_or_default()).collect();
        let res = run_limited(ctx, self.limit(), &arns, |c, arn| {
            let path = format!("/v1/clusters/{}/nodes", escape_label(arn));
            let mut nodes = Vec::new();
            let mut next_token: Option<String> = None;
            loop {
                let mut q = vec![("maxResults".to_string(), "100".to_string())];
                if let Some(t) = &next_token {
                    q.push(("nextToken".to_string(), t.clone()));
                }
                let resp = msk.rest_json(c, "ListNodes", "GET", &path, &q, None).map_err(|e| format!("could not list nodes for cluster {arn}: {e}"))?;
                if let Some(a) = resp.get("nodeInfoList").and_then(|v| v.as_arr()) {
                    nodes.extend(a.iter().map(NodeInfo::from_json));
                }
                match opt_str(&resp, "nextToken") {
                    None => break,
                    t => next_token = t,
                }
            }
            Ok(nodes)
        })?;
        Ok(arns.into_iter().zip(res).collect())
    }

    fn cluster_targets(&self, cluster: &MskCluster, nodes: &[NodeInfo]) -> Vec<LabelSet> {
        let mut out = Vec::new();
        let (broker_software, open_monitoring) = match &cluster.provisioned {
            Some(p) => (p.current_broker_software_info.as_ref(), p.open_monitoring.as_ref()),
            None => (None, None),
        };
        let prometheus = open_monitoring.and_then(|om| om.prometheus.as_ref());
        let port = self.cfg.port.to_string();
        let s = |v: &Option<String>| v.clone().unwrap_or_default();
        for node in nodes {
            let mut labels = LabelSet::new();
            labels.insert(MSK_LABEL_CLUSTER_NAME.to_string(), s(&cluster.cluster_name));
            labels.insert(MSK_LABEL_CLUSTER_ARN.to_string(), s(&cluster.cluster_arn));
            labels.insert(MSK_LABEL_CLUSTER_STATE.to_string(), cluster.state.clone());
            labels.insert(MSK_LABEL_CLUSTER_TYPE.to_string(), cluster.cluster_type.clone());
            labels.insert(MSK_LABEL_CLUSTER_VERSION.to_string(), s(&cluster.current_version));
            labels.insert(MSK_LABEL_NODE_ARN.to_string(), s(&node.node_arn));
            labels.insert(MSK_LABEL_NODE_ADDED_TIME.to_string(), s(&node.added_to_cluster_time));
            labels.insert(MSK_LABEL_NODE_INSTANCE_TYPE.to_string(), s(&node.instance_type));
            if let Some(bs) = broker_software {
                labels.insert(MSK_LABEL_CLUSTER_KAFKA_VERSION.to_string(), s(&bs.kafka_version));
                if let Some(a) = &bs.configuration_arn {
                    labels.insert(MSK_LABEL_CLUSTER_CONFIGURATION_ARN.to_string(), a.clone());
                }
                if let Some(r) = bs.configuration_revision {
                    labels.insert(MSK_LABEL_CLUSTER_CONFIGURATION_REVISION.to_string(), r.to_string());
                }
            }
            if let Some(jmx) = prometheus.and_then(|p| p.jmx_exporter.as_ref()) {
                labels.insert(MSK_LABEL_CLUSTER_JMX_EXPORTER_ENABLED.to_string(), jmx.enabled_in_broker.unwrap_or(false).to_string());
            }
            for (k, v) in &cluster.tags {
                labels.insert(format!("{MSK_LABEL_CLUSTER_TAGS}{}", sanitize_label_name(k)), v.clone());
            }
            let (endpoints, index_label) = match (node_type(node), &node.broker_node_info, &node.controller_node_info) {
                (NODE_TYPE_BROKER, Some(b), _) => {
                    labels.insert(MSK_LABEL_NODE_TYPE.to_string(), NODE_TYPE_BROKER.to_string());
                    labels.insert(MSK_LABEL_NODE_ATTACHED_ENI.to_string(), s(&b.attached_eni_id));
                    labels.insert(MSK_LABEL_BROKER_ID.to_string(), format_broker_id(b.broker_id.unwrap_or(0.0)));
                    labels.insert(MSK_LABEL_BROKER_CLIENT_SUBNET.to_string(), s(&b.client_subnet));
                    labels.insert(MSK_LABEL_BROKER_CLIENT_VPC_IP.to_string(), s(&b.client_vpc_ip_address));
                    if let Some(ne) = prometheus.and_then(|p| p.node_exporter.as_ref()) {
                        labels.insert(MSK_LABEL_BROKER_NODE_EXPORTER_ENABLED.to_string(), ne.enabled_in_broker.unwrap_or(false).to_string());
                    }
                    (&b.endpoints, MSK_LABEL_BROKER_ENDPOINT_INDEX)
                }
                (NODE_TYPE_CONTROLLER, _, Some(c)) => {
                    labels.insert(MSK_LABEL_NODE_TYPE.to_string(), NODE_TYPE_CONTROLLER.to_string());
                    (&c.endpoints, MSK_LABEL_CONTROLLER_ENDPOINT_INDEX)
                }
                _ => continue,
            };
            for (idx, endpoint) in endpoints.iter().enumerate() {
                let mut el = labels.clone();
                el.insert(index_label.to_string(), idx.to_string());
                el.insert(ADDRESS_LABEL.to_string(), join_host_port(endpoint, &port));
                out.push(el);
            }
        }
        out
    }
}

impl Refresher for MskRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.init_msk_client(ctx)?;
        let clusters = if !self.cfg.clusters.is_empty() { self.describe_clusters(ctx, &self.cfg.clusters)? } else { self.list_clusters(ctx)? };
        let node_map = self.list_nodes(ctx, &clusters)?;
        let mut targets = Vec::new();
        for c in &clusters {
            let nodes = node_map.get(c.cluster_arn.as_deref().unwrap_or("")).map(|n| n.as_slice()).unwrap_or(&[]);
            targets.extend(self.cluster_targets(c, nodes));
        }
        Ok(vec![Arc::new(Group { targets, labels: LabelSet::new(), source: self.region.clone() })])
    }
}

/// Go `MSKDiscovery`.
pub struct MskDiscovery {
    inner: refresh::Discovery<MskRefresher>,
}

impl MskDiscovery {
    /// Go `NewMSKDiscovery`.
    pub fn new(conf: &MskSdConfig, opts: DiscovererOptions) -> Result<MskDiscovery, String> {
        let m = crate::downcast_metrics::<MskMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = MskRefresher { logger: opts.logger.clone(), cfg: conf.clone(), msk: None, region: String::new() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "msk",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(MskDiscovery { inner })
    }

    /// The refresh state (the Go discovery's fields and helper methods).
    pub fn refresher(&mut self) -> &mut MskRefresher {
        self.inner.refresher()
    }

    /// Go `MSKDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for MskDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
