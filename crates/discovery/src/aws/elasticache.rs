//! Port of `discovery/aws/elasticache.go`: ElastiCache serverless caches and cache cluster nodes
//! (`elasticache_sd_configs`, and the `elasticache` role of `aws_sd_configs`). The ElastiCache API is the awsQuery
//! protocol (form POST, XML response).
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use super::client::{self, Client, ClientConfig};
use super::labels::{Kind, apply, run_limited, value};
use super::xml::Element;
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::util::{join_host_port, sanitize_label_name};
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const ELASTICACHE_LABEL_DEPLOYMENT_OPTION: &str = "__meta_elasticache_deployment_option";
const CACHE_CLUSTER: &str = "__meta_elasticache_cache_cluster_";
const SERVERLESS_CACHE: &str = "__meta_elasticache_serverless_cache_";

use Kind::{Bool as B, Enum as E, Int as I, Str as S, Time as T};

/// Serverless cache fields (`__meta_elasticache_serverless_cache_<label>`), in the order of elasticache.go.
const SERVERLESS_LABELS: &[(&str, &str, Kind)] = &[
    ("arn", "ARN", S),
    ("name", "ServerlessCacheName", S),
    ("status", "Status", S),
    ("engine", "Engine", S),
    ("full_engine_version", "FullEngineVersion", S),
    ("major_engine_version", "MajorEngineVersion", S),
    ("description", "Description", S),
    ("create_time", "CreateTime", T),
    ("kms_key_id", "KmsKeyId", S),
    ("user_group_id", "UserGroupId", S),
    ("daily_snapshot_time", "DailySnapshotTime", S),
    ("snapshot_retention_limit", "SnapshotRetentionLimit", I),
    ("endpoint_address", "Endpoint/Address", S),
    ("endpoint_port", "Endpoint/Port", I),
    ("endpoint_reader_address", "ReaderEndpoint/Address", S),
    ("endpoint_reader_port", "ReaderEndpoint/Port", I),
];

/// Cache cluster fields (`__meta_elasticache_cache_cluster_<label>`), in the order of elasticache.go.
const CLUSTER_LABELS: &[(&str, &str, Kind)] = &[
    ("arn", "ARN", S),
    ("cache_cluster_id", "CacheClusterId", S),
    ("cache_cluster_status", "CacheClusterStatus", S),
    ("at_rest_encryption_enabled", "AtRestEncryptionEnabled", B),
    ("auth_token_enabled", "AuthTokenEnabled", B),
    ("auth_token_last_modified", "AuthTokenLastModifiedDate", T),
    ("auto_minor_version_upgrade", "AutoMinorVersionUpgrade", B),
    ("cache_cluster_create_time", "CacheClusterCreateTime", T),
    ("cache_node_type", "CacheNodeType", S),
    ("cache_parameter_group", "CacheParameterGroup/CacheParameterGroupName", S),
    ("cache_subnet_group_name", "CacheSubnetGroupName", S),
    ("client_download_landing_page", "ClientDownloadLandingPage", S),
    ("configuration_endpoint_address", "ConfigurationEndpoint/Address", S),
    ("configuration_endpoint_port", "ConfigurationEndpoint/Port", I),
    ("engine", "Engine", S),
    ("engine_version", "EngineVersion", S),
    ("ip_discovery", "IpDiscovery", E),
    ("network_type", "NetworkType", E),
    ("notification_topic_arn", "NotificationConfiguration/TopicArn", S),
    ("notification_topic_status", "NotificationConfiguration/TopicStatus", S),
    ("num_cache_nodes", "NumCacheNodes", I),
    ("preferred_availability_zone", "PreferredAvailabilityZone", S),
    ("preferred_maintenance_window", "PreferredMaintenanceWindow", S),
    ("preferred_outpost_arn", "PreferredOutpostArn", S),
    ("replication_group_id", "ReplicationGroupId", S),
    ("replication_group_log_delivery_enabled", "ReplicationGroupLogDeliveryEnabled", B),
    ("snapshot_retention_limit", "SnapshotRetentionLimit", I),
    ("snapshot_window", "SnapshotWindow", S),
    ("transit_encryption_enabled", "TransitEncryptionEnabled", B),
    ("transit_encryption_mode", "TransitEncryptionMode", E),
];

/// Log delivery configuration fields (`..._log_delivery_configuration_<label>_<i>`).
const LOG_DELIVERY_LABELS: &[(&str, &str, Kind)] = &[
    ("destination_type", "DestinationType", E),
    ("log_format", "LogFormat", E),
    ("log_type", "LogType", E),
    ("status", "Status", E),
    ("message", "Message", S),
    ("log_group", "DestinationDetails/CloudWatchLogsDetails/LogGroup", S),
    ("delivery_stream", "DestinationDetails/KinesisFirehoseDetails/DeliveryStream", S),
];

/// Pending modified values (`..._pending_modified_values_<label>`); cache_node_ids_to_remove is joined.
const PENDING_LABELS: &[(&str, &str, Kind)] = &[
    ("auth_token_status", "AuthTokenStatus", E),
    ("cache_node_type", "CacheNodeType", S),
    ("engine_version", "EngineVersion", S),
    ("num_cache_nodes", "NumCacheNodes", I),
    ("transit_encryption_enabled", "TransitEncryptionEnabled", B),
    ("transit_encryption_mode", "TransitEncryptionMode", E),
];

/// Cache node fields (`__meta_elasticache_cache_cluster_node_<label>`).
const NODE_LABELS: &[(&str, &str, Kind)] = &[
    ("id", "CacheNodeId", S),
    ("status", "CacheNodeStatus", S),
    ("create_time", "CacheNodeCreateTime", T),
    ("availability_zone", "CustomerAvailabilityZone", S),
    ("customer_outpost_arn", "CustomerOutpostArn", S),
    ("source_cache_node_id", "SourceCacheNodeId", S),
    ("parameter_group_status", "ParameterGroupStatus", S),
    ("endpoint_address", "Endpoint/Address", S),
    ("endpoint_port", "Endpoint/Port", I),
];

/// Go `ElasticacheSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ElasticacheSdConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub clusters: Vec<String>,
    pub port: i64,
    pub refresh_interval: yaml::Duration,
    pub request_concurrency: i64,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultElasticacheSDConfig`.
pub fn default_elasticache_sd_config() -> ElasticacheSdConfig {
    ElasticacheSdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), request_concurrency: 10, http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(ElasticacheSdConfig, "aws.plain", [
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

impl Unmarshal for ElasticacheSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_elasticache_sd_config();
            d.inner_struct(n, self)?;
            if self.request_concurrency <= 0 {
                return Err(YError::Msg(format!("elasticache_sd: request_concurrency must be positive, got {}", self.request_concurrency)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.plain".to_string()
    }
}
impl yaml::Marshal for ElasticacheSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for ElasticacheSdConfig {
    const GO_TYPE: &'static str = "aws.ElasticacheSDConfig";
}

impl Config for ElasticacheSdConfig {
    fn name(&self) -> &'static str {
        "elasticache"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(ElasticacheDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        ElasticacheMetrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl ElasticacheSdConfig {
    /// The connection settings for the AWS client.
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

/// Go `elasticacheMetrics`.
pub struct ElasticacheMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl ElasticacheMetrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<ElasticacheMetrics> {
        Arc::new(ElasticacheMetrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for ElasticacheMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh state (Go `ElasticacheDiscovery` without the embedded refresh loop).
pub struct ElasticacheRefresher {
    logger: Logger,
    cfg: ElasticacheSdConfig,
    client: Option<Client>,
}

/// Go `ElasticacheDiscovery`.
pub struct ElasticacheDiscovery {
    inner: refresh::Discovery<ElasticacheRefresher>,
}

impl ElasticacheDiscovery {
    /// Go `NewElasticacheDiscovery`.
    pub fn new(conf: &ElasticacheSdConfig, opts: DiscovererOptions) -> Result<ElasticacheDiscovery, String> {
        let m = crate::downcast_metrics::<ElasticacheMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = ElasticacheRefresher { logger: opts.logger.clone(), cfg: conf.clone(), client: None };
        // as upstream, the refresh metrics of elasticache have no set name
        let inner = refresh::Discovery::new(refresh::Options { logger: opts.logger, mech: "elasticache", set_name: "", interval: conf.refresh_interval.to_std(), refresh_f: refresher, metrics_instantiator: m.refresh_metrics.as_ref() });
        Ok(ElasticacheDiscovery { inner })
    }

    /// Sets the API client, skipping the lazy client creation and its credential test (what the Go tests do by
    /// setting `elasticacheClient`).
    pub fn set_client(&mut self, c: Client) {
        self.inner.refresher().client = Some(c);
    }

    /// Go `ElasticacheDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// Go `describeServerlessCaches` (the client must be set).
    pub fn describe_serverless_caches(&mut self, ctx: &CancelToken, names: &[String]) -> Result<Vec<Element>, String> {
        self.inner.refresher().describe_serverless_caches(ctx, names)
    }

    /// Go `describeCacheClusters` (the client must be set).
    pub fn describe_cache_clusters(&mut self, ctx: &CancelToken, ids: &[String]) -> Result<Vec<Element>, String> {
        self.inner.refresher().describe_cache_clusters(ctx, ids)
    }
}

impl Discoverer for ElasticacheDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

fn p(k: &str, v: impl ToString) -> (String, String) {
    (k.to_string(), v.to_string())
}

impl ElasticacheRefresher {
    /// Go `initElasticacheClient`.
    fn init_client(&mut self, ctx: &CancelToken) -> Result<(), String> {
        if self.client.is_some() {
            return Ok(());
        }
        let c = Client::new(&self.cfg.client_config(), client::ELASTICACHE, "elasticache_sd").inspect_err(|e| {
            if let Some(inner) = e.strip_prefix("could not create aws config: ") {
                self.logger.error("Failed to create AWS config", &[("error", inner)]);
            }
        })?;
        let test = c.query(ctx, "DescribeCacheClusters", &[]);
        self.client = Some(c);
        if let Err(e) = test {
            self.logger.error("Failed to test Elasticache credentials", &[("error", &e)]);
            return Err(format!("elasticache credential test failed: {e}"));
        }
        Ok(())
    }

    fn client(&self) -> Result<&Client, String> {
        self.client.as_ref().ok_or_else(|| "elasticache client is not initialized".to_string())
    }

    fn limit(&self) -> usize {
        self.cfg.request_concurrency.max(1) as usize
    }

    fn describe_serverless_caches(&self, ctx: &CancelToken, names: &[String]) -> Result<Vec<Element>, String> {
        let c = self.client()?;
        if names.is_empty() {
            let mut out = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let mut params = vec![p("MaxResults", 50)];
                if let Some(t) = &token {
                    params.push(p("NextToken", t));
                }
                let root = c.query(ctx, "DescribeServerlessCaches", &params).map_err(|e| format!("failed to describe serverless caches: {e}"))?;
                let res = root.child("DescribeServerlessCachesResult");
                out.extend(res.map(|r| r.list("ServerlessCaches")).unwrap_or(&[]).iter().cloned());
                match res.and_then(|r| r.text_at("NextToken")) {
                    Some(t) => token = Some(t.to_string()),
                    None => break,
                }
            }
            return Ok(out);
        }
        let parts = run_limited(ctx, names, self.limit(), |ectx, name| {
            let root = c.query(ectx, "DescribeServerlessCaches", &[p("MaxResults", 50), p("ServerlessCacheName", name)]).map_err(|e| format!("failed to describe serverless cache {name}: {e}"))?;
            Ok(root.child("DescribeServerlessCachesResult").map(|r| r.list("ServerlessCaches").to_vec()).unwrap_or_default())
        })?;
        Ok(parts.into_iter().flatten().collect())
    }

    fn describe_cache_clusters(&self, ctx: &CancelToken, ids: &[String]) -> Result<Vec<Element>, String> {
        let c = self.client()?;
        // ShowCacheClustersNotInReplicationGroups is deliberately left unset (see elasticache.go).
        if ids.is_empty() {
            let mut out = Vec::new();
            let mut marker: Option<String> = None;
            loop {
                let mut params = vec![];
                if let Some(m) = &marker {
                    params.push(p("Marker", m));
                }
                params.push(p("MaxRecords", 100));
                params.push(p("ShowCacheNodeInfo", true));
                let root = c.query(ctx, "DescribeCacheClusters", &params).map_err(|e| format!("failed to describe cache clusters: {e}"))?;
                let res = root.child("DescribeCacheClustersResult");
                out.extend(res.map(|r| r.list("CacheClusters")).unwrap_or(&[]).iter().cloned());
                match res.and_then(|r| r.text_at("Marker")) {
                    Some(m) => marker = Some(m.to_string()),
                    None => break,
                }
            }
            return Ok(out);
        }
        let parts = run_limited(ctx, ids, self.limit(), |ectx, id| {
            let root = c.query(ectx, "DescribeCacheClusters", &[p("CacheClusterId", id), p("MaxRecords", 100), p("ShowCacheNodeInfo", true)]).map_err(|e| format!("failed to describe cache cluster {id}: {e}"))?;
            Ok(root.child("DescribeCacheClustersResult").map(|r| r.list("CacheClusters").to_vec()).unwrap_or_default())
        })?;
        Ok(parts.into_iter().flatten().collect())
    }

    fn list_tags_for_resource(&self, ctx: &CancelToken, arns: &[String]) -> Result<HashMap<String, Vec<Element>>, String> {
        let c = self.client()?;
        let parts = run_limited(ctx, arns, self.limit(), |ectx, arn| {
            let root = c.query(ectx, "ListTagsForResource", &[p("ResourceName", arn)]).map_err(|e| format!("failed to list tags for resource {arn}: {e}"))?;
            Ok((arn.clone(), root.child("ListTagsForResourceResult").map(|r| r.list("TagList").to_vec()).unwrap_or_default()))
        })?;
        Ok(parts.into_iter().collect())
    }
}

impl Refresher for ElasticacheRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.init_client(ctx)?;
        let (serverless_ids, cluster_ids) = split_cache_deployment_options(&self.cfg.clusters, &self.logger);

        // Both deployment options are described once, concurrently (Go errgroup).
        let dctx = ctx.child();
        let this = &*self;
        let (caches, clusters) = std::thread::scope(|s| {
            let h = s.spawn(|| {
                let r = this.describe_serverless_caches(&dctx, &serverless_ids).map_err(|e| format!("failed to describe serverless caches: {e}"));
                if r.is_err() {
                    dctx.cancel();
                }
                r
            });
            let clusters = this.describe_cache_clusters(&dctx, &cluster_ids).map_err(|e| format!("failed to describe cache clusters: {e}"));
            if clusters.is_err() {
                dctx.cancel();
            }
            let caches = h.join().unwrap_or_else(|_| Err("describe serverless caches panicked".to_string()));
            (caches, clusters)
        });
        // errgroup returns the first error; the serverless describe was started first
        let (caches, clusters) = match (caches, clusters) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => return Err(e),
        };

        // ARN is optional and only used to look up tags.
        let mut arns = Vec::with_capacity(caches.len() + clusters.len());
        for c in caches.iter().chain(clusters.iter()) {
            if let Some(a) = c.text_at("ARN") {
                arns.push(a.to_string());
            }
        }
        let tags = self.list_tags_for_resource(ctx, &arns).map_err(|e| format!("failed to list tags for resources: {e}"))?;

        let mut tg = Group { source: self.client()?.region().to_string(), ..Default::default() };
        for c in &caches {
            add_serverless_cache_targets(&mut tg, c, tags.get(c.text_at("ARN").unwrap_or("")).map(|v| v.as_slice()).unwrap_or(&[]))?;
        }
        for c in &clusters {
            add_cache_cluster_targets(&mut tg, c, tags.get(c.text_at("ARN").unwrap_or("")).map(|v| v.as_slice()).unwrap_or(&[]))?;
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `splitCacheDeploymentOptions`: serverless cache names and replication group IDs from cache ARNs.
pub fn split_cache_deployment_options(caches: &[String], logger: &Logger) -> (Vec<String>, Vec<String>) {
    let (mut serverless, mut clusters) = (Vec::new(), Vec::new());
    for arn in caches {
        if arn.is_empty() {
            continue;
        }
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() < 7 {
            logger.warn("Skipping invalid ElastiCache ARN", &[("arn", arn)]);
            continue;
        }
        match parts[5] {
            "serverlesscache" => serverless.push(parts[6].to_string()),
            "replicationgroup" => clusters.push(parts[6].to_string()),
            _ => {}
        }
    }
    (serverless, clusters)
}

fn add_tags(labels: &mut LabelSet, prefix: &str, tags: &[Element]) {
    for t in tags {
        if let (Some(k), Some(v)) = (t.text_at("Key"), t.text_at("Value")) {
            labels.insert(format!("{prefix}tag_{}", sanitize_label_name(k)), v.to_string());
        }
    }
}

/// Go `addServerlessCacheTargets`: one target for a serverless cache.
pub fn add_serverless_cache_targets(tg: &mut Group, cache: &Element, tags: &[Element]) -> Result<(), String> {
    let mut labels = LabelSet::new();
    labels.insert(ELASTICACHE_LABEL_DEPLOYMENT_OPTION.to_string(), "serverless".to_string());
    apply(cache, SERVERLESS_CACHE, SERVERLESS_LABELS, &mut labels)?;
    for (i, sg) in cache.list("SecurityGroupIds").iter().enumerate() {
        labels.insert(format!("{SERVERLESS_CACHE}security_group_id_{i}"), sg.text.clone());
    }
    for (i, sn) in cache.list("SubnetIds").iter().enumerate() {
        labels.insert(format!("{SERVERLESS_CACHE}subnet_id_{i}"), sn.text.clone());
    }
    if let Some(limits) = cache.child("CacheUsageLimits") {
        if let Some(ds) = limits.child("DataStorage") {
            for (suffix, path) in [("maximum", "Maximum"), ("minimum", "Minimum")] {
                if let Some(v) = value(ds, path, Kind::Int)? {
                    labels.insert(format!("{SERVERLESS_CACHE}cache_usage_limit_data_storage{suffix}"), v);
                }
            }
            labels.insert(format!("{SERVERLESS_CACHE}cache_usage_limit_data_storageunit"), ds.text_at("Unit").unwrap_or("").to_string());
        }
        if let Some(ecpu) = limits.child("ECPUPerSecond") {
            for (suffix, path) in [("maximum", "Maximum"), ("minimum", "Minimum")] {
                if let Some(v) = value(ecpu, path, Kind::Int)? {
                    labels.insert(format!("{SERVERLESS_CACHE}cache_usage_limit_ecpu_per_second{suffix}"), v);
                }
            }
        }
    }
    add_tags(&mut labels, SERVERLESS_CACHE, tags);
    if let (Some(addr), Some(port)) = (cache.text_at("Endpoint/Address"), value(cache, "Endpoint/Port", Kind::Int)?) {
        labels.insert(ADDRESS_LABEL.to_string(), join_host_port(addr, &port));
    }
    tg.targets.push(labels);
    Ok(())
}

/// Go `addCacheClusterTargets`: one target per cache node.
pub fn add_cache_cluster_targets(tg: &mut Group, cluster: &Element, tags: &[Element]) -> Result<(), String> {
    let mut common = LabelSet::new();
    common.insert(ELASTICACHE_LABEL_DEPLOYMENT_OPTION.to_string(), "node".to_string());
    apply(cluster, CACHE_CLUSTER, CLUSTER_LABELS, &mut common)?;
    for (i, ld) in cluster.list("LogDeliveryConfigurations").iter().enumerate() {
        for (suffix, path, kind) in LOG_DELIVERY_LABELS {
            if let Some(v) = value(ld, path, *kind)? {
                common.insert(format!("{CACHE_CLUSTER}log_delivery_configuration_{suffix}_{i}"), v);
            }
        }
    }
    if let Some(pm) = cluster.child("PendingModifiedValues") {
        apply(pm, &format!("{CACHE_CLUSTER}pending_modified_values_"), PENDING_LABELS, &mut common)?;
        let ids: Vec<&str> = pm.list("CacheNodeIdsToRemove").iter().map(|e| e.text.as_str()).collect();
        if !ids.is_empty() {
            common.insert(format!("{CACHE_CLUSTER}pending_modified_values_cache_node_ids_to_remove"), ids.join(","));
        }
    }
    for (i, sg) in cluster.list("SecurityGroups").iter().enumerate() {
        if let Some(id) = sg.text_at("SecurityGroupId") {
            common.insert(format!("{CACHE_CLUSTER}security_group_membership_id_{i}"), id.to_string());
        }
        if let Some(st) = sg.text_at("Status") {
            common.insert(format!("{CACHE_CLUSTER}security_group_membership_status_{i}"), st.to_string());
        }
    }
    add_tags(&mut common, CACHE_CLUSTER, tags);

    for node in cluster.list("CacheNodes") {
        let mut labels = common.clone();
        apply(node, &format!("{CACHE_CLUSTER}node_"), NODE_LABELS, &mut labels)?;
        if let (Some(addr), Some(port)) = (node.text_at("Endpoint/Address"), value(node, "Endpoint/Port", Kind::Int)?) {
            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(addr, &port));
        }
        tg.targets.push(labels);
    }
    Ok(())
}
