//! Port of `discovery/aws/rds.go`: Aurora / RDS DB clusters and their instances (`rds_sd_configs`, and the `rds`
//! role of `aws_sd_configs`). The RDS API is the awsQuery protocol (form POST, XML response).
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use super::Filter;
use super::client::{self, Client, ClientConfig};
use super::labels::{Kind, apply, run_limited, value};
use super::xml::Element;
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const RDS_LABEL_CLUSTER: &str = "__meta_rds_cluster_";
const RDS_LABEL_INSTANCE: &str = "__meta_rds_instance_";

use Kind::{Bool as B, Enum as E, Int as I, Str as S, Time as T};

/// The DB cluster fields and their labels (`__meta_rds_cluster_<label>`), in the order of rds.go.
const CLUSTER_LABELS: &[(&str, &str, Kind)] = &[
    ("arn", "DBClusterArn", S),
    ("identifier", "DBClusterIdentifier", S),
    ("activity_stream_kinesis_stream_name", "ActivityStreamKinesisStreamName", S),
    ("activity_stream_kms_key_id", "ActivityStreamKmsKeyId", S),
    ("activity_stream_mode", "ActivityStreamMode", E),
    ("activity_stream_status", "ActivityStreamStatus", E),
    ("allocated_storage", "AllocatedStorage", I),
    ("auto_minor_version_upgrade", "AutoMinorVersionUpgrade", B),
    ("automatic_restart_time", "AutomaticRestartTime", T),
    ("aws_backup_recovery_point_arn", "AwsBackupRecoveryPointArn", S),
    ("backtrack_consumed_change_records", "BacktrackConsumedChangeRecords", I),
    ("backtrack_window", "BacktrackWindow", I),
    ("backup_retention_period", "BackupRetentionPeriod", I),
    ("capacity", "Capacity", I),
    ("character_set_name", "CharacterSetName", S),
    ("clone_group_id", "CloneGroupId", S),
    ("cluster_create_time", "ClusterCreateTime", T),
    ("cluster_scalability_type", "ClusterScalabilityType", E),
    ("copy_tags_to_snapshot", "CopyTagsToSnapshot", B),
    ("cross_account_clone", "CrossAccountClone", B),
    ("instance_class", "DBClusterInstanceClass", S),
    ("parameter_group", "DBClusterParameterGroup", S),
    ("subnet_group", "DBSubnetGroup", S),
    ("db_system_id", "DBSystemId", S),
    ("database_insights_mode", "DatabaseInsightsMode", E),
    ("database_name", "DatabaseName", S),
    ("resource_id", "DbClusterResourceId", S),
    ("deletion_protection", "DeletionProtection", B),
    ("earliest_backtrack_time", "EarliestBacktrackTime", T),
    ("earliest_restorable_time", "EarliestRestorableTime", T),
    ("endpoint", "Endpoint", S),
    ("engine", "Engine", S),
    ("engine_lifecycle_support", "EngineLifecycleSupport", S),
    ("engine_mode", "EngineMode", S),
    ("engine_version", "EngineVersion", S),
    ("global_cluster_identifier", "GlobalClusterIdentifier", S),
    ("global_write_forwarding_requested", "GlobalWriteForwardingRequested", B),
    ("global_write_forwarding_status", "GlobalWriteForwardingStatus", E),
    ("hosted_zone_id", "HostedZoneId", S),
    ("http_endpoint_enabled", "HttpEndpointEnabled", B),
    ("iam_database_authentication_enabled", "IAMDatabaseAuthenticationEnabled", B),
    ("io_optimized_next_allowed_modification_time", "IOOptimizedNextAllowedModificationTime", T),
    ("iops", "Iops", I),
    ("kms_key_id", "KmsKeyId", S),
    ("latest_restorable_time", "LatestRestorableTime", T),
    ("local_write_forwarding_status", "LocalWriteForwardingStatus", E),
    ("master_username", "MasterUsername", S),
    ("monitoring_interval", "MonitoringInterval", I),
    ("monitoring_role_arn", "MonitoringRoleArn", S),
    ("multi_az", "MultiAZ", B),
    ("network_type", "NetworkType", S),
    ("percent_progress", "PercentProgress", S),
    ("performance_insights_enabled", "PerformanceInsightsEnabled", B),
    ("performance_insights_kms_key_id", "PerformanceInsightsKMSKeyId", S),
    ("performance_insights_retention_period", "PerformanceInsightsRetentionPeriod", I),
    ("port", "Port", I),
    ("preferred_backup_window", "PreferredBackupWindow", S),
    ("preferred_maintenance_window", "PreferredMaintenanceWindow", S),
    ("publicly_accessible", "PubliclyAccessible", B),
    ("reader_endpoint", "ReaderEndpoint", S),
    ("replication_source_identifier", "ReplicationSourceIdentifier", S),
    ("serverless_v2_platform_version", "ServerlessV2PlatformVersion", S),
    ("status", "Status", S),
    ("storage_encrypted", "StorageEncrypted", B),
    ("storage_encryption_type", "StorageEncryptionType", E),
    ("storage_throughput", "StorageThroughput", I),
    ("storage_type", "StorageType", S),
    ("upgrade_rollout_order", "UpgradeRolloutOrder", E),
];

/// The DB instance fields and their labels (`__meta_rds_instance_<label>`), in the order of rds.go
/// (`is_cluster_writer` comes from the cluster members).
const INSTANCE_LABELS: &[(&str, &str, Kind)] = &[
    ("arn", "DBInstanceArn", S),
    ("identifier", "DBInstanceIdentifier", S),
    ("activity_stream_engine_native_audit_fields_included", "ActivityStreamEngineNativeAuditFieldsIncluded", B),
    ("activity_stream_kinesis_stream_name", "ActivityStreamKinesisStreamName", S),
    ("activity_stream_kms_key_id", "ActivityStreamKmsKeyId", S),
    ("activity_stream_mode", "ActivityStreamMode", E),
    ("activity_stream_policy_status", "ActivityStreamPolicyStatus", E),
    ("activity_stream_status", "ActivityStreamStatus", E),
    ("allocated_storage", "AllocatedStorage", I),
    ("auto_minor_version_upgrade", "AutoMinorVersionUpgrade", B),
    ("automatic_restart_time", "AutomaticRestartTime", T),
    ("automation_mode", "AutomationMode", E),
    ("availability_zone", "AvailabilityZone", S),
    ("aws_backup_recovery_point_arn", "AwsBackupRecoveryPointArn", S),
    ("backup_retention_period", "BackupRetentionPeriod", I),
    ("backup_target", "BackupTarget", S),
    ("ca_certificate_identifier", "CACertificateIdentifier", S),
    ("character_set_name", "CharacterSetName", S),
    ("copy_tags_to_snapshot", "CopyTagsToSnapshot", B),
    ("custom_iam_instance_profile", "CustomIamInstanceProfile", S),
    ("customer_owned_ip_enabled", "CustomerOwnedIpEnabled", B),
    ("db_cluster_identifier", "DBClusterIdentifier", S),
    ("class", "DBInstanceClass", S),
    ("status", "DBInstanceStatus", S),
    ("db_name", "DBName", S),
    ("port", "DbInstancePort", I),
    ("resource_id", "DbiResourceId", S),
    ("dedicated_log_volume", "DedicatedLogVolume", B),
    ("deletion_protection", "DeletionProtection", B),
    ("endpoint_address", "Endpoint/Address", S),
    ("endpoint_hosted_zone_id", "Endpoint/HostedZoneId", S),
    ("endpoint_port", "Endpoint/Port", I),
    ("engine", "Engine", S),
    ("engine_lifecycle_support", "EngineLifecycleSupport", S),
    ("engine_version", "EngineVersion", S),
    ("enhanced_monitoring_resource_arn", "EnhancedMonitoringResourceArn", S),
    ("iam_database_authentication_enabled", "IAMDatabaseAuthenticationEnabled", B),
    ("instance_create_time", "InstanceCreateTime", T),
    ("iops", "Iops", I),
    ("is_storage_config_upgrade_available", "IsStorageConfigUpgradeAvailable", B),
    ("kms_key_id", "KmsKeyId", S),
    ("latest_restorable_time", "LatestRestorableTime", T),
    ("license_model", "LicenseModel", S),
    ("listener_endpoint_address", "ListenerEndpoint/Address", S),
    ("listener_endpoint_hosted_zone_id", "ListenerEndpoint/HostedZoneId", S),
    ("listener_endpoint_port", "ListenerEndpoint/Port", I),
    ("master_username", "MasterUsername", S),
    ("max_allocated_storage", "MaxAllocatedStorage", I),
    ("monitoring_interval", "MonitoringInterval", I),
    ("monitoring_role_arn", "MonitoringRoleArn", S),
    ("multi_az", "MultiAZ", B),
    ("multi_tenant", "MultiTenant", B),
    ("nchar_character_set_name", "NcharCharacterSetName", S),
    ("network_type", "NetworkType", S),
    ("percent_progress", "PercentProgress", S),
    ("performance_insights_enabled", "PerformanceInsightsEnabled", B),
    ("performance_insights_kms_key_id", "PerformanceInsightsKMSKeyId", S),
    ("performance_insights_retention_period", "PerformanceInsightsRetentionPeriod", I),
    ("preferred_backup_window", "PreferredBackupWindow", S),
    ("preferred_maintenance_window", "PreferredMaintenanceWindow", S),
    ("promotion_tier", "PromotionTier", I),
    ("publicly_accessible", "PubliclyAccessible", B),
    ("read_replica_source_db_cluster_identifier", "ReadReplicaSourceDBClusterIdentifier", S),
    ("read_replica_source_db_instance_identifier", "ReadReplicaSourceDBInstanceIdentifier", S),
    ("replica_mode", "ReplicaMode", E),
    ("resume_full_automation_mode_time", "ResumeFullAutomationModeTime", T),
    ("secondary_availability_zone", "SecondaryAvailabilityZone", S),
    ("storage_encrypted", "StorageEncrypted", B),
    ("storage_encryption_type", "StorageEncryptionType", E),
    ("storage_throughput", "StorageThroughput", I),
    ("storage_type", "StorageType", S),
    ("storage_volume_status", "StorageVolumeStatus", S),
    ("tde_credential_arn", "TdeCredentialArn", S),
    ("timezone", "Timezone", S),
    ("upgrade_rollout_order", "UpgradeRolloutOrder", E),
    ("subnet_group", "DBSubnetGroup/DBSubnetGroupName", S),
    ("db_system_id", "DBSystemId", S),
    ("database_insights_mode", "DatabaseInsightsMode", E),
];

/// Go `RDSSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RdsSdConfig {
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
    pub filters: Vec<Filter>,
    pub request_concurrency: i64,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultRDSSDConfig`.
pub fn default_rds_sd_config() -> RdsSdConfig {
    RdsSdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), request_concurrency: 10, http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(RdsSdConfig, "aws.plain", [
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
    (filters: "filters")
    (request_concurrency: "request_concurrency", omitempty)
    (inline http_client_config)
]);

impl Unmarshal for RdsSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_rds_sd_config();
            d.inner_struct(n, self)?;
            if self.request_concurrency <= 0 {
                return Err(YError::Msg(format!("rds_sd: request_concurrency must be positive, got {}", self.request_concurrency)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.plain".to_string()
    }
}
impl yaml::Marshal for RdsSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for RdsSdConfig {
    const GO_TYPE: &'static str = "aws.RDSSDConfig";
}

impl Config for RdsSdConfig {
    fn name(&self) -> &'static str {
        "rds"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(RdsDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        RdsMetrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl RdsSdConfig {
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

/// Go `rdsMetrics`.
pub struct RdsMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl RdsMetrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<RdsMetrics> {
        Arc::new(RdsMetrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for RdsMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh state of the RDS discovery (Go `RDSDiscovery` without the embedded refresh loop).
pub struct RdsRefresher {
    logger: Logger,
    cfg: RdsSdConfig,
    client: Option<Client>,
}

/// Go `RDSDiscovery`.
pub struct RdsDiscovery {
    inner: refresh::Discovery<RdsRefresher>,
}

impl RdsDiscovery {
    /// Go `NewRDSDiscovery`.
    pub fn new(conf: &RdsSdConfig, opts: DiscovererOptions) -> Result<RdsDiscovery, String> {
        let m = crate::downcast_metrics::<RdsMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = RdsRefresher { logger: opts.logger.clone(), cfg: conf.clone(), client: None };
        // as upstream, the refresh metrics of rds have no set name
        let inner = refresh::Discovery::new(refresh::Options { logger: opts.logger, mech: "rds", set_name: "", interval: conf.refresh_interval.to_std(), refresh_f: refresher, metrics_instantiator: m.refresh_metrics.as_ref() });
        Ok(RdsDiscovery { inner })
    }

    /// Go `RDSDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// Go `describeAllDBClusters`: the clusters by ARN.
    pub fn describe_all_db_clusters(&mut self, ctx: &CancelToken) -> Result<BTreeMap<String, Element>, String> {
        let r = self.inner.refresher();
        r.init_client(ctx)?;
        r.describe_all_db_clusters(ctx)
    }
}

impl Discoverer for RdsDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

fn int_param(k: &str, v: i64) -> (String, String) {
    (k.to_string(), v.to_string())
}

impl RdsRefresher {
    /// Go `initRdsClient`: builds the client once and tests the credentials with a DescribeDBClusters call.
    fn init_client(&mut self, ctx: &CancelToken) -> Result<(), String> {
        if self.client.is_some() {
            return Ok(());
        }
        let c = Client::new(&self.cfg.client_config(), client::RDS, "rds_sd").inspect_err(|e| {
            if e.starts_with("could not create aws config: ") {
                self.logger.error("Failed to create AWS config", &[("error", e.trim_start_matches("could not create aws config: "))]);
            }
        })?;
        // Go keeps the client even when the test call fails, so later refreshes skip the test.
        let test = c.query(ctx, "DescribeDBClusters", &[]);
        self.client = Some(c);
        if let Err(e) = test {
            self.logger.error("Failed to test RDS credentials", &[("error", &e)]);
            return Err(format!("RDS credential test failed: {e}"));
        }
        Ok(())
    }

    fn client(&self) -> &Client {
        self.client.as_ref().expect("client initialized")
    }

    fn describe_all_db_clusters(&self, ctx: &CancelToken) -> Result<BTreeMap<String, Element>, String> {
        let mut out = BTreeMap::new();
        let mut marker: Option<String> = None;
        loop {
            let mut params = Vec::new();
            if let Some(m) = &marker {
                params.push(("Marker".to_string(), m.clone()));
            }
            params.push(int_param("MaxRecords", 100));
            let root = self.client().query(ctx, "DescribeDBClusters", &params).map_err(|e| format!("failed to describe DB clusters: {e}"))?;
            let res = root.child("DescribeDBClustersResult");
            for c in res.map(|r| r.list("DBClusters")).unwrap_or(&[]) {
                if let Some(arn) = c.text_at("DBClusterArn") {
                    out.insert(arn.to_string(), c.clone());
                }
            }
            match res.and_then(|r| r.text_at("Marker")) {
                Some(m) => marker = Some(m.to_string()),
                None => break,
            }
        }
        Ok(out)
    }

    fn describe_db_clusters(&self, ctx: &CancelToken, arns: &[String]) -> Result<BTreeMap<String, Element>, String> {
        let out: Mutex<BTreeMap<String, Element>> = Mutex::new(BTreeMap::new());
        // Go shares one marker across the goroutines; the pages of a single cluster lookup are followed here
        let marker: Mutex<Option<String>> = Mutex::new(None);
        run_limited(ctx, arns, self.cfg.request_concurrency.max(1) as usize, |ectx, arn| {
            loop {
                let mut params = vec![("DBClusterIdentifier".to_string(), arn.clone())];
                if let Some(m) = marker.lock().unwrap_or_else(|p| p.into_inner()).clone() {
                    params.push(("Marker".to_string(), m));
                }
                params.push(int_param("MaxRecords", 100));
                let root = self.client().query(ectx, "DescribeDBClusters", &params).map_err(|e| format!("failed to describe DB cluster {arn}: {e}"))?;
                let res = root.child("DescribeDBClustersResult");
                let clusters = res.map(|r| r.list("DBClusters")).unwrap_or(&[]);
                if clusters.is_empty() {
                    return Err(format!("no DB cluster found for ARN {arn}"));
                }
                for c in clusters {
                    out.lock().unwrap_or_else(|p| p.into_inner()).insert(arn.clone(), c.clone());
                }
                match res.and_then(|r| r.text_at("Marker")) {
                    Some(m) => *marker.lock().unwrap_or_else(|p| p.into_inner()) = Some(m.to_string()),
                    None => break,
                }
            }
            Ok(())
        })?;
        Ok(out.into_inner().unwrap_or_else(|p| p.into_inner()))
    }

    fn describe_db_instances(&self, ctx: &CancelToken, cluster_arn: &str) -> Result<Vec<Element>, String> {
        let mut filters: Vec<(String, Vec<String>)> = vec![("db-cluster-id".to_string(), vec![cluster_arn.to_string()])];
        for f in &self.cfg.filters {
            filters.push((f.name.clone(), f.values.clone()));
        }
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut params = Vec::new();
            for (i, (name, values)) in filters.iter().enumerate() {
                params.push((format!("Filters.Filter.{}.Name", i + 1), name.clone()));
                if values.is_empty() {
                    params.push((format!("Filters.Filter.{}.Values", i + 1), String::new()));
                }
                for (j, v) in values.iter().enumerate() {
                    params.push((format!("Filters.Filter.{}.Values.Value.{}", i + 1, j + 1), v.clone()));
                }
            }
            if let Some(m) = &marker {
                params.push(("Marker".to_string(), m.clone()));
            }
            params.push(int_param("MaxRecords", 100));
            let root = self.client().query(ctx, "DescribeDBInstances", &params).map_err(|e| format!("failed to describe DB instances for cluster ARN {cluster_arn}: {e}"))?;
            let res = root.child("DescribeDBInstancesResult");
            out.extend(res.map(|r| r.list("DBInstances")).unwrap_or(&[]).iter().cloned());
            match res.and_then(|r| r.text_at("Marker")) {
                Some(m) => marker = Some(m.to_string()),
                None => break,
            }
        }
        Ok(out)
    }

    /// The targets of one cluster.
    fn cluster_targets(&self, ctx: &CancelToken, cluster: &Element) -> Result<Vec<LabelSet>, String> {
        let arn = cluster.text_at("DBClusterArn").unwrap_or("");
        let instances = self.describe_db_instances(ctx, arn).map_err(|e| format!("error describing DB instances: {e}"))?;

        let mut writer: HashMap<&str, bool> = HashMap::new();
        for m in cluster.list("DBClusterMembers") {
            if let (Some(id), Some(Ok(w))) = (m.text_at("DBInstanceIdentifier"), value(m, "IsClusterWriter", Kind::Bool).transpose()) {
                writer.insert(id, w == "true");
            }
        }

        let mut common = LabelSet::new();
        apply(cluster, RDS_LABEL_CLUSTER, CLUSTER_LABELS, &mut common)?;
        for t in cluster.list("TagList") {
            if let (Some(k), Some(v)) = (t.text_at("Key"), t.text_at("Value")) {
                common.insert(format!("{RDS_LABEL_CLUSTER}tag_{}", crate::util::sanitize_label_name(k)), v.to_string());
            }
        }

        let mut out = Vec::with_capacity(instances.len());
        for inst in &instances {
            let mut labels = common.clone();
            apply(inst, RDS_LABEL_INSTANCE, INSTANCE_LABELS, &mut labels)?;
            if let Some(id) = inst.text_at("DBInstanceIdentifier")
                && let Some(w) = writer.get(id)
            {
                labels.insert(format!("{RDS_LABEL_INSTANCE}is_cluster_writer"), w.to_string());
            }
            for t in inst.list("TagList") {
                if let (Some(k), Some(v)) = (t.text_at("Key"), t.text_at("Value")) {
                    labels.insert(format!("{RDS_LABEL_INSTANCE}tag_{}", crate::util::sanitize_label_name(k)), v.to_string());
                }
            }
            if let (Some(addr), Some(_)) = (inst.text_at("Endpoint/Address"), inst.text_at("Endpoint/Port")) {
                labels.insert(ADDRESS_LABEL.to_string(), crate::util::join_host_port(addr, &self.cfg.port.to_string()));
            }
            out.push(labels);
        }
        Ok(out)
    }
}

impl Refresher for RdsRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.init_client(ctx)?;
        let mut tg = Group { source: self.client().region().to_string(), ..Default::default() };
        let clusters = if self.cfg.clusters.is_empty() {
            self.describe_all_db_clusters(ctx).map_err(|e| format!("error describing all DB clusters: {e}"))?
        } else {
            let arns = self.cfg.clusters.clone();
            self.describe_db_clusters(ctx, &arns).map_err(|e| format!("error describing DB clusters: {e}"))?
        };
        let list: Vec<Element> = clusters.into_values().collect();
        let this = &*self;
        let targets = run_limited(ctx, &list, this.cfg.request_concurrency.max(1) as usize, |ectx, c| this.cluster_targets(ectx, c))?;
        tg.targets = targets.into_iter().flatten().collect();
        Ok(vec![Arc::new(tg)])
    }
}



