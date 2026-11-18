//! Port of `discovery/aws`: the `aws_sd_configs` umbrella config (one role per config) and the per-service
//! configs `ec2`, `ecs`, `elasticache`, `lightsail`, `msk`, `rds` (each also registered as `<name>_sd_configs`).
use std::fmt;
use std::sync::Arc;

use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal, Value};

use crate::metrics::Registry;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod client;
pub mod credentials;
pub mod ec2;
pub mod ecs;
pub mod elasticache;
pub mod labels;
pub mod lightsail;
pub mod msk;
pub mod rds;
pub mod xml;

/// Go `Filter`: a filter for AWS resources (ec2, rds).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filter {
    pub name: String,
    pub values: Vec<String>,
}

yaml::yaml_struct!(Filter, "aws.Filter", [
    (name: "name")
    (values: "values")
]);
yaml::impl_plain_struct!(Filter);

pub const ROLE_EC2: &str = "ec2";
pub const ROLE_ECS: &str = "ecs";
pub const ROLE_ELASTICACHE: &str = "elasticache";
pub const ROLE_LIGHTSAIL: &str = "lightsail";
pub const ROLE_MSK: &str = "msk";
pub const ROLE_RDS: &str = "rds";

/// Go `Role`: the service of an `aws_sd_configs` entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Role(pub String);

impl Role {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn unknown_role(r: &str) -> String {
    format!("unknown AWS SD role {}", yaml::go_quote(r))
}

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            if !yaml::types::unmarshal_string_kind(&mut s, n, d, "aws.Role")? {
                return Ok(());
            }
            self.0 = s;
            match self.0.as_str() {
                ROLE_EC2 | ROLE_ECS | ROLE_ELASTICACHE | ROLE_LIGHTSAIL | ROLE_MSK | ROLE_RDS => Ok(()),
                r => Err(YError::Msg(unknown_role(r))),
            }
        })
    }
    fn go_type() -> String {
        "aws.Role".to_string()
    }
}
impl yaml::Marshal for Role {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `aws.SDConfig`: one role, with flat fields copied into the role's own config.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub role: Role,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub http_client_config: HttpClientConfig,
    pub filters: Vec<Filter>,
    pub clusters: Vec<String>,
    // The role configs (Go embedded pointers, not serialized).
    pub ec2: Option<ec2::Ec2SdConfig>,
    pub ecs: Option<ecs::EcsSdConfig>,
    pub elasticache: Option<elasticache::ElasticacheSdConfig>,
    pub lightsail: Option<lightsail::LightsailSdConfig>,
    pub msk: Option<msk::MskSdConfig>,
    pub rds: Option<rds::RdsSdConfig>,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "aws.plain", [
    (role: "role")
    (region: "region", omitempty)
    (endpoint: "endpoint", omitempty)
    (access_key: "access_key", omitempty)
    (secret_key: "secret_key", omitempty)
    (profile: "profile", omitempty)
    (role_arn: "role_arn", omitempty)
    (external_id: "external_id", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port", omitempty)
    (inline http_client_config)
    (filters: "filters", omitempty)
    (clusters: "clusters", omitempty)
]);

/// Copies the flat fields every role shares into a role config (the per-role blocks of Go `UnmarshalYAML`).
macro_rules! copy_common {
    ($c:expr, $sub:expr) => {{
        let sub = &mut $sub;
        sub.http_client_config = $c.http_client_config.clone();
        sub.region = $c.region.clone();
        if !$c.endpoint.is_empty() {
            sub.endpoint = $c.endpoint.clone();
        }
        if !$c.access_key.is_empty() {
            sub.access_key = $c.access_key.clone();
        }
        if !$c.secret_key.is_empty() {
            sub.secret_key = $c.secret_key.clone();
        }
        if !$c.profile.is_empty() {
            sub.profile = $c.profile.clone();
        }
        if !$c.role_arn.is_empty() {
            sub.role_arn = $c.role_arn.clone();
        }
        if !$c.external_id.is_empty() {
            sub.external_id = $c.external_id.clone();
        }
        if $c.port != 0 {
            sub.port = $c.port;
        }
        if $c.refresh_interval.0 != 0 {
            sub.refresh_interval = $c.refresh_interval;
        }
    }};
}

impl SdConfig {
    /// The role-specific part of Go `UnmarshalYAML` (after the plain decode).
    fn fill_role(&mut self) -> Result<(), String> {
        // Go checks `c.Filters != nil` / `c.Clusters != nil`; the role defaults are empty, so an explicit empty
        // list and an absent one give the same role config.
        match self.role.as_str() {
            ROLE_EC2 => {
                let mut sub = self.ec2.take().unwrap_or_else(ec2::default_ec2_sd_config);
                copy_common!(self, sub);
                if !self.filters.is_empty() {
                    sub.filters = self.filters.clone();
                }
                self.ec2 = Some(sub);
            }
            ROLE_ECS => {
                let mut sub = self.ecs.take().unwrap_or_else(ecs::default_ecs_sd_config);
                copy_common!(self, sub);
                if !self.clusters.is_empty() {
                    sub.clusters = self.clusters.clone();
                }
                self.ecs = Some(sub);
            }
            ROLE_ELASTICACHE => {
                let mut sub = self.elasticache.take().unwrap_or_else(elasticache::default_elasticache_sd_config);
                copy_common!(self, sub);
                if !self.clusters.is_empty() {
                    sub.clusters = self.clusters.clone();
                }
                self.elasticache = Some(sub);
            }
            ROLE_LIGHTSAIL => {
                let mut sub = self.lightsail.take().unwrap_or_else(lightsail::default_lightsail_sd_config);
                copy_common!(self, sub);
                self.lightsail = Some(sub);
            }
            ROLE_MSK => {
                let mut sub = self.msk.take().unwrap_or_else(msk::default_msk_sd_config);
                copy_common!(self, sub);
                if !self.clusters.is_empty() {
                    sub.clusters = self.clusters.clone();
                }
                self.msk = Some(sub);
            }
            ROLE_RDS => {
                let mut sub = self.rds.take().unwrap_or_else(rds::default_rds_sd_config);
                copy_common!(self, sub);
                if !self.filters.is_empty() {
                    sub.filters = self.filters.clone();
                }
                if !self.clusters.is_empty() {
                    sub.clusters = self.clusters.clone();
                }
                self.rds = Some(sub);
            }
            r => return Err(unknown_role(r)),
        }
        Ok(())
    }
}

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            // Go decodes into a zero `plain` value (not DefaultSDConfig), and region resolution is deferred to
            // the role's client (see client::load_region).
            *self = SdConfig::default();
            d.inner_struct(n, self)?;
            self.fill_role().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.plain".to_string()
    }
}
impl yaml::Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "aws.SDConfig";
}

/// Go `awsMetrics`.
pub struct AwsMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for AwsMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

fn role_config<T>(c: &Option<T>) -> Result<&T, String> {
    // Go dereferences the nil pointer here; a config built in code without UnmarshalYAML has no role config.
    c.as_ref().ok_or_else(|| "AWS SD role config is not set".to_string())
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "aws"
    }
    fn new_discoverer(&self, mut opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        let m = crate::downcast_metrics::<AwsMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type for AWS SD".to_string())?;
        let rmi = m.refresh_metrics.clone();
        Ok(match self.role.as_str() {
            ROLE_EC2 => {
                opts.metrics = Some(ec2::Ec2Metrics::new(rmi));
                Box::new(ec2::Ec2Discovery::new(role_config(&self.ec2)?, opts)?)
            }
            ROLE_ECS => {
                opts.metrics = Some(ecs::EcsMetrics::new(rmi));
                Box::new(ecs::EcsDiscovery::new(role_config(&self.ecs)?, opts)?)
            }
            ROLE_ELASTICACHE => {
                opts.metrics = Some(elasticache::ElasticacheMetrics::new(rmi));
                Box::new(elasticache::ElasticacheDiscovery::new(role_config(&self.elasticache)?, opts)?)
            }
            ROLE_LIGHTSAIL => {
                opts.metrics = Some(lightsail::LightsailMetrics::new(rmi));
                Box::new(lightsail::LightsailDiscovery::new(role_config(&self.lightsail)?, opts)?)
            }
            ROLE_MSK => {
                opts.metrics = Some(msk::MskMetrics::new(rmi));
                Box::new(msk::MskDiscovery::new(role_config(&self.msk)?, opts)?)
            }
            ROLE_RDS => {
                opts.metrics = Some(rds::RdsMetrics::new(rmi));
                Box::new(rds::RdsDiscovery::new(role_config(&self.rds)?, opts)?)
            }
            r => return Err(unknown_role(r)),
        })
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(AwsMetrics { refresh_metrics: rmi })
    }
    /// Go `SetDirectory`: only the role config (nil-safe).
    fn set_directory(&mut self, dir: &str) {
        match self.role.as_str() {
            ROLE_EC2 => self.ec2.iter_mut().for_each(|c| c.set_directory(dir)),
            ROLE_ECS => self.ecs.iter_mut().for_each(|c| c.set_directory(dir)),
            ROLE_ELASTICACHE => self.elasticache.iter_mut().for_each(|c| c.set_directory(dir)),
            ROLE_LIGHTSAIL => self.lightsail.iter_mut().for_each(|c| c.set_directory(dir)),
            ROLE_MSK => self.msk.iter_mut().for_each(|c| c.set_directory(dir)),
            ROLE_RDS => self.rds.iter_mut().for_each(|c| c.set_directory(dir)),
            _ => {}
        }
    }
}
