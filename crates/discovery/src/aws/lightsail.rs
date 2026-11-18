//! Port of `discovery/aws/lightsail.go` and `metrics_lightsail.go`: Lightsail instances as targets
//! (`lightsail_sd_configs`). One GetInstances call (awsJson1.1) per refresh, as Go (no pagination).
use std::sync::Arc;

use json::Json;
use model::labelset::LabelSet;
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::client::{self, Client, ClientConfig};
use super::ec2::is_api_error;
use crate::util::{join_host_port, sanitize_label_name};
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const LIGHTSAIL_LABEL_AZ: &str = "__meta_lightsail_availability_zone";
pub const LIGHTSAIL_LABEL_BLUEPRINT_ID: &str = "__meta_lightsail_blueprint_id";
pub const LIGHTSAIL_LABEL_BUNDLE_ID: &str = "__meta_lightsail_bundle_id";
pub const LIGHTSAIL_LABEL_INSTANCE_NAME: &str = "__meta_lightsail_instance_name";
pub const LIGHTSAIL_LABEL_INSTANCE_STATE: &str = "__meta_lightsail_instance_state";
pub const LIGHTSAIL_LABEL_INSTANCE_SUPPORT_CODE: &str = "__meta_lightsail_instance_support_code";
pub const LIGHTSAIL_LABEL_IPV6_ADDRESSES: &str = "__meta_lightsail_ipv6_addresses";
pub const LIGHTSAIL_LABEL_PRIVATE_IP: &str = "__meta_lightsail_private_ip";
pub const LIGHTSAIL_LABEL_PUBLIC_IP: &str = "__meta_lightsail_public_ip";
pub const LIGHTSAIL_LABEL_REGION: &str = "__meta_lightsail_region";
pub const LIGHTSAIL_LABEL_TAG: &str = "__meta_lightsail_tag_";
const LIGHTSAIL_LABEL_SEPARATOR: &str = ",";

/// Go `LightsailSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LightsailSdConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: yaml::Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultLightsailSDConfig`.
pub fn default_lightsail_sd_config() -> LightsailSdConfig {
    LightsailSdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(LightsailSdConfig, "aws.plain", [
    (endpoint: "endpoint")
    (region: "region")
    (access_key: "access_key", omitempty)
    (secret_key: "secret_key", omitempty)
    (profile: "profile", omitempty)
    (role_arn: "role_arn", omitempty)
    (external_id: "external_id", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port")
    (inline http_client_config)
]);

impl Unmarshal for LightsailSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_lightsail_sd_config();
            d.inner_struct(n, self)?;
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.plain".to_string()
    }
}

impl yaml::Marshal for LightsailSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for LightsailSdConfig {
    const GO_TYPE: &'static str = "aws.LightsailSDConfig";
}

impl Config for LightsailSdConfig {
    fn name(&self) -> &'static str {
        "lightsail"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(LightsailDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        LightsailMetrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl LightsailSdConfig {
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

/// Go `lightsailMetrics`.
pub struct LightsailMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl LightsailMetrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<LightsailMetrics> {
        Arc::new(LightsailMetrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for LightsailMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// A string member: absent or null = Go nil; another JSON type is a deserialization error.
fn jstr<'a>(o: &'a Json, k: &str) -> Result<Option<&'a str>, String> {
    match o.get(k) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Str(s)) => Ok(Some(s)),
        Some(v) => Err(format!("expected {k} to be a JSON string, got {}", model::labelset::json_kind(v))),
    }
}

/// An object member (absent or null = nil).
fn jobj<'a>(o: &'a Json, k: &str) -> Result<Option<&'a Json>, String> {
    match o.get(k) {
        None | Some(Json::Null) => Ok(None),
        Some(v @ Json::Obj(_)) => Ok(Some(v)),
        Some(v) => Err(format!("expected {k} to be a JSON object, got {}", model::labelset::json_kind(v))),
    }
}

/// A list member (absent or null = nil).
fn jarr<'a>(o: &'a Json, k: &str) -> Result<&'a [Json], String> {
    match o.get(k) {
        None | Some(Json::Null) => Ok(&[]),
        Some(Json::Arr(a)) => Ok(a),
        Some(v) => Err(format!("expected {k} to be a JSON array, got {}", model::labelset::json_kind(v))),
    }
}

/// The refresh state of the Lightsail discoverer.
pub struct LightsailRefresher {
    cfg: LightsailSdConfig,
    client: Option<Client>,
    region: String,
}

impl LightsailRefresher {
    /// Go `lightsailClient`.
    fn lightsail_client(&mut self) -> Result<(), String> {
        if self.client.is_some() {
            return Ok(());
        }
        let c = Client::new(&self.cfg.client_config(), client::LIGHTSAIL, "lightsail_sd")?;
        self.region = c.region().to_string();
        self.client = Some(c);
        Ok(())
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.lightsail_client()?;
        let Some(c) = &self.client else { return Ok(Vec::new()) };
        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: self.region.clone() };
        let out = c.json_rpc(ctx, "GetInstances", &Json::obj()).and_then(|o| {
            self.instances_labels(&o).map_err(|e| format!("operation error Lightsail: GetInstances, deserialization failed, failed to decode response body, {e}"))
        });
        match out {
            Ok(targets) => tg.targets = targets,
            Err(e) => {
                if is_api_error(&e, "AuthFailure") || is_api_error(&e, "UnauthorizedOperation") {
                    self.client = None;
                }
                return Err(format!("could not get instances: {e}"));
            }
        }
        Ok(vec![Arc::new(tg)])
    }

    fn instances_labels(&self, out: &Json) -> Result<Vec<LabelSet>, String> {
        let mut targets = Vec::new();
        for inst in jarr(out, "instances")? {
            if inst.is_null() {
                continue;
            }
            let Some(private_ip) = jstr(inst, "privateIpAddress")? else { continue };
            let mut labels = LabelSet::new();
            let mut set = |k: &str, v: &str| {
                labels.insert(k.to_string(), v.to_string());
            };
            set(LIGHTSAIL_LABEL_PRIVATE_IP, private_ip);
            set(LIGHTSAIL_LABEL_REGION, &self.region);
            if let Some(loc) = jobj(inst, "location")?
                && let Some(az) = jstr(loc, "availabilityZone")?
            {
                set(LIGHTSAIL_LABEL_AZ, az);
            }
            if let Some(v) = jstr(inst, "blueprintId")? {
                set(LIGHTSAIL_LABEL_BLUEPRINT_ID, v);
            }
            if let Some(v) = jstr(inst, "bundleId")? {
                set(LIGHTSAIL_LABEL_BUNDLE_ID, v);
            }
            if let Some(v) = jstr(inst, "name")? {
                set(LIGHTSAIL_LABEL_INSTANCE_NAME, v);
            }
            if let Some(st) = jobj(inst, "state")?
                && let Some(n) = jstr(st, "name")?
            {
                set(LIGHTSAIL_LABEL_INSTANCE_STATE, n);
            }
            if let Some(v) = jstr(inst, "supportCode")? {
                set(LIGHTSAIL_LABEL_INSTANCE_SUPPORT_CODE, v);
            }
            set("__address__", &join_host_port(private_ip, &self.cfg.port.to_string()));
            if let Some(v) = jstr(inst, "publicIpAddress")? {
                set(LIGHTSAIL_LABEL_PUBLIC_IP, v);
            }
            let ipv6 = jarr(inst, "ipv6Addresses")?;
            if !ipv6.is_empty() {
                // A null list entry is an empty string in Go.
                let addrs: Vec<&str> = ipv6.iter().map(|a| a.as_str().unwrap_or("")).collect();
                set(LIGHTSAIL_LABEL_IPV6_ADDRESSES, &format!("{LIGHTSAIL_LABEL_SEPARATOR}{}{LIGHTSAIL_LABEL_SEPARATOR}", addrs.join(LIGHTSAIL_LABEL_SEPARATOR)));
            }
            for t in jarr(inst, "tags")? {
                if t.is_null() {
                    continue;
                }
                let (Some(k), Some(v)) = (jstr(t, "key")?, jstr(t, "value")?) else { continue };
                set(&format!("{LIGHTSAIL_LABEL_TAG}{}", sanitize_label_name(k)), v);
            }
            targets.push(labels);
        }
        Ok(targets)
    }
}

impl Refresher for LightsailRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        LightsailRefresher::refresh(self, ctx)
    }
}

/// Go `LightsailDiscovery`.
pub struct LightsailDiscovery {
    inner: refresh::Discovery<LightsailRefresher>,
}

impl LightsailDiscovery {
    /// Go `NewLightsailDiscovery`.
    pub fn new(conf: &LightsailSdConfig, opts: DiscovererOptions) -> Result<LightsailDiscovery, String> {
        let m = crate::downcast_metrics::<LightsailMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = LightsailRefresher { cfg: conf.clone(), client: None, region: String::new() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "lightsail",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(LightsailDiscovery { inner })
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for LightsailDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
