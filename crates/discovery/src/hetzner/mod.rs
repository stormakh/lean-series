//! Port of `discovery/hetzner`: Hetzner Cloud servers (role `hcloud`, through the port of the hcloud-go client
//! subset in `hcloud.rs`) and Hetzner Robot dedicated servers (role `robot`), refreshed at an interval.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled (it ends at its
//! timeout, the refresh interval); the hcloud retry backoff does stop on cancel, as in Go.
use std::sync::Arc;

use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{
    Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator,
    SdConfigType,
};

mod hcloud;
mod robot;

pub use hcloud::{HcloudDiscovery, new_hcloud_discovery};
pub use robot::{RobotDiscovery, new_robot_discovery};

pub(crate) const HETZNER_LABEL_ROLE: &str = "__meta_hetzner_role";
pub(crate) const HETZNER_LABEL_SERVER_ID: &str = "__meta_hetzner_server_id";
pub(crate) const HETZNER_LABEL_SERVER_NAME: &str = "__meta_hetzner_server_name";
pub(crate) const HETZNER_LABEL_SERVER_STATUS: &str = "__meta_hetzner_server_status";
/// Label name kept for backward compatibility.
pub(crate) const HETZNER_LABEL_DATACENTER: &str = "__meta_hetzner_datacenter";
pub(crate) const HETZNER_LABEL_PUBLIC_IPV4: &str = "__meta_hetzner_public_ipv4";
pub(crate) const HETZNER_LABEL_PUBLIC_IPV6_NETWORK: &str = "__meta_hetzner_public_ipv6_network";

/// Go `hcloud.Endpoint`.
pub const HCLOUD_ENDPOINT: &str = "https://api.hetzner.cloud/v1";
/// The default Robot endpoint.
pub const ROBOT_ENDPOINT: &str = "https://robot-ws.your-server.de";

/// Go `Role`: "robot" or "hcloud".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Role(pub String);

/// Go `HetznerRoleRobot`.
pub const HETZNER_ROLE_ROBOT: &str = "robot";
/// Go `HetznerRoleHcloud`.
pub const HETZNER_ROLE_HCLOUD: &str = "hcloud";

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = s;
            match self.0.as_str() {
                HETZNER_ROLE_ROBOT | HETZNER_ROLE_HCLOUD => Ok(()),
                _ => Err(YError::Msg(format!(
                    "unknown role {}",
                    yaml::go_quote(&self.0)
                ))),
            }
        })
    }
    fn go_type() -> String {
        "hetzner.Role".to_string()
    }
}
impl yaml::Marshal for Role {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        self.0.marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `hetzner.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub role: Role,
    pub label_selector: String,
    /// For tests only (Go `hcloudEndpoint`).
    pub hcloud_endpoint: String,
    /// For tests only (Go `robotEndpoint`).
    pub robot_endpoint: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        port: 80,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "hetzner.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
    (role: "role")
    (label_selector: "label_selector", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.role.0.is_empty() {
                return Err(YError::Msg("role missing (one of: robot, hcloud)".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "hetzner.plain".to_string()
    }
}
impl yaml::Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "hetzner.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "hetzner"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(
        &self,
        _reg: &Arc<Registry>,
        rmi: Arc<dyn RefreshMetricsInstantiator>,
    ) -> Arc<dyn DiscovererMetrics> {
        Arc::new(HetznerMetrics {
            refresh_metrics: rmi,
        })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `hetznerMetrics`: only the refresh metrics.
pub struct HetznerMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for HetznerMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresher of the configured role (Go `refresher`).
pub enum HetznerRefresher {
    Hcloud(HcloudDiscovery),
    Robot(RobotDiscovery),
}

impl Refresher for HetznerRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        match self {
            HetznerRefresher::Hcloud(d) => d.refresh(ctx),
            HetznerRefresher::Robot(d) => d.refresh(ctx),
        }
    }
}

/// Go `newRefresher`.
pub fn new_refresher(conf: &mut SdConfig) -> Result<HetznerRefresher, String> {
    match conf.role.0.as_str() {
        HETZNER_ROLE_HCLOUD => {
            if conf.hcloud_endpoint.is_empty() {
                conf.hcloud_endpoint = HCLOUD_ENDPOINT.to_string();
            }
            Ok(HetznerRefresher::Hcloud(new_hcloud_discovery(conf)?))
        }
        HETZNER_ROLE_ROBOT => {
            if conf.robot_endpoint.is_empty() {
                conf.robot_endpoint = ROBOT_ENDPOINT.to_string();
            }
            Ok(HetznerRefresher::Robot(new_robot_discovery(conf)?))
        }
        _ => Err("unknown Hetzner discovery role".to_string()),
    }
}

/// Go `hetzner.Discovery` (a `refresh.Discovery`).
pub struct Discovery {
    inner: refresh::Discovery<HetznerRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<HetznerMetrics>(&opts.metrics)
            .ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut conf = conf.clone();
        let r = new_refresher(&mut conf)?;
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "hetzner",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: r,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// The HTTP client of both roles: the HTTP client config, with the refresh interval as timeout.
pub(crate) fn http_client(conf: &SdConfig) -> Result<net::Client, String> {
    let mut c =
        net::new_client_from_config(&conf.http_client_config, &net::HttpClientOptions::default())
            .map_err(|e| e.to_string())?;
    // Go's Client.Timeout: zero means none.
    let t = conf.refresh_interval.to_std();
    c.timeout = (!t.is_zero()).then_some(t);
    Ok(c)
}
