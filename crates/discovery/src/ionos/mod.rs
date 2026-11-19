//! Port of `discovery/ionos`: servers of an IONOS Cloud data center, refreshed at an interval. The subset of
//! github.com/ionos-cloud/sdk-go/v6 (v6.3.11) that it uses is in [`server`].
use std::sync::Arc;

use model::labelset::META_LABEL_PREFIX;
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod server;

pub use server::{ServerDiscovery, new_server_discovery};

/// Go `metaLabelPrefix`.
pub fn meta_label_prefix() -> String {
    format!("{META_LABEL_PREFIX}ionos_")
}

/// Go `metaLabelSeparator`.
pub const META_LABEL_SEPARATOR: &str = ",";

/// Go `ionos.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    /// IONOS Cloud data center ID used to discover targets.
    pub datacenter_id: String,
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    /// For tests only (Go `ionosEndpoint`, not in YAML).
    pub ionos_endpoint: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { http_client_config: HttpClientConfig::default_config(), refresh_interval: yaml::Duration::from_secs(60), port: 80, ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "ionos.plain", [
    (datacenter_id: "datacenter_id")
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.datacenter_id.is_empty() {
                return Err(YError::Msg("datacenter id can't be empty".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "ionos.plain".to_string()
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
    const GO_TYPE: &'static str = "ionos.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "ionos"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_discovery(&mut self.clone(), opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(IonosMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `ionosMetrics`.
pub struct IonosMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for IonosMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The IONOS discoverer (Go `refresh.Discovery` from `NewDiscovery`).
pub struct Discovery {
    inner: refresh::Discovery<ServerDiscovery>,
}

/// Go `NewDiscovery`.
pub fn new_discovery(conf: &mut SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
    let m = crate::downcast_metrics::<IonosMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
    if conf.ionos_endpoint.is_empty() {
        conf.ionos_endpoint = "https://api.ionos.com".to_string();
    }
    let d = new_server_discovery(conf, opts.logger.clone())?;
    let inner = refresh::Discovery::new(refresh::Options {
        logger: opts.logger,
        mech: "ionos",
        set_name: &opts.set_name,
        interval: conf.refresh_interval.to_std(),
        refresh_f: d,
        metrics_instantiator: m.refresh_metrics.as_ref(),
    });
    Ok(Discovery { inner })
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
