//! Port of `discovery/outscale`: VMs of an OUTSCALE account (OAPI `ReadVms`), refreshed at an interval. The
//! subset of github.com/outscale/osc-sdk-go/v2 (v2.35.0) that it uses is in [`vm`].
use std::sync::Arc;

use model::labelset::META_LABEL_PREFIX;
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod vm;

pub use vm::{Placement, ResourceTag, Vm, VmDiscovery, new_vm_discovery, vms_to_label_sets};

/// Go `metaLabelPrefix`.
pub fn meta_label_prefix() -> String {
    format!("{META_LABEL_PREFIX}outscale_")
}

/// Go `outscale.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: Secret,
    pub secret_key_file: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config(), region: "eu-west-2".to_string(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "outscale.plain", [
    (endpoint: "endpoint", omitempty)
    (region: "region")
    (access_key: "access_key")
    (secret_key: "secret_key")
    (secret_key_file: "secret_key_file")
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port")
    (inline http_client_config)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.region.is_empty() {
                return Err(YError::Msg("outscale SD configuration requires a region".into()));
            }
            if self.access_key.is_empty() {
                return Err(YError::Msg("outscale SD configuration requires access_key".into()));
            }
            if self.secret_key.0.is_empty() && self.secret_key_file.is_empty() {
                return Err(YError::Msg("one of secret_key & secret_key_file must be configured".into()));
            }
            if !self.secret_key.0.is_empty() && !self.secret_key_file.is_empty() {
                return Err(YError::Msg("at most one of secret_key & secret_key_file must be configured".into()));
            }
            if self.endpoint.is_empty() {
                self.endpoint = format!("https://api.{}.outscale.com/api/v1", self.region);
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "outscale.plain".to_string()
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
    const GO_TYPE: &'static str = "outscale.SDConfig";
}

impl SdConfig {
    /// Go `SecretKeyValue`.
    pub fn secret_key_value(&self) -> Result<Secret, String> {
        if self.secret_key_file.is_empty() {
            return Ok(self.secret_key.clone());
        }
        let b = net::types::read_file(&self.secret_key_file).map_err(|e| format!("unable to read outscale secret key file {}: {}", self.secret_key_file, e))?;
        Ok(Secret(String::from_utf8_lossy(&b).trim().to_string()))
    }
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "outscale"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_discovery(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(OutscaleMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.secret_key_file = net::config::join_dir(dir, &self.secret_key_file);
        self.http_client_config.set_directory(dir);
    }
}

/// Go `outscaleMetrics`.
pub struct OutscaleMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for OutscaleMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The Outscale discoverer (Go `refresh.Discovery` from `NewDiscovery`).
pub struct Discovery {
    inner: refresh::Discovery<VmDiscovery>,
}

/// Go `NewDiscovery`.
pub fn new_discovery(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
    let m = crate::downcast_metrics::<OutscaleMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
    let d = new_vm_discovery(conf)?;
    let inner = refresh::Discovery::new(refresh::Options {
        logger: opts.logger,
        mech: "outscale",
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
