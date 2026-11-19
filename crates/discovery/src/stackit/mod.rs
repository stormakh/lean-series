//! Port of `discovery/stackit`: servers of a STACKIT project (IaaS API), refreshed at an interval. The
//! authentication of github.com/stackitcloud/stackit-sdk-go/core (v0.27.1) that it sets up is in [`auth`].
use std::sync::Arc;

use model::labelset::META_LABEL_PREFIX;
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod auth;
pub mod server;

pub use server::{IaasDiscovery, new_server_discovery};

pub fn label_prefix() -> String {
    format!("{META_LABEL_PREFIX}stackit_")
}

/// Go `version.PrometheusUserAgent()`.
pub const USER_AGENT: &str = crate::http::USER_AGENT;

/// Go `stackit.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub project: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub region: String,
    pub endpoint: String,
    pub service_account_key: Secret,
    pub private_key: Secret,
    pub service_account_key_path: String,
    pub private_key_path: String,
    pub credentials_file_path: String,
    /// For testing only (Go `tokenURL`, not in YAML).
    pub token_url: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { region: "eu01".to_string(), port: 80, refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "stackit.plain", [
    (inline http_client_config)
    (project: "project")
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port", omitempty)
    (region: "region", omitempty)
    (endpoint: "endpoint", omitempty)
    (service_account_key: "service_account_key", omitempty)
    (private_key: "private_key", omitempty)
    (service_account_key_path: "service_account_key_path", omitempty)
    (private_key_path: "private_key_path", omitempty)
    (credentials_file_path: "credentials_file_path", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.endpoint.is_empty() && self.region.is_empty() {
                return Err(YError::Msg("stackit_sd: endpoint and region missing".into()));
            }
            if let Err(e) = yaml::url::parse(&self.endpoint) {
                return Err(YError::Msg(format!("stackit_sd: invalid endpoint {}: {}", yaml::go_quote(&self.endpoint), e)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "stackit.plain".to_string()
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
    const GO_TYPE: &'static str = "stackit.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "stackit"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_discovery(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(StackitMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `stackitMetrics`.
pub struct StackitMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for StackitMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The STACKIT discoverer (Go `refresh.Discovery` from `NewDiscovery`).
pub struct Discovery {
    inner: refresh::Discovery<IaasDiscovery>,
}

/// Go `NewDiscovery`.
pub fn new_discovery(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
    let m = crate::downcast_metrics::<StackitMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
    let r = new_refresher(conf, opts.logger.clone())?;
    let inner = refresh::Discovery::new(refresh::Options {
        logger: opts.logger,
        mech: "stackit",
        set_name: &opts.set_name,
        interval: conf.refresh_interval.to_std(),
        refresh_f: r,
        metrics_instantiator: m.refresh_metrics.as_ref(),
    });
    Ok(Discovery { inner })
}

/// Go `newRefresher`.
pub fn new_refresher(conf: &SdConfig, l: Logger) -> Result<IaasDiscovery, String> {
    new_server_discovery(conf, l)
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
