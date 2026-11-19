//! Port of `discovery/eureka`: targets from the application instances of a Eureka server, refreshed at an
//! interval.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled (as `http`).
pub mod client;
pub mod xml;

use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

pub use client::{Application, Applications, DataCenterInfo, Instance, MetaData, Port, Tag, fetch_apps};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// Go `model.InstanceLabel`.
const INSTANCE_LABEL: &str = "instance";

pub const META_LABEL_PREFIX: &str = "__meta_eureka_";
pub const META_APP_INSTANCE_LABEL_PREFIX: &str = "__meta_eureka_app_instance_";
pub const APP_NAME_LABEL: &str = "__meta_eureka_app_name";
pub const APP_INSTANCE_HOST_NAME_LABEL: &str = "__meta_eureka_app_instance_hostname";
pub const APP_INSTANCE_HOME_PAGE_URL_LABEL: &str = "__meta_eureka_app_instance_homepage_url";
pub const APP_INSTANCE_STATUS_PAGE_URL_LABEL: &str = "__meta_eureka_app_instance_statuspage_url";
pub const APP_INSTANCE_HEALTH_CHECK_URL_LABEL: &str = "__meta_eureka_app_instance_healthcheck_url";
pub const APP_INSTANCE_IP_ADDR_LABEL: &str = "__meta_eureka_app_instance_ip_addr";
pub const APP_INSTANCE_VIP_ADDRESS_LABEL: &str = "__meta_eureka_app_instance_vip_address";
pub const APP_INSTANCE_SECURE_VIP_ADDRESS_LABEL: &str = "__meta_eureka_app_instance_secure_vip_address";
pub const APP_INSTANCE_STATUS_LABEL: &str = "__meta_eureka_app_instance_status";
pub const APP_INSTANCE_PORT_LABEL: &str = "__meta_eureka_app_instance_port";
pub const APP_INSTANCE_PORT_ENABLED_LABEL: &str = "__meta_eureka_app_instance_port_enabled";
pub const APP_INSTANCE_SECURE_PORT_LABEL: &str = "__meta_eureka_app_instance_secure_port";
pub const APP_INSTANCE_SECURE_PORT_ENABLED_LABEL: &str = "__meta_eureka_app_instance_secure_port_enabled";
pub const APP_INSTANCE_DATA_CENTER_INFO_NAME_LABEL: &str = "__meta_eureka_app_instance_datacenterinfo_name";
pub const APP_INSTANCE_DATA_CENTER_INFO_METADATA_PREFIX: &str = "__meta_eureka_app_instance_datacenterinfo_metadata_";
pub const APP_INSTANCE_COUNTRY_ID_LABEL: &str = "__meta_eureka_app_instance_country_id";
pub const APP_INSTANCE_ID_LABEL: &str = "__meta_eureka_app_instance_id";
pub const APP_INSTANCE_METADATA_PREFIX: &str = "__meta_eureka_app_instance_metadata_";

/// Go `eureka.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub server: String,
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { server: String::new(), http_client_config: HttpClientConfig::default_config(), refresh_interval: yaml::Duration::from_secs(30) }
}

yaml::yaml_struct!(SdConfig, "eureka.plain", [
    (server: "server", omitempty)
    (inline http_client_config)
    (refresh_interval: "refresh_interval", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.server.is_empty() {
                return Err(YError::Msg("eureka_sd: empty or null eureka server".into()));
            }
            let u = yaml::url::parse(&self.server).map_err(YError::Msg)?;
            if u.scheme.is_empty() || u.host.is_empty() {
                return Err(YError::Msg("eureka_sd: invalid eureka server URL".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "eureka.plain".to_string()
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
    const GO_TYPE: &'static str = "eureka.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "eureka"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(EurekaMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `eurekaMetrics`.
pub struct EurekaMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for EurekaMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh function of the Eureka discoverer (Go `Discovery.refresh` and its state).
pub struct EurekaRefresher {
    client: net::Client,
    server: String,
}

impl Refresher for EurekaRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let apps = fetch_apps(ctx, &self.server, &self.client)?;
        let mut tg = Group { source: "eureka".to_string(), ..Default::default() };
        for app in &apps.applications {
            tg.targets.extend(targets_for_app(app));
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `targetsForApp`: one target per instance.
pub fn targets_for_app(app: &Application) -> Vec<LabelSet> {
    let mut targets = Vec::with_capacity(app.instances.len());
    for t in &app.instances {
        let target_address = match &t.port {
            Some(p) => join_host_port(&t.host_name, p.port),
            None => join_host_port(&t.host_name, "80"),
        };
        let country_id = t.country_id.to_string();
        let mut target = LabelSet::from_pairs(&[
            (ADDRESS_LABEL, &target_address),
            (INSTANCE_LABEL, &t.instance_id),
            (APP_NAME_LABEL, &app.name),
            (APP_INSTANCE_HOST_NAME_LABEL, &t.host_name),
            (APP_INSTANCE_HOME_PAGE_URL_LABEL, &t.home_page_url),
            (APP_INSTANCE_STATUS_PAGE_URL_LABEL, &t.status_page_url),
            (APP_INSTANCE_HEALTH_CHECK_URL_LABEL, &t.health_check_url),
            (APP_INSTANCE_IP_ADDR_LABEL, &t.ip_addr),
            (APP_INSTANCE_VIP_ADDRESS_LABEL, &t.vip_address),
            (APP_INSTANCE_SECURE_VIP_ADDRESS_LABEL, &t.secure_vip_address),
            (APP_INSTANCE_STATUS_LABEL, &t.status),
            (APP_INSTANCE_COUNTRY_ID_LABEL, &country_id),
            (APP_INSTANCE_ID_LABEL, &t.instance_id),
        ]);
        if let Some(p) = &t.port {
            target.insert(APP_INSTANCE_PORT_LABEL.to_string(), p.port.to_string());
            target.insert(APP_INSTANCE_PORT_ENABLED_LABEL.to_string(), p.enabled.to_string());
        }
        if let Some(p) = &t.secure_port {
            target.insert(APP_INSTANCE_SECURE_PORT_LABEL.to_string(), p.port.to_string());
            target.insert(APP_INSTANCE_SECURE_PORT_ENABLED_LABEL.to_string(), p.enabled.to_string());
        }
        if let Some(dc) = &t.data_center_info {
            target.insert(APP_INSTANCE_DATA_CENTER_INFO_NAME_LABEL.to_string(), dc.name.clone());
            if let Some(md) = &dc.metadata {
                for m in &md.items {
                    target.insert(format!("{APP_INSTANCE_DATA_CENTER_INFO_METADATA_PREFIX}{}", sanitize_label_name(&m.xml_name.local)), m.content.clone());
                }
            }
        }
        if let Some(md) = &t.metadata {
            for m in &md.items {
                target.insert(format!("{APP_INSTANCE_METADATA_PREFIX}{}", sanitize_label_name(&m.xml_name.local)), m.content.clone());
            }
        }
        targets.push(target);
    }
    targets
}

/// Go `eureka.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<EurekaRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<EurekaMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        let refresher = EurekaRefresher { client: net::Client::new(rt), server: conf.server.clone() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "eureka",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
