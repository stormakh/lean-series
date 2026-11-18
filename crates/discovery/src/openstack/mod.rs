//! Port of `discovery/openstack`: hypervisors (Nova), instances (Nova + Neutron floating IPs) and load balancers
//! (Octavia + Neutron) of an OpenStack region, refreshed at an interval.
//!
//! The gophercloud client (Keystone v3 auth, service catalog, version discovery, paging) is ported in
//! `client.rs` over plain blocking HTTP + JSON. Differences from Go:
//! - Keystone identity v2.0 is not supported (only v3); an endpoint that only offers v2.0 fails to authenticate.
//! - A request in flight is not aborted when the discoverer is cancelled; it ends at its timeout (the refresh
//!   interval, as Go's client timeout). A cancelled context is checked before each request.
//! - Response fields that Prometheus does not read are not type-checked; Go's JSON type errors for the fields it
//!   reads have the same meaning but not always the same text.
use std::sync::Arc;

use net::{HttpClientConfig, TlsConfig};
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod client;
pub mod hypervisor;
pub mod instance;
pub mod loadbalancer;

pub use client::{AuthOptions, auth_options_from_env};

/// `__meta_openstack_`.
pub const LABEL_PREFIX: &str = "__meta_openstack_";
pub const LABEL_PROJECT_ID: &str = "__meta_openstack_project_id";

/// Go `openstack.Role`: "hypervisor", "instance" or "loadbalancer".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Role(pub String);

pub const ROLE_HYPERVISOR: &str = "hypervisor";
pub const ROLE_INSTANCE: &str = "instance";
pub const ROLE_LOAD_BALANCER: &str = "loadbalancer";

impl Role {
    pub fn new(s: &str) -> Role {
        Role(s.to_string())
    }
}

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = s;
            match self.0.as_str() {
                ROLE_HYPERVISOR | ROLE_INSTANCE | ROLE_LOAD_BALANCER => Ok(()),
                _ => Err(YError::Msg(format!("unknown OpenStack SD role {}", yaml::go_quote(&self.0)))),
            }
        })
    }
    fn go_type() -> String {
        "openstack.Role".to_string()
    }
}
impl yaml::Marshal for Role {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        Ok(yaml::Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `openstack.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub identity_endpoint: String,
    pub username: String,
    pub user_id: String,
    pub password: yaml::Secret,
    pub project_name: String,
    pub project_id: String,
    pub domain_name: String,
    pub domain_id: String,
    pub application_credential_name: String,
    pub application_credential_id: String,
    pub application_credential_secret: yaml::Secret,
    pub role: Role,
    pub region: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub all_tenants: bool,
    pub tls_config: TlsConfig,
    pub availability: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), availability: "public".to_string(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "openstack.plain", [
    (identity_endpoint: "identity_endpoint")
    (username: "username")
    (user_id: "userid")
    (password: "password")
    (project_name: "project_name")
    (project_id: "project_id")
    (domain_name: "domain_name")
    (domain_id: "domain_id")
    (application_credential_name: "application_credential_name")
    (application_credential_id: "application_credential_id")
    (application_credential_secret: "application_credential_secret")
    (role: "role")
    (region: "region")
    (refresh_interval: "refresh_interval")
    (port: "port")
    (all_tenants: "all_tenants", omitempty)
    (tls_config: "tls_config", omitempty)
    (availability: "availability", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            match self.availability.as_str() {
                "public" | "internal" | "admin" => {}
                _ => return Err(YError::Msg(format!("unknown availability {}, must be one of admin, internal or public", self.availability))),
            }
            if self.role.0.is_empty() {
                return Err(YError::Msg("role missing (one of: instance, hypervisor, loadbalancer)".into()));
            }
            if self.region.is_empty() {
                return Err(YError::Msg("openstack SD configuration requires a region".into()));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "openstack.plain".to_string()
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
    const GO_TYPE: &'static str = "openstack.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "openstack"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(OpenstackMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.tls_config.set_directory(dir);
    }
}

/// Go `openstackMetrics`.
pub struct OpenstackMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for OpenstackMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresher of one role (Go `refresher`).
pub struct RoleRefresher(Box<dyn Refresher>);

impl Refresher for RoleRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.0.refresh(ctx)
    }
}

/// The OpenStack discoverer (Go `refresh.Discovery` built by `NewDiscovery`).
pub struct Discovery {
    inner: refresh::Discovery<RoleRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<OpenstackMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let r = new_refresher(conf, opts.logger.clone())?;
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "openstack",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: RoleRefresher(r),
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

/// Go `newRefresher`: the auth options (from the config, or from the `OS_*` environment when
/// `identity_endpoint` is empty), the provider client and the refresher of the role.
pub fn new_refresher(conf: &SdConfig, logger: Logger) -> Result<Box<dyn Refresher>, String> {
    let opts = if conf.identity_endpoint.is_empty() {
        auth_options_from_env()?
    } else {
        AuthOptions {
            identity_endpoint: conf.identity_endpoint.clone(),
            username: conf.username.clone(),
            user_id: conf.user_id.clone(),
            password: conf.password.0.clone(),
            tenant_name: conf.project_name.clone(),
            tenant_id: conf.project_id.clone(),
            domain_name: conf.domain_name.clone(),
            domain_id: conf.domain_id.clone(),
            application_credential_id: conf.application_credential_id.clone(),
            application_credential_name: conf.application_credential_name.clone(),
            application_credential_secret: conf.application_credential_secret.0.clone(),
            ..Default::default()
        }
    };
    // Go: an http.Transport with only the TLS config (no proxy from the environment), timeout = refresh interval.
    let hc = HttpClientConfig { tls_config: conf.tls_config.clone(), ..HttpClientConfig::default_config() };
    let interval = conf.refresh_interval.to_std();
    let http_opts = net::HttpClientOptions::default().with_idle_conn_timeout(2 * interval);
    let mut http = net::new_client_from_config(&hc, &http_opts).map_err(|e| e.to_string())?;
    http.timeout = if interval.is_zero() { None } else { Some(interval) };
    let provider = client::ProviderClient::new(&opts.identity_endpoint, http)?;
    let availability = conf.availability.clone();
    let region = conf.region.clone();
    match conf.role.0.as_str() {
        ROLE_HYPERVISOR => Ok(Box::new(hypervisor::HypervisorDiscovery::new(provider, opts, conf.port, region, availability, logger))),
        ROLE_INSTANCE => Ok(Box::new(instance::InstanceDiscovery::new(provider, opts, conf.port, region, conf.all_tenants, availability, logger))),
        ROLE_LOAD_BALANCER => Ok(Box::new(loadbalancer::LoadBalancerDiscovery::new(provider, opts, region, availability, logger))),
        _ => Err("unknown OpenStack discovery role".to_string()),
    }
}
