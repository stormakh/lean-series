//! Port of `discovery/scaleway`: Scaleway Instances (role `instance`, with IPAM lookups of private NIC
//! addresses) and Elastic Metal servers (role `baremetal`), with the subset of scaleway-sdk-go v1.39.0 in
//! `scw.rs`.
//!
//! Differs from Go: a null item in a list (servers, IPs) is skipped (Go keeps a nil pointer and the SD code
//! panics on it); a request in flight is not aborted on cancel. `ResourceExpiredError` prints its time in a
//! simplified Go `time.Time.String()` form.
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

mod baremetal;
mod instance;
mod scw;

pub use baremetal::BaremetalDiscovery;
pub use instance::InstanceDiscovery;

pub(crate) const SEPARATOR: &str = ",";

/// Go `roleBaremetal`.
pub const ROLE_BAREMETAL: &str = "baremetal";
/// Go `roleInstance`.
pub const ROLE_INSTANCE: &str = "instance";

/// Go `role`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Role(pub String);

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = s;
            match self.0.as_str() {
                ROLE_INSTANCE | ROLE_BAREMETAL => Ok(()),
                _ => Err(YError::Msg(format!(
                    "unknown role {}",
                    yaml::go_quote(&self.0)
                ))),
            }
        })
    }
    fn go_type() -> String {
        "scaleway.role".to_string()
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

/// Go `scaleway.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    /// The Scaleway Project ID used to filter discovery on.
    pub project: String,
    /// URL of the Scaleway API to use.
    pub api_url: String,
    /// The zone of the scrape targets.
    pub zone: String,
    pub access_key: String,
    pub secret_key: yaml::Secret,
    pub secret_key_file: String,
    pub name_filter: String,
    pub tags_filter: Vec<String>,
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub role: Role,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        port: 80,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: HttpClientConfig::default_config(),
        zone: "fr-par-1".to_string(),
        api_url: "https://api.scaleway.com".to_string(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "scaleway.plain", [
    (project: "project_id")
    (api_url: "api_url", omitempty)
    (zone: "zone")
    (access_key: "access_key")
    (secret_key: "secret_key")
    (secret_key_file: "secret_key_file")
    (name_filter: "name_filter", omitempty)
    (tags_filter: "tags_filter", omitempty)
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
    (role: "role")
]);

impl SdConfig {
    /// Go `secretKeyForConfig`: a UUID-looking placeholder when the secret comes from a file.
    fn secret_key_for_config(&self) -> String {
        if !self.secret_key_file.is_empty() {
            return "00000000-0000-0000-0000-000000000000".to_string();
        }
        self.secret_key.as_str().to_string()
    }
}

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.role.0.is_empty() {
                return Err(YError::Msg(
                    "role missing (one of: instance, baremetal)".into(),
                ));
            }
            if self.project.is_empty() {
                return Err(YError::Msg("project_id is mandatory".into()));
            }
            if self.secret_key.is_empty() && self.secret_key_file.is_empty() {
                return Err(YError::Msg(
                    "one of secret_key & secret_key_file must be configured".into(),
                ));
            }
            if !self.secret_key.is_empty() && !self.secret_key_file.is_empty() {
                return Err(YError::Msg(
                    "at most one of secret_key & secret_key_file must be configured".into(),
                ));
            }
            if self.access_key.is_empty() {
                return Err(YError::Msg("access_key is mandatory".into()));
            }
            // scw.NewClient(scw.WithProfile(loadProfile(c)))
            scw::validate_settings(
                &self.access_key,
                &self.secret_key_for_config(),
                &self.project,
                &self.zone,
                "",
                &self.api_url,
            )
            .map_err(YError::Msg)?;
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "scaleway.plain".to_string()
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
    const GO_TYPE: &'static str = "scaleway.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "scaleway"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(
        &self,
        _reg: &Arc<Registry>,
        rmi: Arc<dyn RefreshMetricsInstantiator>,
    ) -> Arc<dyn DiscovererMetrics> {
        Arc::new(ScalewayMetrics {
            refresh_metrics: rmi,
        })
    }
    fn set_directory(&mut self, dir: &str) {
        self.secret_key_file = net::config::join_dir(dir, &self.secret_key_file);
        self.http_client_config.set_directory(dir);
    }
}

/// Go `scalewayMetrics`: only the refresh metrics.
pub struct ScalewayMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for ScalewayMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `scaleway.Discovery` (a `refresh.Discovery`).
pub struct Discovery {
    inner: refresh::Discovery<ScalewayRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<ScalewayMetrics>(&opts.metrics)
            .ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let r = new_refresher(conf)?;
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "scaleway",
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

/// The refresher of the configured role (Go `refresher`).
pub enum ScalewayRefresher {
    Instance(InstanceDiscovery),
    Baremetal(BaremetalDiscovery),
}

impl Refresher for ScalewayRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        match self {
            ScalewayRefresher::Instance(d) => d.refresh(ctx),
            ScalewayRefresher::Baremetal(d) => d.refresh(ctx),
        }
    }
}

/// Go `newRefresher`.
pub fn new_refresher(conf: &SdConfig) -> Result<ScalewayRefresher, String> {
    match conf.role.0.as_str() {
        ROLE_BAREMETAL => Ok(ScalewayRefresher::Baremetal(
            baremetal::new_baremetal_discovery(conf)?,
        )),
        ROLE_INSTANCE => Ok(ScalewayRefresher::Instance(
            instance::new_instance_discovery(conf)?,
        )),
        _ => Err("unknown Scaleway discovery role".to_string()),
    }
}

/// Go `newScalewayHTTPClient` + `loadProfile` + `scw.NewClient` (with the Prometheus user agent).
pub(crate) fn new_client(conf: &SdConfig) -> Result<scw::Client, String> {
    let mut http =
        net::new_client_from_config(&conf.http_client_config, &net::HttpClientOptions::default())
            .map_err(|e| e.to_string())?;
    let t = conf.refresh_interval.to_std();
    http.timeout = (!t.is_zero()).then_some(t);
    let mut token_file = None;
    if !conf.secret_key_file.is_empty() {
        // fail fast if the file cannot be read
        net::types::read_file(&conf.secret_key_file).map_err(|e| {
            format!(
                "unable to read auth token file {}: {e}",
                conf.secret_key_file
            )
        })?;
        token_file = Some(conf.secret_key_file.clone());
    }
    let secret = conf.secret_key_for_config();
    let user_agent = format!("{} {}", scw::default_user_agent(), crate::http::USER_AGENT);
    scw::validate_settings(
        &conf.access_key,
        &secret,
        &conf.project,
        &conf.zone,
        &user_agent,
        &conf.api_url,
    )
    .map_err(|e| format!("error setting up scaleway client: {e}"))?;
    Ok(scw::Client::new(
        conf.api_url.clone(),
        secret,
        user_agent,
        conf.zone.clone(),
        http,
        token_file,
    ))
}
