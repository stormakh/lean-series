//! Port of `discovery/uyuni`: monitoring endpoints of Uyuni (SUSE Manager) systems through the XML-RPC API,
//! refreshed at an interval. The login token is reused for half its lifetime.
//!
//! Differs from Go: the system IDs sent to the API keep the order of the system groups response (Go: random
//! map order).
pub mod xmlrpc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};
use xmlrpc::{Arg, Schema, Value};

const UYUNI_XMLRPC_API_PATH: &str = "/rpc/api";
const UYUNI_META_LABEL_PREFIX: &str = "__meta_uyuni_";
const TOKEN_DURATION: Duration = Duration::from_secs(10 * 60);

fn uyuni_label(name: &str) -> String {
    format!("{UYUNI_META_LABEL_PREFIX}{name}")
}

/// Go `uyuni.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub server: String,
    pub username: String,
    pub password: Secret,
    pub http_client_config: HttpClientConfig,
    pub entitlement: String,
    pub separator: String,
    pub refresh_interval: yaml::Duration,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        entitlement: "monitoring_entitled".to_string(),
        separator: ",".to_string(),
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "uyuni.plain", [
    (server: "server")
    (username: "username")
    (password: "password")
    (inline http_client_config)
    (entitlement: "entitlement", omitempty)
    (separator: "separator", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.server.is_empty() {
                return Err(YError::Msg("Uyuni SD configuration requires server host".into()));
            }
            if let Err(e) = yaml::url::parse(&self.server) {
                return Err(YError::Msg(format!("Uyuni Server URL is not valid: {e}")));
            }
            if self.username.is_empty() {
                return Err(YError::Msg("Uyuni SD configuration requires a username".into()));
            }
            if self.password.is_empty() {
                return Err(YError::Msg("Uyuni SD configuration requires a password".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "uyuni.plain".to_string()
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
    const GO_TYPE: &'static str = "uyuni.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "uyuni"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(UyuniMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `uyuniMetrics`.
pub struct UyuniMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for UyuniMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// The XML-RPC response types.

/// Go `systemGroupID`.
const SYSTEM_GROUP_ID: Schema = Schema::Struct(&[("id", Schema::Int), ("name", Schema::Str)]);
/// The anonymous struct of `getSystemGroupsInfoOfMonitoredClients`.
const SYSTEM_GROUPS_INFOS: Schema = Schema::Slice(&Schema::Struct(&[("id", Schema::Int), ("system_groups", Schema::Slice(&SYSTEM_GROUP_ID))]));
/// Go `[]networkInfo`.
const NETWORK_INFOS: Schema = Schema::Slice(&Schema::Struct(&[("system_id", Schema::Int), ("hostname", Schema::Str), ("primary_fqdn", Schema::Str), ("ip", Schema::Str)]));
/// Go `[]endpointInfo`.
const ENDPOINT_INFOS: Schema = Schema::Slice(&Schema::Struct(&[
    ("system_id", Schema::Int),
    ("endpoint_name", Schema::Str),
    ("port", Schema::Int),
    ("path", Schema::Str),
    ("module", Schema::Str),
    ("exporter_name", Schema::Str),
    ("tls_enabled", Schema::Bool),
]));

/// Go `systemGroupID`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SystemGroupId {
    pub group_id: i64,
    pub group_name: String,
}

/// Go `networkInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NetworkInfo {
    pub system_id: i64,
    pub hostname: String,
    pub primary_fqdn: String,
    pub ip: String,
}

/// Go `endpointInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointInfo {
    pub system_id: i64,
    pub endpoint_name: String,
    pub port: i64,
    pub path: String,
    pub module: String,
    pub exporter_name: String,
    pub tls_enabled: bool,
}

/// Go `login`.
fn login(c: &mut xmlrpc::Client, user: &str, pass: &str, duration: i64) -> Result<String, String> {
    Ok(c.call("auth.login", &[Arg::Str(user), Arg::Str(pass), Arg::Int(duration)], &Schema::Str)?.str().to_string())
}

/// Go `getSystemGroupsInfoOfMonitoredClients`: the groups by system ID, in response order.
fn get_system_groups_info_of_monitored_clients(c: &mut xmlrpc::Client, token: &str, entitlement: &str) -> Result<Vec<(i64, Vec<SystemGroupId>)>, String> {
    let v = c.call("system.listSystemGroupsForSystemsWithEntitlement", &[Arg::Str(token), Arg::Str(entitlement)], &SYSTEM_GROUPS_INFOS)?;
    let mut result: Vec<(i64, Vec<SystemGroupId>)> = Vec::new();
    for info in v.items() {
        let id = info.field(0).int();
        let groups: Vec<SystemGroupId> = info.field(1).items().iter().map(|g| SystemGroupId { group_id: g.field(0).int(), group_name: g.field(1).str().to_string() }).collect();
        match result.iter_mut().find(|(i, _)| *i == id) {
            Some(e) => e.1 = groups,
            None => result.push((id, groups)),
        }
    }
    Ok(result)
}

/// Go `getNetworkInformationForSystems`.
fn get_network_information_for_systems(c: &mut xmlrpc::Client, token: &str, system_ids: &[i64]) -> Result<Vec<NetworkInfo>, String> {
    let v = c.call("system.getNetworkForSystems", &[Arg::Str(token), Arg::Ints(system_ids)], &NETWORK_INFOS)?;
    Ok(v.items()
        .iter()
        .map(|n| NetworkInfo { system_id: n.field(0).int(), hostname: n.field(1).str().to_string(), primary_fqdn: n.field(2).str().to_string(), ip: n.field(3).str().to_string() })
        .collect())
}

/// Go `getEndpointInfoForSystems`.
fn get_endpoint_info_for_systems(c: &mut xmlrpc::Client, token: &str, system_ids: &[i64]) -> Result<Vec<EndpointInfo>, String> {
    let v = c.call("system.monitoring.listEndpoints", &[Arg::Str(token), Arg::Ints(system_ids)], &ENDPOINT_INFOS)?;
    Ok(v.items()
        .iter()
        .map(|e: &Value| EndpointInfo {
            system_id: e.field(0).int(),
            endpoint_name: e.field(1).str().to_string(),
            port: e.field(2).int(),
            path: e.field(3).str().to_string(),
            module: e.field(4).str().to_string(),
            exporter_name: e.field(5).str().to_string(),
            tls_enabled: e.field(6).bool(),
        })
        .collect())
}

/// The refresh function of the Uyuni discoverer (Go `Discovery.refresh` and its state).
pub struct UyuniRefresher {
    api_url: String,
    round_tripper: Arc<dyn net::RoundTripper>,
    username: String,
    password: String,
    token: String,
    /// None is Go's zero time (always expired).
    token_expiration: Option<Instant>,
    entitlement: String,
    separator: String,
}

impl UyuniRefresher {
    /// Go `getEndpointLabels`.
    fn get_endpoint_labels(&self, endpoint: &EndpointInfo, system_group_ids: &[SystemGroupId], network_info: &NetworkInfo) -> LabelSet {
        let managed_group_names: Vec<&str> = system_group_ids.iter().map(|g| g.group_name.as_str()).collect();
        let addr = format!("{}:{}", network_info.hostname, endpoint.port);
        let scheme = if endpoint.tls_enabled { "https" } else { "http" };
        [
            (ADDRESS_LABEL.to_string(), addr),
            (uyuni_label("minion_hostname"), network_info.hostname.clone()),
            (uyuni_label("primary_fqdn"), network_info.primary_fqdn.clone()),
            (uyuni_label("system_id"), endpoint.system_id.to_string()),
            (uyuni_label("groups"), managed_group_names.join(&self.separator)),
            (uyuni_label("endpoint_name"), endpoint.endpoint_name.clone()),
            (uyuni_label("exporter"), endpoint.exporter_name.clone()),
            (uyuni_label("proxy_module"), endpoint.module.clone()),
            (uyuni_label("metrics_path"), endpoint.path.clone()),
            (uyuni_label("scheme"), scheme.to_string()),
        ]
        .into_iter()
        .collect()
    }

    /// Go `getTargetsForSystems`.
    fn get_targets_for_systems(&self, c: &mut xmlrpc::Client, entitlement: &str) -> Result<Vec<LabelSet>, String> {
        let groups_by_system =
            get_system_groups_info_of_monitored_clients(c, &self.token, entitlement).map_err(|e| format!("unable to get the managed system groups information of monitored clients: {e}"))?;
        let system_ids: Vec<i64> = groups_by_system.iter().map(|(id, _)| *id).collect();
        let endpoint_infos = get_endpoint_info_for_systems(c, &self.token, &system_ids).map_err(|e| format!("unable to get endpoints information: {e}"))?;
        let network_infos = get_network_information_for_systems(c, &self.token, &system_ids).map_err(|e| format!("unable to get the systems network information: {e}"))?;
        let empty_net = NetworkInfo::default();
        let mut result = Vec::new();
        for endpoint in &endpoint_infos {
            let groups = groups_by_system.iter().find(|(id, _)| *id == endpoint.system_id).map(|(_, g)| g.as_slice()).unwrap_or(&[]);
            // a map by system ID: the last network info of a system wins
            let net = network_infos.iter().rev().find(|n| n.system_id == endpoint.system_id).unwrap_or(&empty_net);
            result.push(self.get_endpoint_labels(endpoint, groups, net));
        }
        Ok(result)
    }
}

impl Refresher for UyuniRefresher {
    fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut rpc_client = xmlrpc::Client::new(&self.api_url, self.round_tripper.clone())?;
        if self.token_expiration.is_none_or(|e| Instant::now() > e) {
            // Uyuni API takes duration in seconds.
            self.token = login(&mut rpc_client, &self.username, &self.password, TOKEN_DURATION.as_secs() as i64).map_err(|e| format!("unable to login to Uyuni API: {e}"))?;
            // Login again at half the token lifetime.
            self.token_expiration = Some(Instant::now() + TOKEN_DURATION / 2);
        }
        let entitlement = self.entitlement.clone();
        let targets = match self.get_targets_for_systems(&mut rpc_client, &entitlement) {
            Ok(t) => t,
            Err(e) => {
                // Force the renewal of the token on next refresh.
                self.token_expiration = Some(Instant::now());
                return Err(e);
            }
        };
        Ok(vec![Arc::new(Group { targets, labels: LabelSet::new(), source: self.api_url.clone() })])
    }
}

/// Go `uyuni.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<UyuniRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<UyuniMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut api_url = yaml::url::parse(&conf.server)?;
        api_url.path = crate::puppetdb::path_join(&api_url.path, UYUNI_XMLRPC_API_PATH);
        let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        let refresher = UyuniRefresher {
            api_url: api_url.to_url_string(),
            round_tripper: rt,
            username: conf.username.clone(),
            password: conf.password.as_str().to_string(),
            token: String::new(),
            token_expiration: None,
            entitlement: conf.entitlement.clone(),
            separator: conf.separator.clone(),
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "uyuni",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Sets the cached token and its expiration (Go tests set `d.token` and `d.tokenExpiration`).
    pub fn set_token(&mut self, token: &str, expiration: Instant) {
        let r = self.inner.refresher();
        r.token = token.to_string();
        r.token_expiration = Some(expiration);
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
