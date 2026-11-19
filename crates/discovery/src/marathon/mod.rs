//! Port of `discovery/marathon`: targets from the tasks of the apps of a Marathon server, refreshed at an
//! interval.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled (as `http`), and the
//! groups of one refresh come sorted by source (Go: map order, random).
pub mod gojson;

use std::collections::BTreeMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::{HttpClientConfig, RoundTripper};
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// The meta prefix of all meta labels of this discovery.
pub const META_LABEL_PREFIX: &str = "__meta_marathon_";
/// The prefix of the application labels.
pub const APP_LABEL_PREFIX: &str = "__meta_marathon_app_label_";
/// The name of the app in Marathon.
pub const APP_LABEL: &str = "__meta_marathon_app";
/// The docker image running the service.
pub const IMAGE_LABEL: &str = "__meta_marathon_image";
/// The integer port index when multiple ports are defined.
pub const PORT_INDEX_LABEL: &str = "__meta_marathon_port_index";
/// The mesos task name of the app instance.
pub const TASK_LABEL: &str = "__meta_marathon_task";
/// The prefix of the labels of the app's portMappings.
pub const PORT_MAPPING_LABEL_PREFIX: &str = "__meta_marathon_port_mapping_label_";
/// The prefix of the labels of the app's portDefinitions.
pub const PORT_DEFINITION_LABEL_PREFIX: &str = "__meta_marathon_port_definition_label_";

/// Go `appListPath`.
pub const APP_LIST_PATH: &str = "/v2/apps/?embed=apps.tasks";

/// Go `marathon.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub servers: Vec<String>,
    pub refresh_interval: yaml::Duration,
    pub auth_token: yaml::Secret,
    pub auth_token_file: String,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { refresh_interval: yaml::Duration::from_secs(30), http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "marathon.plain", [
    (servers: "servers", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (auth_token: "auth_token", omitempty)
    (auth_token_file: "auth_token_file", omitempty)
    (inline http_client_config)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            let msg = |s: &str| Err(YError::Msg(s.to_string()));
            if self.servers.is_empty() {
                return msg("marathon_sd: must contain at least one Marathon server");
            }
            if !self.auth_token.is_empty() && !self.auth_token_file.is_empty() {
                return msg("marathon_sd: at most one of auth_token & auth_token_file must be configured");
            }
            if !self.auth_token.is_empty() || !self.auth_token_file.is_empty() {
                let h = &self.http_client_config;
                if h.basic_auth.is_some() {
                    return msg("marathon_sd: at most one of basic_auth, auth_token & auth_token_file must be configured");
                }
                if !h.bearer_token.is_empty() || !h.bearer_token_file.is_empty() {
                    return msg("marathon_sd: at most one of bearer_token, bearer_token_file, auth_token & auth_token_file must be configured");
                }
                if h.authorization.is_some() {
                    return msg("marathon_sd: at most one of auth_token, auth_token_file & authorization must be configured");
                }
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "marathon.plain".to_string()
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
    const GO_TYPE: &'static str = "marathon.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "marathon"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(MarathonMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
        self.auth_token_file = net::config::join_dir(dir, &self.auth_token_file);
    }
}

/// Go `marathonMetrics`.
pub struct MarathonMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for MarathonMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `authTokenRoundTripper`: sets `Authorization: token=<token>` (the DC/OS form).
pub struct AuthTokenRoundTripper {
    auth_token: yaml::Secret,
    rt: Arc<dyn RoundTripper>,
}

impl RoundTripper for AuthTokenRoundTripper {
    fn round_trip(&self, mut req: net::Request) -> Result<net::Response, net::Error> {
        req.header.set("Authorization", &format!("token={}", self.auth_token.0));
        self.rt.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.rt.close_idle_connections()
    }
}

/// Go `newAuthTokenRoundTripper`.
pub fn new_auth_token_round_tripper(token: yaml::Secret, rt: Arc<dyn RoundTripper>) -> Arc<dyn RoundTripper> {
    Arc::new(AuthTokenRoundTripper { auth_token: token, rt })
}

/// Go `authTokenFileRoundTripper`: reads the token from the file at every request.
pub struct AuthTokenFileRoundTripper {
    auth_token_file: String,
    rt: Arc<dyn RoundTripper>,
}

fn read_token_file(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("unable to read auth token file {path}: open {path}: {}", net::types::go_io_error(&e)))
}

/// Go `newAuthTokenFileRoundTripper` (fails at once when the file cannot be read).
pub fn new_auth_token_file_round_tripper(token_file: &str, rt: Arc<dyn RoundTripper>) -> Result<Arc<dyn RoundTripper>, String> {
    read_token_file(token_file)?;
    Ok(Arc::new(AuthTokenFileRoundTripper { auth_token_file: token_file.to_string(), rt }))
}

impl RoundTripper for AuthTokenFileRoundTripper {
    fn round_trip(&self, mut req: net::Request) -> Result<net::Response, net::Error> {
        let b = read_token_file(&self.auth_token_file).map_err(net::Error::other)?;
        let token = String::from_utf8_lossy(&b);
        req.header.set("Authorization", &format!("token={}", token.trim()));
        self.rt.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.rt.close_idle_connections()
    }
}

/// Go `task`: one instance of a service running on Marathon.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Task {
    pub id: String,
    pub host: String,
    pub ports: Vec<u32>,
    pub ip_addresses: Vec<IpAddress>,
}
crate::go_json_struct!(Task, "marathon", "task", [id: "id", host: "host", ports: "ports", ip_addresses: "ipAddresses"]);

/// Go `ipAddress`: the address and protocol the container's network interface is bound to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IpAddress {
    pub address: String,
    pub proto: String,
}
crate::go_json_struct!(IpAddress, "marathon", "ipAddress", [address: "ipAddress", proto: "protocol"]);

/// Go `portMapping`: the port the process binds inside the docker container.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PortMapping {
    pub labels: BTreeMap<String, String>,
    pub container_port: u32,
    pub host_port: u32,
    pub service_port: u32,
}
crate::go_json_struct!(PortMapping, "marathon", "portMapping", [labels: "labels", container_port: "containerPort", host_port: "hostPort", service_port: "servicePort"]);

/// Go `dockerContainer`: a container using the docker runtime.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DockerContainer {
    pub image: String,
    pub port_mappings: Vec<PortMapping>,
}
crate::go_json_struct!(DockerContainer, "marathon", "dockerContainer", [image: "image", port_mappings: "portMappings"]);

/// Go `container`: the runtime an app runs in.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Container {
    pub docker: DockerContainer,
    pub port_mappings: Vec<PortMapping>,
}
crate::go_json_struct!(Container, "marathon", "container", [docker: "docker", port_mappings: "portMappings"]);

/// Go `portDefinition`: the load balancer port of the service.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PortDefinition {
    pub labels: BTreeMap<String, String>,
    pub port: u32,
}
crate::go_json_struct!(PortDefinition, "marathon", "portDefinition", [labels: "labels", port: "port"]);

/// Go `network`: the name and type of the network the container is attached to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Network {
    pub name: String,
    pub mode: String,
}
crate::go_json_struct!(Network, "marathon", "network", [name: "name", mode: "mode"]);

/// Go `app`: a service running on Marathon.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct App {
    pub id: String,
    pub tasks: Vec<Task>,
    pub running_tasks: i64,
    pub labels: BTreeMap<String, String>,
    pub container: Container,
    pub port_definitions: Vec<PortDefinition>,
    pub networks: Vec<Network>,
    pub require_ports: bool,
}
crate::go_json_struct!(App, "marathon", "app", [
    id: "id", tasks: "tasks", running_tasks: "tasksRunning", labels: "labels", container: "container",
    port_definitions: "portDefinitions", networks: "networks", require_ports: "requirePorts",
]);

impl App {
    /// Go `isContainerNet`: whether the app's first network has mode "container".
    pub fn is_container_net(&self) -> bool {
        self.networks.first().is_some_and(|n| n.mode == "container")
    }
}

/// Go `appList`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AppList {
    pub apps: Vec<App>,
}
crate::go_json_struct!(AppList, "marathon", "appList", [apps: "apps"]);

/// Go `appListClient`: gets the application list from a Marathon server.
pub type AppListClient = Box<dyn Fn(&CancelToken, &net::Client, &str) -> Result<AppList, String> + Send>;

/// Go `fetchApps`: requests the application list from a Marathon server.
pub fn fetch_apps(_ctx: &CancelToken, client: &net::Client, url: &str) -> Result<AppList, String> {
    let req = net::Request::get(url).map_err(|e| e.to_string())?;
    let resp = client.do_request(req).map_err(|e| e.to_string())?;
    if resp.status < 200 || resp.status >= 300 {
        return Err(format!("non 2xx status '{}' response during marathon service discovery", resp.status));
    }
    let mut apps = AppList::default();
    gojson::unmarshal(&resp.body, &mut apps).map_err(|e| format!("{}: {e}", yaml::go_quote(url)))?;
    Ok(apps)
}

/// A random index in `0..n` (Go `rand.Intn`).
fn rand_intn(n: usize) -> usize {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) % n as u64) as usize
}

/// Go `randomAppsURL`: the app list URL of a random server.
pub fn random_apps_url(servers: &[String]) -> String {
    let server = &servers[rand_intn(servers.len())];
    format!("{server}{APP_LIST_PATH}")
}

/// Go `appsToTargetGroups`.
pub fn apps_to_target_groups(apps: &AppList) -> BTreeMap<String, Arc<Group>> {
    let mut tgroups = BTreeMap::new();
    for a in &apps.apps {
        let group = create_target_group(a);
        tgroups.insert(group.source.clone(), Arc::new(group));
    }
    tgroups
}

fn create_target_group(app: &App) -> Group {
    let targets = targets_for_app(app);
    let mut labels = LabelSet::from_pairs(&[(APP_LABEL, &app.id), (IMAGE_LABEL, &app.container.docker.image)]);
    for (ln, lv) in &app.labels {
        labels.insert(format!("{APP_LABEL_PREFIX}{}", sanitize_label_name(ln)), lv.clone());
    }
    Group { targets, labels, source: app.id.clone() }
}

fn targets_for_app(app: &App) -> Vec<LabelSet> {
    let mut targets = Vec::with_capacity(app.tasks.len());
    let mut ports: Vec<u32> = Vec::new();
    let mut labels: Vec<BTreeMap<String, String>> = Vec::new();
    let mut prefix = "";

    if !app.container.port_mappings.is_empty() {
        // In Marathon 1.5.x the "container.docker.portMappings" object moved to "container.portMappings".
        (ports, labels) = extract_port_mapping(&app.container.port_mappings, app.is_container_net());
        prefix = PORT_MAPPING_LABEL_PREFIX;
    } else if !app.container.docker.port_mappings.is_empty() {
        // Before Marathon 1.5 the port mappings were at "container.docker.portMappings".
        (ports, labels) = extract_port_mapping(&app.container.docker.port_mappings, app.is_container_net());
        prefix = PORT_MAPPING_LABEL_PREFIX;
    } else if !app.port_definitions.is_empty() {
        // When requirePorts is false, the port is the 'servicePort', not the listen port: the port then comes
        // from the task.
        ports = app.port_definitions.iter().map(|d| if app.require_ports { d.port } else { 0 }).collect();
        labels = app.port_definitions.iter().map(|d| d.labels.clone()).collect();
        prefix = PORT_DEFINITION_LABEL_PREFIX;
    }

    // Each task (container) is reachable at one or more host:port endpoints.
    for t in &app.tasks {
        // With only the task's ports (host networking) there are no labels; the ports are the same for all
        // tasks.
        if ports.is_empty() && !t.ports.is_empty() {
            ports = t.ports.clone();
        }
        for (i, &port) in ports.iter().enumerate() {
            // A zero port is auto-generated by Mesos and has to be looked up in the task.
            let port = if port == 0 && t.ports.len() == ports.len() { t.ports[i] } else { port };
            let target_address = target_endpoint(t, port, app.is_container_net());
            let mut target = LabelSet::from_pairs(&[(ADDRESS_LABEL, &target_address), (TASK_LABEL, &t.id), (PORT_INDEX_LABEL, &i.to_string())]);
            if !labels.is_empty() {
                for (ln, lv) in &labels[i] {
                    target.insert(format!("{prefix}{}", sanitize_label_name(ln)), lv.clone());
                }
            }
            targets.push(target);
        }
    }
    targets
}

/// Go `targetEndpoint`: "host:port", with the task's first IP address in a container network.
fn target_endpoint(task: &Task, port: u32, container_net: bool) -> String {
    let host = if container_net && !task.ip_addresses.is_empty() { &task.ip_addresses[0].address } else { &task.host };
    join_host_port(host, port)
}

/// Go `extractPortMapping`: the ports and labels of port mappings.
fn extract_port_mapping(port_mappings: &[PortMapping], container_net: bool) -> (Vec<u32>, Vec<BTreeMap<String, String>>) {
    let labels = port_mappings.iter().map(|m| m.labels.clone()).collect();
    // In a container network, connect to the container port; otherwise to the allocated host port (0 means
    // auto-generated: taken from the task's ports later).
    let ports = port_mappings.iter().map(|m| if container_net { m.container_port } else { m.host_port }).collect();
    (ports, labels)
}

/// The refresh function of the Marathon discoverer (Go `Discovery.refresh` and its state).
pub struct MarathonRefresher {
    client: net::Client,
    servers: Vec<String>,
    last_refresh: Option<BTreeMap<String, Arc<Group>>>,
    apps_client: AppListClient,
}

impl MarathonRefresher {
    fn fetch_target_groups(&self, ctx: &CancelToken) -> Result<BTreeMap<String, Arc<Group>>, String> {
        let url = random_apps_url(&self.servers);
        let apps = (self.apps_client)(ctx, &self.client, &url)?;
        Ok(apps_to_target_groups(&apps))
    }
}

impl Refresher for MarathonRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let target_map = self.fetch_target_groups(ctx)?;
        let mut all: Vec<Arc<Group>> = target_map.values().cloned().collect();
        if ctx.is_cancelled() {
            return Err("context canceled".to_string());
        }
        // Remove services which disappeared.
        if let Some(last) = &self.last_refresh {
            for source in last.keys() {
                if !target_map.contains_key(source) {
                    all.push(Arc::new(Group { source: source.clone(), ..Default::default() }));
                }
            }
        }
        self.last_refresh = Some(target_map);
        Ok(all)
    }
}

/// Go `marathon.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<MarathonRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<MarathonMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        if !conf.auth_token.is_empty() {
            rt = new_auth_token_round_tripper(conf.auth_token.clone(), rt);
        } else if !conf.auth_token_file.is_empty() {
            rt = new_auth_token_file_round_tripper(&conf.auth_token_file, rt)?;
        }
        let refresher = MarathonRefresher { client: net::Client::new(rt), servers: conf.servers.clone(), last_refresh: None, apps_client: Box::new(fetch_apps) };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "marathon",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Replaces the app list client (Go tests set `md.appsClient`).
    pub fn set_apps_client(&mut self, c: AppListClient) {
        self.inner.refresher().apps_client = c;
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
