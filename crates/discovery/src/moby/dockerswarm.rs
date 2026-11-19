//! Port of `discovery/moby/dockerswarm.go`: Docker Swarm service discovery (roles services, nodes, tasks).
use std::sync::Arc;

use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::client::{Client, Filters};
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const SWARM_LABEL: &str = "__meta_dockerswarm_";

/// Go `Filter`: a Docker API filter (`name` and its values).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filter {
    pub name: String,
    pub values: Vec<String>,
}

yaml::yaml_struct!(Filter, "moby.Filter", [
    (name: "name")
    (values: "values")
]);
yaml::impl_plain_struct!(Filter);

/// Go `DockerSwarmSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DockerSwarmSdConfig {
    pub http_client_config: HttpClientConfig,
    pub host: String,
    pub role: String,
    pub port: i64,
    pub filters: Vec<Filter>,
    pub refresh_interval: yaml::Duration,
}

/// Go `DefaultDockerSwarmSDConfig`.
pub fn default_docker_swarm_sd_config() -> DockerSwarmSdConfig {
    DockerSwarmSdConfig {
        refresh_interval: yaml::Duration::from_secs(60),
        port: 80,
        filters: Vec::new(),
        http_client_config: HttpClientConfig::default_config(),
        host: String::new(),
        role: String::new(),
    }
}

yaml::yaml_struct!(DockerSwarmSdConfig, "moby.plain", [
    (inline http_client_config)
    (host: "host")
    (role: "role")
    (port: "port")
    (filters: "filters")
    (refresh_interval: "refresh_interval")
]);

impl Unmarshal for DockerSwarmSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_docker_swarm_sd_config();
            d.inner_struct(n, self)?;
            if self.host.is_empty() {
                return Err(YError::Msg("host missing".into()));
            }
            yaml::url::parse(&self.host).map_err(YError::Msg)?;
            match self.role.as_str() {
                "services" | "nodes" | "tasks" => {}
                "" => return Err(YError::Msg("role missing (one of: tasks, services, nodes)".into())),
                r => return Err(YError::Msg(format!("invalid role {r}, expected tasks, services, or nodes"))),
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "moby.plain".to_string()
    }
}

impl yaml::Marshal for DockerSwarmSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for DockerSwarmSdConfig {
    const GO_TYPE: &'static str = "moby.DockerSwarmSDConfig";
}

impl Config for DockerSwarmSdConfig {
    fn name(&self) -> &'static str {
        "dockerswarm"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(DockerswarmMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `dockerswarmMetrics` (only the refresh metrics).
pub struct DockerswarmMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for DockerswarmMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh state of [`Discovery`] (Go `Discovery` fields).
pub struct SwarmRefresher {
    pub(super) client: Client,
    pub(super) role: String,
    pub(super) port: i64,
    pub(super) filters: Filters,
}

/// Go `Discovery`: periodically refreshes Docker Swarm targets.
pub struct Discovery {
    inner: refresh::Discovery<SwarmRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &DockerSwarmSdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<DockerswarmMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut filters = Filters::default();
        for f in &conf.filters {
            for v in &f.values {
                filters.add(&f.name, v);
            }
        }
        let client = super::new_client(&conf.host, &conf.http_client_config, conf.refresh_interval, "docker swarm")?;
        let r = SwarmRefresher { client, role: conf.role.clone(), port: conf.port, filters };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "dockerswarm",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: r,
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

impl Refresher for SwarmRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        match self.role.as_str() {
            "services" => self.refresh_services(ctx),
            "nodes" => self.refresh_nodes(ctx),
            "tasks" => self.refresh_tasks(ctx),
            r => panic!("unexpected role {r}"),
        }
    }
}
