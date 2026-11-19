//! Port of `discovery/moby/docker.go`: Docker (non-swarm) service discovery.
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::client::{Client, Filters};
use super::dockerswarm::Filter;
use super::types::ContainerSummary;
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const DOCKER_LABEL: &str = "__meta_docker_";
pub const DOCKER_LABEL_CONTAINER_PREFIX: &str = "__meta_docker_container_";
pub const DOCKER_LABEL_CONTAINER_ID: &str = "__meta_docker_container_id";
pub const DOCKER_LABEL_CONTAINER_IMAGE: &str = "__meta_docker_container_image";
pub const DOCKER_LABEL_CONTAINER_IMAGE_ID: &str = "__meta_docker_container_image_id";
pub const DOCKER_LABEL_CONTAINER_NAME: &str = "__meta_docker_container_name";
pub const DOCKER_LABEL_CONTAINER_NETWORK_MODE: &str = "__meta_docker_container_network_mode";
pub const DOCKER_LABEL_CONTAINER_LABEL_PREFIX: &str = "__meta_docker_container_label_";
pub const DOCKER_LABEL_NETWORK_PREFIX: &str = "__meta_docker_network_";
pub const DOCKER_LABEL_NETWORK_IP: &str = "__meta_docker_network_ip";
pub const DOCKER_LABEL_PORT_PREFIX: &str = "__meta_docker_port_";
pub const DOCKER_LABEL_PORT_PRIVATE: &str = "__meta_docker_port_private";
pub const DOCKER_LABEL_PORT_PUBLIC: &str = "__meta_docker_port_public";
pub const DOCKER_LABEL_PORT_PUBLIC_IP: &str = "__meta_docker_port_public_ip";

/// Go `DockerSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DockerSdConfig {
    pub http_client_config: HttpClientConfig,
    pub host: String,
    pub port: i64,
    pub filters: Vec<Filter>,
    pub host_networking_host: String,
    pub refresh_interval: yaml::Duration,
    pub match_first_network: bool,
}

/// Go `DefaultDockerSDConfig`.
pub fn default_docker_sd_config() -> DockerSdConfig {
    DockerSdConfig {
        refresh_interval: yaml::Duration::from_secs(60),
        port: 80,
        filters: Vec::new(),
        host_networking_host: "localhost".to_string(),
        http_client_config: HttpClientConfig::default_config(),
        match_first_network: true,
        host: String::new(),
    }
}

yaml::yaml_struct!(DockerSdConfig, "moby.plain", [
    (inline http_client_config)
    (host: "host")
    (port: "port")
    (filters: "filters")
    (host_networking_host: "host_networking_host")
    (refresh_interval: "refresh_interval")
    (match_first_network: "match_first_network")
]);

impl Unmarshal for DockerSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_docker_sd_config();
            d.inner_struct(n, self)?;
            if self.host.is_empty() {
                return Err(YError::Msg("host missing".into()));
            }
            yaml::url::parse(&self.host).map_err(YError::Msg)?;
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "moby.plain".to_string()
    }
}

impl yaml::Marshal for DockerSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for DockerSdConfig {
    const GO_TYPE: &'static str = "moby.DockerSDConfig";
}

impl Config for DockerSdConfig {
    fn name(&self) -> &'static str {
        "docker"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(DockerDiscovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(DockerMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `dockerMetrics` (only the refresh metrics).
pub struct DockerMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for DockerMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh state of [`DockerDiscovery`].
pub struct DockerRefresher {
    client: Client,
    port: i64,
    host_networking_host: String,
    filters: Filters,
    match_first_network: bool,
}

/// Go `DockerDiscovery`.
pub struct DockerDiscovery {
    inner: refresh::Discovery<DockerRefresher>,
}

impl DockerDiscovery {
    /// Go `NewDockerDiscovery`.
    pub fn new(conf: &DockerSdConfig, opts: DiscovererOptions) -> Result<DockerDiscovery, String> {
        let m = crate::downcast_metrics::<DockerMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut filters = Filters::default();
        for f in &conf.filters {
            for v in &f.values {
                filters.add(&f.name, v);
            }
        }
        let client = super::new_client(&conf.host, &conf.http_client_config, conf.refresh_interval, "docker")?;
        let r = DockerRefresher {
            client,
            port: conf.port,
            host_networking_host: conf.host_networking_host.clone(),
            filters,
            match_first_network: conf.match_first_network,
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "docker",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: r,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(DockerDiscovery { inner })
    }

    /// Go `DockerDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for DockerDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// The IP of an endpoint: IPv4 address, else global IPv6 address, else "".
fn endpoint_ip(n: &super::types::EndpointSettings) -> String {
    if let Some(a) = &n.ip_address {
        a.to_string()
    } else if let Some(a) = &n.global_ipv6_address {
        a.to_string()
    } else {
        String::new()
    }
}

/// Go `container.NetworkMode.ConnectedContainer` (with `IsContainer`): the id after "container:".
fn connected_container(mode: &str) -> Option<&str> {
    match mode.split_once(':') {
        Some(("container", id)) => Some(id),
        _ => None,
    }
}

impl Refresher for DockerRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "Docker".to_string(), ..Default::default() };

        let containers = super::container_list(&mut self.client, ctx, &self.filters).map_err(|e| format!("error while listing containers: {e}"))?;
        let network_labels =
            super::network::get_networks_labels(&mut self.client, ctx, DOCKER_LABEL).map_err(|e| format!("error while computing network labels: {e}"))?;

        let mut all_containers: HashMap<&str, &ContainerSummary> = HashMap::new();
        for c in &containers {
            all_containers.insert(&c.id, c);
        }

        for c in &containers {
            if c.names.is_empty() {
                continue;
            }
            let mut common_labels: Vec<(String, String)> = vec![
                (DOCKER_LABEL_CONTAINER_ID.to_string(), c.id.clone()),
                (DOCKER_LABEL_CONTAINER_NAME.to_string(), c.names[0].clone()),
                (DOCKER_LABEL_CONTAINER_NETWORK_MODE.to_string(), c.network_mode.clone()),
            ];
            if !c.image.is_empty() {
                common_labels.push((DOCKER_LABEL_CONTAINER_IMAGE.to_string(), c.image.clone()));
            }
            if !c.image_id.is_empty() {
                common_labels.push((DOCKER_LABEL_CONTAINER_IMAGE_ID.to_string(), c.image_id.clone()));
            }
            for (k, v) in &c.labels {
                common_labels.push((format!("{DOCKER_LABEL_CONTAINER_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone()));
            }

            let mut networks = &c.networks;
            if networks.is_empty() {
                // Try to lookup shared networks. Go loops forever on a cycle of "container:" modes without
                // networks; this stops after visiting every container once.
                let mut mode = c.network_mode.as_str();
                let mut steps = 0;
                while let Some(id) = connected_container(mode) {
                    let Some(tmp) = all_containers.get(id) else { break };
                    networks = &tmp.networks;
                    mode = tmp.network_mode.as_str();
                    steps += 1;
                    if !networks.is_empty() || steps > containers.len() {
                        break;
                    }
                }
            }

            let first;
            let mut networks: &[(String, Option<super::types::EndpointSettings>)] = networks;
            if self.match_first_network && networks.len() > 1 {
                // Sort networks by name and take first non-nil network.
                let mut keys: Vec<&(String, Option<super::types::EndpointSettings>)> = networks.iter().filter(|(_, n)| n.is_some()).collect();
                if !keys.is_empty() {
                    keys.sort_by(|a, b| a.0.cmp(&b.0));
                    first = [keys[0].clone()];
                    networks = &first;
                }
            }

            for (_, n) in networks {
                let Some(n) = n else { continue };
                let mut added = false;
                let empty = HashMap::new();
                let net_labels = network_labels.get(&n.network_id).unwrap_or(&empty);

                for p in &c.ports {
                    if p.type_ != "tcp" {
                        continue;
                    }
                    let ip_addr = endpoint_ip(n);
                    let mut labels = LabelSet::new();
                    labels.insert(DOCKER_LABEL_NETWORK_IP.to_string(), ip_addr.clone());
                    labels.insert(DOCKER_LABEL_PORT_PRIVATE.to_string(), p.private_port.to_string());
                    if p.public_port > 0 {
                        labels.insert(DOCKER_LABEL_PORT_PUBLIC.to_string(), p.public_port.to_string());
                        let public_ip = p.ip.as_ref().map(|a| a.to_string()).unwrap_or_default();
                        labels.insert(DOCKER_LABEL_PORT_PUBLIC_IP.to_string(), public_ip);
                    }
                    for (k, v) in &common_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    for (k, v) in net_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&ip_addr, p.private_port));
                    tg.targets.push(labels);
                    added = true;
                }

                if !added {
                    // Use fallback port when no exposed ports are available or if all are non-TCP.
                    let ip_addr = endpoint_ip(n);
                    let mut labels = LabelSet::new();
                    labels.insert(DOCKER_LABEL_NETWORK_IP.to_string(), ip_addr.clone());
                    for (k, v) in &common_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    for (k, v) in net_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    // Containers in host networking mode don't have ports, so they only end up here, not in the
                    // previous loop.
                    let addr = if c.network_mode != "host" { join_host_port(&ip_addr, self.port) } else { self.host_networking_host.clone() };
                    labels.insert(ADDRESS_LABEL.to_string(), addr);
                    tg.targets.push(labels);
                }
            }
        }
        Ok(vec![Arc::new(tg)])
    }
}
