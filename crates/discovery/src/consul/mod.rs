//! Port of `discovery/consul`: targets from the Consul catalog, kept up to date with blocking queries.
//!
//! Go's goroutines are threads here: one for the catalog watch (`Discoverer::run`) and one per watched service.
//! A request in flight is abandoned when the discoverer (or the service watch) is cancelled, as Go cancels it
//! through the request context; its thread ends in the background at the latest at the client timeout.
//!
//! The Consul API client is ported in [`api`] (the part that upstream uses), Go standard library helpers in
//! [`goutil`] (shared with `nomad`).
pub mod api;
pub mod goutil;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{CounterVec, MetricRegisterer, Observer, Opts, Registry, SummaryVec, Value};
use crate::sdutil::{join_host_port, join_tags, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

use goutil::{Caller, Ticker};

pub const WATCH_TIMEOUT: Duration = Duration::from_secs(120);
pub const RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// The name for the label containing a target's address.
pub const ADDRESS_LABEL_NAME: &str = "__meta_consul_address";
/// The name for the label containing a target's node name.
pub const NODE_LABEL: &str = "__meta_consul_node";
/// The prefix for the labels mapping to a target's metadata.
pub const META_DATA_LABEL: &str = "__meta_consul_metadata_";
/// The prefix for the labels mapping to a target's service metadata.
pub const SERVICE_META_DATA_LABEL: &str = "__meta_consul_service_metadata_";
/// The name of the label containing the tags assigned to the target.
pub const TAGS_LABEL: &str = "__meta_consul_tags";
/// The name of the label containing the service name.
pub const SERVICE_LABEL: &str = "__meta_consul_service";
/// The name of the label containing the health of the service instance.
pub const HEALTH_LABEL: &str = "__meta_consul_health";
/// The name of the label containing the (optional) service address.
pub const SERVICE_ADDRESS_LABEL: &str = "__meta_consul_service_address";
/// The name of the label containing the service port.
pub const SERVICE_PORT_LABEL: &str = "__meta_consul_service_port";
/// The name of the label containing the datacenter ID.
pub const DATACENTER_LABEL: &str = "__meta_consul_dc";
/// The name of the label containing the namespace (Consul Enterprise only).
pub const NAMESPACE_LABEL: &str = "__meta_consul_namespace";
/// The name of the label containing the Admin Partition (Consul Enterprise only).
pub const PARTITION_LABEL: &str = "__meta_consul_partition";
/// The prefix for the labels mapping to a target's tagged addresses.
pub const TAGGED_ADDRESSES_LABEL: &str = "__meta_consul_tagged_address_";
/// The name of the label containing the service ID.
pub const SERVICE_ID_LABEL: &str = "__meta_consul_service_id";

/// Go `consul.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub server: String,
    pub path_prefix: String,
    pub token: Secret,
    pub datacenter: String,
    pub namespace: String,
    pub partition: String,
    pub tag_separator: String,
    pub scheme: String,
    pub username: String,
    pub password: Secret,
    /// Stale reads are a lot cheaper and are a necessity with more than 5k targets.
    pub allow_stale: bool,
    /// Blocking queries are used, but updates can be throttled with this interval.
    pub refresh_interval: yaml::Duration,
    /// The services for which targets are discovered; all services when empty.
    pub services: Vec<String>,
    /// Tags that the instances of a service must all have (Go `ServiceTags`).
    pub service_tags: Vec<String>,
    /// Desired node metadata.
    pub node_meta: BTreeMap<String, String>,
    /// Filter expression for the Catalog API.
    pub filter: String,
    /// Filter expression for the Health API.
    pub health_filter: String,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        tag_separator: ",".to_string(),
        scheme: "http".to_string(),
        server: "localhost:8500".to_string(),
        allow_stale: true,
        refresh_interval: yaml::Duration::from_secs(30),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "consul.plain", [
    (server: "server", omitempty)
    (path_prefix: "path_prefix", omitempty)
    (token: "token", omitempty)
    (datacenter: "datacenter", omitempty)
    (namespace: "namespace", omitempty)
    (partition: "partition", omitempty)
    (tag_separator: "tag_separator", omitempty)
    (scheme: "scheme", omitempty)
    (username: "username", omitempty)
    (password: "password", omitempty)
    (allow_stale: "allow_stale")
    (refresh_interval: "refresh_interval", omitempty)
    (services: "services", omitempty)
    (service_tags: "tags", omitempty)
    (node_meta: "node_meta", omitempty)
    (filter: "filter", omitempty)
    (health_filter: "health_filter", omitempty)
    (inline http_client_config)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if goutil::trim_space(&self.server).is_empty() {
                return Err(YError::Msg("consul SD configuration requires a server address".into()));
            }
            if !self.username.is_empty() || !self.password.0.is_empty() {
                if self.http_client_config.basic_auth.is_some() {
                    return Err(YError::Msg("at most one of consul SD configuration username and password and basic auth can be configured".into()));
                }
                self.http_client_config.basic_auth = Some(net::BasicAuth { username: self.username.clone(), password: self.password.clone(), ..Default::default() });
            }
            if !self.token.0.is_empty() && (self.http_client_config.authorization.is_some() || self.http_client_config.oauth2.is_some()) {
                return Err(YError::Msg("at most one of consul SD token, authorization, or oauth2 can be configured".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "consul.plain".to_string()
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
    const GO_TYPE: &'static str = "consul.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "consul"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts.logger, opts.metrics)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, _rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        ConsulMetrics::new(reg)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `consulMetrics`.
pub struct ConsulMetrics {
    pub rpc_failures_count: Arc<Value>,
    pub rpc_duration: Arc<SummaryVec>,
    pub services_rpc_duration: Arc<Observer>,
    pub service_rpc_duration: Arc<Observer>,
    registerer: MetricRegisterer,
}

impl ConsulMetrics {
    /// Go `newDiscovererMetrics`.
    pub fn new(reg: &Arc<Registry>) -> Arc<ConsulMetrics> {
        let failures = CounterVec::new_counter(Opts::new("prometheus_sd_consul_rpc_failures_total", "The number of Consul RPC call failures."));
        let rpc_duration = SummaryVec::new_summary_vec(Opts::new("prometheus_sd_consul_rpc_duration_seconds", "The duration of a Consul RPC call in seconds."), &["endpoint", "call"]);
        let registerer = MetricRegisterer::new(reg.clone(), vec![failures.clone(), rpc_duration.clone()]);
        let services_rpc_duration = rpc_duration.with_label_values(&["catalog", "services"]);
        let service_rpc_duration = rpc_duration.with_label_values(&["catalog", "service"]);
        Arc::new(ConsulMetrics { rpc_failures_count: failures.get(), rpc_duration, services_rpc_duration, service_rpc_duration, registerer })
    }
}

impl DiscovererMetrics for ConsulMetrics {
    fn register(&self) -> Result<(), String> {
        self.registerer.register_metrics()
    }
    fn unregister(&self) {
        self.registerer.unregister_metrics()
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// What the service watches share (Go reads it through the `*Discovery` pointer).
struct Shared {
    client: api::Client,
    tag_separator: String,
    watched_tags: Vec<String>,
    watched_node_meta: BTreeMap<String, String>,
    watched_health_filter: String,
    allow_stale: bool,
    refresh_interval: Duration,
    metrics: Arc<ConsulMetrics>,
}

/// Go `consul.Discovery`: retrieves target information from a Consul server and updates it via watches.
pub struct Discovery {
    shared: Arc<Shared>,
    client_datacenter: String,
    client_namespace: String,
    client_partition: String,
    watched_services: Vec<String>,
    watched_filter: String,
    logger: Logger,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, logger: Logger, metrics: Option<Arc<dyn DiscovererMetrics>>) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<ConsulMetrics>(&metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let opts = net::HttpClientOptions::default().with_idle_conn_timeout(2 * WATCH_TIMEOUT);
        let mut wrapper = net::new_client_from_config(&conf.http_client_config, &opts).map_err(|e| e.to_string())?;
        wrapper.timeout = Some(WATCH_TIMEOUT + Duration::from_secs(15));
        let client_conf = api::Config {
            address: conf.server.clone(),
            path_prefix: conf.path_prefix.clone(),
            scheme: conf.scheme.clone(),
            datacenter: conf.datacenter.clone(),
            namespace: conf.namespace.clone(),
            partition: conf.partition.clone(),
            token: conf.token.0.clone(),
            token_file: String::new(),
        };
        let client = api::new_client(client_conf, wrapper)?;
        let shared = Arc::new(Shared {
            client,
            tag_separator: conf.tag_separator.clone(),
            watched_tags: conf.service_tags.clone(),
            watched_node_meta: conf.node_meta.clone(),
            watched_health_filter: conf.health_filter.clone(),
            allow_stale: conf.allow_stale,
            refresh_interval: conf.refresh_interval.to_std(),
            metrics: m,
        });
        Ok(Discovery {
            shared,
            client_datacenter: conf.datacenter.clone(),
            client_namespace: conf.namespace.clone(),
            client_partition: conf.partition.clone(),
            watched_services: conf.services.clone(),
            watched_filter: conf.filter.clone(),
            logger,
        })
    }

    /// The datacenter of the queries ("" until `get_datacenter` found it, when not configured).
    pub fn client_datacenter(&self) -> &str {
        &self.client_datacenter
    }
    /// The configured namespace (Consul Enterprise).
    pub fn client_namespace(&self) -> &str {
        &self.client_namespace
    }
    /// The configured Admin Partition (Consul Enterprise).
    pub fn client_partition(&self) -> &str {
        &self.client_partition
    }

    /// Go `shouldWatch`: whether the service of the given name should be watched.
    pub fn should_watch(&self, name: &str, tags: &[String]) -> bool {
        self.should_watch_from_name(name) && self.should_watch_from_tags(tags)
    }

    /// Go `shouldWatchFromName`.
    fn should_watch_from_name(&self, name: &str) -> bool {
        // If there's no fixed set of watched services, we watch everything.
        self.watched_services.is_empty() || self.watched_services.iter().any(|s| s == name)
    }

    /// Go `shouldWatchFromTags`: the service has every watched tag.
    fn should_watch_from_tags(&self, tags: &[String]) -> bool {
        self.shared.watched_tags.iter().all(|w| tags.contains(w))
    }

    /// Go `getDatacenter`: gets the local datacenter from the agent if it is not configured.
    pub fn get_datacenter(&mut self) -> Result<(), String> {
        if !self.client_datacenter.is_empty() {
            return Ok(());
        }
        let info = match self.shared.client.agent_self() {
            Ok(i) => i,
            Err(e) => {
                self.logger.error("Error retrieving datacenter name", &[("err", &e)]);
                self.shared.metrics.rpc_failures_count.inc();
                return Err(e);
            }
        };
        // Go map lookups: info["Config"]["Datacenter"]
        fn lookup<'a>(v: Option<&'a json::Json>, k: &str) -> Option<&'a json::Json> {
            v.and_then(|v| v.as_obj()).and_then(|o| o.iter().rev().find(|(n, _)| n == k)).map(|(_, v)| v)
        }
        let dc = lookup(lookup(Some(&info), "Config"), "Datacenter");
        let Some(dc) = dc.and_then(|v| v.as_str()) else {
            let err = format!("invalid value '{}' for Config.Datacenter", goutil::fmt_v(dc));
            self.logger.error("Error retrieving datacenter name", &[("err", &err)]);
            return Err(err);
        };
        self.client_datacenter = dc.to_string();
        self.logger = self.logger.with(&[("datacenter", dc)]);
        Ok(())
    }

    /// Go `initialize`: loops until the local datacenter is known.
    fn initialize(&mut self, ctx: &CancelToken) {
        loop {
            if ctx.is_cancelled() {
                return;
            }
            if self.get_datacenter().is_err() {
                if ctx.wait_timeout(RETRY_INTERVAL) {
                    return;
                }
                continue;
            }
            return;
        }
    }

    /// Go `watchServices`: watches the catalog for services to watch (when the service names are not known or
    /// tags or a filter select them).
    fn watch_services(&self, ctx: &CancelToken, ch: &Sender, last_index: &mut u64, services: &mut BTreeMap<String, CancelToken>, caller: &mut Caller) {
        self.logger.debug("Watching services", &[("tags", &self.shared.watched_tags.join(",")), ("filter", &self.watched_filter)]);
        let opts = api::QueryOptions {
            wait_index: *last_index,
            wait_time: WATCH_TIMEOUT,
            allow_stale: self.shared.allow_stale,
            node_meta: self.shared.watched_node_meta.clone(),
            filter: self.watched_filter.clone(),
        };
        let t0 = Instant::now();
        let r = self.shared.client.catalog_services(&opts, Some(caller));
        self.shared.metrics.services_rpc_duration.observe(t0.elapsed().as_secs_f64());

        // Check the context before in order to exit early.
        if ctx.is_cancelled() {
            return;
        }
        let (srvs, meta) = match r {
            Ok(r) => r,
            Err(e) => {
                self.logger.error("Error refreshing service list", &[("err", &e)]);
                self.shared.metrics.rpc_failures_count.inc();
                ctx.wait_timeout(RETRY_INTERVAL);
                return;
            }
        };
        // If the index equals the previous one, the watch timed out with no update.
        if meta.last_index == *last_index {
            return;
        }
        *last_index = meta.last_index;

        // Check for new services.
        for (name, tags) in &srvs {
            if !self.should_watch(name, tags) {
                continue;
            }
            if services.contains_key(name) {
                continue; // We are already watching the service.
            }
            let wctx = ctx.child();
            self.watch_service(&wctx, ch, name);
            services.insert(name.clone(), wctx);
        }

        // Check for removed services.
        let removed: Vec<String> = services.keys().filter(|n| !srvs.contains_key(*n)).cloned().collect();
        for name in removed {
            if let Some(cancel) = services.remove(&name) {
                cancel.cancel();
            }
            // Send clearing target group.
            if ctx.is_cancelled() || !ch.send(vec![Arc::new(Group { source: name, ..Default::default() })]) {
                return;
            }
        }

        // Send targetgroup with no targets if nothing was discovered.
        if services.is_empty() && !ctx.is_cancelled() {
            ch.send(vec![Arc::new(Group::default())]);
        }
    }

    /// Go `watchService`: starts watching a service on its own thread.
    fn watch_service(&self, ctx: &CancelToken, ch: &Sender, name: &str) {
        let srv = ConsulService {
            name: name.to_string(),
            labels: LabelSet::from_pairs(&[(SERVICE_LABEL, name), (DATACENTER_LABEL, &self.client_datacenter)]),
            shared: self.shared.clone(),
            logger: self.logger.clone(),
        };
        let (ctx, ch) = (ctx.clone(), ch.clone());
        let r = std::thread::Builder::new().name("consul-service".to_string()).spawn(move || {
            let mut ticker = Ticker::new(srv.shared.refresh_interval);
            let mut last_index = 0u64;
            let mut caller = Caller::new(&ctx);
            loop {
                if ctx.is_cancelled() {
                    return;
                }
                srv.watch(&ctx, &ch, &mut last_index, &mut caller);
                if ticker.wait(&ctx) {
                    return;
                }
            }
        });
        if let Err(e) = r {
            self.logger.error("Error starting service watch", &[("service", name), ("err", &e.to_string())]);
        }
    }
}

impl Discoverer for Discovery {
    /// Go `Run`.
    fn run(&mut self, ctx: &CancelToken, ch: Sender) {
        self.initialize(ctx);

        if self.watched_services.is_empty() || !self.shared.watched_tags.is_empty() || !self.watched_filter.is_empty() {
            // We need to watch the catalog.
            let mut ticker = Ticker::new(self.shared.refresh_interval);
            // Watched services and their cancellation tokens.
            let mut services: BTreeMap<String, CancelToken> = BTreeMap::new();
            let mut last_index = 0u64;
            let mut caller = Caller::new(ctx);
            loop {
                if ctx.is_cancelled() {
                    return;
                }
                self.watch_services(ctx, &ch, &mut last_index, &mut services, &mut caller);
                if ticker.wait(ctx) {
                    return;
                }
            }
        } else {
            // We only have fully defined services.
            for name in &self.watched_services {
                self.watch_service(ctx, &ch, name);
            }
            ctx.wait();
        }
    }
}

/// Go `consulService`: the data of one watched service.
struct ConsulService {
    name: String,
    labels: LabelSet,
    shared: Arc<Shared>,
    logger: Logger,
}

impl ConsulService {
    /// Go `consulService.watch`: gets updates for a service.
    fn watch(&self, ctx: &CancelToken, ch: &Sender, last_index: &mut u64, caller: &mut Caller) {
        let tags = self.shared.watched_tags.join(",");
        self.logger.debug("Watching service", &[("service", &self.name), ("tags", &tags)]);
        let opts = api::QueryOptions {
            wait_index: *last_index,
            wait_time: WATCH_TIMEOUT,
            allow_stale: self.shared.allow_stale,
            node_meta: self.shared.watched_node_meta.clone(),
            filter: self.shared.watched_health_filter.clone(),
        };
        let t0 = Instant::now();
        let r = self.shared.client.health_service_multiple_tags(&self.name, &self.shared.watched_tags, false, &opts, Some(caller));
        self.shared.metrics.service_rpc_duration.observe(t0.elapsed().as_secs_f64());

        // Check the context before in order to exit early.
        if ctx.is_cancelled() {
            return;
        }
        let (service_nodes, meta) = match r {
            Ok(r) => r,
            Err(e) => {
                self.logger.error("Error refreshing service", &[("service", &self.name), ("tags", &tags), ("err", &e)]);
                self.shared.metrics.rpc_failures_count.inc();
                ctx.wait_timeout(RETRY_INTERVAL);
                return;
            }
        };
        // If the index equals the previous one, the watch timed out with no update.
        if meta.last_index == *last_index {
            return;
        }
        *last_index = meta.last_index;

        let sep = &self.shared.tag_separator;
        let mut tgroup = Group { source: self.name.clone(), labels: self.labels.clone(), targets: Vec::with_capacity(service_nodes.len()) };
        for sn in &service_nodes {
            // We surround the separated list with the separator as well. This way regular expressions
            // in relabeling rules don't have to consider tag positions.
            let tags = join_tags(&sn.service.tags, sep);
            let port = sn.service.port.to_string();
            // If the service address is not empty it should be used instead of the node address
            // since the service may be registered remotely through a different node.
            let addr = if !sn.service.address.is_empty() { join_host_port(&sn.service.address, &port) } else { join_host_port(&sn.node.address, &port) };
            let mut labels = LabelSet::from_pairs(&[
                (ADDRESS_LABEL, &addr),
                (ADDRESS_LABEL_NAME, &sn.node.address),
                (NODE_LABEL, &sn.node.node),
                (NAMESPACE_LABEL, &sn.service.namespace),
                (PARTITION_LABEL, &sn.service.partition),
                (TAGS_LABEL, &tags),
                (SERVICE_ADDRESS_LABEL, &sn.service.address),
                (SERVICE_PORT_LABEL, &port),
                (SERVICE_ID_LABEL, &sn.service.id),
                (HEALTH_LABEL, api::aggregated_status(&sn.checks)),
            ]);
            // Add all key/value pairs from the node's metadata as their own labels.
            for (k, v) in &sn.node.meta {
                labels.insert(format!("{META_DATA_LABEL}{}", sanitize_label_name(k)), v.clone());
            }
            // Add all key/value pairs from the service's metadata as their own labels.
            for (k, v) in &sn.service.meta {
                labels.insert(format!("{SERVICE_META_DATA_LABEL}{}", sanitize_label_name(k)), v.clone());
            }
            // Add all key/value pairs from the service's tagged addresses as their own labels.
            for (k, v) in &sn.node.tagged_addresses {
                labels.insert(format!("{TAGGED_ADDRESSES_LABEL}{}", sanitize_label_name(k)), v.clone());
            }
            tgroup.targets.push(labels);
        }
        if !ctx.is_cancelled() {
            ch.send(vec![Arc::new(tgroup)]);
        }
    }
}
