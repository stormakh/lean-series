//! Port of `discovery/xds`: Kuma service discovery over the xDS monitoring assignment service (MADS), polled with
//! REST-JSON (`kuma_sd_configs`).
//!
//! Differs from Go:
//! - Waiting for the next refresh tick stops early when the discoverer is cancelled (Go waits for the tick).
//! - A request in flight is not aborted on cancel; it ends at its timeout (fetch_timeout + 15s).
//! - User labels are converted in sorted key order, so when two keys sanitize to the same label name the
//!   last one in sorted order wins (Go: random map order).
//! - The default client ID (Go `osutil.GetFQDN`): the reverse lookup reads `/etc/hosts` only (no DNS PTR query),
//!   and the forward lookup of the hostname uses the system resolver.
//! - See `pb.rs` for the protobuf and protojson differences.
pub mod client;
pub mod pb;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

pub use client::{HttpResourceClient, HttpResourceClientConfig, USER_AGENT, make_xds_resource_http_endpoint_url};
pub use pb::{Any, DiscoveryRequest, DiscoveryResponse, Message, MonitoringAssignment, MonitoringAssignmentTarget};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry, SummaryVec};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// Go `ProtocolVersion`: the xDS protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolVersion(pub &'static str);

pub const PROTOCOL_V3: ProtocolVersion = ProtocolVersion("v3");

pub const KUMA_MADS_V1_RESOURCE_TYPE_URL: &str = "type.googleapis.com/kuma.observability.v1.MonitoringAssignment";
pub const KUMA_MADS_V1_RESOURCE_TYPE: &str = "monitoringassignments";

const KUMA_MESH_LABEL: &str = "__meta_kuma_mesh";
const KUMA_SERVICE_LABEL: &str = "__meta_kuma_service";
const KUMA_DATAPLANE_LABEL: &str = "__meta_kuma_dataplane";
const KUMA_USER_LABEL_PREFIX: &str = "__meta_kuma_label_";

// ---------------- config ----------------

/// Go `xds.SDConfig` (`KumaSDConfig` is an alias of it).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub fetch_timeout: yaml::Duration,
    pub server: String,
    pub client_id: String,
}

/// Go `KumaSDConfig`.
pub type KumaSdConfig = SdConfig;

/// Go `DefaultKumaSDConfig`.
pub fn default_kuma_sd_config() -> SdConfig {
    SdConfig {
        http_client_config: HttpClientConfig::default_config(),
        refresh_interval: yaml::Duration::from_secs(15),
        fetch_timeout: yaml::Duration::from_secs(120),
        server: String::new(),
        client_id: String::new(),
    }
}

yaml::yaml_struct!(SdConfig, "xds.plainKumaConf", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval", omitempty)
    (fetch_timeout: "fetch_timeout", omitempty)
    (server: "server", omitempty)
    (client_id: "client_id", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_kuma_sd_config();
            d.inner_struct(n, self)?;
            if self.server.is_empty() {
                return Err(YError::Msg(format!("kuma SD server must not be empty: {}", self.server)));
            }
            let u = yaml::url::parse(&self.server).map_err(YError::Msg)?;
            if u.scheme.is_empty() || u.host.is_empty() {
                return Err(YError::Msg(format!("kuma SD server must not be empty and have a scheme: {}", self.server)));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "xds.plainKumaConf".to_string()
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
    const GO_TYPE: &'static str = "xds.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "kuma"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_kuma_http_discovery(self, opts.logger, &opts.metrics)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        XdsMetrics::new(reg, rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

// ---------------- metrics ----------------

/// Go `xdsMetrics`.
pub struct XdsMetrics {
    pub fetch_duration: Arc<SummaryVec>,
    pub fetch_skip_update_count: Arc<CounterVec>,
    pub fetch_failures_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl XdsMetrics {
    /// Go `newDiscovererMetrics`.
    pub fn new(reg: &Arc<Registry>, _rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<XdsMetrics> {
        let fetch_failures_count = CounterVec::new_counter(Opts::new("prometheus_sd_kuma_fetch_failures_total", "The number of Kuma MADS fetch call failures."));
        let fetch_skip_update_count =
            CounterVec::new_counter(Opts::new("prometheus_sd_kuma_fetch_skipped_updates_total", "The number of Kuma MADS fetch calls that result in no updates to the targets."));
        let fetch_duration = SummaryVec::new_summary(Opts::new("prometheus_sd_kuma_fetch_duration_seconds", "The duration of a Kuma MADS fetch call."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![fetch_failures_count.clone(), fetch_skip_update_count.clone(), fetch_duration.clone()]);
        Arc::new(XdsMetrics { fetch_duration, fetch_skip_update_count, fetch_failures_count, registerer })
    }
}

impl DiscovererMetrics for XdsMetrics {
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

// ---------------- fetch discovery ----------------

/// Go `ResourceClient`: the xDS protocol for one resource type.
pub trait ResourceClient: Send {
    /// The type URL of the resource.
    fn resource_type_url(&self) -> &str;
    /// The xDS management server.
    fn server(&self) -> &str;
    /// The latest view of the whole resource state, or `None` when nothing changed since the last request.
    fn fetch(&mut self, ctx: &CancelToken) -> Result<Option<DiscoveryResponse>, String>;
    /// The ID of the client sent to the xDS server.
    fn id(&self) -> &str;
    /// Releases held resources.
    fn close(&mut self);
    /// For downcasting (Go `client.(*HTTPResourceClient)`).
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Go `resourceParser`: turns the discovered resources into targets. On an error no update is sent and the
/// failure count goes up.
pub type ResourceParser = Box<dyn FnMut(&[Any], &str) -> Result<Vec<LabelSet>, String> + Send>;

/// Go `fetchDiscovery`: polling via xDS Fetch REST-JSON.
pub struct FetchDiscovery {
    pub client: Box<dyn ResourceClient>,
    pub source: String,
    pub refresh_interval: Duration,
    pub parse_resources: ResourceParser,
    pub logger: Logger,
    pub metrics: Arc<XdsMetrics>,
}

impl Discoverer for FetchDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        if self.refresh_interval.is_zero() {
            // Go: time.NewTicker panics.
            panic!("non-positive interval for NewTicker");
        }
        let interval = self.refresh_interval;
        let mut next_tick = Instant::now() + interval;
        while !ctx.is_cancelled() {
            self.poll(ctx, &up);
            // Go `<-ticker.C`: a tick missed during the poll is delivered at once; ticks stay on the grid.
            let now = Instant::now();
            if now >= next_tick {
                let missed = (now - next_tick).as_nanos() / interval.as_nanos() + 1;
                next_tick += interval * missed as u32;
                continue;
            }
            // Differs from Go: stops waiting when cancelled.
            if ctx.wait_timeout(next_tick - now) {
                break;
            }
            next_tick += interval;
        }
        self.client.close();
    }
}

impl FetchDiscovery {
    /// Go `poll`: one fetch; sends the parsed targets as one group unless nothing changed or it failed.
    pub fn poll(&mut self, ctx: &CancelToken, ch: &Sender) {
        let t0 = Instant::now();
        let response = self.client.fetch(ctx);
        self.metrics.fetch_duration.get().observe(t0.elapsed().as_secs_f64());

        // Check the context before in order to exit early.
        if ctx.is_cancelled() {
            return;
        }
        let response = match response {
            Err(e) => {
                self.logger.error("error parsing resources", &[("err", &e)]);
                self.metrics.fetch_failures_count.get().inc();
                return;
            }
            Ok(None) => {
                // No update needed.
                self.metrics.fetch_skip_update_count.get().inc();
                return;
            }
            Ok(Some(r)) => r,
        };
        let parsed_targets = match (self.parse_resources)(&response.resources, &response.type_url) {
            Ok(t) => t,
            Err(e) => {
                self.logger.error("error parsing resources", &[("err", &e)]);
                self.metrics.fetch_failures_count.get().inc();
                return;
            }
        };
        self.logger.debug("Updated to version", &[("version", &response.version_info), ("targets", &parsed_targets.len().to_string())]);
        ch.send(vec![Arc::new(Group { source: self.source.clone(), targets: parsed_targets, labels: LabelSet::new() })]);
    }
}

// ---------------- kuma ----------------

/// Go `convertKumaV1MonitoringAssignment`.
pub fn convert_kuma_v1_monitoring_assignment(assignment: &MonitoringAssignment) -> Vec<LabelSet> {
    let mut common = convert_kuma_user_labels(&assignment.labels);
    common.insert(KUMA_MESH_LABEL.to_string(), assignment.mesh.clone());
    common.insert(KUMA_SERVICE_LABEL.to_string(), assignment.service.clone());

    let mut targets = Vec::with_capacity(assignment.targets.len());
    for t in &assignment.targets {
        let mut l = convert_kuma_user_labels(&t.labels).merge(&common);
        l.insert(KUMA_DATAPLANE_LABEL.to_string(), t.name.clone());
        l.insert(ADDRESS_LABEL.to_string(), t.address.clone());
        l.insert("instance".to_string(), t.name.clone());
        l.insert("__scheme__".to_string(), t.scheme.clone());
        l.insert("__metrics_path__".to_string(), t.metrics_path.clone());
        targets.push(l);
    }
    targets
}

/// Go `convertKumaUserLabels`.
fn convert_kuma_user_labels(labels: &BTreeMap<String, String>) -> LabelSet {
    let mut ls = LabelSet::new();
    for (k, v) in labels {
        ls.insert(format!("{KUMA_USER_LABEL_PREFIX}{}", crate::sanitize_label_name(k)), v.clone());
    }
    ls
}

/// Go `kumaMadsV1ResourceParser`.
pub fn kuma_mads_v1_resource_parser(resources: &[Any], type_url: &str) -> Result<Vec<LabelSet>, String> {
    if type_url != KUMA_MADS_V1_RESOURCE_TYPE_URL {
        return Err(format!("received invalid typeURL for Kuma MADS v1 Resource: {type_url}"));
    }
    let mut targets = Vec::new();
    for r in resources {
        let assignment: MonitoringAssignment = r.unmarshal_to()?;
        targets.extend(convert_kuma_v1_monitoring_assignment(&assignment));
    }
    Ok(targets)
}

/// Go `NewKumaHTTPDiscovery`.
pub fn new_kuma_http_discovery(conf: &SdConfig, logger: Logger, metrics: &Option<Arc<dyn DiscovererMetrics>>) -> Result<FetchDiscovery, String> {
    let m = crate::downcast_metrics::<XdsMetrics>(metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;

    // Default to "prometheus" if hostname is unavailable.
    let mut client_id = conf.client_id.clone();
    if client_id.is_empty() {
        client_id = match get_fqdn() {
            Ok(h) => h,
            Err(e) => {
                logger.debug("error getting FQDN", &[("err", &e)]);
                "prometheus".to_string()
            }
        };
    }

    let client_config = HttpResourceClientConfig {
        http_client_config: conf.http_client_config.clone(),
        name: String::new(),
        extra_query_params: Some(vec![("fetch-timeout".to_string(), conf.fetch_timeout.go_string())]),
        // Allow 15s of buffer over the timeout sent to the xDS server for connection overhead.
        timeout: Duration::from_nanos((conf.fetch_timeout.nanos() + 15_000_000_000).max(0) as u64),
        resource_type: KUMA_MADS_V1_RESOURCE_TYPE.to_string(),
        resource_type_url: KUMA_MADS_V1_RESOURCE_TYPE_URL.to_string(),
        server: conf.server.clone(),
        client_id,
    };
    let client = HttpResourceClient::new(client_config, PROTOCOL_V3).map_err(|e| format!("kuma_sd: {e}"))?;

    Ok(FetchDiscovery {
        client: Box::new(client),
        logger,
        refresh_interval: conf.refresh_interval.to_std(),
        source: "kuma".to_string(),
        parse_resources: Box::new(kuma_mads_v1_resource_parser),
        metrics: m,
    })
}

// ---------------- osutil ----------------

/// Go `osutil.GetFQDN`: the FQDN when it can be found, otherwise the hostname.
pub fn get_fqdn() -> Result<String, String> {
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname").map_err(|e| e.to_string())?.trim_end_matches('\n').to_string();
    let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(hostname.as_str(), 0)) else {
        // Return the system hostname if we can't look up the IP address.
        return Ok(hostname);
    };
    let hosts = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
    for a in addrs {
        let ip = a.ip();
        for line in hosts.lines() {
            let line = line.split('#').next().unwrap_or("");
            let mut f = line.split_ascii_whitespace();
            let (Some(addr), Some(name)) = (f.next(), f.next()) else { continue };
            if addr.parse::<std::net::IpAddr>().ok() == Some(ip) {
                // Go's hosts file lookup returns absolute names.
                return Ok(if name.ends_with('.') { name.to_string() } else { format!("{name}.") });
            }
        }
    }
    Ok(hostname)
}
