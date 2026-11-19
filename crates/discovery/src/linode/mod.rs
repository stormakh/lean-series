//! Port of `discovery/linode`: Linode instances with their IP addresses and IPv6 ranges, refreshed at an
//! interval. After the first refresh, the account events since the last refresh decide whether a full refresh
//! is needed (at least every 10 polls); a token without the events scope (HTTP 401) turns event polling off.
//!
//! Differs from Go: an instance with a JSON null IPv4 item, null `specs` or null `backups` gets zero values
//! (Go panics on the nil pointer).
pub mod client;
pub mod gojson;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry};
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};
use client::{ListOptions, ip_string};

const LINODE_LABEL: &str = "__meta_linode_";

fn linode_label(name: &str) -> String {
    format!("{LINODE_LABEL}{name}")
}

/// Go `linode.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub tag_separator: String,
    pub region: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { tag_separator: ",".to_string(), port: 80, region: String::new(), refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config() }
}

yaml::yaml_struct!(SdConfig, "linode.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
    (tag_separator: "tag_separator", omitempty)
    (region: "region", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            self.http_client_config.validate().map_err(yaml::Error::Msg)
        })
    }
    fn go_type() -> String {
        "linode.plain".to_string()
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
    const GO_TYPE: &'static str = "linode.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "linode"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        LinodeMetrics::new(reg, rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `linodeMetrics`.
pub struct LinodeMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
    pub failures_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl LinodeMetrics {
    pub fn new(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<LinodeMetrics> {
        let failures_count = CounterVec::new_counter(Opts::new("prometheus_sd_linode_failures_total", "Number of Linode service discovery refresh failures."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![failures_count.clone()]);
        Arc::new(LinodeMetrics { refresh_metrics: rmi, failures_count, registerer })
    }
}

impl DiscovererMetrics for LinodeMetrics {
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

/// Go `t.UTC().Format("2006-01-02T15:04:05")`.
fn format_utc(t: SystemTime) -> String {
    let secs = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => -(e.duration().as_secs_f64().ceil() as i64),
    };
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    // civil from days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", sod / 3600, sod % 3600 / 60, sod % 60)
}

/// The refresh function of the Linode discoverer (Go `Discovery.refresh` and its state).
pub struct LinodeRefresher {
    client: client::Client,
    port: i64,
    region: String,
    tag_separator: String,
    last_refresh_timestamp: SystemTime,
    poll_count: i64,
    last_results: Option<Vec<Arc<Group>>>,
    event_polling_enabled: bool,
    metrics: Arc<LinodeMetrics>,
}

impl Refresher for LinodeRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut needs_refresh = true;
        let ts = SystemTime::now();

        if self.last_results.is_some() && self.event_polling_enabled {
            // Check to see if there have been any events. If so, refresh our data.
            let mut events_opts = ListOptions { page: 1, page_size: 25, filter: format!(r#"{{"created": {{"+gte": "{}"}}}}"#, format_utc(self.last_refresh_timestamp)), ..Default::default() };
            match self.client.list_events(ctx, &mut events_opts) {
                Err(e) => {
                    if !e.ptr || e.code != 401 {
                        return Err(e.to_string());
                    }
                    // If we get a 401, the token doesn't have `events:read_only` scope.
                    // Disable event polling and fallback to doing a full refresh every interval.
                    self.event_polling_enabled = false;
                }
                Ok(events) => {
                    // Event polling tells us changes the Linode API is aware of. Actions issued outside of the
                    // Linode API, such as issuing a `shutdown` at the VM's console instead of using the API to
                    // power off an instance, can potentially cause us to return stale data. Just in case, trigger
                    // a full refresh after ~10 polling intervals of no events.
                    self.poll_count += 1;
                    if events == 0 && self.poll_count < 10 {
                        needs_refresh = false;
                    }
                }
            }
        }

        if needs_refresh {
            let new_data = self.refresh_data(ctx)?;
            self.poll_count = 0;
            self.last_results = Some(new_data);
        }
        self.last_refresh_timestamp = ts;
        Ok(self.last_results.clone().unwrap_or_default())
    }
}

impl LinodeRefresher {
    fn fail(&self, e: client::Error) -> String {
        self.metrics.failures_count.get().inc();
        e.to_string()
    }

    /// Go `refreshData`.
    fn refresh_data(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "Linode".to_string(), ..Default::default() };
        let filter = if self.region.is_empty() { String::new() } else { format!(r#"{{"region": "{}"}}"#, self.region) };
        // We need 3 of these because Linodego writes into the structure during pagination
        let mut list_instances_opts = ListOptions { page_size: 500, filter: filter.clone(), ..Default::default() };
        let mut list_ip_addresses_opts = ListOptions { page_size: 500, filter: filter.clone(), ..Default::default() };
        let mut list_ipv6_ranges_opts = ListOptions { page_size: 500, filter, ..Default::default() };

        // Gather all linode instances.
        let instances = self.client.list_instances(ctx, &mut list_instances_opts).map_err(|e| self.fail(e))?;
        // Gather detailed IP address info for all IPs on all linode instances.
        let detailed_ips = self.client.list_ip_addresses(ctx, &mut list_ip_addresses_opts).map_err(|e| self.fail(e))?;
        // Gather detailed IPv6 Range info for all linode instances.
        let ipv6_range_list = self.client.list_ipv6_ranges(ctx, &mut list_ipv6_ranges_opts).map_err(|e| self.fail(e))?;

        let rdns = |r: &str| if !r.is_empty() && r != "null" { r.to_string() } else { String::new() };
        for instance in &instances {
            if instance.ipv4.is_empty() {
                continue;
            }
            let (mut private_ipv4, mut public_ipv4, mut public_ipv6) = (String::new(), String::new(), String::new());
            let (mut private_ipv4_rdns, mut public_ipv4_rdns, mut public_ipv6_rdns) = (String::new(), String::new(), String::new());
            let mut extra_ips: Vec<String> = Vec::new();
            let mut ipv6_ranges: Vec<String> = Vec::new();

            for ip in instance.ipv4.iter().flatten() {
                let ip = ip_string(ip);
                for detailed_ip in &detailed_ips {
                    if detailed_ip.address != ip {
                        continue;
                    }
                    if detailed_ip.public && public_ipv4.is_empty() {
                        public_ipv4 = detailed_ip.address.clone();
                        public_ipv4_rdns = rdns(&detailed_ip.rdns);
                    } else if !detailed_ip.public && private_ipv4.is_empty() {
                        private_ipv4 = detailed_ip.address.clone();
                        private_ipv4_rdns = rdns(&detailed_ip.rdns);
                    } else {
                        extra_ips.push(detailed_ip.address.clone());
                    }
                }
            }

            if !instance.ipv6.is_empty() {
                let slaac = instance.ipv6.split_once('/').map_or(instance.ipv6.as_str(), |(a, _)| a);
                for detailed_ip in &detailed_ips {
                    if detailed_ip.address != slaac {
                        continue;
                    }
                    public_ipv6 = detailed_ip.address.clone();
                    public_ipv6_rdns = rdns(&detailed_ip.rdns);
                }
                for r in &ipv6_range_list {
                    if r.route_target != slaac {
                        continue;
                    }
                    ipv6_ranges.push(format!("{}/{}", r.range, r.prefix));
                }
            }

            let backups_status = if instance.backups_enabled { "enabled" } else { "disabled" };
            let specs = &instance.specs;
            let mut labels: LabelSet = [
                (linode_label("instance_id"), instance.id.to_string()),
                (linode_label("instance_label"), instance.label.clone()),
                (linode_label("image"), instance.image.clone()),
                (linode_label("private_ipv4"), private_ipv4),
                (linode_label("public_ipv4"), public_ipv4.clone()),
                (linode_label("public_ipv6"), public_ipv6),
                (linode_label("private_ipv4_rdns"), private_ipv4_rdns),
                (linode_label("public_ipv4_rdns"), public_ipv4_rdns),
                (linode_label("public_ipv6_rdns"), public_ipv6_rdns),
                (linode_label("region"), instance.region.clone()),
                (linode_label("type"), instance.type_.clone()),
                (linode_label("status"), instance.status.clone()),
                (linode_label("group"), instance.group.clone()),
                (linode_label("gpus"), specs.gpus.to_string()),
                (linode_label("hypervisor"), instance.hypervisor.clone()),
                (linode_label("backups"), backups_status.to_string()),
                (linode_label("specs_disk_bytes"), (specs.disk << 20).to_string()),
                (linode_label("specs_memory_bytes"), (specs.memory << 20).to_string()),
                (linode_label("specs_vcpus"), specs.vcpus.to_string()),
                (linode_label("specs_transfer_bytes"), (specs.transfer << 20).to_string()),
            ]
            .into_iter()
            .collect();

            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&public_ipv4, self.port as u64));

            if !instance.tags.is_empty() {
                // We surround the separated list with the separator as well. This way regular expressions
                // in relabeling rules don't have to consider tag positions.
                labels.insert(linode_label("tags"), join_tags(&instance.tags, &self.tag_separator));
            }
            if !extra_ips.is_empty() {
                // This instance has more than one of at least one type of IP address (public, private,
                // IPv4,etc. We provide those extra IPs found here just like we do for instance
                // tags, we surround a separated list with the tagSeparator config.
                labels.insert(linode_label("extra_ips"), join_tags(&extra_ips, &self.tag_separator));
            }
            if !ipv6_ranges.is_empty() {
                // This instance has more than one IPv6 Ranges routed to it we provide these
                // Ranges found here just like we do for instance tags, we surround a separated
                // list with the tagSeparator config.
                labels.insert(linode_label("ipv6_ranges"), join_tags(&ipv6_ranges, &self.tag_separator));
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `linode.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<LinodeRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<LinodeMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        let mut client = client::Client::new(rt, conf.refresh_interval.to_std());
        client.set_user_agent(crate::http::USER_AGENT);
        let refresher = LinodeRefresher {
            client,
            port: conf.port,
            region: conf.region.clone(),
            tag_separator: conf.tag_separator.clone(),
            last_refresh_timestamp: SystemTime::now(),
            poll_count: 0,
            last_results: None,
            event_polling_enabled: true,
            metrics: m.clone(),
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "linode",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `d.client.SetBaseURL`.
    pub fn set_base_url(&mut self, url: &str) {
        self.inner.refresher().client.set_base_url(url);
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// The failures counter (Go `d.metrics.failuresCount`).
    pub fn failures_count(&mut self) -> f64 {
        self.inner.refresher().metrics.failures_count.get().get()
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
