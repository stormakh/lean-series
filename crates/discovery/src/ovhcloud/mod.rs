//! Port of `discovery/ovhcloud`: VPS and dedicated servers of an OVHcloud account, refreshed at an interval.
//! The go-ovh client subset is in [`client`].
use std::net::IpAddr;
use std::sync::Arc;

use model::labelset::META_LABEL_PREFIX;
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub mod client;
pub mod dedicated_server;
pub mod vps;

pub use dedicated_server::DedicatedServerDiscovery;
pub use vps::VpsDiscovery;

/// Go `metaLabelPrefix`.
pub fn meta_label_prefix() -> String {
    format!("{META_LABEL_PREFIX}ovhcloud_")
}

/// Go `ovhcloud.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub endpoint: String,
    pub application_key: String,
    pub application_secret: Secret,
    pub consumer_key: Secret,
    pub refresh_interval: yaml::Duration,
    pub service: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { endpoint: "ovh-eu".to_string(), refresh_interval: yaml::Duration::from_secs(60), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "ovhcloud.plain", [
    (endpoint: "endpoint")
    (application_key: "application_key")
    (application_secret: "application_secret")
    (consumer_key: "consumer_key")
    (refresh_interval: "refresh_interval")
    (service: "service")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.endpoint.is_empty() {
                return Err(YError::Msg("endpoint can not be empty".into()));
            }
            if self.application_key.is_empty() {
                return Err(YError::Msg("application key can not be empty".into()));
            }
            if self.application_secret.0.is_empty() {
                return Err(YError::Msg("application secret can not be empty".into()));
            }
            if self.consumer_key.0.is_empty() {
                return Err(YError::Msg("consumer key can not be empty".into()));
            }
            match self.service.as_str() {
                "dedicated_server" | "vps" => Ok(()),
                s => Err(YError::Msg(format!("unknown service: {s}"))),
            }
        })
    }
    fn go_type() -> String {
        "ovhcloud.plain".to_string()
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
    const GO_TYPE: &'static str = "ovhcloud.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "ovhcloud"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_discovery(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(OvhcloudMetrics { refresh_metrics: rmi })
    }
}

/// Go `ovhcloudMetrics`.
pub struct OvhcloudMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for OvhcloudMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `createClient`.
pub fn create_client(config: &SdConfig) -> Result<client::Client, String> {
    client::new_client(&config.endpoint, &config.application_key, &config.application_secret.0, &config.consumer_key.0)
}

/// A `netip.Addr` (an address with an optional IPv6 zone).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Addr {
    pub ip: IpAddr,
    pub zone: String,
}

impl Addr {
    /// Go `Addr.Is4` (IPv4, not IPv4-mapped IPv6).
    pub fn is4(&self) -> bool {
        self.ip.is_ipv4()
    }
    /// Go `Addr.Is6`.
    pub fn is6(&self) -> bool {
        self.ip.is_ipv6()
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ip)?;
        if !self.zone.is_empty() {
            write!(f, "%{}", self.zone)?;
        }
        Ok(())
    }
}

/// Go `netip.ParseAddr` (dotted IPv4 without leading zeros, IPv6 with an optional zone).
pub fn parse_addr(s: &str) -> Option<Addr> {
    if let Some((ip, zone)) = s.split_once('%') {
        if zone.is_empty() {
            return None;
        }
        let ip: std::net::Ipv6Addr = ip.parse().ok()?;
        return Some(Addr { ip: IpAddr::V6(ip), zone: zone.to_string() });
    }
    s.parse::<IpAddr>().ok().map(|ip| Addr { ip, zone: String::new() })
}

/// Go `netip.ParsePrefix`: the address and the bit count.
pub fn parse_prefix(s: &str) -> Option<(Addr, u8)> {
    let (a, bits) = s.rsplit_once('/')?;
    if a.contains('%') {
        return None;
    }
    let addr = parse_addr(a)?;
    if bits.is_empty() || !bits.bytes().all(|c| c.is_ascii_digit()) || (bits.len() > 1 && bits.starts_with('0')) {
        return None;
    }
    let n: u32 = bits.parse().ok()?;
    let max = if addr.is4() { 32 } else { 128 };
    if n > max {
        return None;
    }
    Some((addr, n as u8))
}

/// Go `parseIPList`.
pub fn parse_ip_list(ip_list: &[&str]) -> Result<Vec<Addr>, String> {
    let mut out = Vec::new();
    for ip in ip_list {
        let addr = match parse_addr(ip) {
            Some(a) => a,
            None => {
                let Some((a, bits)) = parse_prefix(ip) else {
                    return Err("could not parse IP addresses from list".to_string());
                };
                if bits != 32 {
                    continue;
                }
                a
            }
        };
        if !addr.ip.is_unspecified() {
            out.push(addr);
        }
    }
    if out.is_empty() {
        return Err("could not parse IP addresses from list".to_string());
    }
    Ok(out)
}

/// The IPv4 and IPv6 labels and the address: the last of each family, IPv4 preferred.
pub(crate) fn pick_ips(ips: &[Addr]) -> (String, String, String) {
    let (mut v4, mut v6) = (String::new(), String::new());
    for ip in ips {
        if ip.is4() {
            v4 = ip.to_string();
        }
        if ip.is6() {
            v6 = ip.to_string();
        }
    }
    let default = if v4.is_empty() { v6.clone() } else { v4.clone() };
    (v4, v6, default)
}

/// Go `path.Join` of the API path and the escaped name (with the extra elements).
pub(crate) fn join_path(elems: &[&str]) -> String {
    let joined = elems.iter().filter(|e| !e.is_empty()).copied().collect::<Vec<_>>().join("/");
    if joined.is_empty() {
        return String::new();
    }
    clean_path(&joined)
}

/// Go `path.Clean`.
pub(crate) fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|s| *s != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            s => out.push(s),
        }
    }
    let body = out.join("/");
    match (rooted, body.is_empty()) {
        (true, _) => format!("/{body}"),
        (false, true) => ".".to_string(),
        (false, false) => body,
    }
}

/// The refresher of the configured service (Go `refresher`).
pub enum OvhRefresher {
    Vps(VpsDiscovery),
    DedicatedServer(DedicatedServerDiscovery),
}

impl Refresher for OvhRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        match self {
            OvhRefresher::Vps(d) => d.refresh(ctx),
            OvhRefresher::DedicatedServer(d) => d.refresh(ctx),
        }
    }
}

/// Go `newRefresher`.
pub fn new_refresher(conf: &SdConfig, logger: Logger) -> Result<OvhRefresher, String> {
    match conf.service.as_str() {
        "vps" => Ok(OvhRefresher::Vps(VpsDiscovery::new(conf, logger))),
        "dedicated_server" => Ok(OvhRefresher::DedicatedServer(DedicatedServerDiscovery::new(conf, logger))),
        s => Err(format!("unknown OVHcloud discovery service '{s}'")),
    }
}

/// The OVHcloud discoverer (Go `refresh.Discovery` from `NewDiscovery`).
pub struct Discovery {
    inner: refresh::Discovery<OvhRefresher>,
}

/// Go `NewDiscovery`.
pub fn new_discovery(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
    let m = crate::downcast_metrics::<OvhcloudMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
    let r = new_refresher(conf, opts.logger.clone())?;
    let inner = refresh::Discovery::new(refresh::Options {
        logger: opts.logger,
        mech: "ovhcloud",
        set_name: &opts.set_name,
        interval: conf.refresh_interval.to_std(),
        refresh_f: r,
        metrics_instantiator: m.refresh_metrics.as_ref(),
    });
    Ok(Discovery { inner })
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
