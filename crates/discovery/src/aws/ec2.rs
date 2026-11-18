//! Port of `discovery/aws/ec2.go` and `metrics_ec2.go`: EC2 instances as targets (`ec2_sd_configs`).
//!
//! The DescribeAvailabilityZones and DescribeInstances calls go through the shared AWS client (EC2 Query
//! protocol); the response XML is read the way the aws-sdk-go-v2 deserializers read it (element names
//! compared case-insensitively, list members in `item`).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use model::labelset::{LabelSet, META_LABEL_PREFIX};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use super::Filter;
use super::client::{self, Client, ClientConfig};
use super::xml::Element;
use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const EC2_LABEL: &str = "__meta_ec2_";
pub const EC2_LABEL_AMI: &str = "__meta_ec2_ami";
pub const EC2_LABEL_AZ: &str = "__meta_ec2_availability_zone";
pub const EC2_LABEL_AZ_ID: &str = "__meta_ec2_availability_zone_id";
pub const EC2_LABEL_ARCH: &str = "__meta_ec2_architecture";
pub const EC2_LABEL_IPV6_ADDRESSES: &str = "__meta_ec2_ipv6_addresses";
pub const EC2_LABEL_INSTANCE_ID: &str = "__meta_ec2_instance_id";
pub const EC2_LABEL_INSTANCE_LIFECYCLE: &str = "__meta_ec2_instance_lifecycle";
pub const EC2_LABEL_INSTANCE_STATE: &str = "__meta_ec2_instance_state";
pub const EC2_LABEL_INSTANCE_TYPE: &str = "__meta_ec2_instance_type";
pub const EC2_LABEL_OWNER_ID: &str = "__meta_ec2_owner_id";
pub const EC2_LABEL_PLATFORM: &str = "__meta_ec2_platform";
pub const EC2_LABEL_DEFAULT_IPV6_ADDRESS: &str = "__meta_ec2_default_ipv6_address";
pub const EC2_LABEL_PRIMARY_IPV6_ADDRESSES: &str = "__meta_ec2_primary_ipv6_addresses";
pub const EC2_LABEL_PRIMARY_SUBNET_ID: &str = "__meta_ec2_primary_subnet_id";
pub const EC2_LABEL_PRIVATE_DNS: &str = "__meta_ec2_private_dns_name";
pub const EC2_LABEL_PRIVATE_IP: &str = "__meta_ec2_private_ip";
pub const EC2_LABEL_PUBLIC_DNS: &str = "__meta_ec2_public_dns_name";
pub const EC2_LABEL_PUBLIC_IP: &str = "__meta_ec2_public_ip";
pub const EC2_LABEL_REGION: &str = "__meta_ec2_region";
pub const EC2_LABEL_SUBNET_ID: &str = "__meta_ec2_subnet_id";
pub const EC2_LABEL_TAG: &str = "__meta_ec2_tag_";
pub const EC2_LABEL_VPC_ID: &str = "__meta_ec2_vpc_id";
const EC2_LABEL_SEPARATOR: &str = ",";

const _: () = assert!(META_LABEL_PREFIX.len() + 4 == EC2_LABEL.len());

/// Go `EC2SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ec2SdConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: yaml::Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub filters: Vec<Filter>,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultEC2SDConfig`.
pub fn default_ec2_sd_config() -> Ec2SdConfig {
    Ec2SdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config(), ..Default::default() }
}

yaml::yaml_struct!(Ec2SdConfig, "aws.plain", [
    (endpoint: "endpoint")
    (region: "region")
    (access_key: "access_key", omitempty)
    (secret_key: "secret_key", omitempty)
    (profile: "profile", omitempty)
    (role_arn: "role_arn", omitempty)
    (external_id: "external_id", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port")
    (ptrs filters: "filters" as "[]*aws.Filter")
    (inline http_client_config)
]);

impl Unmarshal for Ec2SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_ec2_sd_config();
            // A null filter is a nil *Filter in Go, whose empty values fail the check below (Go dereferences it
            // and panics).
            let nulls = d.inner_struct_nulls(n, self)?;
            if nulls.iter().any(|(k, _)| *k == "filters") || self.filters.iter().any(|f| f.values.is_empty()) {
                return Err(YError::Msg("EC2 SD configuration filter values cannot be empty".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "aws.plain".to_string()
    }
}

impl yaml::Marshal for Ec2SdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for Ec2SdConfig {
    const GO_TYPE: &'static str = "aws.EC2SDConfig";
}

impl Config for Ec2SdConfig {
    fn name(&self) -> &'static str {
        "ec2"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Ec2Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Ec2Metrics::new(rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

impl Ec2SdConfig {
    /// The connection settings for the shared AWS client.
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            profile: self.profile.clone(),
            role_arn: self.role_arn.clone(),
            external_id: self.external_id.clone(),
            http_client_config: self.http_client_config.clone(),
        }
    }
}

/// Go `ec2Metrics`.
pub struct Ec2Metrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl Ec2Metrics {
    pub fn new(rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<Ec2Metrics> {
        Arc::new(Ec2Metrics { refresh_metrics: rmi })
    }
}

impl DiscovererMetrics for Ec2Metrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// ---- reading the EC2 Query XML like the SDK deserializers ----

/// The last child with this name, compared case-insensitively (the SDK uses `strings.EqualFold`; a repeated
/// member overwrites the earlier one).
pub(crate) fn xchild<'a>(e: &'a Element, name: &str) -> Option<&'a Element> {
    e.children.iter().rev().find(|c| c.name.eq_ignore_ascii_case(name))
}

/// The text of a string member (Go: a non-nil `*string`, empty for `<x/>`).
pub(crate) fn xstr<'a>(e: &'a Element, name: &str) -> Option<&'a str> {
    xchild(e, name).map(|c| c.text.as_str())
}

/// The `item`s of every list wrapper with this name (the SDK appends repeated wrappers).
pub(crate) fn xitems<'a>(e: &'a Element, name: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
    e.children.iter().filter(move |c| c.name.eq_ignore_ascii_case(name)).flat_map(|w| w.children.iter().filter(|i| i.name.eq_ignore_ascii_case("item")))
}

/// An int32 member: Go `strconv.ParseInt(v, 10, 64)` then `int32(i64)`.
fn xint32(e: &Element, name: &str) -> Result<Option<i32>, String> {
    match xstr(e, name) {
        None => Ok(None),
        Some(v) => match v.parse::<i64>() {
            Ok(i) => Ok(Some(i as i32)),
            Err(err) => {
                let kind = if matches!(err.kind(), std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow) { "value out of range" } else { "invalid syntax" };
                Err(format!("strconv.ParseInt: parsing {}: {kind}", yaml::go_quote(v)))
            }
        },
    }
}

/// A boolean member: Go `strconv.ParseBool`.
fn xbool(e: &Element, name: &str) -> Result<Option<bool>, String> {
    match xstr(e, name) {
        None => Ok(None),
        Some("1" | "t" | "T" | "TRUE" | "true" | "True") => Ok(Some(true)),
        Some("0" | "f" | "F" | "FALSE" | "false" | "False") => Ok(Some(false)),
        Some(_) => Err("expected Boolean to be of type *bool, got xml.CharData instead".to_string()),
    }
}

/// The members of `ec2Types.Instance` that discovery reads.
#[derive(Debug, Default)]
struct Instance {
    architecture: String,
    image_id: Option<String>,
    instance_id: Option<String>,
    instance_lifecycle: String,
    instance_type: String,
    /// Placement present, and its availability zone.
    placement: Option<Option<String>>,
    platform: String,
    private_dns_name: Option<String>,
    private_ip_address: Option<String>,
    public_dns_name: Option<String>,
    public_ip_address: Option<String>,
    /// State present, and its name.
    state: Option<String>,
    subnet_id: Option<String>,
    vpc_id: Option<String>,
    network_interfaces: Vec<NetworkInterface>,
    tags: Vec<(Option<String>, Option<String>)>,
}

#[derive(Debug, Default)]
struct NetworkInterface {
    subnet_id: Option<String>,
    /// Attachment present, and its device index.
    attachment: Option<Option<i32>>,
    ipv6_addresses: Vec<(Option<String>, Option<bool>)>,
}

fn own(s: Option<&str>) -> Option<String> {
    s.map(str::to_string)
}

fn parse_instance(e: &Element) -> Result<Instance, String> {
    let mut nis = Vec::new();
    for ni in xitems(e, "networkInterfaceSet") {
        let attachment = match xchild(ni, "attachment") {
            None => None,
            Some(a) => Some(xint32(a, "deviceIndex")?),
        };
        let mut ipv6 = Vec::new();
        for a in xitems(ni, "ipv6AddressesSet") {
            ipv6.push((own(xstr(a, "ipv6Address")), xbool(a, "isPrimaryIpv6")?));
        }
        nis.push(NetworkInterface { subnet_id: own(xstr(ni, "subnetId")), attachment, ipv6_addresses: ipv6 });
    }
    Ok(Instance {
        architecture: xstr(e, "architecture").unwrap_or("").to_string(),
        image_id: own(xstr(e, "imageId")),
        instance_id: own(xstr(e, "instanceId")),
        instance_lifecycle: xstr(e, "instanceLifecycle").unwrap_or("").to_string(),
        instance_type: xstr(e, "instanceType").unwrap_or("").to_string(),
        placement: xchild(e, "placement").map(|p| own(xstr(p, "availabilityZone"))),
        platform: xstr(e, "platform").unwrap_or("").to_string(),
        private_dns_name: own(xstr(e, "privateDnsName")),
        private_ip_address: own(xstr(e, "privateIpAddress")),
        public_dns_name: own(xstr(e, "dnsName")),
        public_ip_address: own(xstr(e, "ipAddress")),
        state: xchild(e, "instanceState").map(|s| xstr(s, "name").unwrap_or("").to_string()),
        subnet_id: own(xstr(e, "subnetId")),
        vpc_id: own(xstr(e, "vpcId")),
        network_interfaces: nis,
        tags: xitems(e, "tagSet").map(|t| (own(xstr(t, "key")), own(xstr(t, "value")))).collect(),
    })
}

/// Go `net.JoinHostPort`.
pub(crate) fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// Go `strutil.SanitizeLabelName`: every character outside `[a-zA-Z0-9_]` becomes `_` (an invalid UTF-8 byte
/// too, one `_` per byte, as Go's regexp replaces each).
pub(crate) fn sanitize_label_name(name: &str) -> String {
    name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Whether an SDK error is the API error `code` (Go `errors.As(err, &smithy.APIError)` and `ErrorCode()`).
pub(crate) fn is_api_error(err: &str, code: &str) -> bool {
    err.contains(&format!("api error {code}: "))
}

/// Go `getInstanceIPv6Addresses`: the default IPv6 address, the primary addresses by device index, and all
/// addresses (`None` = a nil slice).
fn get_instance_ipv6_addresses(i: &Instance) -> (Option<String>, Option<Vec<String>>, Option<Vec<String>>) {
    let mut primary: Option<Vec<String>> = None;
    let mut all: Option<Vec<String>> = None;
    if i.vpc_id.is_some() {
        for eni in &i.network_interfaces {
            if eni.subnet_id.is_none() {
                continue;
            }
            for (addr, is_primary) in &eni.ipv6_addresses {
                let Some(addr) = addr else { continue };
                all.get_or_insert_with(Vec::new).push(addr.clone());
                if !is_primary.unwrap_or(false) {
                    continue;
                }
                let Some(Some(idx)) = eni.attachment else { continue };
                if idx < 0 {
                    continue;
                }
                let p = primary.get_or_insert_with(Vec::new);
                while p.len() as i64 <= idx as i64 {
                    p.push(String::new());
                }
                p[idx as usize] = addr.clone();
            }
        }
        let first = primary.iter().flatten().chain(all.iter().flatten()).find(|a| !a.is_empty()).cloned();
        if first.is_some() {
            return (first, primary, all);
        }
    }
    (None, primary, all)
}

fn join_sep(v: &[String]) -> String {
    format!("{EC2_LABEL_SEPARATOR}{}{EC2_LABEL_SEPARATOR}", v.join(EC2_LABEL_SEPARATOR))
}

/// The refresh state of the EC2 discoverer (Go `EC2Discovery` without the embedded refresh.Discovery).
pub struct Ec2Refresher {
    logger: Logger,
    cfg: Ec2SdConfig,
    client: Option<Client>,
    /// The resolved region (set when the client is built).
    region: String,
    /// This account's availability zones to their AZ IDs; `None` until DescribeAvailabilityZones succeeded.
    az_to_az_id: Option<HashMap<String, String>>,
}

impl Ec2Refresher {
    /// Go `ec2Client`: builds the client on first use and keeps it.
    fn ec2_client(&mut self) -> Result<(), String> {
        if self.client.is_some() {
            return Ok(());
        }
        let c = Client::new(&self.cfg.client_config(), client::EC2, "ec2_sd")?;
        self.region = c.region().to_string();
        self.client = Some(c);
        Ok(())
    }

    /// Go `refreshAZIDs`.
    pub fn refresh_az_ids(&mut self, ctx: &CancelToken) -> Result<(), String> {
        self.ec2_client()?;
        let Some(c) = &self.client else { return Ok(()) };
        let root = c.query(ctx, "DescribeAvailabilityZones", &[])?;
        let mut m = HashMap::new();
        for az in xitems(&root, "availabilityZoneInfo") {
            if let (Some(name), Some(id)) = (xstr(az, "zoneName"), xstr(az, "zoneId")) {
                m.insert(name.to_string(), id.to_string());
            }
        }
        self.az_to_az_id = Some(m);
        Ok(())
    }

    /// The AZ name to AZ ID map (Go `d.azToAZID`; `None` = nil).
    pub fn az_to_az_id(&self) -> Option<&HashMap<String, String>> {
        self.az_to_az_id.as_ref()
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.ec2_client()?;
        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: self.region.clone() };

        // Only refresh the AZ ID map if we have never been able to build one.
        // Prometheus requires a reload if AWS adds a new AZ to the region.
        if self.az_to_az_id.is_none()
            && let Err(e) = self.refresh_az_ids(ctx)
        {
            self.logger.debug("Unable to describe availability zones", &[("err", &e)]);
        }

        let mut filter_params: Vec<(String, String)> = Vec::new();
        for (i, f) in self.cfg.filters.iter().enumerate() {
            filter_params.push((format!("Filter.{}.Name", i + 1), f.name.clone()));
            for (j, v) in f.values.iter().enumerate() {
                filter_params.push((format!("Filter.{}.Value.{}", i + 1, j + 1), v.clone()));
            }
        }

        let mut next_token: Option<String> = None;
        let mut first = true;
        // Go `NewDescribeInstancesPaginator`: a first page, then while the next token is non-empty.
        while first || next_token.as_deref().is_some_and(|t| !t.is_empty()) {
            first = false;
            let mut params = filter_params.clone();
            if let Some(t) = &next_token {
                params.push(("NextToken".to_string(), t.clone()));
            }
            let Some(c) = &self.client else { break };
            let page = c.query(ctx, "DescribeInstances", &params).and_then(|root| parse_instances_page(&root).map_err(|e| format!("operation error EC2: DescribeInstances, deserialization failed, failed to decode response body, {e}")));
            let (token, reservations) = match page {
                Ok(p) => p,
                Err(e) => {
                    if is_api_error(&e, "AuthFailure") || is_api_error(&e, "UnauthorizedOperation") {
                        self.client = None;
                    }
                    return Err(format!("could not describe instances: {e}"));
                }
            };
            next_token = token;
            for (owner_id, instances) in &reservations {
                for inst in instances {
                    if let Some(labels) = self.instance_labels(owner_id.as_deref(), inst) {
                        tg.targets.push(labels);
                    }
                }
            }
        }
        Ok(vec![Arc::new(tg)])
    }

    fn instance_labels(&self, owner_id: Option<&str>, inst: &Instance) -> Option<LabelSet> {
        let (default_ipv6, primary_ipv6, ipv6) = get_instance_ipv6_addresses(inst);
        if inst.private_ip_address.is_none() && default_ipv6.is_none() {
            return None;
        }
        let mut labels = LabelSet::new();
        let mut set = |k: &str, v: &str| {
            labels.insert(k.to_string(), v.to_string());
        };
        set(EC2_LABEL_REGION, &self.region);
        if let Some(v) = &inst.instance_id {
            set(EC2_LABEL_INSTANCE_ID, v);
        }
        if let Some(v) = owner_id {
            set(EC2_LABEL_OWNER_ID, v);
        }
        if let Some(v) = &default_ipv6 {
            set(EC2_LABEL_DEFAULT_IPV6_ADDRESS, v);
        }
        match (&inst.private_ip_address, &default_ipv6) {
            (Some(ip), _) => {
                set(EC2_LABEL_PRIVATE_IP, ip);
                set("__address__", &join_host_port(ip, self.cfg.port));
            }
            (None, Some(ip6)) => set("__address__", &join_host_port(ip6, self.cfg.port)),
            (None, None) => {}
        }
        if let Some(v) = &inst.private_dns_name {
            set(EC2_LABEL_PRIVATE_DNS, v);
        }
        if !inst.platform.is_empty() {
            set(EC2_LABEL_PLATFORM, &inst.platform);
        }
        if let Some(v) = &inst.public_ip_address {
            set(EC2_LABEL_PUBLIC_IP, v);
            if let Some(d) = &inst.public_dns_name {
                set(EC2_LABEL_PUBLIC_DNS, d);
            }
        }
        if let Some(p) = &primary_ipv6 {
            set(EC2_LABEL_PRIMARY_IPV6_ADDRESSES, &join_sep(p));
        }
        if let Some(a) = &ipv6 {
            set(EC2_LABEL_IPV6_ADDRESSES, &join_sep(a));
        }
        if let Some(v) = &inst.image_id {
            set(EC2_LABEL_AMI, v);
        }
        // The availability zone ID is looked up by zone name, so both labels are omitted when the placement is
        // absent.
        if let Some(Some(az)) = &inst.placement {
            set(EC2_LABEL_AZ, az);
            let az_id = self.az_to_az_id.as_ref().and_then(|m| m.get(az));
            if az_id.is_none() && self.az_to_az_id.is_some() {
                self.logger.debug("Availability zone ID not found", &[("az", az)]);
            }
            set(EC2_LABEL_AZ_ID, az_id.map(String::as_str).unwrap_or(""));
        }
        if let Some(s) = &inst.state {
            set(EC2_LABEL_INSTANCE_STATE, s);
        }
        set(EC2_LABEL_INSTANCE_TYPE, &inst.instance_type);
        if !inst.instance_lifecycle.is_empty() {
            set(EC2_LABEL_INSTANCE_LIFECYCLE, &inst.instance_lifecycle);
        }
        if !inst.architecture.is_empty() {
            set(EC2_LABEL_ARCH, &inst.architecture);
        }
        if let Some(vpc) = &inst.vpc_id {
            set(EC2_LABEL_VPC_ID, vpc);
            if let Some(s) = &inst.subnet_id {
                set(EC2_LABEL_PRIMARY_SUBNET_ID, s);
            }
            // Deduplicate VPC Subnet IDs maintaining the order of the subnets returned by EC2.
            let mut seen = HashSet::new();
            let subnets: Vec<String> = inst.network_interfaces.iter().filter_map(|eni| eni.subnet_id.clone()).filter(|s| seen.insert(s.clone())).collect();
            set(EC2_LABEL_SUBNET_ID, &join_sep(&subnets));
        }
        for (k, v) in &inst.tags {
            let (Some(k), Some(v)) = (k, v) else { continue };
            set(&format!("{EC2_LABEL_TAG}{}", sanitize_label_name(k)), v);
        }
        Some(labels)
    }
}

/// One DescribeInstances page: the next token and the reservations (owner ID, instances).
type Page = (Option<String>, Vec<(Option<String>, Vec<Instance>)>);

fn parse_instances_page(root: &Element) -> Result<Page, String> {
    let mut reservations = Vec::new();
    for r in xitems(root, "reservationSet") {
        let mut instances = Vec::new();
        for i in xitems(r, "instancesSet") {
            instances.push(parse_instance(i)?);
        }
        reservations.push((own(xstr(r, "ownerId")), instances));
    }
    Ok((own(xstr(root, "nextToken")), reservations))
}

impl Refresher for Ec2Refresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        Ec2Refresher::refresh(self, ctx)
    }
}

/// Go `EC2Discovery`.
pub struct Ec2Discovery {
    inner: refresh::Discovery<Ec2Refresher>,
}

impl Ec2Discovery {
    /// Go `NewEC2Discovery`.
    pub fn new(conf: &Ec2SdConfig, opts: DiscovererOptions) -> Result<Ec2Discovery, String> {
        let m = crate::downcast_metrics::<Ec2Metrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = Ec2Refresher { logger: opts.logger.clone(), cfg: conf.clone(), client: None, region: String::new(), az_to_az_id: None };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "ec2",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Ec2Discovery { inner })
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// Go `refreshAZIDs` (builds the client first if needed).
    pub fn refresh_az_ids(&mut self, ctx: &CancelToken) -> Result<(), String> {
        self.inner.refresher().refresh_az_ids(ctx)
    }

    /// Go `d.azToAZID`.
    pub fn az_to_az_id(&mut self) -> Option<HashMap<String, String>> {
        self.inner.refresher().az_to_az_id().cloned()
    }
}

impl Discoverer for Ec2Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
