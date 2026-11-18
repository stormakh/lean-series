//! Port of `discovery/oci`: Oracle Cloud Infrastructure compute instances as targets, refreshed at an interval.
//!
//! The OCI Go SDK is replaced by a small blocking client (`client.rs`): the same REST calls, query parameters,
//! headers, request signature, default retry policy, pagination and error texts.
//!
//! Differs from Go:
//! - `auth: instance_principal` fails when the discoverer is created: it needs an ephemeral RSA session key
//!   pair and the allowed crates cannot generate RSA keys.
//! - Encrypted keys: PKCS#8 PBES2 (PBKDF2 + AES-CBC) and legacy PEM with AES-CBC are decrypted; DES/3DES,
//!   AES-GCM and scrypt-encrypted keys fail with a clear error. ring signs only with RSA keys of 2048 bits
//!   or more.
//! - A request in flight is not aborted when the discoverer is cancelled; it ends at its timeout (the refresh
//!   interval, at least 60 seconds). Waits (rate limiter, retry backoff) end at once.
//! - The SDK's developer tool configuration file, instance metadata region lookup and circuit breaker are
//!   not ported (all off by default in the SDK).
pub mod client;
pub mod key;
pub mod regions;

use std::sync::Arc;
use std::time::Duration;

use json::Json;
use model::labelset::LabelSet;
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

pub use client::{Compartment, Endpoints, Instance, Vnic, VnicAttachment};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const OCI_LABEL_INSTANCE_ID: &str = "__meta_oci_instance_id";
pub const OCI_LABEL_INSTANCE_NAME: &str = "__meta_oci_instance_name";
pub const OCI_LABEL_INSTANCE_STATE: &str = "__meta_oci_instance_state";
pub const OCI_LABEL_INSTANCE_SHAPE: &str = "__meta_oci_instance_shape";
pub const OCI_LABEL_AVAILABILITY_DOMAIN: &str = "__meta_oci_availability_domain";
pub const OCI_LABEL_FAULT_DOMAIN: &str = "__meta_oci_fault_domain";
pub const OCI_LABEL_REGION: &str = "__meta_oci_region";
pub const OCI_LABEL_TENANCY_ID: &str = "__meta_oci_tenancy_id";
pub const OCI_LABEL_COMPARTMENT_ID: &str = "__meta_oci_compartment_id";
pub const OCI_LABEL_IMAGE_ID: &str = "__meta_oci_image_id";
pub const OCI_LABEL_VNIC_ID: &str = "__meta_oci_vnic_id";
pub const OCI_LABEL_PRIVATE_IP: &str = "__meta_oci_private_ip";
pub const OCI_LABEL_PUBLIC_IP: &str = "__meta_oci_public_ip";
pub const OCI_LABEL_IPV6_ADDRESSES: &str = "__meta_oci_ipv6_addresses";
pub const OCI_LABEL_HOSTNAME_LABEL: &str = "__meta_oci_hostname_label";
pub const OCI_LABEL_FREEFORM_TAG_PREFIX: &str = "__meta_oci_tag_";
pub const OCI_LABEL_DEFINED_TAG_PREFIX: &str = "__meta_oci_defined_tag_";

pub const AUTH_API_KEY: &str = "api_key";
pub const AUTH_INSTANCE_PRINCIPAL: &str = "instance_principal";

/// Go `rateLimitRPS`: OCI API calls per second across all operations.
pub const RATE_LIMIT_RPS: f64 = 10.0;
/// Go `rateLimitBurst`.
pub const RATE_LIMIT_BURST: usize = 50;

/// Go `oci.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub auth: String,
    pub tenancy: String,
    pub user: String,
    pub fingerprint: String,
    pub key_file: String,
    pub key_passphrase: yaml::Secret,
    pub key_passphrase_file: String,
    pub region: String,
    pub compartments: Vec<String>,
    pub http_client_config: HttpClientConfig,
    pub port: i64,
    pub refresh_interval: yaml::Duration,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        port: 80,
        refresh_interval: yaml::Duration::from_secs(60),
        auth: AUTH_API_KEY.to_string(),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "oci.plain", [
    (auth: "auth", omitempty)
    (tenancy: "tenancy", omitempty)
    (user: "user", omitempty)
    (fingerprint: "fingerprint", omitempty)
    (key_file: "key_file", omitempty)
    (key_passphrase: "key_passphrase", omitempty)
    (key_passphrase_file: "key_passphrase_file", omitempty)
    (region: "region")
    (compartments: "compartments", omitempty)
    (inline http_client_config)
    (port: "port")
    (refresh_interval: "refresh_interval")
]);

impl SdConfig {
    /// Go `SDConfig.validate`.
    pub fn validate(&mut self) -> Result<(), String> {
        if self.region.is_empty() {
            return Err("oci_sd: region is required".to_string());
        }
        match self.auth.as_str() {
            AUTH_API_KEY => {
                if self.tenancy.is_empty() {
                    return Err("oci_sd: api_key auth requires tenancy".to_string());
                }
                if self.user.is_empty() {
                    return Err("oci_sd: api_key auth requires user".to_string());
                }
                if self.fingerprint.is_empty() {
                    return Err("oci_sd: api_key auth requires fingerprint".to_string());
                }
                if self.key_file.is_empty() {
                    return Err("oci_sd: api_key auth requires key_file".to_string());
                }
                if !self.key_passphrase.is_empty() && !self.key_passphrase_file.is_empty() {
                    return Err("oci_sd: at most one of key_passphrase and key_passphrase_file must be configured".to_string());
                }
            }
            AUTH_INSTANCE_PRINCIPAL => {
                if !self.tenancy.is_empty() || !self.user.is_empty() || !self.fingerprint.is_empty() || !self.key_file.is_empty() || !self.key_passphrase.is_empty() || !self.key_passphrase_file.is_empty() {
                    return Err("oci_sd: instance_principal auth must not have tenancy, user, fingerprint, key_file, key_passphrase, or key_passphrase_file set".to_string());
                }
            }
            other => {
                return Err(format!(
                    "oci_sd: unknown auth method {}, expected {} or {}",
                    yaml::go_quote(other),
                    yaml::go_quote(AUTH_API_KEY),
                    yaml::go_quote(AUTH_INSTANCE_PRINCIPAL)
                ));
            }
        }
        for (i, cid) in self.compartments.iter().enumerate() {
            if cid.is_empty() {
                return Err(format!("oci_sd: compartments[{i}] must not be empty"));
            }
        }
        if self.refresh_interval.nanos() <= 0 {
            return Err(format!("oci_sd: refresh_interval must be positive, got {}", self.refresh_interval));
        }
        self.http_client_config.validate()
    }
}

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "oci.plain".to_string()
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
    const GO_TYPE: &'static str = "oci.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "oci"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(OciMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
        self.key_file = net::config::join_dir(dir, &self.key_file);
        self.key_passphrase_file = net::config::join_dir(dir, &self.key_passphrase_file);
    }
}

/// Go `ociMetrics` (`metrics.go`): only the refresh metrics.
pub struct OciMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for OciMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `resolveKeyPassphrase`: the inline passphrase, or the passphrase file without trailing CR/LF.
pub fn resolve_key_passphrase(conf: &SdConfig) -> Result<String, String> {
    if !conf.key_passphrase_file.is_empty() {
        let b = net::types::read_file(&conf.key_passphrase_file).map_err(|e| format!("reading OCI key passphrase file {}: {}", yaml::go_quote(&conf.key_passphrase_file), e))?;
        return Ok(String::from_utf8_lossy(&b).trim_end_matches(['\r', '\n']).to_string());
    }
    Ok(conf.key_passphrase.as_str().to_string())
}

/// Go `compartmentLister`.
pub type CompartmentLister = Box<dyn FnMut(&CancelToken) -> Result<Vec<String>, String> + Send>;
/// Go `instancesLister`.
pub type InstancesLister = Box<dyn FnMut(&CancelToken, &str) -> Result<Vec<Instance>, String> + Send>;
/// Go `vnicAttachmentLister`.
pub type VnicAttachmentLister = Box<dyn FnMut(&CancelToken, &str, &str) -> Result<Vec<VnicAttachment>, String> + Send>;
/// Go `vnicFetcher` (`Ok(None)` is a nil VNIC).
pub type VnicFetcher = Box<dyn FnMut(&CancelToken, &str) -> Result<Option<Vnic>, String> + Send>;

/// Go `compartmentPaginator`: one page of direct children of a compartment, and the next page token.
pub type CompartmentPaginator<'a> = dyn FnMut(&CancelToken, &str, Option<&str>) -> Result<(Vec<Compartment>, Option<String>), String> + 'a;

/// Go `listAllCompartments`: every active compartment reachable from `root` (breadth first, root included).
/// A compartment whose children cannot be listed is logged and skipped; a cancelled `ctx` aborts the walk.
pub fn list_all_compartments(ctx: &CancelToken, paginate: &mut CompartmentPaginator<'_>, root: &str, logger: &Logger) -> Result<Vec<String>, String> {
    let mut all = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::from([root.to_string()]);
    while let Some(current) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }
        all.push(current.clone());
        let mut page: Option<String> = None;
        loop {
            let (items, next) = match paginate(ctx, &current, page.as_deref()) {
                Ok(r) => r,
                Err(e) => {
                    if ctx.is_cancelled() {
                        return Err(client::CONTEXT_CANCELED.to_string());
                    }
                    logger.warn("OCI SD failed to list child compartments, skipping subtree", &[("compartment_id", &current), ("err", &e)]);
                    break;
                }
            };
            for comp in items {
                if let Some(id) = comp.id
                    && comp.lifecycle_state == "ACTIVE"
                    && !visited.contains(&id)
                {
                    queue.push_back(id);
                }
            }
            match next {
                Some(n) => page = Some(n),
                None => break,
            }
        }
    }
    Ok(all)
}

/// Go `vnicInfo`: the subset of a VNIC exposed through meta labels.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VnicInfo {
    pub id: String,
    pub private_ip: String,
    pub public_ip: String,
    pub ipv6_addresses: Vec<String>,
    pub hostname_label: String,
}

/// Go `instanceVnics`: the primary VNIC of an instance.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstanceVnics {
    pub primary: VnicInfo,
}

/// The `vnicInfo` of a primary VNIC: OCI does not order the IPv6 addresses, so a sorted copy is kept.
pub fn primary_vnic_info(v: &Vnic) -> VnicInfo {
    let mut ipv6 = v.ipv6_addresses.clone();
    ipv6.sort();
    VnicInfo {
        id: v.id.clone().unwrap_or_default(),
        private_ip: v.private_ip.clone().unwrap_or_default(),
        public_ip: v.public_ip.clone().unwrap_or_default(),
        ipv6_addresses: ipv6,
        hostname_label: v.hostname_label.clone().unwrap_or_default(),
    }
}

/// Go `strutil.SanitizeLabelName`: every character outside `[a-zA-Z0-9_]` becomes `_`.
pub fn sanitize_label_name(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Go `net.JoinHostPort`.
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// Go `instanceLabels`: the label set of one instance.
pub fn instance_labels(inst: &Instance, vnics: &InstanceVnics, tenancy: &str, port: i64) -> LabelSet {
    let p = &vnics.primary;
    let mut addr = p.private_ip.as_str();
    if addr.is_empty() {
        addr = &p.public_ip;
    }
    if addr.is_empty()
        && let Some(first) = p.ipv6_addresses.first()
    {
        addr = first;
    }
    let s = |v: &Option<String>| v.clone().unwrap_or_default();
    let mut labels = LabelSet::new();
    let mut set = |k: &str, v: String| {
        labels.0.insert(k.to_string(), v);
    };
    set(model::labelset::ADDRESS_LABEL, join_host_port(addr, &port.to_string()));
    set(OCI_LABEL_INSTANCE_ID, s(&inst.id));
    set(OCI_LABEL_INSTANCE_NAME, s(&inst.display_name));
    set(OCI_LABEL_INSTANCE_STATE, inst.lifecycle_state.clone());
    set(OCI_LABEL_INSTANCE_SHAPE, s(&inst.shape));
    set(OCI_LABEL_AVAILABILITY_DOMAIN, s(&inst.availability_domain));
    set(OCI_LABEL_FAULT_DOMAIN, s(&inst.fault_domain));
    set(OCI_LABEL_REGION, s(&inst.region));
    set(OCI_LABEL_TENANCY_ID, tenancy.to_string());
    set(OCI_LABEL_COMPARTMENT_ID, s(&inst.compartment_id));
    set(OCI_LABEL_IMAGE_ID, s(&inst.image_id));
    set(OCI_LABEL_VNIC_ID, p.id.clone());
    set(OCI_LABEL_PRIVATE_IP, p.private_ip.clone());
    set(OCI_LABEL_PUBLIC_IP, p.public_ip.clone());
    set(OCI_LABEL_HOSTNAME_LABEL, p.hostname_label.clone());
    set(OCI_LABEL_IPV6_ADDRESSES, join_ipv6(&p.ipv6_addresses));
    for (k, v) in &inst.freeform_tags {
        set(&format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}{}", sanitize_label_name(k)), v.clone());
    }
    // Scalars (string, number, bool) are stringified; null and non-scalars are skipped.
    for (ns, tags) in &inst.defined_tags {
        for (k, v) in tags {
            let Some(sv) = stringify_defined_tag(v) else { continue };
            set(&format!("{OCI_LABEL_DEFINED_TAG_PREFIX}{}_{}", sanitize_label_name(ns), sanitize_label_name(k)), sv);
        }
    }
    labels
}

/// Go `stringifyDefinedTag`: a scalar defined tag value as a string. The SDK decodes JSON numbers as
/// float64, so integers are printed through float64 too (Go `strconv.FormatFloat(f, 'f', -1, 64)`).
pub fn stringify_defined_tag(v: &Json) -> Option<String> {
    match v {
        Json::Str(s) => Some(s.clone()),
        Json::Bool(b) => Some(b.to_string()),
        Json::Float(f) => Some(format!("{f}")),
        Json::Int(i) => Some(format!("{}", *i as f64)),
        Json::Null | Json::Arr(_) | Json::Obj(_) => None,
    }
}

/// Go `joinIPv6`: ",a,b," (comma wrapped so `.*,addr,.*` matches any address), "" for none.
pub fn join_ipv6(addrs: &[String]) -> String {
    if addrs.is_empty() {
        return String::new();
    }
    format!(",{},", addrs.join(","))
}

/// The refresh function of the OCI discoverer (Go `Discovery.refresh` and its closures).
pub struct OciRefresher {
    pub list_compartments: CompartmentLister,
    pub list_instances: InstancesLister,
    pub list_vnic_attachments: VnicAttachmentLister,
    pub get_vnic: VnicFetcher,
    pub tenancy: String,
    pub port: i64,
    pub logger: Logger,
}

impl OciRefresher {
    /// Go `Discovery.resolveVnics`: the primary VNIC; stops at the first one found.
    pub fn resolve_vnics(&mut self, ctx: &CancelToken, inst: &Instance) -> Result<InstanceVnics, String> {
        let iid = inst.id.clone().unwrap_or_default();
        let attachments = (self.list_vnic_attachments)(ctx, inst.compartment_id.as_deref().unwrap_or(""), &iid).map_err(|e| format!("listing VNIC attachments for instance {iid}: {e}"))?;
        let mut out = InstanceVnics::default();
        for att in attachments {
            let Some(vid) = att.vnic_id else { continue };
            if att.lifecycle_state != "ATTACHED" {
                continue;
            }
            let Some(vnic) = (self.get_vnic)(ctx, &vid).map_err(|e| format!("fetching VNIC {vid}: {e}"))? else { continue };
            if vnic.is_primary == Some(true) {
                out.primary = primary_vnic_info(&vnic);
                break;
            }
        }
        Ok(out)
    }

    /// Go `Discovery.refresh`: one group "OCI" with every instance of every compartment.
    pub fn refresh_groups(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let compartments = (self.list_compartments)(ctx).map_err(|e| format!("oci_sd: failed to list compartments: {e}"))?;
        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: "OCI".to_string() };
        for cid in &compartments {
            let instances = (self.list_instances)(ctx, cid).map_err(|e| format!("listing OCI instances in compartment {cid}: {e}"))?;
            for inst in &instances {
                let (Some(iid), Some(_)) = (&inst.id, &inst.compartment_id) else {
                    self.logger.warn("OCI SD skipping instance with missing OCID fields", &[("compartment_id", cid)]);
                    continue;
                };
                let vnics = match self.resolve_vnics(ctx, inst) {
                    Ok(v) => v,
                    Err(e) => {
                        self.logger.warn("OCI SD failed to resolve VNIC IPs, skipping instance", &[("instance_id", iid), ("err", &e)]);
                        continue;
                    }
                };
                let p = &vnics.primary;
                if p.private_ip.is_empty() && p.public_ip.is_empty() && p.ipv6_addresses.is_empty() {
                    self.logger.debug("OCI SD skipping instance with no usable IP address", &[("instance_id", iid)]);
                    continue;
                }
                tg.targets.push(instance_labels(inst, &vnics, &self.tenancy, self.port));
            }
        }
        Ok(vec![Arc::new(tg)])
    }
}

impl Refresher for OciRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.refresh_groups(ctx)
    }
}

/// Go `oci.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<OciRefresher>,
}

/// Go `newConfigurationProvider` for `api_key`: the signer, or the error of reading the key files. The key
/// itself is parsed separately because Go parses it when each SDK client is created.
fn read_key(conf: &SdConfig) -> Result<(Vec<u8>, String), String> {
    let pem = net::types::read_file(&conf.key_file).map_err(|e| format!("reading OCI key file {}: {}", yaml::go_quote(&conf.key_file), e))?;
    let passphrase = resolve_key_passphrase(conf)?;
    Ok((pem, passphrase))
}

/// Go `common.IsConfigurationProviderValid` for the raw provider, then the parsed key.
fn validate_provider(conf: &SdConfig, pem: &[u8], passphrase: &str) -> Result<ring::signature::RsaKeyPair, String> {
    let checks = [
        (conf.tenancy.is_empty(), "tenancy OCID can not be empty"),
        (conf.user.is_empty(), "user OCID can not be empty"),
        (conf.fingerprint.is_empty(), "fingerprint can not be empty"),
        (conf.region.is_empty() || conf.region.chars().any(char::is_whitespace), "region can not be empty or have spaces"),
    ];
    for (bad, msg) in checks {
        if bad {
            return Err(format!("can not create client, bad configuration: {msg}"));
        }
    }
    let pw = (!passphrase.is_empty()).then_some(passphrase.as_bytes());
    key::private_key_from_bytes(pem, pw).map_err(|e| format!("can not create client, bad configuration: {e}"))
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        Discovery::new_with_endpoints(conf, opts, None)
    }

    /// `NewDiscovery` with the service endpoints replaced (tests and recorded-response runs); `None` builds
    /// them from the region as the SDK does.
    pub fn new_with_endpoints(conf: &SdConfig, opts: DiscovererOptions, endpoints: Option<Endpoints>) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<OciMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut http = net::new_client_from_config(&conf.http_client_config, &opts.http_client_options).map_err(|e| format!("oci_sd: failed to build HTTP client: {}", e.msg))?;
        http.timeout = Some(conf.refresh_interval.to_std().max(Duration::from_secs(60)));
        if conf.auth == AUTH_INSTANCE_PRINCIPAL {
            return Err("oci_sd: failed to build configuration provider: instance_principal auth is not supported by prometheus-rs (it needs an ephemeral RSA session key, and RSA key generation is not available)".to_string());
        }
        let (pem, passphrase) = read_key(conf).map_err(|e| format!("oci_sd: failed to build configuration provider: {e}"))?;
        let tenancy = conf.tenancy.clone();
        let endpoints = endpoints.unwrap_or_else(|| Endpoints::for_region(&conf.region));
        let limiter = Arc::new(client::Limiter::new(RATE_LIMIT_RPS, RATE_LIMIT_BURST));
        let new_client = |key: ring::signature::RsaKeyPair| {
            Arc::new(client::Client {
                http: http.clone(),
                signer: Arc::new(client::Signer { key_id: format!("{}/{}/{}", conf.tenancy, conf.user, conf.fingerprint), key }),
                endpoints: endpoints.clone(),
                user_agent: crate::http::USER_AGENT.to_string(),
                limiter: limiter.clone(),
            })
        };

        // Go newCompartmentLister: no Identity client when the compartments are explicit.
        let list_compartments: CompartmentLister = if !conf.compartments.is_empty() {
            let ids = conf.compartments.clone();
            Box::new(move |_ctx: &CancelToken| Ok(ids.clone()))
        } else {
            let key = validate_provider(conf, &pem, &passphrase).map_err(|e| format!("oci_sd: failed to create compartment lister: creating OCI identity client: {e}"))?;
            let id_client = new_client(key);
            let logger = opts.logger.clone();
            let root = tenancy.clone();
            Box::new(move |ctx: &CancelToken| {
                let mut paginate = |ctx: &CancelToken, parent: &str, page: Option<&str>| id_client.list_compartments_page(ctx, parent, page);
                list_all_compartments(ctx, &mut paginate, &root, &logger)
            })
        };

        let key = validate_provider(conf, &pem, &passphrase).map_err(|e| format!("oci_sd: failed to create clients: creating OCI compute client: {e}"))?;
        let c = new_client(key);
        let (c1, c2, c3) = (c.clone(), c.clone(), c);
        let refresher = OciRefresher {
            list_compartments,
            list_instances: Box::new(move |ctx: &CancelToken, cid: &str| c1.list_instances(ctx, cid)),
            list_vnic_attachments: Box::new(move |ctx: &CancelToken, cid: &str, iid: &str| c2.list_vnic_attachments(ctx, cid, iid)),
            get_vnic: Box::new(move |ctx: &CancelToken, vid: &str| c3.get_vnic(ctx, vid).map(Some)),
            tenancy,
            port: conf.port,
            logger: opts.logger.clone(),
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "oci",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// The refresh function and its closures (tests replace closures as the Go tests do).
    pub fn refresher(&mut self) -> &mut OciRefresher {
        self.inner.refresher()
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh_groups(ctx)
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

