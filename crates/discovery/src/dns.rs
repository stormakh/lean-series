//! Port of `discovery/dns`: targets from DNS SRV, A, AAAA, MX and NS records, refreshed at an interval.
//!
//! The DNS client is a small in-house port of the parts of `github.com/miekg/dns` (v1.1.73) that upstream uses:
//! `ClientConfigFromFile` and `NameList` (resolv.conf and the search list), `Msg` pack (query with EDNS0 OPT)
//! and unpack (with name compression), and `Client.Exchange` over UDP or TCP with its default 2s dial, write
//! and read timeouts.
//!
//! Differs from Go:
//! - Record types other than A, AAAA, NS, CNAME, MX, SRV, TXT and OPT are kept as raw rdata (miekg's
//!   `RFC3597` form): their rdata is not validated, and the "Invalid record" log prints them as `\# len hex`.
//! - EDNS0 option payloads in an OPT record are not validated (miekg checks some option kinds).
//! - Error texts of network failures follow Go's `net.OpError` form, but an IPv6 zone prints as a number.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use model::labelset::LabelSet;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry};
use crate::refresh::{self, Refresher};
use crate::sdutil::join_host_port;
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const RESOLV_CONF: &str = "/etc/resolv.conf";

const DNS_NAME_LABEL: &str = "__meta_dns_name";
const DNS_SRV_RECORD_TARGET_LABEL: &str = "__meta_dns_srv_record_target";
const DNS_SRV_RECORD_PORT_LABEL: &str = "__meta_dns_srv_record_port";
const DNS_MX_RECORD_TARGET_LABEL: &str = "__meta_dns_mx_record_target";
const DNS_NS_RECORD_TARGET_LABEL: &str = "__meta_dns_ns_record_target";

/// Go `dns.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub names: Vec<String>,
    pub refresh_interval: yaml::Duration,
    pub r#type: String,
    /// Ignored for SRV records.
    pub port: i64,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { refresh_interval: yaml::Duration::from_secs(30), r#type: "SRV".to_string(), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "dns.plain", [
    (names: "names")
    (refresh_interval: "refresh_interval", omitempty)
    (r#type: "type")
    (port: "port")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.names.is_empty() {
                return Err(YError::Msg("DNS-SD config must contain at least one SRV record name".into()));
            }
            match self.r#type.to_uppercase().as_str() {
                "SRV" => {}
                "A" | "AAAA" | "MX" | "NS" => {
                    if self.port == 0 {
                        return Err(YError::Msg("a port is required in DNS-SD configs for all record types except SRV".into()));
                    }
                }
                _ => {
                    return Err(YError::Msg(format!("invalid DNS-SD records type {}", self.r#type)));
                }
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "dns.plain".to_string()
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
    const GO_TYPE: &'static str = "dns.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "dns"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        DnsMetrics::new(reg, rmi)
    }
}

/// Go `dnsMetrics`.
pub struct DnsMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
    pub dns_sd_lookups_count: Arc<CounterVec>,
    pub dns_sd_lookup_failures_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl DnsMetrics {
    pub fn new(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<DnsMetrics> {
        let lookups = CounterVec::new_counter(Opts::new("prometheus_sd_dns_lookups_total", "The number of DNS-SD lookups."));
        let failures = CounterVec::new_counter(Opts::new("prometheus_sd_dns_lookup_failures_total", "The number of DNS-SD lookup failures."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![lookups.clone(), failures.clone()]);
        Arc::new(DnsMetrics { refresh_metrics: rmi, dns_sd_lookups_count: lookups, dns_sd_lookup_failures_count: failures, registerer })
    }
}

impl DiscovererMetrics for DnsMetrics {
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

/// The lookup function (Go `Discovery.lookupFn`): name, query type, logger.
pub type LookupFn = Arc<dyn Fn(&str, u16, &Logger) -> Result<Msg, String> + Send + Sync>;

/// The refresh function of the DNS discoverer and its state.
pub struct DnsRefresher {
    names: Vec<String>,
    port: i64,
    qtype: u16,
    logger: Logger,
    metrics: Arc<DnsMetrics>,
    lookup_fn: LookupFn,
}

/// Go `dns.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<DnsRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<DnsMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let qtype = match conf.r#type.to_uppercase().as_str() {
            "A" => TYPE_A,
            "AAAA" => TYPE_AAAA,
            "MX" => TYPE_MX,
            "NS" => TYPE_NS,
            _ => TYPE_SRV,
        };
        let refresher =
            DnsRefresher { names: conf.names.clone(), port: conf.port, qtype, logger: opts.logger.clone(), metrics: m.clone(), lookup_fn: Arc::new(lookup_with_search_path) };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "dns",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Replaces the lookup function (Go tests set `sd.lookupFn`).
    pub fn set_lookup_fn(&mut self, f: LookupFn) {
        self.inner.refresher().lookup_fn = f;
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

const CONTEXT_CANCELED: &str = "context canceled";

impl Refresher for DnsRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let this = &*self;
        let (tx, rx) = mpsc::channel::<Group>();
        let mut tgs = Vec::with_capacity(this.names.len());
        std::thread::scope(|s| {
            for name in &this.names {
                let tx = tx.clone();
                s.spawn(move || {
                    if let Err(e) = this.refresh_one(ctx, name, &tx)
                        && e != CONTEXT_CANCELED
                    {
                        this.logger.error("Error refreshing DNS targets", &[("err", &e)]);
                    }
                });
            }
            drop(tx);
            for tg in rx {
                tgs.push(Arc::new(tg));
            }
        });
        Ok(tgs)
    }
}

impl DnsRefresher {
    fn refresh_one(&self, ctx: &CancelToken, name: &str, ch: &mpsc::Sender<Group>) -> Result<(), String> {
        let response = (self.lookup_fn)(name, self.qtype, &self.logger);
        self.metrics.dns_sd_lookups_count.get().inc();
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                self.metrics.dns_sd_lookup_failures_count.get().inc();
                return Err(e);
            }
        };

        let mut tg = Group::default();
        for record in &response.answer {
            let (mut srv_target, mut srv_port, mut mx_target, mut ns_target) = (String::new(), String::new(), String::new(), String::new());
            let target = match &record.data {
                RrData::Srv { port, target, .. } => {
                    srv_target = target.clone();
                    srv_port = port.to_string();
                    // Remove the final dot from rooted DNS names to make them look more usual.
                    join_host_port(target.trim_end_matches('.'), port)
                }
                RrData::Mx { mx, .. } => {
                    mx_target = mx.clone();
                    join_host_port(mx.trim_end_matches('.'), self.port)
                }
                RrData::Ns(ns) => {
                    ns_target = ns.clone();
                    join_host_port(ns.trim_end_matches('.'), self.port)
                }
                RrData::A(ip) | RrData::Aaaa(ip) => join_host_port(&go_ip_string(ip), self.port),
                // CNAME responses can occur with "Type: A" dns_sd_config requests.
                RrData::Cname(_) => continue,
                _ => {
                    self.logger.warn("Invalid record", &[("record", &record.to_string())]);
                    continue;
                }
            };
            tg.targets.push(LabelSet::from_pairs(&[
                ("__address__", &target),
                (DNS_NAME_LABEL, name),
                (DNS_SRV_RECORD_TARGET_LABEL, &srv_target),
                (DNS_SRV_RECORD_PORT_LABEL, &srv_port),
                (DNS_MX_RECORD_TARGET_LABEL, &mx_target),
                (DNS_NS_RECORD_TARGET_LABEL, &ns_target),
            ]));
        }

        tg.source = name.to_string();
        if ctx.is_cancelled() {
            return Err(CONTEXT_CANCELED.to_string());
        }
        let _ = ch.send(tg);
        Ok(())
    }
}

/// Go `lookupWithSearchPath`: tries the name with each search domain (miekg `NameList`) and returns the first
/// successful answer; an empty message when every name is NXDOMAIN; an error when some name got only errors.
pub fn lookup_with_search_path(name: &str, qtype: u16, logger: &Logger) -> Result<Msg, String> {
    let conf = ClientConfig::from_file(RESOLV_CONF).map_err(|e| format!("could not load resolv.conf: {e}"))?;
    let mut all_responses_valid = true;
    for lname in conf.name_list(name) {
        match lookup_from_any_server(&lname, qtype, &conf, logger) {
            Err(_) => all_responses_valid = false,
            Ok(r) if r.rcode == RCODE_SUCCESS => return Ok(r),
            Ok(_) => {}
        }
    }
    if all_responses_valid {
        return Ok(Msg::default());
    }
    Err(format!("could not resolve {}: all servers responded with errors to at least one search domain", yaml::go_quote(name)))
}

/// Go `lookupFromAnyServer`: the first viable answer (NOERROR or NXDOMAIN) from the configured servers.
pub fn lookup_from_any_server(name: &str, qtype: u16, conf: &ClientConfig, logger: &Logger) -> Result<Msg, String> {
    let mut client = Client::default();
    for server in &conf.servers {
        let serv_addr = join_host_port(server, &conf.port);
        match ask_server_for_name(name, qtype, &mut client, &serv_addr, true) {
            Err(e) => {
                logger.warn("DNS resolution failed", &[("server", server.as_str()), ("name", name), ("err", e.as_str())]);
                continue;
            }
            Ok(msg) => {
                if msg.rcode == RCODE_SUCCESS || msg.rcode == RCODE_NAME_ERROR {
                    return Ok(msg);
                }
            }
        }
    }
    Err(format!("could not resolve {name}: no servers returned a viable answer"))
}

/// Go `askServerForName`: one query, retried over TCP when the answer is truncated. As in Go, the client stays
/// on TCP for the next servers of the same lookup once it switched.
pub fn ask_server_for_name(name: &str, query_type: u16, client: &mut Client, serv_addr: &str, edns: bool) -> Result<Msg, String> {
    let mut msg = Msg::default();
    msg.set_question(&fqdn(name), query_type);
    if edns {
        msg.set_edns0(DEFAULT_MSG_SIZE, false);
    }
    let response = client.exchange(&msg, serv_addr)?;
    if response.truncated {
        if client.net == "tcp" {
            return Err("got truncated message on TCP (64kiB limit exceeded?)".to_string());
        }
        client.net = "tcp".to_string();
        return ask_server_for_name(name, query_type, client, serv_addr, false);
    }
    Ok(response)
}

// ---------------------------------------------------------------------------------------------------------
// miekg/dns: constants, names, resolv.conf.

pub const TYPE_A: u16 = 1;
pub const TYPE_NS: u16 = 2;
pub const TYPE_CNAME: u16 = 5;
pub const TYPE_MX: u16 = 15;
pub const TYPE_TXT: u16 = 16;
pub const TYPE_AAAA: u16 = 28;
pub const TYPE_SRV: u16 = 33;
pub const TYPE_OPT: u16 = 41;
pub const CLASS_INET: u16 = 1;
pub const RCODE_SUCCESS: i32 = 0;
pub const RCODE_NAME_ERROR: i32 = 3;
pub const DEFAULT_MSG_SIZE: u16 = 4096;
pub const MIN_MSG_SIZE: u16 = 512;
pub const MAX_MSG_SIZE: usize = 65535;
const HEADER_SIZE: usize = 12;
const MAX_DOMAIN_NAME_WIRE_OCTETS: i32 = 255;
const MAX_COMPRESSION_POINTERS: i32 = (MAX_DOMAIN_NAME_WIRE_OCTETS + 1) / 2 - 2;
/// Go `dnsTimeout`: the client's default dial, write and read timeout.
const DNS_TIMEOUT: Duration = Duration::from_secs(2);

/// miekg `*dns.Error` text.
fn derr(s: &str) -> String {
    format!("dns: {s}")
}

/// miekg `IsFqdn`: ends with a dot that is not escaped.
pub fn is_fqdn(s: &str) -> bool {
    let b = s.as_bytes();
    if b.last() != Some(&b'.') {
        return false;
    }
    let b = &b[..b.len() - 1];
    if b.last() != Some(&b'\\') {
        return true;
    }
    // An odd number of backslashes before the dot escapes it.
    let n = b.iter().rev().take_while(|&&c| c == b'\\').count();
    n % 2 == 0
}

/// miekg `Fqdn`.
pub fn fqdn(s: &str) -> String {
    if is_fqdn(s) { s.to_string() } else { format!("{s}.") }
}

/// miekg `NextLabel`.
fn next_label(s: &[u8], offset: usize) -> (usize, bool) {
    if s.is_empty() {
        return (0, true);
    }
    let mut i = offset;
    while i + 1 < s.len() {
        if s[i] == b'.' {
            let mut j = i as isize - 1;
            while j >= 0 && s[j as usize] == b'\\' {
                j -= 1;
            }
            if (j - i as isize) % 2 != 0 {
                return (i + 1, false);
            }
        }
        i += 1;
    }
    (i.max(offset) + 1, true)
}

/// miekg `CountLabel`.
pub fn count_label(s: &str) -> i64 {
    if s == "." {
        return 0;
    }
    let (mut off, mut labels) = (0, 0);
    loop {
        let (o, end) = next_label(s.as_bytes(), off);
        off = o;
        labels += 1;
        if end {
            return labels;
        }
    }
}

/// Go `strconv.Atoi` with the error ignored: 0 on bad syntax, saturated on overflow.
fn go_atoi(s: &str) -> i64 {
    let (neg, digits) = match s.as_bytes().first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return 0;
    }
    match s.parse::<i64>() {
        Ok(v) => v,
        Err(_) if neg => i64::MIN,
        Err(_) => i64::MAX,
    }
}

/// miekg `ClientConfig`: the contents of resolv.conf.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientConfig {
    pub servers: Vec<String>,
    pub search: Vec<String>,
    pub port: String,
    pub ndots: i64,
    pub timeout: i64,
    pub attempts: i64,
}

impl ClientConfig {
    /// miekg `ClientConfigFromFile`. As in Go, a read error ends parsing without an error.
    pub fn from_file(path: &str) -> Result<ClientConfig, String> {
        let mut f = std::fs::File::open(path).map_err(|e| format!("open {path}: {}", net::types::go_io_error(&e)))?;
        let mut data = Vec::new();
        let _ = f.read_to_end(&mut data);
        Ok(ClientConfig::from_reader(&data))
    }

    /// miekg `ClientConfigFromReader` (a `bufio.Scanner` over lines: a line of 64 KiB or more ends parsing).
    pub fn from_reader(data: &[u8]) -> ClientConfig {
        let mut c = ClientConfig { servers: Vec::new(), search: Vec::new(), port: "53".to_string(), ndots: 1, timeout: 5, attempts: 2 };
        let mut rest = data;
        while !rest.is_empty() {
            let (line, next) = match rest.iter().position(|&b| b == b'\n') {
                Some(i) => (&rest[..i], &rest[i + 1..]),
                None => (rest, &rest[rest.len()..]),
            };
            if line.len() >= 64 * 1024 {
                break;
            }
            rest = next;
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let line = String::from_utf8_lossy(line);
            let f: Vec<&str> = line.split_whitespace().collect();
            let Some(&key) = f.first() else { continue };
            match key {
                "nameserver" => {
                    if f.len() > 1 {
                        c.servers.push(f[1].to_string());
                    }
                }
                "domain" => c.search = if f.len() > 1 { vec![f[1].to_string()] } else { Vec::new() },
                "search" => c.search = f[1..].iter().map(|s| s.to_string()).collect(),
                "options" => {
                    for s in &f[1..] {
                        if let Some(v) = s.strip_prefix("ndots:") {
                            c.ndots = go_atoi(v).clamp(0, 15);
                        } else if let Some(v) = s.strip_prefix("timeout:") {
                            c.timeout = go_atoi(v).max(1);
                        } else if let Some(v) = s.strip_prefix("attempts:") {
                            c.attempts = go_atoi(v).max(1);
                        }
                    }
                }
                _ => {}
            }
        }
        c
    }

    /// miekg `ClientConfig.NameList`: the names to query, with the search domains, in ndots order.
    pub fn name_list(&self, name: &str) -> Vec<String> {
        if is_fqdn(name) {
            return vec![name.to_string()];
        }
        let has_ndots = count_label(name) > self.ndots;
        let name = fqdn(name);
        let mut names = Vec::new();
        if has_ndots {
            names.push(name.clone());
        }
        for s in &self.search {
            names.push(fqdn(&format!("{name}{s}")));
        }
        if !has_ndots {
            names.push(name);
        }
        names
    }
}

// ---------------------------------------------------------------------------------------------------------
// miekg/dns: messages.

/// miekg `Question`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Question {
    pub name: String,
    pub qtype: u16,
    pub qclass: u16,
}

/// miekg `RR_Header`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RrHeader {
    pub name: String,
    pub rrtype: u16,
    pub class: u16,
    pub ttl: u32,
    pub rdlength: u16,
}

/// The record data of the types this port decodes; others keep their raw rdata (miekg `RFC3597`).
/// Addresses are Go `net.IP` bytes (empty when the record has no rdata).
#[derive(Clone, Debug, PartialEq)]
pub enum RrData {
    A(Vec<u8>),
    Aaaa(Vec<u8>),
    Ns(String),
    Cname(String),
    Mx {
        preference: u16,
        mx: String,
    },
    Srv {
        priority: u16,
        weight: u16,
        port: u16,
        target: String,
    },
    Txt(Vec<String>),
    /// EDNS0 options, raw.
    Opt(Vec<u8>),
    Other(Vec<u8>),
}

/// miekg `RR`.
#[derive(Clone, Debug, PartialEq)]
pub struct Rr {
    pub hdr: RrHeader,
    pub data: RrData,
}

impl Rr {
    /// A record with a zero header (as Go literals like `&dns.A{A: ip}`).
    pub fn new(data: RrData) -> Rr {
        Rr { hdr: RrHeader::default(), data }
    }
}

fn type_string(t: u16) -> String {
    let s = match t {
        0 => "None",
        1 => "A",
        2 => "NS",
        5 => "CNAME",
        6 => "SOA",
        12 => "PTR",
        15 => "MX",
        16 => "TXT",
        28 => "AAAA",
        33 => "SRV",
        39 => "DNAME",
        41 => "OPT",
        43 => "DS",
        46 => "RRSIG",
        47 => "NSEC",
        48 => "DNSKEY",
        50 => "NSEC3",
        64 => "SVCB",
        65 => "HTTPS",
        255 => "ANY",
        257 => "CAA",
        _ => return format!("TYPE{t}"),
    };
    s.to_string()
}

fn class_string(c: u16) -> String {
    let s = match c {
        1 => "IN",
        2 => "CS",
        3 => "CH",
        4 => "HS",
        254 => "NONE",
        255 => "ANY",
        _ => return format!("CLASS{c}"),
    };
    s.to_string()
}

/// miekg `sprintName` for a name already in presentation form.
fn sprint_name(s: &str) -> &str {
    if s == "." { "" } else { s }
}

impl std::fmt::Display for Rr {
    /// miekg `RR.String()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h = &self.hdr;
        if let RrData::Other(b) = &self.data {
            write!(f, "{}\t{}\tCLASS{}\tTYPE{}\t\\# {} ", sprint_name(&h.name), h.ttl, h.class, h.rrtype, b.len())?;
            return b.iter().try_for_each(|x| write!(f, "{x:02x}"));
        }
        let semi = if h.rrtype == TYPE_OPT { ";" } else { "" };
        write!(f, "{semi}{}\t{}\t{}\t{}\t", sprint_name(&h.name), h.ttl, class_string(h.class), type_string(h.rrtype))?;
        match &self.data {
            RrData::A(ip) | RrData::Aaaa(ip) => f.write_str(&go_ip_string(ip)),
            RrData::Ns(n) | RrData::Cname(n) => f.write_str(sprint_name(n)),
            RrData::Mx { preference, mx } => write!(f, "{preference} {}", sprint_name(mx)),
            RrData::Srv { priority, weight, port, target } => write!(f, "{priority} {weight} {port} {}", sprint_name(target)),
            RrData::Txt(t) => {
                let q: Vec<String> = t.iter().map(|s| format!("\"{s}\"")).collect();
                f.write_str(&q.join(" "))
            }
            RrData::Opt(b) | RrData::Other(b) => b.iter().try_for_each(|x| write!(f, "{x:02x}")),
        }
    }
}

/// Go `net.IP.String()`.
pub fn go_ip_string(ip: &[u8]) -> String {
    match ip.len() {
        0 => "<nil>".to_string(),
        4 => format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3]),
        16 if ip[..10].iter().all(|&b| b == 0) && ip[10] == 0xff && ip[11] == 0xff => {
            format!("{}.{}.{}.{}", ip[12], ip[13], ip[14], ip[15])
        }
        16 => {
            let g: Vec<u16> = ip.chunks(2).map(|c| u16::from_be_bytes([c[0], c[1]])).collect();
            // The longest run of two or more zero groups (the first one on a tie) becomes "::".
            let (mut best, mut best_len, mut i) = (usize::MAX, 1, 0);
            while i < 8 {
                let j = (i..8).find(|&k| g[k] != 0).unwrap_or(8);
                if j - i > best_len {
                    best = i;
                    best_len = j - i;
                }
                i = j.max(i + 1);
            }
            let mut s = String::new();
            let mut k = 0;
            while k < 8 {
                if k == best {
                    s.push_str("::");
                    k += best_len;
                    continue;
                }
                if k > 0 && !s.ends_with(':') {
                    s.push(':');
                }
                s.push_str(&format!("{:x}", g[k]));
                k += 1;
            }
            s
        }
        _ => format!("?{}", ip.iter().map(|b| format!("{b:02x}")).collect::<String>()),
    }
}

/// miekg `Msg`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Msg {
    pub id: u16,
    pub response: bool,
    pub opcode: i32,
    pub authoritative: bool,
    pub truncated: bool,
    pub recursion_desired: bool,
    pub recursion_available: bool,
    pub zero: bool,
    pub authenticated_data: bool,
    pub checking_disabled: bool,
    pub rcode: i32,
    pub question: Vec<Question>,
    pub answer: Vec<Rr>,
    pub ns: Vec<Rr>,
    pub extra: Vec<Rr>,
}

fn random_id() -> u16 {
    let mut b = [0u8; 2];
    crypto::random_bytes(&mut b);
    u16::from_be_bytes(b)
}

impl Msg {
    /// miekg `Msg.SetQuestion`.
    pub fn set_question(&mut self, z: &str, t: u16) {
        self.id = random_id();
        self.recursion_desired = true;
        self.question = vec![Question { name: z.to_string(), qtype: t, qclass: CLASS_INET }];
    }

    /// miekg `Msg.SetEdns0`.
    pub fn set_edns0(&mut self, udpsize: u16, do_bit: bool) {
        let ttl = if do_bit { 0x8000 } else { 0 };
        self.extra.push(Rr { hdr: RrHeader { name: ".".to_string(), rrtype: TYPE_OPT, class: udpsize, ttl, rdlength: 0 }, data: RrData::Opt(Vec::new()) });
    }

    /// miekg `Msg.IsEdns0`: the last OPT record of the additional section.
    pub fn is_edns0(&self) -> Option<&Rr> {
        self.extra.iter().rev().find(|r| r.hdr.rrtype == TYPE_OPT)
    }

    /// miekg `Msg.Pack` without compression (queries: header, questions, and records with raw rdata).
    pub fn pack(&self) -> Result<Vec<u8>, String> {
        if self.rcode < 0 || self.rcode > 0xFFF {
            return Err(derr("bad rcode"));
        }
        if self.is_edns0().is_none() && self.rcode > 0xF {
            return Err(derr("bad extended rcode"));
        }
        let mut bits = ((self.opcode as u16) << 11) | (self.rcode as u16 & 0xF);
        for (set, bit) in [
            (self.response, 1 << 15),
            (self.authoritative, 1 << 10),
            (self.truncated, 1 << 9),
            (self.recursion_desired, 1 << 8),
            (self.recursion_available, 1 << 7),
            (self.zero, 1 << 6),
            (self.authenticated_data, 1 << 5),
            (self.checking_disabled, 1 << 4),
        ] {
            if set {
                bits |= bit;
            }
        }
        let mut out = Vec::with_capacity(64);
        for v in [self.id, bits, self.question.len() as u16, self.answer.len() as u16, self.ns.len() as u16, self.extra.len() as u16] {
            out.extend_from_slice(&v.to_be_bytes());
        }
        for q in &self.question {
            pack_domain_name(&q.name, &mut out)?;
            out.extend_from_slice(&q.qtype.to_be_bytes());
            out.extend_from_slice(&q.qclass.to_be_bytes());
        }
        let ext_rcode = (self.rcode as u32 >> 4) << 24;
        for r in self.answer.iter().chain(&self.ns).chain(&self.extra) {
            let rdata = match &r.data {
                RrData::Opt(b) | RrData::Other(b) => b,
                _ => return Err(derr("packing this record type is not supported")),
            };
            pack_domain_name(&r.hdr.name, &mut out)?;
            let ttl = if r.hdr.rrtype == TYPE_OPT { (r.hdr.ttl & 0x00FF_FFFF) | ext_rcode } else { r.hdr.ttl };
            out.extend_from_slice(&r.hdr.rrtype.to_be_bytes());
            out.extend_from_slice(&r.hdr.class.to_be_bytes());
            out.extend_from_slice(&ttl.to_be_bytes());
            out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
            out.extend_from_slice(rdata);
        }
        Ok(out)
    }

    /// miekg `Msg.Unpack`.
    pub fn unpack(msg: &[u8]) -> Result<Msg, String> {
        let u16_at = |o: usize| u16::from_be_bytes([msg[o], msg[o + 1]]);
        if msg.len() < HEADER_SIZE {
            let what = ["id", "bits", "question count", "answer count", "ns count", "extra count"][msg.len() / 2];
            return Err(format!("bad header {what}: {}", derr("overflow unpacking uint16")));
        }
        let bits = u16_at(2);
        let mut m = Msg {
            id: u16_at(0),
            response: bits & (1 << 15) != 0,
            opcode: ((bits >> 11) & 0xF) as i32,
            authoritative: bits & (1 << 10) != 0,
            truncated: bits & (1 << 9) != 0,
            recursion_desired: bits & (1 << 8) != 0,
            recursion_available: bits & (1 << 7) != 0,
            zero: bits & (1 << 6) != 0,
            authenticated_data: bits & (1 << 5) != 0,
            checking_disabled: bits & (1 << 4) != 0,
            rcode: (bits & 0xF) as i32,
            ..Default::default()
        };
        let (qd, an, ns, ar) = (u16_at(4), u16_at(6), u16_at(8), u16_at(10));
        let mut off = HEADER_SIZE;
        if off == msg.len() {
            return Ok(m);
        }
        for _ in 0..qd {
            let off1 = off;
            let q;
            (q, off) = unpack_question(msg, off)?;
            if off1 == off {
                break;
            }
            m.question.push(q);
        }
        let mut res = unpack_rr_slice(an as usize, msg, off).map(|(v, o)| {
            m.answer = v;
            off = o;
        });
        if res.is_ok() {
            res = unpack_rr_slice(ns as usize, msg, off).map(|(v, o)| {
                m.ns = v;
                off = o;
            });
        }
        if res.is_ok() {
            res = unpack_rr_slice(ar as usize, msg, off).map(|(v, _)| m.extra = v);
        }
        if let Some(opt) = m.is_edns0() {
            m.rcode |= (((opt.hdr.ttl & 0xFF00_0000) >> 24) as i32) << 4;
        }
        res.map(|_| m)
    }
}

/// miekg `packDomainName` without compression (escapes `\X` and `\DDD`).
fn pack_domain_name(s: &str, out: &mut Vec<u8>) -> Result<(), String> {
    if s.is_empty() {
        return Ok(());
    }
    if !is_fqdn(s) {
        return Err(derr("domain must be fully qualified"));
    }
    if s == "." {
        out.push(0);
        return Ok(());
    }
    let b = s.as_bytes();
    let (mut label, mut i, mut was_dot) = (Vec::with_capacity(63), 0, false);
    while i < b.len() {
        match b[i] {
            b'\\' => {
                let r = &b[i + 1..];
                if r.len() >= 3 && r[..3].iter().all(u8::is_ascii_digit) {
                    label.push(((r[0] - b'0') as u32 * 100 + (r[1] - b'0') as u32 * 10 + (r[2] - b'0') as u32) as u8);
                    i += 4;
                } else {
                    if let Some(&c) = r.first() {
                        label.push(c);
                    }
                    i += 2;
                }
                was_dot = false;
                continue;
            }
            b'.' => {
                if i == 0 || was_dot {
                    return Err(derr("bad rdata"));
                }
                was_dot = true;
                if label.len() >= 64 {
                    return Err(derr("bad rdata"));
                }
                out.push(label.len() as u8);
                out.extend_from_slice(&label);
                label.clear();
            }
            c => {
                was_dot = false;
                label.push(c);
            }
        }
        i += 1;
    }
    out.push(0);
    Ok(())
}

/// miekg `UnpackDomainName`: the name in presentation form (escaped) and the offset after it.
pub fn unpack_domain_name(msg: &[u8], mut off: usize) -> Result<(String, usize), String> {
    let mut s: Vec<u8> = Vec::with_capacity(64);
    let (mut off1, mut budget, mut ptr) = (0, MAX_DOMAIN_NAME_WIRE_OCTETS, 0);
    loop {
        if off >= msg.len() {
            return Err(derr("buffer size too small"));
        }
        let c = msg[off] as usize;
        off += 1;
        match c & 0xC0 {
            0x00 => {
                if c == 0 {
                    break;
                }
                if off + c > msg.len() {
                    return Err(derr("buffer size too small"));
                }
                budget -= c as i32 + 1;
                if budget <= 0 {
                    return Err(derr("domain name exceeded 255 wire-format octets"));
                }
                for &b in &msg[off..off + c] {
                    if matches!(b, b'.' | b' ' | b'\'' | b'@' | b';' | b'(' | b')' | b'"' | b'\\') {
                        s.extend_from_slice(&[b'\\', b]);
                    } else if !(b' '..=b'~').contains(&b) {
                        s.extend_from_slice(format!("\\{b:03}").as_bytes());
                    } else {
                        s.push(b);
                    }
                }
                s.push(b'.');
                off += c;
            }
            0xC0 => {
                if off >= msg.len() {
                    return Err(derr("buffer size too small"));
                }
                let c1 = msg[off] as usize;
                off += 1;
                if ptr == 0 {
                    off1 = off;
                }
                ptr += 1;
                if ptr > MAX_COMPRESSION_POINTERS {
                    return Err(derr("too many compression pointers"));
                }
                off = ((c ^ 0xC0) << 8) | c1;
            }
            _ => return Err(derr("bad rdata")),
        }
    }
    if ptr == 0 {
        off1 = off;
    }
    if s.is_empty() {
        return Ok((".".to_string(), off1));
    }
    // Only ASCII is written above.
    Ok((String::from_utf8(s).unwrap_or_default(), off1))
}

fn unpack_u16(msg: &[u8], off: usize) -> Result<(u16, usize), String> {
    if off + 2 > msg.len() {
        return Err(derr("overflow unpacking uint16"));
    }
    Ok((u16::from_be_bytes([msg[off], msg[off + 1]]), off + 2))
}

fn unpack_u32(msg: &[u8], off: usize) -> Result<(u32, usize), String> {
    if off + 4 > msg.len() {
        return Err(derr("overflow unpacking uint32"));
    }
    Ok((u32::from_be_bytes([msg[off], msg[off + 1], msg[off + 2], msg[off + 3]]), off + 4))
}

/// miekg `unpackQuestion`.
fn unpack_question(msg: &[u8], off: usize) -> Result<(Question, usize), String> {
    let mut q = Question::default();
    let (name, mut off) = unpack_domain_name(msg, off).map_err(|e| format!("bad question name: {e}"))?;
    q.name = name;
    if off == msg.len() {
        return Ok((q, off));
    }
    (q.qtype, off) = unpack_u16(msg, off).map_err(|e| format!("bad question qtype: {e}"))?;
    if off == msg.len() {
        return Ok((q, off));
    }
    (q.qclass, off) = unpack_u16(msg, off).map_err(|e| format!("bad question qclass: {e}"))?;
    Ok((q, off))
}

/// miekg `unpackRRslice`: stops early when the counts are larger than the data.
fn unpack_rr_slice(l: usize, msg: &[u8], mut off: usize) -> Result<(Vec<Rr>, usize), String> {
    let mut dst = Vec::new();
    for _ in 0..l {
        if off == msg.len() {
            break;
        }
        let r;
        (r, off) = unpack_rr(msg, off)?;
        dst.push(r);
    }
    Ok((dst, off))
}

/// miekg `UnpackRR` for a record that starts before the end of `msg`.
fn unpack_rr(msg: &[u8], off: usize) -> Result<(Rr, usize), String> {
    let (name, off) = unpack_domain_name(msg, off)?;
    let (rrtype, off) = unpack_u16(msg, off)?;
    let (class, off) = unpack_u16(msg, off)?;
    let (ttl, off) = unpack_u32(msg, off)?;
    let (rdlength, off) = unpack_u16(msg, off)?;
    let end = off + rdlength as usize;
    if end > msg.len() {
        return Err(derr("overflowing header size"));
    }
    // Names inside the rdata may only point before its end.
    let msg = &msg[..end];
    let hdr = RrHeader { name, rrtype, class, ttl, rdlength };
    if rdlength == 0 {
        let data = match rrtype {
            TYPE_A => RrData::A(Vec::new()),
            TYPE_AAAA => RrData::Aaaa(Vec::new()),
            TYPE_NS => RrData::Ns(String::new()),
            TYPE_CNAME => RrData::Cname(String::new()),
            TYPE_MX => RrData::Mx { preference: 0, mx: String::new() },
            TYPE_SRV => RrData::Srv { priority: 0, weight: 0, port: 0, target: String::new() },
            TYPE_TXT => RrData::Txt(Vec::new()),
            TYPE_OPT => RrData::Opt(Vec::new()),
            _ => RrData::Other(Vec::new()),
        };
        return Ok((Rr { hdr, data }, off));
    }
    let name_at = |off: usize, what: &str| unpack_domain_name(msg, off).map_err(|e| format!("{what}: {e}"));
    let u16_at = |off: usize, what: &str| unpack_u16(msg, off).map_err(|e| format!("{what}: {e}"));
    let (data, off) = match rrtype {
        TYPE_A | TYPE_AAAA => {
            let (n, what) = if rrtype == TYPE_A { (4, "a") } else { (16, "aaaa") };
            if off + n > msg.len() {
                return Err(format!("{}: {}", what.to_uppercase(), derr(&format!("overflow unpacking {what}"))));
            }
            let ip = msg[off..off + n].to_vec();
            (if n == 4 { RrData::A(ip) } else { RrData::Aaaa(ip) }, off + n)
        }
        TYPE_NS => {
            let (n, off) = name_at(off, "NS.Ns")?;
            (RrData::Ns(n), off)
        }
        TYPE_CNAME => {
            let (n, off) = name_at(off, "CNAME.Target")?;
            (RrData::Cname(n), off)
        }
        TYPE_MX => {
            let (preference, off) = u16_at(off, "MX.Preference")?;
            if off == msg.len() {
                (RrData::Mx { preference, mx: String::new() }, off)
            } else {
                let (mx, off) = name_at(off, "MX.Mx")?;
                (RrData::Mx { preference, mx }, off)
            }
        }
        TYPE_SRV => {
            let (mut v, mut target, mut o) = ([0u16; 3], String::new(), off);
            for (i, what) in ["SRV.Priority", "SRV.Weight", "SRV.Port"].iter().enumerate() {
                (v[i], o) = u16_at(o, what)?;
                if o == msg.len() {
                    break;
                }
            }
            if o < msg.len() {
                (target, o) = name_at(o, "SRV.Target")?;
            }
            (RrData::Srv { priority: v[0], weight: v[1], port: v[2], target }, o)
        }
        TYPE_TXT => {
            let (mut ss, mut o) = (Vec::new(), off);
            while o < msg.len() {
                let l = msg[o] as usize;
                o += 1;
                if o + l > msg.len() {
                    return Err(format!("TXT.Txt: {}", derr("overflow unpacking txt")));
                }
                let mut s = String::with_capacity(l);
                for &b in &msg[o..o + l] {
                    match b {
                        b'"' | b'\\' => {
                            s.push('\\');
                            s.push(b as char);
                        }
                        b' '..=b'~' => s.push(b as char),
                        _ => s.push_str(&format!("\\{b:03}")),
                    }
                }
                ss.push(s);
                o += l;
            }
            (RrData::Txt(ss), o)
        }
        TYPE_OPT => {
            let mut o = off;
            while o < msg.len() {
                if o + 4 > msg.len() || o + 4 + u16::from_be_bytes([msg[o + 2], msg[o + 3]]) as usize > msg.len() {
                    return Err(format!("OPT.Option: {}", derr("overflow unpacking opt")));
                }
                o += 4 + u16::from_be_bytes([msg[o + 2], msg[o + 3]]) as usize;
            }
            (RrData::Opt(msg[off..o].to_vec()), o)
        }
        _ => (RrData::Other(msg[off..end].to_vec()), end),
    };
    if off != end {
        return Err(derr("bad rdlength"));
    }
    Ok((Rr { hdr, data }, off))
}

// ---------------------------------------------------------------------------------------------------------
// miekg/dns: the client.

/// miekg `dns.Client` with its default timeouts. `net` is "" (UDP) or "tcp".
#[derive(Clone, Debug, Default)]
pub struct Client {
    pub net: String,
}

/// Go `net.OpError` text: "read udp 10.0.0.1:5353->10.0.0.2:53: i/o timeout".
fn op_err(op: &str, netw: &str, local: Option<SocketAddr>, remote: &str, e: &std::io::Error) -> String {
    let src = local.map(|l| format!("{l}->")).unwrap_or_default();
    format!("{op} {netw} {src}{remote}: {}", net::types::go_io_error(e))
}

fn dial_err(netw: &str, addr: &str, e: &std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::TimedOut => format!("dial {netw} {addr}: i/o timeout"),
        _ => format!("dial {netw} {addr}: connect: {}", net::types::go_io_error(e)),
    }
}

fn resolve(netw: &str, address: &str) -> Result<Vec<SocketAddr>, String> {
    // "host:port" or "[v6%zone]:port"; the host may also be a name.
    let (host, port) = address.rsplit_once(':').ok_or_else(|| format!("dial {netw} {address}: missing port in address"))?;
    let host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    let port: u16 = port.parse().map_err(|_| format!("dial {netw} {address}: unknown port"))?;
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs().map_err(|e| format!("dial {netw} {address}: lookup {host}: {e}"))?.collect();
    if addrs.is_empty() {
        return Err(format!("dial {netw} {address}: lookup {host}: no such host"));
    }
    Ok(addrs)
}

/// Waits until `deadline`; `Err` when it has passed.
fn until(deadline: Instant) -> std::io::Result<Duration> {
    let d = deadline.saturating_duration_since(Instant::now());
    if d.is_zero() { Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "i/o timeout")) } else { Ok(d) }
}

impl Client {
    /// miekg `Client.Exchange`: sends `m` to `address` and waits for the reply with the same id (UDP: other
    /// replies are skipped; TCP: another id is an error). No retry and no TCP fallback here.
    pub fn exchange(&self, m: &Msg, address: &str) -> Result<Msg, String> {
        let tcp = self.net == "tcp";
        let netw = if tcp { "tcp" } else { "udp" };
        let addrs = resolve(netw, address)?;
        let out = m.pack()?;
        if out.len() > MAX_MSG_SIZE {
            return Err(derr("message too large"));
        }
        if tcp {
            let mut last = None;
            let mut conn = None;
            for a in &addrs {
                match TcpStream::connect_timeout(a, DNS_TIMEOUT) {
                    Ok(c) => {
                        conn = Some(c);
                        break;
                    }
                    Err(e) => last = Some(e),
                }
            }
            let Some(mut c) = conn else {
                return Err(dial_err(netw, address, &last.expect("one address")));
            };
            let t = Instant::now();
            let (wd, rd) = (t + DNS_TIMEOUT, t + DNS_TIMEOUT);
            let local = c.local_addr().ok();
            let remote = c.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| address.to_string());
            let mut frame = Vec::with_capacity(2 + out.len());
            frame.extend_from_slice(&(out.len() as u16).to_be_bytes());
            frame.extend_from_slice(&out);
            until(wd).and_then(|d| c.set_write_timeout(Some(d))).and_then(|_| c.write_all(&frame)).map_err(|e| op_err("write", netw, local, &remote, &e))?;
            let mut read_full = |buf: &mut [u8]| -> Result<usize, String> {
                let mut n = 0;
                while n < buf.len() {
                    let r = until(rd).and_then(|d| c.set_read_timeout(Some(d))).and_then(|_| c.read(&mut buf[n..]));
                    match r {
                        Ok(0) => {
                            return Err(if n == 0 { "EOF".to_string() } else { "unexpected EOF".to_string() });
                        }
                        Ok(k) => n += k,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => return Err(op_err("read", netw, local, &remote, &e)),
                    }
                }
                Ok(n)
            };
            let mut lb = [0u8; 2];
            read_full(&mut lb)?;
            let mut p = vec![0u8; u16::from_be_bytes(lb) as usize];
            read_full(&mut p)?;
            if p.len() < HEADER_SIZE {
                return Err(derr("short read"));
            }
            let r = Msg::unpack(&p)?;
            if r.id != m.id {
                return Err(derr("id mismatch"));
            }
            return Ok(r);
        }

        let a = addrs[0];
        let bind: SocketAddr = if a.is_ipv4() { "0.0.0.0:0".parse().expect("addr") } else { "[::]:0".parse().expect("addr") };
        let sock = UdpSocket::bind(bind).and_then(|s| s.connect(a).map(|_| s)).map_err(|e| dial_err(netw, address, &e))?;
        let t = Instant::now();
        let (wd, rd) = (t + DNS_TIMEOUT, t + DNS_TIMEOUT);
        let local = sock.local_addr().ok();
        let remote = a.to_string();
        until(wd).and_then(|d| sock.set_write_timeout(Some(d))).and_then(|_| sock.send(&out)).map_err(|e| op_err("write", netw, local, &remote, &e))?;
        let size = match m.is_edns0() {
            Some(opt) if opt.hdr.class >= MIN_MSG_SIZE => opt.hdr.class as usize,
            _ => MIN_MSG_SIZE as usize,
        };
        let mut buf = vec![0u8; size];
        loop {
            let n = until(rd).and_then(|d| sock.set_read_timeout(Some(d))).and_then(|_| sock.recv(&mut buf)).map_err(|e| op_err("read", netw, local, &remote, &e))?;
            if n < HEADER_SIZE {
                return Err(derr("short read"));
            }
            let r = Msg::unpack(&buf[..n])?;
            // Ignore replies with mismatched ids: they may answer earlier queries that timed out.
            if r.id == m.id {
                return Ok(r);
            }
        }
    }
}
