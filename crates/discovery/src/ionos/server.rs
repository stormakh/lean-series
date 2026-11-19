//! Port of `discovery/ionos/server.go` and of the sdk-go/v6 pieces it calls: `NewConfiguration` (server URL,
//! host and scheme from the endpoint), `ServersApi.DatacentersServersGet(...).Depth(3).Execute()` (query
//! parameters, headers, `IONOS_CONTRACT_NUMBER`, the retry loop of `callAPI`, `GenericOpenAPIError` texts and
//! the JSON decoding of `Servers`).
//!
//! Differs from the SDK: `IONOS_PINNED_CERT` (certificate pinning that replaces the transport) and the
//! `IONOS_LOG_LEVEL` debug dumps are not ported; XML responses are not decoded (an XML body is treated as an
//! empty `Servers`); the request is not sent with `Connection: close`; fields of the SDK models that the SD
//! does not read are not type-checked.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use json::Json;
use model::labelset::LabelSet;

use super::{META_LABEL_SEPARATOR, SdConfig, meta_label_prefix};
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::refresh::Refresher;
use crate::sd_json::{self, INT32, INT64, STRINGS, StructTy, Ty};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

/// Go `DefaultIonosServerUrl`.
pub const DEFAULT_IONOS_SERVER_URL: &str = "https://api.ionos.com/cloudapi/v6";
/// Go `DefaultIonosBasePath`.
pub const DEFAULT_IONOS_BASE_PATH: &str = "/cloudapi/v6";
/// Go `nicDefaultName`.
pub const NIC_DEFAULT_NAME: &str = "unnamed";

const MAX_RETRIES: u32 = 3;
const WAIT_TIME: Duration = Duration::from_millis(100);
const MAX_WAIT_TIME: Duration = Duration::from_millis(2000);

const TYPE: Ty = Ty::NamedStr("ionoscloud.Type");
const PAGINATION_LINKS: Ty = Ty::Struct(&StructTy { go: "ionoscloud.PaginationLinks", fields: &[("prev", Ty::Str), ("self", Ty::Str), ("next", Ty::Str)] });
const RESOURCE_REFERENCE: Ty = Ty::Struct(&StructTy { go: "ionoscloud.ResourceReference", fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str)] });
const METADATA: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.DatacenterElementMetadata",
    fields: &[("etag", Ty::Str), ("createdBy", Ty::Str), ("createdByUserId", Ty::Str), ("lastModifiedBy", Ty::Str), ("lastModifiedByUserId", Ty::Str), ("state", Ty::Str)],
});
const SERVER_PROPERTIES: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.ServerProperties",
    fields: &[
        ("templateUuid", Ty::Str),
        ("name", Ty::Str),
        ("hostname", Ty::Str),
        ("cores", INT32),
        ("ram", INT32),
        ("availabilityZone", Ty::Str),
        ("vmState", Ty::Str),
        ("bootCdrom", RESOURCE_REFERENCE),
        ("bootVolume", RESOURCE_REFERENCE),
        ("cpuFamily", Ty::Str),
        ("type", Ty::Str),
        ("enabledFeatures", STRINGS),
        ("placementGroupId", Ty::Str),
        ("nicMultiQueue", Ty::Bool),
    ],
});
const NIC_PROPERTIES: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.NicProperties",
    fields: &[
        ("name", Ty::Str),
        ("mac", Ty::Str),
        ("ips", STRINGS),
        ("dhcp", Ty::Bool),
        ("ipv6Ips", STRINGS),
        ("ipv6CidrBlock", Ty::Str),
        ("dhcpv6", Ty::Bool),
        ("lan", INT32),
        ("firewallActive", Ty::Bool),
        ("firewallType", Ty::Str),
        ("deviceNumber", INT32),
        ("pciSlot", INT32),
        ("vnet", Ty::Str),
    ],
});
const NIC: StructTy = StructTy { go: "ionoscloud.Nic", fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("metadata", METADATA), ("properties", NIC_PROPERTIES)] };
const NICS: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.Nics",
    fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("items", Ty::Slice(&Ty::Struct(&NIC))), ("offset", Ty::Float32), ("limit", Ty::Float32), ("_links", PAGINATION_LINKS)],
});
const VOLUME_PROPERTIES: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.VolumeProperties",
    fields: &[("name", Ty::Str), ("type", Ty::Str), ("size", Ty::Float32), ("availabilityZone", Ty::Str), ("image", Ty::Str), ("imageAlias", Ty::Str), ("bus", Ty::Str), ("licenceType", Ty::Str), ("deviceNumber", INT64)],
});
const VOLUME: StructTy = StructTy { go: "ionoscloud.Volume", fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("metadata", METADATA), ("properties", VOLUME_PROPERTIES)] };
const ATTACHED_VOLUMES: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.AttachedVolumes",
    fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("items", Ty::Slice(&Ty::Struct(&VOLUME))), ("offset", Ty::Float32), ("limit", Ty::Float32), ("_links", PAGINATION_LINKS)],
});
const SERVER_ENTITIES: Ty = Ty::Struct(&StructTy { go: "ionoscloud.ServerEntities", fields: &[("volumes", ATTACHED_VOLUMES), ("nics", NICS)] });
const SERVER: StructTy = StructTy {
    go: "ionoscloud.Server",
    fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("metadata", METADATA), ("properties", SERVER_PROPERTIES), ("entities", SERVER_ENTITIES)],
};
/// Go `ionoscloud.Servers`.
const SERVERS: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.Servers",
    fields: &[("id", Ty::Str), ("type", TYPE), ("href", Ty::Str), ("items", Ty::Slice(&Ty::Struct(&SERVER))), ("offset", Ty::Float32), ("limit", Ty::Float32), ("_links", PAGINATION_LINKS)],
});
/// Go `ionoscloud.Error`.
const ERROR: Ty = Ty::Struct(&StructTy {
    go: "ionoscloud.Error",
    fields: &[("httpStatus", INT32), ("messages", Ty::Slice(&Ty::Struct(&StructTy { go: "ionoscloud.ErrorMessage", fields: &[("errorCode", Ty::Str), ("message", Ty::Str)] })))],
});

/// Go `jsonCheck`: `(?i:(?:application|text)/(?:vnd\.[^;]+\+)?json)` (unanchored).
pub fn json_check(ct: &str) -> bool {
    let l = ct.to_ascii_lowercase();
    for prefix in ["application/", "text/"] {
        let mut from = 0;
        while let Some(i) = l[from..].find(prefix) {
            let rest = &l[from + i + prefix.len()..];
            if rest.starts_with("json") {
                return true;
            }
            if let Some(v) = rest.strip_prefix("vnd.") {
                // [^;]+ then "+json": any '+json' before the first ';' after at least one char
                let seg = v.split(';').next().unwrap_or("");
                if seg.char_indices().any(|(j, _)| j > 0 && seg[j..].starts_with("+json")) {
                    return true;
                }
            }
            from += i + 1;
        }
    }
    false
}

/// Go `xmlCheck`: `(?i:(?:application|text)/xml)` (unanchored).
pub fn xml_check(ct: &str) -> bool {
    let l = ct.to_ascii_lowercase();
    l.contains("application/xml") || l.contains("text/xml")
}

/// Go `EnsureURLFormat`.
pub fn ensure_url_format(url: &str) -> String {
    if url.len() <= 1 {
        return url.to_string();
    }
    let url = url.strip_suffix('/').unwrap_or(url);
    if !url.starts_with("https://") && !url.starts_with("http://") {
        return format!("https://{url}");
    }
    url.to_string()
}

/// Go `getServerUrl`.
pub fn get_server_url(server_url: &str) -> String {
    if server_url.is_empty() {
        return DEFAULT_IONOS_SERVER_URL.to_string();
    }
    let mut u = server_url.to_string();
    if !u.ends_with(DEFAULT_IONOS_BASE_PATH) {
        u.push_str(DEFAULT_IONOS_BASE_PATH);
    }
    ensure_url_format(&u)
}

/// Go `getHost`.
pub fn get_host(server_url: &str) -> String {
    let u = if !server_url.starts_with("https://") && !server_url.starts_with("http://") { format!("http://{server_url}") } else { server_url.to_string() };
    yaml::url::parse(&u).map(|u| u.host).unwrap_or_default()
}

/// Go `getScheme`.
pub fn get_scheme(server_url: &str) -> String {
    yaml::url::parse(server_url).map(|u| u.scheme).unwrap_or_default()
}

/// Go `time.ParseDuration`.
pub fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let orig = s;
    let invalid = || format!("time: invalid duration {}", yaml::go_quote(orig));
    let (neg, mut s) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    let mut total = 0f64;
    while !s.is_empty() {
        let int_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (ip, rest) = s.split_at(int_end);
        let (fp, rest) = match rest.strip_prefix('.') {
            Some(r) => {
                let e = r.find(|c: char| !c.is_ascii_digit()).unwrap_or(r.len());
                (&r[..e], &r[e..])
            }
            None => ("", rest),
        };
        if ip.is_empty() && fp.is_empty() {
            return Err(invalid());
        }
        let unit_end = rest.find(|c: char| c == '.' || c.is_ascii_digit()).unwrap_or(rest.len());
        let (unit, rest) = rest.split_at(unit_end);
        if unit.is_empty() {
            return Err(format!("time: missing unit in duration {}", yaml::go_quote(orig)));
        }
        let mult = match unit {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return Err(format!("time: unknown unit {} in duration {}", yaml::go_quote(unit), yaml::go_quote(orig))),
        };
        let v: f64 = format!("{}.{}", if ip.is_empty() { "0" } else { ip }, if fp.is_empty() { "0" } else { fp }).parse().map_err(|_| invalid())?;
        total += v * mult;
        s = rest;
    }
    if neg || total <= 0.0 {
        return Ok(Duration::ZERO);
    }
    Ok(Duration::from_secs_f64(total))
}

/// The capacity Go's JSON decoder gives a decoded slice of `n` strings (`reflect.Value.Grow(1)` per element).
fn go_decoded_cap(n: usize) -> usize {
    let mut cap = 0usize;
    for i in 0..n {
        if i >= cap {
            let new_len = i + 1;
            let double = cap * 2;
            cap = if new_len > double {
                new_len
            } else if cap < 256 {
                double
            } else {
                let mut c = cap;
                while c < new_len {
                    c += (c + 3 * 256) >> 2;
                }
                c
            };
        }
    }
    cap
}

/// Go `serverDiscovery`.
pub struct ServerDiscovery {
    http: net::Client,
    servers_url: String,
    host: String,
    scheme: String,
    user_agent: String,
    port: i64,
    datacenter_id: String,
}

/// Go `newServerDiscovery`.
pub fn new_server_discovery(conf: &SdConfig, _logger: Logger) -> Result<ServerDiscovery, String> {
    let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
    let mut http = net::Client::new(rt);
    http.timeout = Some(conf.refresh_interval.to_std());
    let servers_url = if conf.ionos_endpoint.is_empty() { DEFAULT_IONOS_SERVER_URL.to_string() } else { get_server_url(&conf.ionos_endpoint) };
    Ok(ServerDiscovery {
        http,
        servers_url,
        host: get_host(&conf.ionos_endpoint),
        scheme: get_scheme(&conf.ionos_endpoint),
        user_agent: crate::http::USER_AGENT.to_string(),
        port: conf.port,
        datacenter_id: conf.datacenter_id.clone(),
    })
}

impl ServerDiscovery {
    /// `DatacentersServersGet(ctx, id).Depth(3).Execute()`: the decoded `Servers`.
    fn datacenters_servers_get(&self, ctx: &CancelToken) -> Result<Json, String> {
        let base = &self.servers_url;
        if !base.contains("http://") && !base.contains("https://") {
            return Err(format!("the URL provided appears to be missing the protocol scheme prefix (\"https://\" or \"http://\"), please verify and try again: {base}"));
        }
        let path = format!("{base}/datacenters/{}/servers", yaml::url::path_escape(&self.datacenter_id));
        let mut u = yaml::url::parse(&path)?;
        if !self.host.is_empty() {
            u.host = self.host.clone();
        }
        if !self.scheme.is_empty() {
            u.scheme = self.scheme.clone();
        }
        let mut q = u.query_pairs();
        for (k, v) in [("pretty", "true"), ("depth", "3"), ("offset", "0"), ("limit", "1000")] {
            q.push((k.to_string(), v.to_string()));
        }
        u.raw_query = yaml::url::encode_query(&q);
        let mut req = net::Request::new("GET", &u.to_url_string(), Vec::new()).map_err(|e| e.to_string())?;
        req.header.set("Accept", "application/json");
        if let Ok(v) = std::env::var("IONOS_CONTRACT_NUMBER") {
            req.header.set("X-Contract-Number", &v);
        }
        req.header.add("User-Agent", &self.user_agent);

        let resp = self.call_api(ctx, req)?;
        let ct = resp.header.get_str("Content-Type");
        if resp.status >= 300 {
            let mut err = format!("{} {}", resp.status_text, String::from_utf8_lossy(&resp.body));
            if let Err(e) = decode(&resp.body, ct, &ERROR) {
                err = format!("{} {}", resp.status_text, e);
            }
            return Err(err);
        }
        decode(&resp.body, ct, &SERVERS)
    }

    /// Go `APIClient.callAPI`: retries 500 (GET), 502, 503, 504 and 429 up to 3 attempts.
    fn call_api(&self, ctx: &CancelToken, req: net::Request) -> Result<net::Response, String> {
        let mut retry_count = 0;
        loop {
            retry_count += 1;
            let resp = self.http.do_request(req.clone()).map_err(|e| e.to_string())?;
            let backoff = match resp.status {
                500 => {
                    if req.method != "GET" {
                        return Ok(resp);
                    }
                    WAIT_TIME
                }
                502..=504 => {
                    if req.method == "POST" {
                        return Ok(resp);
                    }
                    WAIT_TIME
                }
                429 => match resp.header.get("Retry-After").filter(|v| !v.is_empty()) {
                    Some(v) => parse_go_duration(&format!("{v}s"))?,
                    None => WAIT_TIME,
                },
                _ => return Ok(resp),
            };
            if retry_count >= MAX_RETRIES {
                return Ok(resp);
            }
            let t = backoff.min(MAX_WAIT_TIME);
            if !t.is_zero() {
                ctx.wait_timeout(t);
            }
        }
    }

    /// Go `serverDiscovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let servers = self.datacenters_servers_get(ctx)?;
        let p = format!("{}server_", meta_label_prefix());
        let items: &[Json] = sd_json::ptr(&servers, "items").and_then(Json::as_arr).map(|v| v.as_slice()).unwrap_or(&[]);
        let servers_id = sd_json::opt_str(&servers, "id");
        let mut targets = Vec::new();
        for server in items {
            let mut ips: Vec<String> = Vec::new();
            let mut ips_by_nic_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
            let entities = sd_json::ptr(server, "entities");
            let nic_items = entities.and_then(|e| sd_json::ptr(e, "nics")).and_then(|n| sd_json::ptr(n, "items")).and_then(Json::as_arr);
            for nic in nic_items.map(|v| v.as_slice()).unwrap_or(&[]) {
                let Some(props) = sd_json::ptr(nic, "properties") else { continue };
                let nic_name = sd_json::opt_str(props, "name").unwrap_or_else(|| NIC_DEFAULT_NAME.to_string());
                let nic_ips = sd_json::ptr(props, "ips").and_then(Json::as_arr).map(|a| a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect::<Vec<_>>()).unwrap_or_default();
                // Go: ips = append(nicIPs, ips...); byName[n] = append(nicIPs, byName[n]...). When the decoded
                // nicIPs has spare capacity, both appends write into its array and the second overwrites the
                // tail of ips.
                let cap = go_decoded_cap(nic_ips.len());
                let l = nic_ips.len();
                let mut new_ips = nic_ips.clone();
                new_ips.extend(ips.iter().cloned());
                let ips_aliased = !ips.is_empty() && l + ips.len() <= cap;
                let prev = ips_by_nic_name.remove(&nic_name).unwrap_or_default();
                let mut new_by_name = nic_ips.clone();
                new_by_name.extend(prev.iter().cloned());
                if ips_aliased && l + prev.len() <= cap {
                    for (i, v) in prev.iter().enumerate() {
                        new_ips[l + i] = v.clone();
                    }
                }
                ips = new_ips;
                ips_by_nic_name.insert(nic_name, new_by_name);
            }
            // A server without IP addresses is dropped.
            if ips.is_empty() {
                continue;
            }
            let mut labels = LabelSet::new();
            labels.insert("__address__".to_string(), join_host_port(&ips[0], self.port as u64));
            labels.insert(format!("{p}ip"), join(&ips, META_LABEL_SEPARATOR));
            let empty = Json::Obj(Vec::new());
            let props = sd_json::ptr(server, "properties").unwrap_or(&empty);
            let mut set = |k: &str, v: Option<String>| {
                if let Some(v) = v {
                    labels.insert(format!("{p}{k}"), v);
                }
            };
            set("availability_zone", sd_json::opt_str(props, "availabilityZone"));
            set("cpu_family", sd_json::opt_str(props, "cpuFamily"));
            set("servers_id", servers_id.clone());
            set("id", sd_json::opt_str(server, "id"));
            set("lifecycle", sd_json::ptr(server, "metadata").and_then(|m| sd_json::opt_str(m, "state")));
            set("name", sd_json::opt_str(props, "name"));
            set("state", sd_json::opt_str(props, "vmState"));
            set("type", sd_json::opt_str(props, "type"));
            for (nic_name, nic_ips) in &ips_by_nic_name {
                set(&format!("nic_ip_{}", sanitize_label_name(nic_name)), Some(join(nic_ips, META_LABEL_SEPARATOR)));
            }
            set("boot_cdrom_id", sd_json::ptr(props, "bootCdrom").and_then(|r| sd_json::opt_str(r, "id")));
            set("boot_volume_id", sd_json::ptr(props, "bootVolume").and_then(|r| sd_json::opt_str(r, "id")));
            let volumes = entities.and_then(|e| sd_json::ptr(e, "volumes")).and_then(|v| sd_json::ptr(v, "items")).and_then(Json::as_arr);
            if let Some(first) = volumes.and_then(|v| v.first())
                && let Some(vp) = sd_json::ptr(first, "properties")
            {
                set("boot_image_id", sd_json::opt_str(vp, "image"));
            }
            targets.push(labels);
        }
        Ok(vec![Arc::new(Group { source: "ionos".to_string(), targets, labels: LabelSet::new() })])
    }
}

impl Refresher for ServerDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        ServerDiscovery::refresh(self, ctx)
    }
}

/// Go `APIClient.decode` for a model type.
fn decode(body: &[u8], content_type: &str, ty: &Ty) -> Result<Json, String> {
    if body.is_empty() {
        return Ok(Json::Obj(Vec::new()));
    }
    if xml_check(content_type) {
        return Ok(Json::Obj(Vec::new()));
    }
    if json_check(content_type) {
        return sd_json::unmarshal(body, ty);
    }
    Err(format!("undefined response type for content {content_type}"))
}

/// Go `join`: `strings.Join` with the separator also at the start and the end.
pub fn join(elems: &[String], sep: &str) -> String {
    crate::sdutil::join_tags(elems, sep)
}
