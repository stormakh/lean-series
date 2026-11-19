//! The part of `github.com/hashicorp/consul/api` (v1.34.5) that Prometheus uses: `NewClient` (with its
//! environment variables), `Agent().Self()`, `Catalog().Services()` and `Health().ServiceMultipleTags()`, with
//! the same request paths, query parameters, headers, JSON decoding and error texts.
use std::collections::BTreeMap;
use std::time::Duration;

use json::Json;
use yaml::url::GoUrl;

use super::goutil::{self, Caller, StructTy, Ty, Values};

pub const HTTP_ADDR_ENV_NAME: &str = "CONSUL_HTTP_ADDR";
pub const HTTP_TOKEN_ENV_NAME: &str = "CONSUL_HTTP_TOKEN";
pub const HTTP_TOKEN_FILE_ENV_NAME: &str = "CONSUL_HTTP_TOKEN_FILE";
pub const HTTP_SSL_ENV_NAME: &str = "CONSUL_HTTP_SSL";
pub const HTTP_NAMESPACE_ENV_NAME: &str = "CONSUL_NAMESPACE";
pub const HTTP_PARTITION_ENV_NAME: &str = "CONSUL_PARTITION";

pub const HEALTH_PASSING: &str = "passing";
pub const HEALTH_WARNING: &str = "warning";
pub const HEALTH_CRITICAL: &str = "critical";
pub const HEALTH_MAINT: &str = "maintenance";
pub const NODE_MAINT: &str = "_node_maintenance";
pub const SERVICE_MAINT_PREFIX: &str = "_service_maintenance:";

const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";

/// Go `api.Config` (the fields Prometheus sets; the HTTP client is Prometheus's).
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub address: String,
    pub scheme: String,
    pub path_prefix: String,
    pub datacenter: String,
    pub namespace: String,
    pub partition: String,
    pub token: String,
    pub token_file: String,
}

/// Go `api.DefaultConfig()`: the defaults and the `CONSUL_*` environment variables that `NewClient` reads.
pub fn default_config() -> Config {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let mut c = Config { address: "localhost:8500".to_string(), scheme: "http".to_string(), ..Default::default() };
    let addr = env(HTTP_ADDR_ENV_NAME);
    if !addr.is_empty() {
        c.address = addr;
    }
    let token_file = env(HTTP_TOKEN_FILE_ENV_NAME);
    if !token_file.is_empty() {
        c.token_file = token_file;
    }
    let token = env(HTTP_TOKEN_ENV_NAME);
    if !token.is_empty() {
        c.token = token;
    }
    let ssl = env(HTTP_SSL_ENV_NAME);
    if !ssl.is_empty() && goutil::parse_bool(&ssl) == Some(true) {
        c.scheme = "https".to_string();
    }
    let ns = env(HTTP_NAMESPACE_ENV_NAME);
    if !ns.is_empty() {
        c.namespace = ns;
    }
    let p = env(HTTP_PARTITION_ENV_NAME);
    if !p.is_empty() {
        c.partition = p;
    }
    c
}

/// Go `api.Client`.
pub struct Client {
    pub config: Config,
    http: net::Client,
}

/// Go `api.NewClient` with `config.HttpClient` set.
pub fn new_client(mut config: Config, http: net::Client) -> Result<Client, String> {
    let def = default_config();
    if config.address.is_empty() {
        config.address = def.address.clone();
    }
    if config.scheme.is_empty() {
        config.scheme = def.scheme.clone();
    }
    if config.namespace.is_empty() {
        config.namespace = def.namespace.clone();
    }
    if config.partition.is_empty() {
        config.partition = def.partition.clone();
    }
    if let Some((scheme, rest)) = config.address.clone().split_once("://") {
        let scheme = scheme.to_string();
        match scheme.as_str() {
            "http" => {}
            "https" => config.scheme = "https".to_string(),
            // Go dials the socket path with its own HTTP client; this client only dials TCP, so requests to
            // "unix://" addresses fail.
            "unix" => {}
            _ => return Err(format!("Unknown protocol scheme: {scheme}")),
        }
        config.address = rest.to_string();
        // separate out a reverse proxy prefix, if it is present
        if (scheme == "http" || scheme == "https")
            && let Some((host, prefix)) = rest.split_once('/')
        {
            config.path_prefix = format!("/{prefix}");
            config.address = host.to_string();
        }
    }
    if !config.token_file.is_empty() && config.token_file != def.token_file {
        let data = std::fs::read(&config.token_file).map_err(|e| format!("Error loading token file {} : open {}: {}", config.token_file, config.token_file, net::types::go_io_error(&e)))?;
        let t = goutil::trim_space(&String::from_utf8_lossy(&data)).to_string();
        if !t.is_empty() {
            config.token = t;
        }
    } else if !config.token.is_empty() && def.token != config.token {
        // keep the configured token
    } else if !def.token_file.is_empty() {
        let data = std::fs::read(&def.token_file).map_err(|e| format!("Error loading token file {} : open {}: {}", def.token_file, def.token_file, net::types::go_io_error(&e)))?;
        let t = goutil::trim_space(&String::from_utf8_lossy(&data)).to_string();
        if !t.is_empty() {
            config.token = t;
            config.token_file = def.token_file.clone();
        }
    } else {
        config.token = def.token.clone();
    }
    Ok(Client { config, http })
}

/// Go `api.QueryOptions` (the fields Prometheus sets).
#[derive(Clone, Debug, Default)]
pub struct QueryOptions {
    pub allow_stale: bool,
    pub wait_index: u64,
    pub wait_time: Duration,
    pub node_meta: BTreeMap<String, String>,
    pub filter: String,
}

/// Go `api.QueryMeta` (the field Prometheus reads).
#[derive(Clone, Debug, Default)]
pub struct QueryMeta {
    pub last_index: u64,
}

/// Go `durToMsec`.
fn dur_to_msec(d: Duration) -> String {
    let mut ms = d.as_millis();
    if !d.is_zero() && ms == 0 {
        ms = 1;
    }
    format!("{ms}ms")
}

/// Go `api.request` before `toHTTP`.
struct Request {
    url: GoUrl,
    params: Values,
    header: net::Header,
}

impl Request {
    /// Go `request.setQueryOptions`.
    fn set_query_options(&mut self, q: &QueryOptions) {
        if q.allow_stale {
            self.params.set("stale", "");
        }
        if q.wait_index != 0 {
            self.params.set("index", &q.wait_index.to_string());
        }
        if !q.wait_time.is_zero() {
            self.params.set("wait", &dur_to_msec(q.wait_time));
        }
        if !q.filter.is_empty() {
            self.params.set("filter", &q.filter);
        }
        for (k, v) in &q.node_meta {
            self.params.add("node-meta", &format!("{k}:{v}"));
        }
    }

    /// Go `request.toHTTP` plus the Content-Type that `doRequest` sets for GET requests.
    fn into_http(mut self) -> net::Request {
        self.url.raw_query = self.params.encode();
        if self.url.host.starts_with('/') {
            self.url.host = "localhost".to_string();
        }
        self.header.set("Content-Type", JSON_CONTENT_TYPE);
        net::Request { method: "GET".to_string(), url: self.url, header: self.header, host: String::new(), body: Vec::new(), timeout: None, max_body_bytes: None, decompress: false, redirect_hosts: Vec::new(), abort: None, body_sink: None }
    }
}

/// Go `api.StatusError`.
fn status_error(resp: &net::Response) -> String {
    format!("Unexpected response code: {} ({})", resp.status, goutil::trim_space(&String::from_utf8_lossy(&resp.body)))
}

/// Go `parseQueryMeta` (its errors are ignored upstream; only the index is used).
fn parse_query_meta(resp: &net::Response) -> QueryMeta {
    let mut q = QueryMeta::default();
    let s = resp.header.get_str("X-Consul-Index");
    if !s.is_empty()
        && let Ok(i) = s.parse::<u64>()
    {
        q.last_index = i;
    }
    q
}

impl Client {
    /// Go `Client.newRequest("GET", path)`.
    fn new_request(&self, path: &str) -> Request {
        let c = &self.config;
        let url = GoUrl { scheme: c.scheme.clone(), host: c.address.clone(), path: format!("{}{}", c.path_prefix, path), ..Default::default() };
        let mut r = Request { url, params: Values::default(), header: net::Header::new() };
        if !c.datacenter.is_empty() {
            r.params.set("dc", &c.datacenter);
        }
        if !c.namespace.is_empty() {
            r.params.set("ns", &c.namespace);
        }
        if !c.partition.is_empty() {
            r.params.set("partition", &c.partition);
        }
        if !c.token.is_empty() {
            r.header.set("X-Consul-Token", &c.token);
        }
        r
    }

    /// Go `Client.doRequest` + `requireOK`.
    fn do_request(&self, r: Request, caller: Option<&mut Caller>) -> Result<net::Response, String> {
        let req = r.into_http();
        let resp = match caller {
            Some(c) => c.call(&self.http, req),
            None => self.http.do_request(req),
        }
        .map_err(|e| e.to_string())?;
        if resp.status != 200 {
            return Err(status_error(&resp));
        }
        Ok(resp)
    }

    /// Go `Agent().Self()`: `map[string]map[string]interface{}` as JSON.
    pub fn agent_self(&self) -> Result<Json, String> {
        let r = self.new_request("/v1/agent/self");
        let resp = self.do_request(r, None)?;
        static ANY_MAP: Ty = Ty::Map(&Ty::Any, "map[string]interface {}");
        static T: Ty = Ty::Map(&ANY_MAP, "map[string]map[string]interface {}");
        goutil::decode_body(&resp.body, &T)
    }

    /// Go `Catalog().Services(q)`: service names and their tags.
    pub fn catalog_services(&self, q: &QueryOptions, caller: Option<&mut Caller>) -> Result<(BTreeMap<String, Vec<String>>, QueryMeta), String> {
        let mut r = self.new_request("/v1/catalog/services");
        r.set_query_options(q);
        let resp = self.do_request(r, caller)?;
        let qm = parse_query_meta(&resp);
        static STRS: Ty = Ty::Slice(&Ty::Str, "[]string");
        static T: Ty = Ty::Map(&STRS, "map[string][]string");
        let v = goutil::decode_body(&resp.body, &T)?;
        let mut out = BTreeMap::new();
        if let Some(o) = v.as_obj() {
            for (k, tags) in o {
                out.insert(k.clone(), goutil::str_slice(Some(tags)));
            }
        }
        Ok((out, qm))
    }

    /// Go `Health().ServiceMultipleTags(service, tags, passingOnly, q)`.
    pub fn health_service_multiple_tags(&self, service: &str, tags: &[String], passing_only: bool, q: &QueryOptions, caller: Option<&mut Caller>) -> Result<(Vec<ServiceEntry>, QueryMeta), String> {
        let mut r = self.new_request(&format!("/v1/health/service/{service}"));
        r.set_query_options(q);
        for t in tags {
            r.params.add("tag", t);
        }
        if passing_only {
            r.params.set(HEALTH_PASSING, "1");
        }
        let resp = self.do_request(r, caller)?;
        let qm = parse_query_meta(&resp);
        static T: Ty = Ty::Slice(&SERVICE_ENTRY_TY, "[]*api.ServiceEntry");
        let v = goutil::decode_body(&resp.body, &T)?;
        let out = v.as_arr().map(|a| a.iter().map(ServiceEntry::from_json).collect()).unwrap_or_default();
        Ok((out, qm))
    }
}

/// Go `api.Node` (the fields Prometheus reads).
#[derive(Clone, Debug, Default)]
pub struct Node {
    pub node: String,
    pub address: String,
    pub tagged_addresses: BTreeMap<String, String>,
    pub meta: BTreeMap<String, String>,
}

/// Go `api.AgentService` (the fields Prometheus reads).
#[derive(Clone, Debug, Default)]
pub struct AgentService {
    pub id: String,
    pub tags: Vec<String>,
    pub meta: BTreeMap<String, String>,
    pub port: i64,
    pub address: String,
    pub namespace: String,
    pub partition: String,
}

/// Go `api.HealthCheck` (the fields Prometheus reads).
#[derive(Clone, Debug, Default)]
pub struct HealthCheck {
    pub check_id: String,
    pub status: String,
}

/// Go `api.ServiceEntry`. A null `Node`, `Service` or check (Go: a nil pointer that Prometheus dereferences)
/// reads as the zero value.
#[derive(Clone, Debug, Default)]
pub struct ServiceEntry {
    pub node: Node,
    pub service: AgentService,
    pub checks: Vec<HealthCheck>,
}

impl ServiceEntry {
    fn from_json(v: &Json) -> ServiceEntry {
        let mut e = ServiceEntry::default();
        if let Some(n) = goutil::field(v, "Node") {
            e.node = Node {
                node: goutil::str_field(n, "Node"),
                address: goutil::str_field(n, "Address"),
                tagged_addresses: goutil::str_map(goutil::field(n, "TaggedAddresses")),
                meta: goutil::str_map(goutil::field(n, "Meta")),
            };
        }
        if let Some(s) = goutil::field(v, "Service") {
            e.service = AgentService {
                id: goutil::str_field(s, "ID"),
                tags: goutil::str_slice(goutil::field(s, "Tags")),
                meta: goutil::str_map(goutil::field(s, "Meta")),
                port: goutil::int_field(s, "Port"),
                address: goutil::str_field(s, "Address"),
                namespace: goutil::str_field(s, "Namespace"),
                partition: goutil::str_field(s, "Partition"),
            };
        }
        if let Some(a) = goutil::field(v, "Checks").and_then(Json::as_arr) {
            e.checks = a.iter().map(|c| HealthCheck { check_id: goutil::str_field(c, "CheckID"), status: goutil::str_field(c, "Status") }).collect();
        }
        e
    }
}

/// Go `HealthChecks.AggregatedStatus`.
pub fn aggregated_status(checks: &[HealthCheck]) -> &'static str {
    let (mut passing, mut warning, mut critical, mut maintenance) = (false, false, false, false);
    for c in checks {
        if c.check_id == NODE_MAINT || c.check_id.starts_with(SERVICE_MAINT_PREFIX) {
            maintenance = true;
            continue;
        }
        match c.status.as_str() {
            HEALTH_PASSING => passing = true,
            HEALTH_WARNING => warning = true,
            HEALTH_CRITICAL => critical = true,
            _ => return "",
        }
    }
    if maintenance {
        HEALTH_MAINT
    } else if critical {
        HEALTH_CRITICAL
    } else if warning {
        HEALTH_WARNING
    } else {
        let _ = passing;
        HEALTH_PASSING
    }
}

// The Go types of the health response, for Go's type checks while decoding. Fields that Prometheus does not
// read and whose type is not a plain string, number or bool are not checked.
static LOCALITY_TY: StructTy = StructTy { name: "Locality", go_type: "api.Locality", fields: &[("Region", Ty::Str), ("Zone", Ty::Str)] };
static WEIGHTS_TY: StructTy = StructTy { name: "AgentWeights", go_type: "api.AgentWeights", fields: &[("Passing", Ty::Int), ("Warning", Ty::Int)] };
static NODE_TY: StructTy = StructTy {
    name: "Node",
    go_type: "api.Node",
    fields: &[
        ("ID", Ty::Str),
        ("Node", Ty::Str),
        ("Address", Ty::Str),
        ("Datacenter", Ty::Str),
        ("TaggedAddresses", Ty::Map(&Ty::Str, "map[string]string")),
        ("Meta", Ty::Map(&Ty::Str, "map[string]string")),
        ("CreateIndex", Ty::Uint64),
        ("ModifyIndex", Ty::Uint64),
        ("Partition", Ty::Str),
        ("PeerName", Ty::Str),
        ("Locality", Ty::Struct(&LOCALITY_TY)),
    ],
};
static AGENT_SERVICE_TY: StructTy = StructTy {
    name: "AgentService",
    go_type: "api.AgentService",
    fields: &[
        ("Kind", Ty::Str),
        ("ID", Ty::Str),
        ("Service", Ty::Str),
        ("Tags", Ty::Slice(&Ty::Str, "[]string")),
        ("Meta", Ty::Map(&Ty::Str, "map[string]string")),
        ("Port", Ty::Int),
        ("Ports", Ty::Any),
        ("Address", Ty::Str),
        ("SocketPath", Ty::Str),
        ("TaggedAddresses", Ty::Any),
        ("Weights", Ty::Struct(&WEIGHTS_TY)),
        ("EnableTagOverride", Ty::Bool),
        ("CreateIndex", Ty::Uint64),
        ("ModifyIndex", Ty::Uint64),
        ("ContentHash", Ty::Str),
        ("Proxy", Ty::Any),
        ("Connect", Ty::Any),
        ("PeerName", Ty::Str),
        ("AI", Ty::Any),
        ("Namespace", Ty::Str),
        ("Partition", Ty::Str),
        ("Datacenter", Ty::Str),
        ("Locality", Ty::Struct(&LOCALITY_TY)),
    ],
};
static HEALTH_CHECK_TY: StructTy = StructTy {
    name: "HealthCheck",
    go_type: "api.HealthCheck",
    fields: &[
        ("Node", Ty::Str),
        ("CheckID", Ty::Str),
        ("Name", Ty::Str),
        ("Status", Ty::Str),
        ("Notes", Ty::Str),
        ("Output", Ty::Str),
        ("ServiceID", Ty::Str),
        ("ServiceName", Ty::Str),
        ("ServiceTags", Ty::Slice(&Ty::Str, "[]string")),
        ("Type", Ty::Str),
        ("Namespace", Ty::Str),
        ("Partition", Ty::Str),
        ("ExposedPort", Ty::Int),
        ("PeerName", Ty::Str),
        ("Definition", Ty::Any),
        ("CreateIndex", Ty::Uint64),
        ("ModifyIndex", Ty::Uint64),
    ],
};
static SERVICE_ENTRY_TY: Ty = Ty::Struct(&SERVICE_ENTRY_STRUCT);
static SERVICE_ENTRY_STRUCT: StructTy = StructTy {
    name: "ServiceEntry",
    go_type: "api.ServiceEntry",
    fields: &[("Node", Ty::Struct(&NODE_TY)), ("Service", Ty::Struct(&AGENT_SERVICE_TY)), ("Checks", Ty::Slice(&Ty::Struct(&HEALTH_CHECK_TY), "api.HealthChecks"))],
};

