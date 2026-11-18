//! Port of `discovery/digitalocean`: Droplets (role `droplets`) and Managed Databases (role `databases`), with
//! the subset of godo v1.229.0 they use (`Droplets.List` with `links.pages` pagination, `Databases.List`,
//! request building against `BaseURL`, `CheckResponse` errors, JSON decoding).
//!
//! Differs from Go: a droplet without `image` or `region` gets "" labels (Go dereferences nil and panics); a
//! request in flight is not aborted on cancel. The test-only `httpclient` key (Go's exported `HTTPClient
//! *http.Client` field) accepts null or a mapping with `timeout` (nanoseconds) only.
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::url::GoUrl;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::gojson::{self, Dec, key_is, sub};
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;
use crate::{
    Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator,
    SdConfigType,
};

mod db;

pub use db::DatabasesDiscovery;

const SEPARATOR: &str = ",";
const DO_LABEL_ID: &str = "__meta_digitalocean_droplet_id";
const DO_LABEL_NAME: &str = "__meta_digitalocean_droplet_name";
const DO_LABEL_IMAGE: &str = "__meta_digitalocean_image";
const DO_LABEL_IMAGE_NAME: &str = "__meta_digitalocean_image_name";
const DO_LABEL_PRIVATE_IPV4: &str = "__meta_digitalocean_private_ipv4";
const DO_LABEL_PUBLIC_IPV4: &str = "__meta_digitalocean_public_ipv4";
const DO_LABEL_PUBLIC_IPV6: &str = "__meta_digitalocean_public_ipv6";
const DO_LABEL_REGION: &str = "__meta_digitalocean_region";
const DO_LABEL_SIZE: &str = "__meta_digitalocean_size";
const DO_LABEL_STATUS: &str = "__meta_digitalocean_status";
const DO_LABEL_FEATURES: &str = "__meta_digitalocean_features";
const DO_LABEL_TAGS: &str = "__meta_digitalocean_tags";
const DO_LABEL_VPC: &str = "__meta_digitalocean_vpc";

/// Go `godo.defaultBaseURL`.
pub const DEFAULT_BASE_URL: &str = "https://api.digitalocean.com/";
/// `godo.SetUserAgent(version.PrometheusUserAgent())`: "<ua> godo/<version>".
pub const GODO_USER_AGENT: &str = "Prometheus/3.16.0 godo/1.229.0";

/// Go `DropletsRole`.
pub const DROPLETS_ROLE: &str = "droplets";
/// Go `DatabasesRole`.
pub const DATABASES_ROLE: &str = "databases";

/// Go `Role`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Role(pub String);

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = s;
            match self.0.as_str() {
                DROPLETS_ROLE | DATABASES_ROLE => Ok(()),
                _ => Err(YError::Msg(format!(
                    "unknown DigitalOcean SD role {}",
                    yaml::go_quote(&self.0)
                ))),
            }
        })
    }
    fn go_type() -> String {
        "digitalocean.Role".to_string()
    }
}
impl yaml::Marshal for Role {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        self.0.marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `*http.Client` as the YAML of the test-only `HTTPClient` field: only `timeout` (nanoseconds).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GoHttpClient {
    pub timeout: i64,
}
yaml::yaml_struct!(GoHttpClient, "http.Client", [
    (timeout: "timeout")
]);
yaml::impl_plain_struct!(GoHttpClient);

/// Go `digitalocean.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub role: Role,
    /// Internal field for testing (Go `HTTPClient`): when set, a plain HTTP client is used instead of
    /// `http_client_config`.
    pub http_client: Option<GoHttpClient>,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        port: 80,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: HttpClientConfig::default_config(),
        role: Role(DROPLETS_ROLE.to_string()),
        http_client: None,
    }
}

yaml::yaml_struct!(SdConfig, "digitalocean.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
    (role: "role")
    (http_client: "httpclient")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.role.0.is_empty() {
                return Err(YError::Msg(
                    "role missing (one of: droplets, databases)".into(),
                ));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "digitalocean.plain".to_string()
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
    const GO_TYPE: &'static str = "digitalocean.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "digitalocean"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(
        &self,
        _reg: &Arc<Registry>,
        rmi: Arc<dyn RefreshMetricsInstantiator>,
    ) -> Arc<dyn DiscovererMetrics> {
        Arc::new(DigitaloceanMetrics {
            refresh_metrics: rmi,
        })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `digitaloceanMetrics`: only the refresh metrics.
pub struct DigitaloceanMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for DigitaloceanMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `digitalocean.Discovery` (a `refresh.Discovery`).
pub struct Discovery {
    inner: refresh::Discovery<DoRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<DigitaloceanMetrics>(&opts.metrics)
            .ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let r = new_refresher(conf)?;
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "digitalocean",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: r,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// The refresher of the configured role (Go `refresher`).
pub enum DoRefresher {
    Droplets(DropletsDiscovery),
    Databases(DatabasesDiscovery),
}

impl DoRefresher {
    /// The godo client (tests set its `base_url`).
    pub fn client(&mut self) -> &mut GodoClient {
        match self {
            DoRefresher::Droplets(d) => &mut d.client,
            DoRefresher::Databases(d) => &mut d.client,
        }
    }
}

impl Refresher for DoRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        match self {
            DoRefresher::Droplets(d) => d.refresh(ctx),
            DoRefresher::Databases(d) => d.refresh(ctx),
        }
    }
}

fn timeout(d: std::time::Duration) -> Option<std::time::Duration> {
    (!d.is_zero()).then_some(d)
}

/// Go `newRefresher`.
pub fn new_refresher(conf: &SdConfig) -> Result<DoRefresher, String> {
    let http = match &conf.http_client {
        Some(c) => {
            // a plain Go http.Client: default transport, its own timeout
            let mut h = net::new_client_from_config(
                &HttpClientConfig::default_config(),
                &net::HttpClientOptions::default(),
            )
            .map_err(|e| e.to_string())?;
            h.timeout = timeout(std::time::Duration::from_nanos(c.timeout.max(0) as u64));
            h
        }
        None => {
            let mut h = net::new_client_from_config(
                &conf.http_client_config,
                &net::HttpClientOptions::default(),
            )
            .map_err(|e| e.to_string())?;
            h.timeout = timeout(conf.refresh_interval.to_std());
            h
        }
    };
    let client = GodoClient {
        base_url: yaml::url::parse(DEFAULT_BASE_URL)
            .map_err(|e| format!("error setting up digital ocean agent: {e}"))?,
        http,
    };
    match conf.role.0.as_str() {
        DROPLETS_ROLE => Ok(DoRefresher::Droplets(DropletsDiscovery {
            client,
            port: conf.port,
        })),
        DATABASES_ROLE => Ok(DoRefresher::Databases(DatabasesDiscovery {
            client,
            port: conf.port,
        })),
        r => Err(format!(
            "unknown DigitalOcean SD role {}",
            yaml::go_quote(r)
        )),
    }
}

/// Go `godo.Client`, the parts used.
pub struct GodoClient {
    /// Go `BaseURL`.
    pub base_url: GoUrl,
    http: net::Client,
}

/// Go `godo.ListOptions` (page and per_page, omitted when zero).
#[derive(Clone, Copy, Default)]
pub(crate) struct ListOptions {
    pub page: i64,
    pub per_page: i64,
}

/// Go `addOptions`: merges the options into the query of `s`.
fn add_options(s: &str, opt: ListOptions) -> Result<String, String> {
    let mut u = yaml::url::parse(s)?;
    let (mut q, _) = yaml::url::parse_query(&u.raw_query);
    for (k, v) in [("page", opt.page), ("per_page", opt.per_page)] {
        if v != 0 {
            q.retain(|(n, _)| n != k);
            q.push((k.to_string(), v.to_string()));
        }
    }
    u.raw_query = yaml::url::encode_query(&q);
    Ok(u.to_url_string())
}

/// The `links` of a list response (Go `godo.Links`; only `pages` matters).
#[derive(Default)]
pub(crate) struct Links {
    pub pages: Option<Pages>,
}

/// Go `godo.Pages`.
#[derive(Default)]
pub(crate) struct Pages {
    pub prev: String,
    pub next: String,
}

impl Links {
    /// Go `IsLastPage`.
    fn is_last_page(&self) -> bool {
        self.pages.as_ref().is_none_or(|p| p.next.is_empty())
    }

    /// Go `CurrentPage`.
    fn current_page(&self) -> Result<i64, String> {
        match &self.pages {
            None => Ok(1),
            Some(p) if p.prev.is_empty() && !p.next.is_empty() => Ok(1),
            Some(p) if !p.prev.is_empty() => Ok(page_for_url(&p.prev)? + 1),
            Some(_) => Ok(0),
        }
    }
}

/// Go `pageForURL`.
fn page_for_url(s: &str) -> Result<i64, String> {
    let u = yaml::url::parse_request_uri(s)?;
    let (q, _) = yaml::url::parse_query(&u.raw_query);
    let page = q
        .iter()
        .find(|(k, _)| k == "page")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    gojson::atoi(page)
}

impl GodoClient {
    /// Go `NewRequest` + `Do` for a GET of `url_str` (relative to `BaseURL`): the decoded body (`Json::Null`
    /// for 204), or the error.
    fn get(&self, url_str: &str) -> Result<Json, String> {
        let u = self.base_url.join(url_str)?;
        let mut req = net::Request::get(&u.to_url_string()).map_err(|e| e.to_string())?;
        req.header.set("Accept", "application/json");
        req.header.set("User-Agent", GODO_USER_AGENT);
        let resp = self.http.do_request(req).map_err(|e| e.to_string())?;
        if !(200..=299).contains(&resp.status) {
            return Err(check_response(&resp));
        }
        if resp.status == 204 {
            return Ok(Json::Null);
        }
        gojson::parse(&resp.body, true)
    }
}

/// Go `CheckResponse` for a non-2xx response: the `ErrorResponse` text.
fn check_response(r: &net::Response) -> String {
    let (mut message, mut request_id, mut nested) = (String::new(), String::new(), String::new());
    if !r.body.is_empty() {
        let ok = match gojson::parse(&r.body, false) {
            Ok(v) => {
                let mut d = Dec::new();
                if let Some(o) = d.obj(&v, "", "", "godo.ErrorResponse") {
                    for (k, e) in o {
                        if key_is(k, "message") {
                            d.str(e, &mut message, "ErrorResponse", k);
                        } else if key_is(k, "request_id") {
                            d.str(e, &mut request_id, "ErrorResponse", k);
                        } else if key_is(k, "error") {
                            let ty = "struct { Code int \"json:\\\"code\\\"\"; Message string \"json:\\\"message\\\"\" }";
                            if let Some(eo) = d.obj(e, "ErrorResponse", k, ty) {
                                for (k2, x) in eo {
                                    if key_is(k2, "code") {
                                        d.int(x, &mut 0, "", &sub(k, k2), "int");
                                    } else if key_is(k2, "message") {
                                        d.str(x, &mut nested, "", &sub(k, k2));
                                    }
                                }
                            } else if e.is_null() {
                                nested.clear();
                            }
                        }
                    }
                }
                d.finish().is_ok()
            }
            Err(_) => false,
        };
        if !ok {
            message = String::from_utf8_lossy(&r.body).into_owned();
        }
    }
    if request_id.is_empty() {
        request_id = r.header.get_str("x-request-id").to_string();
    }
    let attempted = match gojson::atoi(r.header.get_str("X-Godo-Retry-Attempts")) {
        Ok(n) if n > 0 => format!("; giving up after {n} attempt(s)"),
        _ => String::new(),
    };
    if message.is_empty() {
        message = nested;
    }
    let url = r.request.url.to_url_string();
    if !request_id.is_empty() {
        format!(
            "{} {}: {} (request {}) {}{}",
            r.request.method,
            url,
            r.status,
            yaml::go_quote(&request_id),
            message,
            attempted
        )
    } else {
        format!(
            "{} {}: {} {}{}",
            r.request.method, url, r.status, message, attempted
        )
    }
}

/// Decodes `links` of a list root (struct `root_name`) into `Links`.
pub(crate) fn decode_links(d: &mut Dec, v: &Json, root_name: &str) -> Option<Links> {
    let o = d.obj(v, root_name, "links", "godo.Links")?;
    let mut l = Links::default();
    for (k, e) in o {
        if !key_is(k, "pages") {
            continue;
        }
        let p = sub("links", k);
        match d.obj(e, "Links", &p, "godo.Pages") {
            Some(po) => {
                let pages = l.pages.get_or_insert_with(Pages::default);
                for (k2, x) in po {
                    let p2 = sub(&p, k2);
                    if key_is(k2, "prev") {
                        d.str(x, &mut pages.prev, "Pages", &p2);
                    } else if key_is(k2, "next") {
                        d.str(x, &mut pages.next, "Pages", &p2);
                    } else if key_is(k2, "first") || key_is(k2, "last") {
                        d.str(x, &mut String::new(), "Pages", &p2);
                    }
                }
            }
            None if e.is_null() => l.pages = None,
            None => {}
        }
    }
    Some(l)
}

/// A `NetworkV4`/`NetworkV6`: (ip_address, type).
type Net = (String, String);

/// The fields of `godo.Droplet` the discovery reads.
#[derive(Default)]
struct Droplet {
    id: i64,
    name: String,
    region_slug: String,
    image_slug: String,
    image_name: String,
    size_slug: String,
    features: Vec<String>,
    status: String,
    /// `None`: no `networks` object (Go nil).
    networks: Option<(Vec<Net>, Vec<Net>)>,
    tags: Vec<String>,
    vpc_uuid: String,
}

/// Decodes `[]NetworkV4` / `[]NetworkV6` items: (ip_address, type).
fn decode_nets(d: &mut Dec, v: &Json, path: &str, ty: &str) -> Option<Vec<(String, String)>> {
    let a = d.arr(v, "Networks", path, &format!("[]godo.{ty}"))?;
    let mut out = Vec::with_capacity(a.len());
    for e in a {
        let mut n = (String::new(), String::new());
        if let Some(o) = d.obj(e, "Networks", path, &format!("godo.{ty}")) {
            for (k, x) in o {
                if key_is(k, "ip_address") {
                    d.str(x, &mut n.0, ty, &sub(path, k));
                } else if key_is(k, "type") {
                    d.str(x, &mut n.1, ty, &sub(path, k));
                }
            }
        }
        out.push(n);
    }
    Some(out)
}

fn decode_droplet(d: &mut Dec, v: &Json) -> Droplet {
    let mut dr = Droplet::default();
    let Some(o) = d.obj(v, "dropletsRoot", "droplets", "godo.Droplet") else {
        return dr;
    };
    let st = "Droplet";
    for (k, e) in o {
        let p = sub("droplets", k);
        if key_is(k, "id") {
            d.int(e, &mut dr.id, st, &p, "int");
        } else if key_is(k, "name") {
            d.str(e, &mut dr.name, st, &p);
        } else if key_is(k, "region") {
            if let Some(ro) = d.obj(e, st, &p, "godo.Region") {
                for (k2, x) in ro {
                    if key_is(k2, "slug") {
                        d.str(x, &mut dr.region_slug, "Region", &sub(&p, k2));
                    }
                }
            } else if e.is_null() {
                dr.region_slug.clear();
            }
        } else if key_is(k, "image") {
            if let Some(io) = d.obj(e, st, &p, "godo.Image") {
                for (k2, x) in io {
                    if key_is(k2, "slug") {
                        d.str(x, &mut dr.image_slug, "Image", &sub(&p, k2));
                    } else if key_is(k2, "name") {
                        d.str(x, &mut dr.image_name, "Image", &sub(&p, k2));
                    }
                }
            } else if e.is_null() {
                dr.image_slug.clear();
                dr.image_name.clear();
            }
        } else if key_is(k, "size_slug") {
            d.str(e, &mut dr.size_slug, st, &p);
        } else if key_is(k, "features") {
            d.str_slice(e, &mut dr.features, st, &p);
        } else if key_is(k, "status") {
            d.str(e, &mut dr.status, st, &p);
        } else if key_is(k, "networks") {
            match d.obj(e, st, &p, "godo.Networks") {
                Some(no) => {
                    let nets = dr.networks.get_or_insert_with(Default::default);
                    for (k2, x) in no {
                        let p2 = sub(&p, k2);
                        if key_is(k2, "v4") {
                            if let Some(n) = decode_nets(d, x, &p2, "NetworkV4") {
                                nets.0 = n;
                            }
                        } else if key_is(k2, "v6")
                            && let Some(n) = decode_nets(d, x, &p2, "NetworkV6")
                        {
                            nets.1 = n;
                        }
                    }
                }
                None if e.is_null() => dr.networks = None,
                None => {}
            }
        } else if key_is(k, "tags") {
            d.str_slice(e, &mut dr.tags, st, &p);
        } else if key_is(k, "vpc_uuid") {
            d.str(e, &mut dr.vpc_uuid, st, &p);
        }
    }
    dr
}

/// Go `dropletsDiscovery`.
pub struct DropletsDiscovery {
    pub client: GodoClient,
    port: i64,
}

impl DropletsDiscovery {
    /// Go `Droplets.List`.
    fn list(&self, opts: ListOptions) -> Result<(Vec<Droplet>, Option<Links>), String> {
        let v = self.client.get(&add_options("v2/droplets", opts)?)?;
        let mut d = Dec::new();
        let (mut droplets, mut links) = (Vec::new(), None);
        if let Some(o) = d.obj(&v, "", "", "godo.dropletsRoot") {
            for (k, e) in o {
                if key_is(k, "droplets") {
                    if let Some(a) = d.arr(e, "dropletsRoot", "droplets", "[]godo.Droplet") {
                        droplets = a.iter().map(|x| decode_droplet(&mut d, x)).collect();
                    }
                } else if key_is(k, "links") {
                    links = decode_links(&mut d, e, "dropletsRoot");
                }
            }
        }
        d.finish()?;
        Ok((droplets, links))
    }

    /// Go `listDroplets`.
    fn list_droplets(&self) -> Result<Vec<Droplet>, String> {
        let mut droplets = Vec::new();
        let mut opts = ListOptions::default();
        loop {
            let (page, links) = self
                .list(opts)
                .map_err(|e| format!("error while listing droplets page {}: {e}", opts.page))?;
            droplets.extend(page);
            let Some(links) = links else { break };
            if links.is_last_page() {
                break;
            }
            opts.page = links.current_page()? + 1;
        }
        Ok(droplets)
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group {
            source: "DigitalOcean".to_string(),
            ..Default::default()
        };
        let port = (self.port as u64).to_string();
        for droplet in self.list_droplets()? {
            let Some((v4, v6)) = &droplet.networks else {
                continue;
            };
            if v4.is_empty() {
                continue;
            }
            let find = |nets: &[(String, String)], ty: &str| {
                nets.iter()
                    .find(|(_, t)| t == ty)
                    .map(|(ip, _)| ip.clone())
                    .unwrap_or_default()
            };
            let private_ipv4 = find(v4, "private");
            let public_ipv4 = find(v4, "public");
            let public_ipv6 = find(v6, "public");
            let mut labels = LabelSet::from_pairs(&[
                (DO_LABEL_ID, &droplet.id.to_string()),
                (DO_LABEL_NAME, &droplet.name),
                (DO_LABEL_IMAGE, &droplet.image_slug),
                (DO_LABEL_IMAGE_NAME, &droplet.image_name),
                (DO_LABEL_PRIVATE_IPV4, &private_ipv4),
                (DO_LABEL_PUBLIC_IPV4, &public_ipv4),
                (DO_LABEL_PUBLIC_IPV6, &public_ipv6),
                (DO_LABEL_REGION, &droplet.region_slug),
                (DO_LABEL_SIZE, &droplet.size_slug),
                (DO_LABEL_STATUS, &droplet.status),
                (DO_LABEL_VPC, &droplet.vpc_uuid),
            ]);
            labels.insert(
                ADDRESS_LABEL.to_string(),
                join_host_port(&public_ipv4, &port),
            );
            if !droplet.features.is_empty() {
                labels.insert(
                    DO_LABEL_FEATURES.to_string(),
                    join_tags(&droplet.features, SEPARATOR),
                );
            }
            if !droplet.tags.is_empty() {
                labels.insert(
                    DO_LABEL_TAGS.to_string(),
                    join_tags(&droplet.tags, SEPARATOR),
                );
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}
