//! Port of `discovery/vultr`: Vultr instances, listed with cursor pagination, refreshed at an interval. The API
//! client is the subset of `github.com/vultr/govultr/v3` (v3.33.2, on `hashicorp/go-retryablehttp` v0.7.8)
//! that `Instance.List` uses: request building, retries, response checks and error texts.
//!
//! Differs from Go: only the instance fields the discoverer reads are decoded and type-checked; a `Retry-After`
//! date is read only in the RFC 1123 form with the GMT zone.
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::linode::gojson::{self, Ctx};
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const VULTR_INSTANCE_LABEL: &str = "__meta_vultr_instance_";
const SEPARATOR: &str = ",";

fn vultr_label(name: &str) -> String {
    format!("{VULTR_INSTANCE_LABEL}{name}")
}

/// Go `vultr.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { port: 80, refresh_interval: yaml::Duration::from_secs(60), http_client_config: HttpClientConfig::default_config() }
}

yaml::yaml_struct!(SdConfig, "vultr.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval")
    (port: "port")
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
        "vultr.plain".to_string()
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
    const GO_TYPE: &'static str = "vultr.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "vultr"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(VultrMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `vultrMetrics`.
pub struct VultrMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for VultrMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// ---------------- govultr ----------------

/// Go `govultr.Instance` (the fields the discoverer reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Instance {
    pub id: String,
    pub os: String,
    pub ram: i64,
    pub disk: i64,
    pub plan: String,
    pub main_ip: String,
    pub vcpu_count: i64,
    pub region: String,
    pub allowed_bandwidth: i64,
    pub server_status: String,
    pub v6_main_ip: String,
    pub label: String,
    pub internal_ip: String,
    pub os_id: i64,
    pub features: Vec<String>,
    pub hostname: String,
    pub tags: Vec<String>,
}

fn decode_instance(v: &Json, ctx: &Ctx) -> gojson::Result<Instance> {
    let mut i = Instance::default();
    let Some(o) = gojson::object(v, ctx, "govultr.Instance")? else { return Ok(i) };
    let f = |n: &str| ctx.field("Instance", n);
    for (k, v) in o {
        match k.to_ascii_lowercase().as_str() {
            "id" => i.id = gojson::string(v, &f("id"))?,
            "os" => i.os = gojson::string(v, &f("os"))?,
            "ram" => i.ram = gojson::int(v, &f("ram"))?,
            "disk" => i.disk = gojson::int(v, &f("disk"))?,
            "plan" => i.plan = gojson::string(v, &f("plan"))?,
            "main_ip" => i.main_ip = gojson::string(v, &f("main_ip"))?,
            "vcpu_count" => i.vcpu_count = gojson::int(v, &f("vcpu_count"))?,
            "region" => i.region = gojson::string(v, &f("region"))?,
            "allowed_bandwidth" => i.allowed_bandwidth = gojson::int(v, &f("allowed_bandwidth"))?,
            "server_status" => i.server_status = gojson::string(v, &f("server_status"))?,
            "v6_main_ip" => i.v6_main_ip = gojson::string(v, &f("v6_main_ip"))?,
            "label" => i.label = gojson::string(v, &f("label"))?,
            "internal_ip" => i.internal_ip = gojson::string(v, &f("internal_ip"))?,
            "os_id" => i.os_id = gojson::int(v, &f("os_id"))?,
            "features" => i.features = gojson::strings(v, &f("features"))?,
            "hostname" => i.hostname = gojson::string(v, &f("hostname"))?,
            "tags" => i.tags = gojson::strings(v, &f("tags"))?,
            _ => {}
        }
    }
    Ok(i)
}

/// Go `json.Unmarshal(body, &instancesBase)`: the instances and `meta.links.next` (None when meta or links is
/// null or absent).
fn decode_instances_base(body: &[u8]) -> gojson::Result<(Vec<Instance>, Option<String>)> {
    let v = gojson::parse(body)?;
    let top = Ctx::top();
    let (mut instances, mut next) = (Vec::new(), None);
    let Some(o) = gojson::object(&v, &top, "govultr.instancesBase")? else { return Ok((instances, next)) };
    for (k, v) in o {
        if gojson::key_is(k, "instances") {
            let c = top.field("instancesBase", "instances");
            instances = gojson::array(v, &c, "[]govultr.Instance")?.iter().map(|e| decode_instance(e, &c)).collect::<gojson::Result<_>>()?;
        } else if gojson::key_is(k, "meta") {
            let c = top.field("instancesBase", "meta");
            next = None;
            if let Some(mo) = gojson::object(v, &c, "govultr.Meta")? {
                for (k, v) in mo {
                    if gojson::key_is(k, "total") {
                        gojson::int(v, &c.field("Meta", "total"))?;
                    } else if gojson::key_is(k, "links") {
                        let lc = c.field("Meta", "links");
                        next = None;
                        if let Some(lo) = gojson::object(v, &lc, "govultr.Links")? {
                            let mut n = String::new();
                            for (k, v) in lo {
                                if gojson::key_is(k, "next") {
                                    n = gojson::string(v, &lc.field("Links", "next"))?;
                                } else if gojson::key_is(k, "prev") {
                                    gojson::string(v, &lc.field("Links", "prev"))?;
                                }
                            }
                            next = Some(n);
                        }
                    }
                }
            }
        }
    }
    Ok((instances, next))
}

/// Go `json.Unmarshal(body, &apiError)`: the `error` message.
fn decode_api_error(body: &[u8]) -> gojson::Result<String> {
    let v = gojson::parse(body)?;
    let top = Ctx::top();
    let mut msg = String::new();
    if let Some(o) = gojson::object(&v, &top, "govultr.apiError")? {
        for (k, v) in o {
            if gojson::key_is(k, "status") {
                gojson::int(v, &top.field("apiError", "status"))?;
            } else if gojson::key_is(k, "error") {
                msg = gojson::string(v, &top.field("apiError", "error"))?;
            }
        }
    }
    Ok(msg)
}

const RETRY_MAX: usize = 3;
/// Go `SetRateLimit(500ms)`: `RetryWaitMin = t / 3 * 2`, `RetryWaitMax = t`.
const RETRY_WAIT_MIN: Duration = Duration::from_nanos(500_000_000 / 3 * 2);
const RETRY_WAIT_MAX: Duration = Duration::from_millis(500);

/// Go `parseRetryAfterHeader`.
fn parse_retry_after(h: &str) -> Option<Duration> {
    if h.is_empty() {
        return None;
    }
    if let Ok(s) = h.parse::<i64>() {
        return if s < 0 { None } else { Some(Duration::from_secs(s as u64)) };
    }
    // time.RFC1123 ("Mon, 02 Jan 2006 15:04:05 MST"), GMT only
    let p: Vec<&str> = h.split(' ').collect();
    if p.len() != 6 || p[5] != "GMT" {
        return None;
    }
    let day: i64 = p[1].parse().ok()?;
    let mon = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"].iter().position(|m| *m == p[2])? as i64 + 1;
    let year: i64 = p[3].parse().ok()?;
    let hms: Vec<i64> = p[4].split(':').map(|x| x.parse().ok()).collect::<Option<_>>()?;
    if hms.len() != 3 {
        return None;
    }
    // days from civil (Howard Hinnant)
    let y = if mon <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if mon > 2 { mon - 3 } else { mon + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    let t = days * 86400 + hms[0] * 3600 + hms[1] * 60 + hms[2];
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0);
    let until = t as f64 - now;
    Some(if until > 0.0 { Duration::from_secs_f64(until) } else { Duration::ZERO })
}

/// Go `DefaultBackoff`.
fn default_backoff(attempt: usize, resp: Option<&net::Response>) -> Duration {
    if let Some(r) = resp
        && (r.status == 429 || r.status == 503)
        && let Some(d) = parse_retry_after(r.header.values("Retry-After").first().copied().unwrap_or(""))
    {
        return d;
    }
    let mult = 2f64.powi(attempt as i32) * RETRY_WAIT_MIN.as_nanos() as f64;
    if mult > RETRY_WAIT_MAX.as_nanos() as f64 { RETRY_WAIT_MAX } else { Duration::from_nanos(mult as u64) }
}

/// Go `baseRetryPolicy` for a transport error (always a `*url.Error` from `http.Client.Do`).
fn retry_on_error(e: &str) -> bool {
    let redirects = e.ends_with(" redirects") && e.rsplit(' ').nth(1).is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit())) && e.contains("stopped after ");
    let cert = e.contains("x509: ") || e.contains("tls: failed to verify certificate");
    !(redirects || e.contains("unsupported protocol scheme") || e.contains("invalid header") || e.contains("certificate is not trusted") || cert)
}

/// Go `govultr.Client` (the parts `Instance.List` uses).
pub struct Client {
    http: net::Client,
    /// Go `BaseURL`.
    pub base_url: yaml::url::GoUrl,
    pub user_agent: String,
}

impl Client {
    /// Go `govultr.NewClient(&http.Client{Transport: rt, Timeout: timeout})`.
    pub fn new(transport: Arc<dyn net::RoundTripper>, timeout: Duration) -> Client {
        let mut http = net::Client::new(transport);
        http.timeout = Some(timeout);
        Client { http, base_url: yaml::url::parse("https://api.vultr.com").unwrap_or_default(), user_agent: "govultr/3.33.2".to_string() }
    }

    /// retryablehttp `Client.Do` with govultr's error handler.
    fn do_retry(&self, ctx: &CancelToken, req: net::Request) -> Result<net::Response, String> {
        let mut attempt = 0usize;
        let mut i = 0usize;
        loop {
            attempt += 1;
            let r = self.http.do_request(req.clone()).map_err(|e| e.to_string());
            let (should_retry, check_err) = if ctx.is_cancelled() {
                (false, Some("context canceled".to_string()))
            } else {
                match &r {
                    Err(e) => (retry_on_error(e), None),
                    Ok(resp) => (resp.status == 429 || resp.status == 0 || (resp.status >= 500 && resp.status != 501), None),
                }
            };
            let remain = RETRY_MAX as i64 - i as i64;
            if !should_retry || remain <= 0 {
                if r.is_ok() && check_err.is_none() && !should_retry {
                    return r;
                }
                // vultrErrorHandler
                return Err(match (r, check_err) {
                    // http.Client returns the last response with its body closed on a redirect policy error
                    (Err(e), None) if !retry_on_error(&e) && e.ends_with(" redirects") => {
                        format!("gave up after {attempt} attempts, last error unavailable (error reading response body: http: read on closed response body)")
                    }
                    (Ok(resp), _) => format!("gave up after {attempt} attempts, last error: {}", yaml::go_quote(String::from_utf8_lossy(&resp.body).trim())),
                    (Err(e), c) => format!("gave up after {attempt} attempts, last error : {}", c.unwrap_or(e)),
                });
            }
            let wait = default_backoff(i, r.as_ref().ok());
            if ctx.wait_timeout(wait) {
                return Err("context canceled".to_string());
            }
            i += 1;
        }
    }

    /// Go `InstanceServiceHandler.List`: one page of instances and the next cursor (None without meta/links).
    pub fn list_instances(&self, ctx: &CancelToken, per_page: i64, cursor: &str) -> Result<(Vec<Instance>, Option<String>, u16), String> {
        let resolved = self.base_url.join("/v2/instances")?;
        let mut q: Vec<(String, String)> = Vec::new();
        if !cursor.is_empty() {
            q.push(("cursor".to_string(), cursor.to_string()));
        }
        if per_page != 0 {
            q.push(("per_page".to_string(), per_page.to_string()));
        }
        let mut u = resolved;
        u.raw_query = yaml::url::encode_query(&q);
        u.force_query = false;
        let mut req = net::Request::get(&u.to_url_string()).map_err(|e| e.to_string())?;
        req.header.add("User-Agent", &self.user_agent);
        req.header.add("Accept", "application/json");
        req.header.add("Content-Type", "application/json");

        // DoWithContext
        let res = self.do_retry(ctx, req)?;
        let body = &res.body;
        if !body.is_empty() {
            let msg = decode_api_error(body).map_err(|e| format!("error unmarshaling response body : {e}"))?;
            if !msg.is_empty() {
                return Err(format!("error returned from api : {msg}"));
            }
        }
        match res.status {
            200 | 201 | 202 | 203 | 206 => {
                let content_length: i64 = match res.header.get("Content-Length") {
                    Some(v) if !res.header.get_str("Transfer-Encoding").eq_ignore_ascii_case("chunked") => v.trim().parse().unwrap_or(-1),
                    _ => -1,
                };
                if content_length == 0 || res.header.get_str("Content-Type") != "application/json" {
                    return Ok((Vec::new(), None, res.status));
                }
                let (instances, next) = decode_instances_base(body)?;
                Ok((instances, next, res.status))
            }
            204 | 205 => Ok((Vec::new(), None, res.status)),
            _ => Err(String::from_utf8_lossy(body).into_owned()),
        }
    }
}

// ---------------- vultr.go ----------------

/// The refresh function of the Vultr discoverer (Go `Discovery.refresh` and its state).
pub struct VultrRefresher {
    client: Client,
    port: i64,
}

impl VultrRefresher {
    /// Go `listInstances`.
    fn list_instances(&self, ctx: &CancelToken) -> Result<Vec<Instance>, String> {
        let mut instances = Vec::new();
        let mut cursor = String::new();
        loop {
            let (paged, next, status) = self.client.list_instances(ctx, 100, &cursor)?;
            if status / 100 != 2 {
                return Err(format!("vultr API returned unexpected status {status}"));
            }
            instances.extend(paged);
            match next {
                Some(n) if !n.is_empty() => cursor = n,
                _ => break,
            }
        }
        Ok(instances)
    }
}

impl Refresher for VultrRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "Vultr".to_string(), ..Default::default() };
        for instance in self.list_instances(ctx)? {
            let mut labels: LabelSet = [
                (vultr_label("id"), instance.id.clone()),
                (vultr_label("label"), instance.label.clone()),
                (vultr_label("os"), instance.os.clone()),
                (vultr_label("os_id"), instance.os_id.to_string()),
                (vultr_label("region"), instance.region.clone()),
                (vultr_label("plan"), instance.plan.clone()),
                (vultr_label("vcpu_count"), instance.vcpu_count.to_string()),
                (vultr_label("ram_mb"), instance.ram.to_string()),
                (vultr_label("allowed_bandwidth_gb"), instance.allowed_bandwidth.to_string()),
                (vultr_label("disk_gb"), instance.disk.to_string()),
                (vultr_label("main_ip"), instance.main_ip.clone()),
                (vultr_label("main_ipv6"), instance.v6_main_ip.clone()),
                (vultr_label("internal_ip"), instance.internal_ip.clone()),
                (vultr_label("hostname"), instance.hostname.clone()),
                (vultr_label("server_status"), instance.server_status.clone()),
            ]
            .into_iter()
            .collect();
            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&instance.main_ip, self.port as u64));
            // We surround the separated list with the separator as well. This way regular expressions
            // in relabeling rules don't have to consider feature positions.
            if !instance.features.is_empty() {
                labels.insert(vultr_label("features"), join_tags(&instance.features, SEPARATOR));
            }
            if !instance.tags.is_empty() {
                labels.insert(vultr_label("tags"), join_tags(&instance.tags, SEPARATOR));
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `vultr.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<VultrRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<VultrMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        let mut client = Client::new(rt, conf.refresh_interval.to_std());
        client.user_agent = crate::http::USER_AGENT.to_string();
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "vultr",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: VultrRefresher { client, port: conf.port },
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `d.client.BaseURL = u`.
    pub fn set_base_url(&mut self, u: yaml::url::GoUrl) {
        self.inner.refresher().client.base_url = u;
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
