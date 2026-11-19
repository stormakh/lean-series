//! The subset of `github.com/linode/linodego` (v1.69.1, on `go-resty/resty` v2.17.2) that the Linode
//! discoverer uses: `ListEvents`, `ListInstances`, `ListIPAddresses` and `ListIPv6Ranges` with their pagination,
//! headers, retries and error texts.
//!
//! Differs from Go: only the fields the discoverer reads are decoded and type-checked (Go also rejects malformed
//! `created`/`updated` timestamps and type errors in other fields); a 2xx response with an XML content type
//! gives an empty page (Go decodes it as XML into the JSON-tagged structs, which also gives an empty page for
//! valid XML); `LINODE_CA` and `LINODE_DEBUG` are ignored (Go ignores the CA too with a custom transport).
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use json::Json;

use super::gojson::{self, Ctx};
use crate::cancel::CancelToken;

/// Go `linodego.APIHost`, `APIVersion`, `APIProto`.
const API_HOST: &str = "api.linode.com";
const API_VERSION: &str = "v4";
const API_PROTO: &str = "https";
/// Go `linodego.DefaultUserAgent`.
pub const DEFAULT_USER_AGENT: &str = "linodego/v1.69.1 https://github.com/linode/linodego";
const RETRY_COUNT: usize = 1000;
const RETRY_WAIT_MIN: Duration = Duration::from_secs(3);
const RETRY_WAIT_MAX: Duration = Duration::from_secs(30);

/// Go `linodego.Error`. `ptr` tells whether Go returns it as `*Error` (only those match
/// `errors.As(err, &(*linodego.Error))`) or as an `Error` value.
#[derive(Clone, Debug, PartialEq)]
pub struct Error {
    pub code: i64,
    pub message: String,
    pub ptr: bool,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:03}] {}", self.code, self.message)
    }
}

/// Go `NewError(err)` for a Go error: `ErrorFromError`.
fn from_error(msg: impl Into<String>) -> Error {
    Error { code: 2, message: msg.into(), ptr: true }
}

/// Go `linodego.ListOptions` (the parts the discoverer sets).
#[derive(Clone, Debug, Default)]
pub struct ListOptions {
    pub page: i64,
    pub pages: i64,
    pub results: i64,
    pub page_size: i64,
    pub filter: String,
}

/// Go `linodego.Instance` (the fields the discoverer reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Instance {
    pub id: i64,
    pub region: String,
    pub backups_enabled: bool,
    pub image: String,
    pub group: String,
    /// None for a JSON null item.
    pub ipv4: Vec<Option<IpAddr>>,
    pub ipv6: String,
    pub label: String,
    pub type_: String,
    pub status: String,
    pub hypervisor: String,
    pub specs: InstanceSpec,
    pub tags: Vec<String>,
}

/// Go `linodego.InstanceSpec`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstanceSpec {
    pub disk: i64,
    pub memory: i64,
    pub vcpus: i64,
    pub transfer: i64,
    pub gpus: i64,
}

/// Go `linodego.InstanceIP` (the fields the discoverer reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InstanceIp {
    pub address: String,
    pub public: bool,
    pub rdns: String,
}

/// Go `linodego.IPv6Range` (the fields the discoverer reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ipv6Range {
    pub range: String,
    pub prefix: i64,
    pub route_target: String,
}

/// Go `net.IP.String()`.
pub fn ip_string(ip: &IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4.to_string();
            }
            let g = v6.segments();
            // the longest run (>= 2) of zero groups, first one wins
            let (mut zs, mut ze) = (8usize, 8usize);
            let mut i = 0;
            while i < 8 {
                let mut j = i;
                while j < 8 && g[j] == 0 {
                    j += 1;
                }
                if j - i >= 2 && j - i > ze - zs {
                    zs = i;
                    ze = j;
                }
                i = j + 1;
            }
            let mut s = String::new();
            let mut i = 0;
            while i < 8 {
                if i == zs {
                    s.push_str("::");
                    i = ze;
                    if i >= 8 {
                        break;
                    }
                } else if i > 0 {
                    s.push(':');
                }
                s.push_str(&format!("{:x}", g[i]));
                i += 1;
            }
            s
        }
    }
}

/// Go `net.ParseIP` (as `net.IP.UnmarshalText` uses it).
fn parse_ip(s: &str) -> Option<IpAddr> {
    if let Ok(v4) = s.parse::<Ipv4Addr>() {
        return Some(IpAddr::V4(v4));
    }
    if s.contains('%') {
        return None;
    }
    s.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
}

// ---------------- JSON decoding ----------------

fn paginated_name(t: &str) -> String {
    format!("paginatedResponse[github.com/linode/linodego.{t}]")
}

/// Go `json.Unmarshal(body, &paginatedResponse[T])`: page metadata and the data items.
fn decode_page<'a>(v: &'a Json, t: &str) -> gojson::Result<(i64, i64, i64, &'a [Json], Ctx)> {
    let name = paginated_name(t);
    let top = Ctx::top();
    let (mut page, mut pages, mut results, mut data): (i64, i64, i64, &[Json]) = (0, 0, 0, &[]);
    let mut data_ctx = top.field(&name, "data");
    if let Some(o) = gojson::object(v, &top, &format!("linodego.{name}"))? {
        for (k, v) in o {
            let k = k.to_ascii_lowercase();
            match k.as_str() {
                "page" => page = gojson::int(v, &top.field(&name, "page"))?,
                "pages" => pages = gojson::int(v, &top.field(&name, "pages"))?,
                "results" => results = gojson::int(v, &top.field(&name, "results"))?,
                "data" => {
                    data_ctx = top.field(&name, "data");
                    data = gojson::array(v, &data_ctx, &format!("[]linodego.{t}"))?;
                }
                _ => {}
            }
        }
    }
    Ok((page, pages, results, data, data_ctx))
}

/// The anonymous struct of a linodego `UnmarshalJSON` (`struct { *linodego.Mask; ... }`).
fn mask_type(extra: &[(&str, &str, &str)]) -> String {
    let mut s = String::from("struct { *linodego.Mask");
    for (f, t, tag) in extra {
        s.push_str(&format!("; {f} {t} \"json:\\\"{tag}\\\"\""));
    }
    s.push_str(" }");
    s
}

const PTIME: &str = "*parseabletime.ParseableTime";

fn decode_instance(v: &Json, data_ctx: &Ctx) -> gojson::Result<Instance> {
    let mut i = Instance::default();
    let ty = mask_type(&[("Created", PTIME, "created"), ("Updated", PTIME, "updated")]);
    let Some(o) = gojson::object(v, data_ctx, &ty)? else { return Ok(i) };
    let outer = data_ctx.strukt.clone().unwrap_or_default();
    let mut mctx = data_ctx.clone();
    mctx.stack.push("Mask".to_string());
    let f = |n: &str| mctx.field(&outer, n);
    for (k, v) in o {
        let k = k.to_ascii_lowercase();
        match k.as_str() {
            "id" => i.id = gojson::int(v, &f("id"))?,
            "region" => i.region = gojson::string(v, &f("region"))?,
            "image" => i.image = gojson::string(v, &f("image"))?,
            "group" => i.group = gojson::string(v, &f("group"))?,
            "ipv6" => i.ipv6 = gojson::string(v, &f("ipv6"))?,
            "label" => i.label = gojson::string(v, &f("label"))?,
            "type" => i.type_ = gojson::string(v, &f("type"))?,
            "status" => i.status = gojson::string_ty(v, &f("status"), "linodego.InstanceStatus")?,
            "hypervisor" => i.hypervisor = gojson::string(v, &f("hypervisor"))?,
            "tags" => i.tags = gojson::strings(v, &f("tags"))?,
            "ipv4" => {
                let c = f("ipv4");
                i.ipv4.clear();
                for e in gojson::array(v, &c, "[]*net.IP")? {
                    match e {
                        Json::Null => i.ipv4.push(None),
                        Json::Str(s) => match parse_ip(s) {
                            Some(ip) => i.ipv4.push(Some(ip)),
                            None => return Err(gojson::Error::Other(format!("invalid IP address: {s}"))),
                        },
                        e => return Err(c.err(gojson::kind(e), "*net.IP")),
                    }
                }
            }
            "specs" => {
                let c = f("specs");
                i.specs = InstanceSpec::default();
                if let Some(o) = gojson::object(v, &c, "linodego.InstanceSpec")? {
                    for (k, v) in o {
                        let sf = |n: &str| c.field(&outer, n);
                        match k.to_ascii_lowercase().as_str() {
                            "disk" => i.specs.disk = gojson::int(v, &sf("disk"))?,
                            "memory" => i.specs.memory = gojson::int(v, &sf("memory"))?,
                            "vcpus" => i.specs.vcpus = gojson::int(v, &sf("vcpus"))?,
                            "transfer" => i.specs.transfer = gojson::int(v, &sf("transfer"))?,
                            "gpus" => i.specs.gpus = gojson::int(v, &sf("gpus"))?,
                            _ => {}
                        }
                    }
                }
            }
            "backups" => {
                let c = f("backups");
                i.backups_enabled = false;
                if let Some(o) = gojson::object(v, &c, "linodego.InstanceBackup")? {
                    let mut bc = c.clone();
                    bc.stack.push("Mask".to_string());
                    for (k, v) in o {
                        if gojson::key_is(k, "enabled") {
                            i.backups_enabled = gojson::bool(v, &bc.field(&outer, "enabled"))?;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(i)
}

fn decode_instance_ip(v: &Json, data_ctx: &Ctx) -> gojson::Result<InstanceIp> {
    let mut ip = InstanceIp::default();
    let Some(o) = gojson::object(v, data_ctx, "linodego.InstanceIP")? else { return Ok(ip) };
    let f = |n: &str| data_ctx.field("InstanceIP", n);
    for (k, v) in o {
        match k.to_ascii_lowercase().as_str() {
            "address" => ip.address = gojson::string(v, &f("address"))?,
            "public" => ip.public = gojson::bool(v, &f("public"))?,
            "rdns" => ip.rdns = gojson::string(v, &f("rdns"))?,
            _ => {}
        }
    }
    Ok(ip)
}

fn decode_ipv6_range(v: &Json, data_ctx: &Ctx) -> gojson::Result<Ipv6Range> {
    let mut r = Ipv6Range::default();
    let Some(o) = gojson::object(v, data_ctx, "linodego.IPv6Range")? else { return Ok(r) };
    let f = |n: &str| data_ctx.field("IPv6Range", n);
    for (k, v) in o {
        match k.to_ascii_lowercase().as_str() {
            "range" => r.range = gojson::string(v, &f("range"))?,
            "prefix" => r.prefix = gojson::int(v, &f("prefix"))?,
            "route_target" => r.route_target = gojson::string(v, &f("route_target"))?,
            _ => {}
        }
    }
    Ok(r)
}

/// Go `Event` items: only their number is used; items must be objects (or null).
fn decode_event(v: &Json, data_ctx: &Ctx) -> gojson::Result<()> {
    let ty = mask_type(&[
        ("Created", PTIME, "created"),
        ("TimeRemaining", "json.RawMessage", "time_remaining"),
        ("NotBefore", PTIME, "not_before"),
        ("StartTime", PTIME, "start_time"),
        ("CompleteTime", PTIME, "complete_time"),
    ]);
    gojson::object(v, data_ctx, &ty)?;
    Ok(())
}

/// Go `APIError` as resty decodes it on an error response (type errors only logged: what decodes stays).
fn decode_api_error(body: &[u8]) -> Vec<(String, String)> {
    let Ok(v) = gojson::parse(body) else { return Vec::new() };
    let mut out = Vec::new();
    if let Json::Obj(o) = &v {
        for (k, v) in o {
            if !gojson::key_is(k, "errors") {
                continue;
            }
            out.clear();
            if let Json::Arr(a) = v {
                for e in a {
                    let (mut reason, mut field) = (String::new(), String::new());
                    if let Json::Obj(eo) = e {
                        for (k, v) in eo {
                            if let Json::Str(s) = v {
                                if gojson::key_is(k, "reason") {
                                    reason = s.clone();
                                } else if gojson::key_is(k, "field") {
                                    field = s.clone();
                                }
                            }
                        }
                    }
                    out.push((reason, field));
                }
            }
        }
    }
    out
}

/// Go `APIError.Error()`.
fn api_error_text(errs: &[(String, String)]) -> String {
    errs.iter().map(|(reason, field)| if field.is_empty() { reason.clone() } else { format!("[{field}] {reason}") }).collect::<Vec<_>>().join("; ")
}

/// resty's `IsJSONType` / `IsXMLType`: `(?i:(application|text)/(.*json.*)(;|$))` (unanchored).
fn is_type(ct: &str, sub: &str) -> bool {
    let l = ct.to_ascii_lowercase();
    ["application/", "text/"].iter().any(|p| l.match_indices(p).any(|(i, _)| l[i + p.len()..].contains(sub)))
}

// ---------------- the client ----------------

/// The resty response state after `parseResponseBody`.
struct RestyResponse {
    status: u16,
    header: net::Header,
    body: Vec<u8>,
    /// `Request.Error`: None when resty cleared it; Some(errors) when set (empty when not decoded).
    api_error: Option<Vec<(String, String)>>,
    /// The result is decoded (a 2xx JSON response).
    decode: bool,
}

/// Go `linodego.Client` with its resty client.
pub struct Client {
    http: net::Client,
    user_agent: String,
    base_url: String,
    api_proto: String,
    api_version: String,
    /// resty's `BaseURL` (`proto://base/version`, no trailing slash).
    host_url: String,
}

impl Client {
    /// Go `linodego.NewClient(&http.Client{Transport: rt, Timeout: timeout})`.
    pub fn new(transport: std::sync::Arc<dyn net::RoundTripper>, timeout: Duration) -> Client {
        let mut http = net::Client::new(transport);
        http.timeout = Some(timeout);
        let mut c = Client { http, user_agent: DEFAULT_USER_AGENT.to_string(), base_url: String::new(), api_proto: String::new(), api_version: String::new(), host_url: String::new() };
        if let Ok(u) = std::env::var("LINODE_URL") {
            c.set_base_url(&u);
        }
        match std::env::var("LINODE_API_VERSION") {
            Ok(v) => c.set_api_version(&v),
            Err(_) => c.set_api_version(API_VERSION),
        }
        c
    }

    /// Go `SetUserAgent`.
    pub fn set_user_agent(&mut self, ua: &str) {
        self.user_agent = ua.to_string();
    }

    /// Go `SetBaseURL`.
    pub fn set_base_url(&mut self, base_url: &str) {
        let u = yaml::url::parse(base_url).unwrap_or_default();
        self.base_url = crate::puppetdb::path_join(&u.host, &u.path);
        self.api_proto = u.scheme;
        self.update_host_url();
    }

    /// Go `SetAPIVersion`.
    pub fn set_api_version(&mut self, v: &str) {
        self.api_version = v.to_string();
        self.update_host_url();
    }

    fn update_host_url(&mut self) {
        let proto = if self.api_proto.is_empty() { API_PROTO } else { &self.api_proto };
        let base = if self.base_url.is_empty() { API_HOST } else { &self.base_url };
        let version = if self.api_version.is_empty() { API_VERSION } else { &self.api_version };
        let u = format!("{proto}://{base}/{}", yaml::url::path_escape(version));
        self.host_url = u.trim_end_matches('/').to_string();
    }

    /// One HTTP exchange and resty's `parseResponseBody` (the result is decoded by the caller).
    fn execute(&self, url: &str, filter: &str) -> Result<RestyResponse, String> {
        let mut req = net::Request::get(url).map_err(|e| e.to_string())?;
        req.header.set("User-Agent", &self.user_agent);
        req.header.set("Content-Type", "application/json");
        req.header.set("Accept", "application/json");
        if !filter.is_empty() {
            req.header.set("X-Filter", filter);
        }
        let resp = self.http.do_request(req).map_err(|e| e.to_string())?;
        let mut r = RestyResponse { status: resp.status, header: resp.header, body: resp.body, api_error: Some(Vec::new()), decode: false };
        if r.status == 204 {
            r.api_error = None;
            return Ok(r);
        }
        let hct = r.header.get_str("Content-Type");
        let ct = if hct.is_empty() { "application/json" } else { hct };
        if is_type(ct, "json") || is_type(ct, "xml") {
            if (200..300).contains(&r.status) {
                r.api_error = None;
                r.decode = is_type(ct, "json");
            } else if r.status > 399 && is_type(ct, "json") {
                r.api_error = Some(decode_api_error(&r.body));
            }
        }
        Ok(r)
    }

    /// resty `Request.Get` with linodego's retry policy, then `coupleAPIErrors`. Returns the response body
    /// of a successful (or tolerated) response, or None when there is no result to decode.
    fn get(&self, ctx: &CancelToken, endpoint: &str, query: &[(String, String)], filter: &str) -> Result<Option<Vec<u8>>, Error> {
        let mut url = format!("{}/{}", self.host_url, endpoint.trim_start_matches('/'));
        if !query.is_empty() {
            url.push('?');
            url.push_str(&yaml::url::encode_query(query));
        }
        let mut attempt = 0usize;
        let result = loop {
            let r = self.execute(&url, filter);
            if ctx.is_cancelled() {
                break r;
            }
            // a transport error matches no retry conditional
            let resp = match &r {
                Ok(resp) if self.should_retry(resp) && attempt < RETRY_COUNT => resp,
                _ => break r,
            };
            let wait = retry_wait(resp, attempt).map_err(from_error)?;
            if ctx.wait_timeout(wait) {
                return Err(from_error("context canceled"));
            }
            attempt += 1;
        };
        let r = result.map_err(from_error)?;
        // coupleAPIErrors
        let Some(errs) = &r.api_error else { return Ok(if r.decode { Some(r.body) } else { None }) };
        let rct = r.header.get_str("Content-Type");
        if r.status == 502 && rct == "text/html" {
            return Err(Error { code: 502, message: "Bad Gateway".to_string(), ptr: false });
        }
        if rct != "application/json" {
            let msg = format!("Unexpected Content-Type: Expected: application/json, Received: {rct}\nResponse body: {}", String::from_utf8_lossy(&r.body));
            return Err(Error { code: r.status as i64, message: msg, ptr: false });
        }
        if errs.is_empty() {
            return Ok(None);
        }
        Err(Error { code: r.status as i64, message: api_error_text(errs), ptr: true })
    }

    /// linodego's retry conditionals.
    fn should_retry(&self, r: &RestyResponse) -> bool {
        let busy = r.api_error.as_ref().is_some_and(|e| api_error_text(e) == "Linode busy.");
        (r.status == 400 && busy)
            || r.status == 429
            || (r.status == 503 && r.header.get_str("X-Maintenance-Mode").is_empty())
            || r.status == 408
            || (r.status == 400 && r.header.get_str("Server") == "nginx" && r.header.get_str("Content-Type") == "text/html")
    }

    /// Go `handlePaginatedResults`: every page (or only `opts.page` when set), decoded with `item`.
    fn paginated<T>(&self, ctx: &CancelToken, endpoint: &str, t: &str, opts: &mut ListOptions, item: impl Fn(&Json, &Ctx) -> gojson::Result<T>) -> Result<Vec<T>, Error> {
        let mut result = Vec::new();
        let mut handle_page = |opts: &mut ListOptions, page: i64| -> Result<(), Error> {
            opts.page = page;
            let mut query: Vec<(String, String)> = Vec::new();
            if opts.page > 0 {
                query.push(("page".to_string(), opts.page.to_string()));
            }
            if opts.page_size > 0 {
                query.push(("page_size".to_string(), opts.page_size.to_string()));
            }
            query.sort();
            let body = self.get(ctx, endpoint, &query, &opts.filter)?;
            let (mut pages, mut results) = (0, 0);
            if let Some(b) = body {
                let v = gojson::parse(&b).map_err(|e| from_error(e.to_string()))?;
                let (_, p, r, data, data_ctx) = decode_page(&v, t).map_err(|e| from_error(e.to_string()))?;
                for e in data {
                    result.push(item(e, &data_ctx).map_err(|e| from_error(e.to_string()))?);
                }
                (pages, results) = (p, r);
            }
            opts.page = page;
            opts.pages = pages;
            opts.results = results;
            Ok(())
        };
        let page_defined = opts.page > 0;
        let starting_page = if page_defined { opts.page } else { 1 };
        handle_page(opts, starting_page)?;
        if page_defined {
            return Ok(result);
        }
        let mut page = 2;
        while page <= opts.pages {
            handle_page(opts, page)?;
            page += 1;
        }
        Ok(result)
    }

    /// Go `ListEvents`: the number of events.
    pub fn list_events(&self, ctx: &CancelToken, opts: &mut ListOptions) -> Result<usize, Error> {
        Ok(self.paginated(ctx, "account/events", "Event", opts, decode_event)?.len())
    }

    /// Go `ListInstances`.
    pub fn list_instances(&self, ctx: &CancelToken, opts: &mut ListOptions) -> Result<Vec<Instance>, Error> {
        self.paginated(ctx, "linode/instances", "Instance", opts, decode_instance)
    }

    /// Go `ListIPAddresses`.
    pub fn list_ip_addresses(&self, ctx: &CancelToken, opts: &mut ListOptions) -> Result<Vec<InstanceIp>, Error> {
        self.paginated(ctx, "networking/ips", "InstanceIP", opts, decode_instance_ip)
    }

    /// Go `ListIPv6Ranges`.
    pub fn list_ipv6_ranges(&self, ctx: &CancelToken, opts: &mut ListOptions) -> Result<Vec<Ipv6Range>, Error> {
        self.paginated(ctx, "networking/ipv6/ranges", "IPv6Range", opts, decode_ipv6_range)
    }
}

/// resty `sleepDuration` with linodego's `respectRetryAfter`.
fn retry_wait(r: &RestyResponse, attempt: usize) -> Result<Duration, String> {
    let ra = r.header.get_str("Retry-After");
    if ra.is_empty() {
        return Ok(jitter_backoff(attempt));
    }
    let secs: i64 = ra.parse().map_err(|_| {
        let digits = ra.strip_prefix(['+', '-']).unwrap_or(ra);
        let why = if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) { "value out of range" } else { "invalid syntax" };
        format!("strconv.Atoi: parsing {}: {why}", yaml::go_quote(ra))
    })?;
    if secs == 0 {
        return Ok(jitter_backoff(attempt));
    }
    let result = if secs < 0 || secs as u64 > RETRY_WAIT_MAX.as_secs() { RETRY_WAIT_MAX } else { Duration::from_secs(secs as u64) };
    Ok(result.max(RETRY_WAIT_MIN))
}

/// resty `jitterBackoff(3s, 30s, attempt)`.
fn jitter_backoff(attempt: usize) -> Duration {
    let base = RETRY_WAIT_MIN.as_nanos() as f64;
    let cap = RETRY_WAIT_MAX.as_nanos() as f64;
    let temp = cap.min(base * 2f64.powi(attempt.min(1000) as i32));
    let ri = ((temp / 2.0) as u64).max(1);
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    let jitter = u64::from_le_bytes(b) % ri;
    Duration::from_nanos(ri + jitter).max(RETRY_WAIT_MIN)
}
