//! Hetzner Cloud role (Go `hcloud.go`) with the subset of hcloud-go v2.52.0 it uses: `Server.AllWithOpts`
//! and `Network.All` (pages of 50, `meta.pagination.next_page`), the handler chain (HTTP, rate limit headers,
//! API errors, retries with exponential backoff and jitter, response parsing) and the schema fields read.
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::*;
use crate::gojson::{self, Dec, Obj, key_is, sub};
use crate::sdutil::{join_host_port, sanitize_label_name};

const HCLOUD_PREFIX: &str = "__meta_hetzner_hcloud_";

/// Go `hcloud.UserAgent` with `WithApplication("Prometheus", version.Version)`.
pub const HCLOUD_USER_AGENT: &str = "Prometheus/3.16.0 hcloud-go/2.52.0";

/// Go `hcloudDiscovery`.
pub struct HcloudDiscovery {
    client: Client,
    port: i64,
    label_selector: String,
}

/// Go `newHcloudDiscovery`.
pub fn new_hcloud_discovery(conf: &SdConfig) -> Result<HcloudDiscovery, String> {
    let http = http_client(conf)?;
    Ok(HcloudDiscovery {
        client: Client { endpoint: conf.hcloud_endpoint.trim_end_matches('/').to_string(), http, max_retries: 5 },
        port: conf.port,
        label_selector: conf.label_selector.clone(),
    })
}

/// Go `hcloud.Client`, the parts used.
struct Client {
    endpoint: String,
    http: net::Client,
    max_retries: u32,
}

/// An error of the handler chain.
enum ApiError {
    /// Go `hcloud.Error` (message, code, correlation id).
    Api { code: String, message: String, correlation_id: String },
    /// Go `ErrStatusCode` wrapped with the status.
    Status(u16),
    /// Transport errors.
    Net(net::Error),
}

impl ApiError {
    fn text(&self) -> String {
        match self {
            ApiError::Api { code, message, correlation_id } if !correlation_id.is_empty() => format!("{message} ({code}, {correlation_id})"),
            ApiError::Api { code, message, .. } => format!("{message} ({code})"),
            ApiError::Status(c) => format!("hcloud: server responded with status code {c}"),
            ApiError::Net(e) => e.to_string(),
        }
    }

    /// Go `retryPolicy`.
    fn retry(&self) -> bool {
        match self {
            ApiError::Api { code, .. } => matches!(code.as_str(), "conflict" | "rate_limit_exceeded" | "bad_gateway" | "timeout"),
            ApiError::Status(c) => *c == 502 || *c == 504,
            ApiError::Net(e) => e.is_timeout(),
        }
    }
}

fn has_json_body(r: &net::Response) -> bool {
    !r.body.is_empty() && r.header.get_str("Content-Type").starts_with("application/json")
}

/// Go `errorFromBody`: an `hcloud.Error` when the body is a JSON error object with a code or message.
fn error_from_body(r: &net::Response) -> Option<ApiError> {
    if !has_json_body(r) {
        return None;
    }
    let v = gojson::parse(&r.body, false).ok()?;
    let mut d = Dec::new();
    let (mut code, mut message) = (String::new(), String::new());
    let mut details: Option<&Json> = None;
    let o = d.obj(&v, "", "", "schema.ErrorResponse")?;
    for (k, e) in o {
        if !key_is(k, "error") {
            continue;
        }
        let Some(eo) = d.obj(e, "ErrorResponse", "error", "schema.Error") else { continue };
        for (k2, e2) in eo {
            if key_is(k2, "code") {
                d.str(e2, &mut code, "Error", &sub("error", k2));
            } else if key_is(k2, "message") {
                d.str(e2, &mut message, "Error", &sub("error", k2));
            } else if key_is(k2, "details") {
                details = Some(e2);
            }
        }
    }
    // Error.UnmarshalJSON decodes the details of two codes; a bad shape fails the whole unmarshal.
    if let Some(det) = details {
        if code == "invalid_input" {
            if let Some(o) = d.obj(det, "", "", "schema.ErrorDetailsInvalidInput") {
                for (k, f) in o {
                    if key_is(k, "fields") {
                        if let Some(a) = d.arr(f, "ErrorDetailsInvalidInput", "fields", "[]struct { Name string \"json:\\\"name\\\"\"; Messages []string \"json:\\\"messages\\\"\" }") {
                            for fe in a {
                                if let Some(fo) = d.obj(fe, "ErrorDetailsInvalidInput", "fields", "struct { Name string \"json:\\\"name\\\"\"; Messages []string \"json:\\\"messages\\\"\" }") {
                                    for (k3, x) in fo {
                                        if key_is(k3, "name") {
                                            d.str(x, &mut String::new(), "", &sub("fields", k3));
                                        } else if key_is(k3, "messages") {
                                            d.str_slice(x, &mut Vec::new(), "", &sub("fields", k3));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else if code == "deprecated_api_endpoint" {
            if let Some(o) = d.obj(det, "", "", "schema.ErrorDetailsDeprecatedAPIEndpoint") {
                for (k, x) in o {
                    if key_is(k, "announcement") {
                        d.str(x, &mut String::new(), "ErrorDetailsDeprecatedAPIEndpoint", k);
                    }
                }
            }
        }
    }
    d.finish().ok()?;
    if code.is_empty() && message.is_empty() {
        return None;
    }
    Some(ApiError::Api { code, message, correlation_id: r.header.get_str("X-Correlation-Id").to_string() })
}

/// Go `ExponentialBackoffWithOpts{Base: 1s, Multiplier: 2, Cap: 1m, Jitter: true}`.
fn backoff(retries: u32) -> Duration {
    let base = 1.0f64;
    let b = (base * 2f64.powi(retries as i32)).min(60.0);
    let mut r = [0u8; 8];
    crypto::random_bytes(&mut r);
    let rand = (u64::from_le_bytes(r) >> 11) as f64 / (1u64 << 53) as f64;
    Duration::from_secs_f64((b - base) * rand + base)
}

impl Client {
    /// GET `path` through the handler chain; returns the response with its parsed body and pagination.
    fn get(&self, ctx: &CancelToken, path: &str) -> Result<(Json, Option<i64>), String> {
        let url = format!("{}{}", self.endpoint, path);
        let mut retries = 0;
        let resp = loop {
            let mut req = net::Request::get(&url).map_err(|e| e.to_string())?;
            req.header.set("User-Agent", HCLOUD_USER_AGENT);
            req.header.set("Accept", "application/json");
            let err = match self.http.do_request(req) {
                Ok(r) if (400..=599).contains(&r.status) => error_from_body(&r).unwrap_or(ApiError::Status(r.status)),
                Ok(r) => break r,
                Err(e) => ApiError::Net(e),
            };
            if ctx.is_cancelled() {
                return Err(err.text());
            }
            if retries < self.max_retries && err.retry() {
                if ctx.wait_timeout(backoff(retries)) {
                    return Err(err.text());
                }
                retries += 1;
                continue;
            }
            return Err(err.text());
        };
        // Go parseHandler: the meta first (JSON bodies only), then the schema.
        let parsed = gojson::parse(&resp.body, false);
        let mut next_page = None;
        if has_json_body(&resp) {
            let v = parsed.as_ref().map_err(|e| format!("hcloud: error reading response meta data: {e}"))?;
            next_page = meta_next_page(v).map_err(|e| format!("hcloud: error reading response meta data: {e}"))?;
        }
        Ok((parsed?, next_page))
    }

    /// Go `iterPages` over `/<resource>?<opts>` (`List` + `AllWithOpts`): decodes every item of the list field
    /// `resource` of `schema.<list_type>` with `item` (a null item is the zero value).
    fn all<T: Default>(&self, ctx: &CancelToken, resource: &str, label_selector: &str, list_type: &str, item_type: &str, item: fn(&mut Dec, &Json) -> Option<T>) -> Result<Vec<T>, String> {
        let mut page = 1i64;
        let mut out = Vec::new();
        loop {
            let mut q: Vec<(String, String)> = vec![("page".into(), page.to_string()), ("per_page".into(), "50".into())];
            if !label_selector.is_empty() {
                q.push(("label_selector".into(), label_selector.to_string()));
            }
            let (v, next) = self.get(ctx, &format!("/{resource}?{}", yaml::url::encode_query(&q)))?;
            let mut d = Dec::new();
            let mut items = Vec::new();
            if let Some(o) = d.obj(&v, "", "", &format!("schema.{list_type}")) {
                for (k, e) in o {
                    if key_is(k, resource)
                        && let Some(a) = d.arr(e, list_type, resource, &format!("[]schema.{item_type}"))
                    {
                        items = a.iter().map(|x| item(&mut d, x).unwrap_or_default()).collect();
                    }
                }
            }
            d.finish()?;
            out.extend(items);
            match next {
                Some(n) if n != 0 => page = n,
                _ => return Ok(out),
            }
        }
    }
}

/// Go `schema.MetaResponse`: `meta.pagination.next_page` (None when there is no pagination object).
fn meta_next_page(v: &Json) -> Result<Option<i64>, String> {
    let mut d = Dec::new();
    let mut next = None;
    if let Some(o) = d.obj(v, "", "", "schema.MetaResponse") {
        for (k, m) in o {
            if !key_is(k, "meta") {
                continue;
            }
            let Some(mo) = d.obj(m, "MetaResponse", "meta", "schema.Meta") else { continue };
            for (k2, p) in mo {
                if !key_is(k2, "pagination") {
                    continue;
                }
                let path = sub("meta", k2);
                match d.obj(p, "Meta", &path, "schema.MetaPagination") {
                    Some(po) => {
                        let mut n = 0i64;
                        for (k3, x) in po {
                            let mut dummy = 0i64;
                            let f = if key_is(k3, "next_page") { &mut n } else { &mut dummy };
                            if ["page", "per_page", "previous_page", "next_page", "last_page", "total_entries"].iter().any(|n| key_is(k3, n)) {
                                d.int(x, f, "MetaPagination", &sub(&path, k3), "int");
                            }
                        }
                        next = Some(n);
                    }
                    None if p.is_null() => next = None,
                    None => {}
                }
            }
        }
    }
    d.finish()?;
    Ok(next)
}

/// The fields of `schema.Server` the discovery reads.
#[derive(Default)]
struct Server {
    id: i64,
    name: String,
    status: String,
    ipv4: String,
    ipv6: String,
    private_net: Vec<(i64, String)>,
    server_type_name: String,
    cores: i64,
    memory: f32,
    disk: i64,
    cpu_type: String,
    location_name: String,
    network_zone: String,
    image: Option<Image>,
    labels: Vec<(String, String)>,
}

#[derive(Default)]
struct Image {
    name: Option<String>,
    description: String,
    os_flavor: String,
    os_version: Option<String>,
}

fn decode_server(d: &mut Dec, v: &Json) -> Option<Server> {
    let base = "servers";
    let o = d.obj(v, "ServerListResponse", base, "schema.Server")?;
    let mut s = Server::default();
    let st = "Server";
    for (k, e) in o {
        let p = sub(base, k);
        if key_is(k, "id") {
            d.int(e, &mut s.id, st, &p, "int64");
        } else if key_is(k, "name") {
            d.str(e, &mut s.name, st, &p);
        } else if key_is(k, "status") {
            d.str(e, &mut s.status, st, &p);
        } else if key_is(k, "public_net") {
            if let Some(po) = d.obj(e, st, &p, "schema.ServerPublicNet") {
                for (k2, e2) in po {
                    let p2 = sub(&p, k2);
                    let (dst, ty) = if key_is(k2, "ipv4") {
                        (&mut s.ipv4, "ServerPublicNetIPv4")
                    } else if key_is(k2, "ipv6") {
                        (&mut s.ipv6, "ServerPublicNetIPv6")
                    } else {
                        continue;
                    };
                    if let Some(io) = d.obj(e2, "ServerPublicNet", &p2, &format!("schema.{ty}")) {
                        for (k3, e3) in io {
                            if key_is(k3, "ip") {
                                d.str(e3, dst, ty, &sub(&p2, k3));
                            }
                        }
                    }
                }
            }
        } else if key_is(k, "private_net") {
            if let Some(a) = d.arr(e, st, &p, "[]schema.ServerPrivateNet") {
                s.private_net = Vec::with_capacity(a.len());
                for pe in a {
                    let mut pn = (0i64, String::new());
                    if let Some(po) = d.obj(pe, st, &p, "schema.ServerPrivateNet") {
                        for (k2, e2) in po {
                            if key_is(k2, "network") {
                                d.int(e2, &mut pn.0, "ServerPrivateNet", &sub(&p, k2), "int64");
                            } else if key_is(k2, "ip") {
                                d.str(e2, &mut pn.1, "ServerPrivateNet", &sub(&p, k2));
                            }
                        }
                    }
                    s.private_net.push(pn);
                }
            }
        } else if key_is(k, "server_type") {
            if let Some(to) = d.obj(e, st, &p, "schema.ServerType") {
                for (k2, e2) in to {
                    let p2 = sub(&p, k2);
                    if key_is(k2, "name") {
                        d.str(e2, &mut s.server_type_name, "ServerType", &p2);
                    } else if key_is(k2, "cores") {
                        d.int(e2, &mut s.cores, "ServerType", &p2, "int");
                    } else if key_is(k2, "memory") {
                        d.f32(e2, &mut s.memory, "ServerType", &p2);
                    } else if key_is(k2, "disk") {
                        d.int(e2, &mut s.disk, "ServerType", &p2, "int");
                    } else if key_is(k2, "cpu_type") {
                        d.str(e2, &mut s.cpu_type, "ServerType", &p2);
                    }
                }
            }
        } else if key_is(k, "location") {
            if let Some(lo) = d.obj(e, st, &p, "schema.Location") {
                for (k2, e2) in lo {
                    if key_is(k2, "name") {
                        d.str(e2, &mut s.location_name, "Location", &sub(&p, k2));
                    } else if key_is(k2, "network_zone") {
                        d.str(e2, &mut s.network_zone, "Location", &sub(&p, k2));
                    }
                }
            }
        } else if key_is(k, "image") {
            match d.obj(e, st, &p, "schema.Image") {
                Some(io) => {
                    let img = s.image.get_or_insert_with(Image::default);
                    for (k2, e2) in io {
                        let p2 = sub(&p, k2);
                        if key_is(k2, "name") {
                            d.opt_str(e2, &mut img.name, "Image", &p2);
                        } else if key_is(k2, "description") {
                            d.str(e2, &mut img.description, "Image", &p2);
                        } else if key_is(k2, "os_flavor") {
                            d.str(e2, &mut img.os_flavor, "Image", &p2);
                        } else if key_is(k2, "os_version") {
                            d.opt_str(e2, &mut img.os_version, "Image", &p2);
                        }
                    }
                }
                None if e.is_null() => s.image = None,
                None => {}
            }
        } else if key_is(k, "labels") {
            d.str_map(e, &mut s.labels, st, &p);
        }
    }
    Some(s)
}

fn decode_network(d: &mut Dec, v: &Json) -> Option<(i64, String)> {
    let o: &Obj = d.obj(v, "NetworkListResponse", "networks", "schema.Network")?;
    let mut n = (0i64, String::new());
    for (k, e) in o {
        if key_is(k, "id") {
            d.int(e, &mut n.0, "Network", &sub("networks", k), "int64");
        } else if key_is(k, "name") {
            d.str(e, &mut n.1, "Network", &sub("networks", k));
        }
    }
    Some(n)
}

/// Go `ipFromString(s).String()`.
fn ip_text(s: &str) -> String {
    gojson::ip_string(&gojson::parse_ip(s))
}

/// Go `ipNetFromString(s).String()`.
fn ipnet_text(s: &str) -> String {
    let n: Option<(IpAddr, u8)> = gojson::parse_cidr(s).ok().map(|(_, net, bits)| (net, bits));
    gojson::ipnet_string(&n)
}

impl HcloudDiscovery {
    /// Go `refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let servers: Vec<Server> = self.client.all(ctx, "servers", &self.label_selector, "ServerListResponse", "Server", decode_server)?;
        let networks: Vec<(i64, String)> = self.client.all(ctx, "networks", "", "NetworkListResponse", "Network", decode_network)?;

        let port = (self.port as u64).to_string();
        let mut targets = Vec::with_capacity(servers.len());
        for server in &servers {
            let ipv4 = ip_text(&server.ipv4);
            let mut labels = LabelSet::new();
            let mut set = |k: &str, v: String| {
                labels.insert(k.to_string(), v);
            };
            set(HETZNER_LABEL_ROLE, HETZNER_ROLE_HCLOUD.to_string());
            set(HETZNER_LABEL_SERVER_ID, server.id.to_string());
            set(HETZNER_LABEL_SERVER_NAME, server.name.clone());
            set(HETZNER_LABEL_PUBLIC_IPV4, ipv4.clone());
            set(HETZNER_LABEL_PUBLIC_IPV6_NETWORK, ipnet_text(&server.ipv6));
            set(HETZNER_LABEL_SERVER_STATUS, server.status.clone());
            set(&format!("{HCLOUD_PREFIX}location"), server.location_name.clone());
            set(&format!("{HCLOUD_PREFIX}location_network_zone"), server.network_zone.clone());
            // label names kept for backward compatibility
            set(&format!("{HCLOUD_PREFIX}datacenter_location"), server.location_name.clone());
            set(&format!("{HCLOUD_PREFIX}datacenter_location_network_zone"), server.network_zone.clone());
            set(&format!("{HCLOUD_PREFIX}server_type"), server.server_type_name.clone());
            set(&format!("{HCLOUD_PREFIX}cpu_cores"), server.cores.to_string());
            set(&format!("{HCLOUD_PREFIX}cpu_type"), server.cpu_type.clone());
            set(&format!("{HCLOUD_PREFIX}memory_size_gb"), (server.memory as i64).to_string());
            set(&format!("{HCLOUD_PREFIX}disk_size_gb"), server.disk.to_string());
            set(ADDRESS_LABEL, join_host_port(&ipv4, &port));
            if let Some(img) = &server.image {
                set(&format!("{HCLOUD_PREFIX}image_name"), img.name.clone().unwrap_or_default());
                set(&format!("{HCLOUD_PREFIX}image_description"), img.description.clone());
                set(&format!("{HCLOUD_PREFIX}image_os_version"), img.os_version.clone().unwrap_or_default());
                set(&format!("{HCLOUD_PREFIX}image_os_flavor"), img.os_flavor.clone());
            }
            for (net_id, ip) in &server.private_net {
                for (id, name) in &networks {
                    if net_id == id {
                        set(&format!("{HCLOUD_PREFIX}private_ipv4_{}", sanitize_label_name(name)), ip_text(ip));
                    }
                }
            }
            for (k, v) in &server.labels {
                let k = sanitize_label_name(k);
                set(&format!("{HCLOUD_PREFIX}labelpresent_{k}"), "true".to_string());
                set(&format!("{HCLOUD_PREFIX}label_{k}"), v.clone());
            }
            targets.push(labels);
        }
        Ok(vec![Arc::new(Group { source: "hetzner".to_string(), targets, labels: LabelSet::new() })])
    }
}
