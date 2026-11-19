//! Instance role (Go `instance.go`) with the scaleway-sdk-go calls it makes: instance `ListServers`
//! (`/instance/v1/zones/<zone>/servers`) and IPAM `ListIPs` (`/ipam/v1/regions/<region>/ips`), all pages.
use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::scw::{self, Body, decode_list, unmarshal_enum, unmarshal_zone, zone_region};
use super::{SEPARATOR, SdConfig, new_client};
use crate::cancel::CancelToken;
use crate::gojson::{self, Dec, key_is, sub};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;

const P: &str = "__meta_scaleway_instance_";

/// Go `instanceDiscovery`.
pub struct InstanceDiscovery {
    client: scw::Client,
    port: i64,
    name_filter: String,
    tags_filter: Vec<String>,
}

/// Go `newInstanceDiscovery`.
pub(crate) fn new_instance_discovery(conf: &SdConfig) -> Result<InstanceDiscovery, String> {
    Ok(InstanceDiscovery {
        client: new_client(conf)?,
        port: conf.port,
        name_filter: conf.name_filter.clone(),
        tags_filter: conf.tags_filter.clone(),
    })
}

/// Go `instance.ServerIP`, the fields used.
#[derive(Default)]
struct ServerIp {
    address: Option<IpAddr>,
    family: String,
}

/// The fields of `instance.Server` the discovery reads.
#[derive(Default)]
struct Server {
    id: String,
    name: String,
    organization: String,
    project: String,
    tags: Vec<String>,
    commercial_type: String,
    hostname: String,
    /// (id, name, arch)
    image: Option<(String, String, String)>,
    private_ip: Option<String>,
    public_ip: Option<ServerIp>,
    public_ips: Vec<ServerIp>,
    state: String,
    /// (cluster_id, hypervisor_id, node_id)
    location: Option<(String, String, String)>,
    ipv6: Option<Option<IpAddr>>,
    boot_type: String,
    /// (id, name)
    security_group: Option<(String, String)>,
    /// ids of the non-nil private NICs
    private_nics: Vec<String>,
    zone: String,
}

fn decode_server_ip(
    d: &mut Dec,
    v: &Json,
    st: &str,
    path: &str,
) -> Result<Option<ServerIp>, String> {
    let Some(o) = d.obj(v, st, path, "instance.ServerIP") else {
        return Ok(None);
    };
    let mut ip = ServerIp::default();
    for (k, e) in o {
        if key_is(k, "address") {
            d.ip(e, &mut ip.address, "ServerIP", &sub(path, k))?;
        } else if key_is(k, "family") {
            unmarshal_enum(e, &mut ip.family, "inet")?;
        }
    }
    Ok(Some(ip))
}

/// Decodes string fields `names` of a nested object of Go type `instance.<ty>`.
fn strs(
    d: &mut Dec,
    v: &Json,
    st: &str,
    path: &str,
    ty: &str,
    names: &[&str],
) -> Option<Vec<String>> {
    let o = d.obj(v, st, path, &format!("instance.{ty}"))?;
    let mut out = vec![String::new(); names.len()];
    for (k, e) in o {
        for (i, n) in names.iter().enumerate() {
            if key_is(k, n) {
                d.str(e, &mut out[i], ty, &sub(path, k));
            }
        }
    }
    Some(out)
}

fn decode_server(d: &mut Dec, v: &Json) -> Result<Option<Server>, String> {
    let Some(o) = d.obj(v, "ListServersResponse", "servers", "instance.Server") else {
        return Ok(None);
    };
    let mut s = Server::default();
    let st = "Server";
    for (k, e) in o {
        let p = sub("servers", k);
        let simple = [
            ("id", &mut s.id),
            ("name", &mut s.name),
            ("organization", &mut s.organization),
            ("project", &mut s.project),
            ("commercial_type", &mut s.commercial_type),
            ("hostname", &mut s.hostname),
        ];
        if let Some((_, dst)) = simple.into_iter().find(|(n, _)| key_is(k, n)) {
            d.str(e, dst, st, &p);
        } else if key_is(k, "tags") {
            d.str_slice(e, &mut s.tags, st, &p);
        } else if key_is(k, "image") {
            match d.obj(e, st, &p, "instance.Image") {
                Some(io) => {
                    let img = s.image.get_or_insert_with(Default::default);
                    for (k2, x) in io {
                        let p2 = sub(&p, k2);
                        if key_is(k2, "id") {
                            d.str(x, &mut img.0, "Image", &p2);
                        } else if key_is(k2, "name") {
                            d.str(x, &mut img.1, "Image", &p2);
                        } else if key_is(k2, "arch") {
                            unmarshal_enum(x, &mut img.2, "unknown_arch")?;
                        } else if key_is(k2, "zone") {
                            unmarshal_zone(x, &mut String::new())?;
                        }
                    }
                }
                None if e.is_null() => s.image = None,
                None => {}
            }
        } else if key_is(k, "private_ip") {
            d.opt_str(e, &mut s.private_ip, st, &p);
        } else if key_is(k, "public_ip") {
            if e.is_null() {
                s.public_ip = None;
            } else if let Some(ip) = decode_server_ip(d, e, st, &p)? {
                s.public_ip = Some(ip);
            }
        } else if key_is(k, "public_ips") {
            if let Some(a) = d.arr(e, st, &p, "[]*instance.ServerIP") {
                s.public_ips = Vec::with_capacity(a.len());
                for x in a {
                    if let Some(ip) = decode_server_ip(d, x, st, &p)? {
                        s.public_ips.push(ip);
                    }
                }
            }
        } else if key_is(k, "state") {
            unmarshal_enum(e, &mut s.state, "running")?;
        } else if key_is(k, "location") {
            match strs(
                d,
                e,
                st,
                &p,
                "ServerLocation",
                &["cluster_id", "hypervisor_id", "node_id"],
            ) {
                Some(f) => s.location = Some((f[0].clone(), f[1].clone(), f[2].clone())),
                None if e.is_null() => s.location = None,
                None => {}
            }
        } else if key_is(k, "ipv6") {
            match d.obj(e, st, &p, "instance.ServerIPv6") {
                Some(io) => {
                    let addr = s.ipv6.get_or_insert(None);
                    for (k2, x) in io {
                        if key_is(k2, "address") {
                            d.ip(x, addr, "ServerIPv6", &sub(&p, k2))?;
                        }
                    }
                }
                None if e.is_null() => s.ipv6 = None,
                None => {}
            }
        } else if key_is(k, "boot_type") {
            unmarshal_enum(e, &mut s.boot_type, "local")?;
        } else if key_is(k, "security_group") {
            match strs(d, e, st, &p, "SecurityGroupSummary", &["id", "name"]) {
                Some(f) => s.security_group = Some((f[0].clone(), f[1].clone())),
                None if e.is_null() => s.security_group = None,
                None => {}
            }
        } else if key_is(k, "private_nics") {
            if let Some(a) = d.arr(e, st, &p, "[]*instance.PrivateNIC") {
                s.private_nics.clear();
                for x in a {
                    if let Some(o2) = d.obj(x, st, &p, "instance.PrivateNIC") {
                        let mut id = String::new();
                        for (k2, y) in o2 {
                            if key_is(k2, "id") {
                                d.str(y, &mut id, "PrivateNIC", &sub(&p, k2));
                            } else if key_is(k2, "zone") {
                                unmarshal_zone(y, &mut String::new())?;
                            }
                        }
                        s.private_nics.push(id);
                    }
                }
            }
        } else if key_is(k, "zone") {
            unmarshal_zone(e, &mut s.zone)?;
        }
    }
    Ok(Some(s))
}

impl InstanceDiscovery {
    /// Go `instanceAPI.ListServers(req, scw.WithAllPages())`.
    fn list_servers(&mut self) -> Result<Vec<Server>, String> {
        let zone = self.client.default_zone.clone();
        let mut q: Vec<(String, String)> = Vec::new();
        if !self.name_filter.is_empty() {
            q.push(("name".into(), self.name_filter.clone()));
        }
        if !self.tags_filter.is_empty() {
            q.push(("tags".into(), self.tags_filter.join(",")));
        }
        // ListServersRequestOrder's zero value prints as its default
        q.push(("order".into(), "creation_date_desc".into()));
        if zone.is_empty() {
            return Err("scaleway-sdk-go: field Zone cannot be empty in request".to_string());
        }
        let mut servers = Vec::new();
        self.client.list_all(
            &format!("/instance/v1/zones/{zone}/servers"),
            q,
            true,
            |b: &Body| {
                decode_list(
                    b,
                    "instance",
                    "ListServersResponse",
                    "servers",
                    "Server",
                    "uint32",
                    |d, x| {
                        if let Some(s) = decode_server(d, x)? {
                            servers.push(s);
                        }
                        Ok(())
                    },
                )
            },
        )?;
        self.client.api_metadata();
        Ok(servers)
    }

    /// Go `privateNICIPs`: the IPv4 address of each private NIC of the servers without a public or private IP.
    fn private_nic_ips(&mut self, servers: &[Server]) -> Result<BTreeMap<String, String>, String> {
        let mut by_region: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for s in servers {
            if s.private_ip.is_some() || s.public_ip.is_some() || s.ipv6.is_some() {
                continue;
            }
            let region = zone_region(&s.zone)?;
            for id in &s.private_nics {
                if !id.is_empty() {
                    by_region
                        .entry(region.clone())
                        .or_default()
                        .push(id.clone());
                }
            }
        }
        let mut out = BTreeMap::new();
        for (region, ids) in by_region {
            let mut q: Vec<(String, String)> = vec![("order_by".into(), "created_at_desc".into())];
            for id in &ids {
                q.push(("resource_ids".into(), id.clone()));
            }
            // ResourceType's zero value prints as its default
            q.push(("resource_type".into(), "unknown_type".into()));
            q.push(("resource_types".into(), "instance_private_nic".into()));
            let mut ips: Vec<(Option<IpAddr>, bool, Option<String>)> = Vec::new();
            self.client.list_all(
                &format!("/ipam/v1/regions/{region}/ips"),
                q,
                false,
                |b: &Body| {
                    decode_list(
                        b,
                        "ipam",
                        "ListIPsResponse",
                        "ips",
                        "IP",
                        "uint64",
                        |d, x| {
                            let Some(o) = d.obj(x, "ListIPsResponse", "ips", "ipam.IP") else {
                                return Ok(());
                            };
                            let mut ip = (None, false, None);
                            for (k, e) in o {
                                let p = sub("ips", k);
                                if key_is(k, "address") {
                                    ip.0 = unmarshal_ipnet(e)?;
                                } else if key_is(k, "is_ipv6") {
                                    d.bool(e, &mut ip.1, "IP", &p);
                                } else if key_is(k, "resource") {
                                    match d.obj(e, "IP", &p, "ipam.Resource") {
                                        Some(ro) => {
                                            let id = ip.2.get_or_insert_with(String::new);
                                            for (k2, y) in ro {
                                                if key_is(k2, "id") {
                                                    d.str(y, id, "Resource", &sub(&p, k2));
                                                } else if key_is(k2, "type") {
                                                    unmarshal_enum(
                                                        y,
                                                        &mut String::new(),
                                                        "unknown_type",
                                                    )?;
                                                }
                                            }
                                        }
                                        None if e.is_null() => ip.2 = None,
                                        None => {}
                                    }
                                } else if key_is(k, "region") {
                                    unmarshal_region(e)?;
                                } else if key_is(k, "zone") && !e.is_null() {
                                    unmarshal_zone(e, &mut String::new())?;
                                }
                            }
                            ips.push(ip);
                            Ok(())
                        },
                    )
                },
            )?;
            self.client.api_metadata();
            for (addr, is_ipv6, resource) in ips {
                if let (Some(id), false, Some(_)) = (&resource, is_ipv6, &addr) {
                    out.insert(id.clone(), gojson::ip_string(&addr));
                }
            }
        }
        Ok(out)
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let servers = self.list_servers()?;
        let nic_ips = self.private_nic_ips(&servers)?;
        let port = (self.port as u64).to_string();
        let mut targets = Vec::new();
        for s in &servers {
            let mut labels = LabelSet::new();
            let mut set = |k: &str, v: &str| {
                labels.insert(format!("{P}{k}"), v.to_string());
            };
            set("boot_type", &s.boot_type);
            set("hostname", &s.hostname);
            set("id", &s.id);
            set("name", &s.name);
            set("organization_id", &s.organization);
            set("project_id", &s.project);
            set("status", &s.state);
            set("type", &s.commercial_type);
            set("zone", &s.zone);
            if let Some((id, name, arch)) = &s.image {
                set("image_arch", arch);
                set("image_id", id);
                set("image_name", name);
            }
            if let Some((c, h, n)) = &s.location {
                set("location_cluster_id", c);
                set("location_hypervisor_id", h);
                set("location_node_id", n);
            }
            if let Some((id, name)) = &s.security_group {
                set("security_group_id", id);
                set("security_group_name", name);
            }
            if let Ok(region) = zone_region(&s.zone) {
                set("region", &region);
            }
            if !s.tags.is_empty() {
                set("tags", &join_tags(&s.tags, SEPARATOR));
            }
            let mut addr = String::new();
            if !s.public_ips.is_empty() {
                let (mut v4, mut v6) = (Vec::new(), Vec::new());
                for ip in &s.public_ips {
                    match ip.family.as_str() {
                        "inet" => v4.push(gojson::ip_string(&ip.address)),
                        "inet6" => v6.push(gojson::ip_string(&ip.address)),
                        _ => {}
                    }
                }
                if !v6.is_empty() {
                    set("public_ipv6_addresses", &join_tags(&v6, SEPARATOR));
                }
                if !v4.is_empty() {
                    set("public_ipv4_addresses", &join_tags(&v4, SEPARATOR));
                }
            }
            if let Some(ipv6) = &s.ipv6 {
                let a = gojson::ip_string(ipv6);
                set("public_ipv6", &a);
                addr = a;
            }
            if let Some(ip) = &s.public_ip {
                let a = gojson::ip_string(&ip.address);
                if ip.family != "inet6" {
                    set("public_ipv4", &a);
                }
                addr = a;
            }
            if let Some(ip) = &s.private_ip {
                set("private_ipv4", ip);
                addr = ip.clone();
            }
            if addr.is_empty() {
                for id in &s.private_nics {
                    if let Some(ip) = nic_ips.get(id).filter(|ip| !ip.is_empty()) {
                        set("private_ipv4", ip);
                        addr = ip.clone();
                        break;
                    }
                }
            }
            if !addr.is_empty() {
                labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&addr, &port));
                targets.push(labels);
            }
        }
        Ok(vec![Arc::new(Group {
            source: "scaleway".to_string(),
            targets,
            labels: LabelSet::new(),
        })])
    }
}

/// Go `scw.IPNet.UnmarshalJSON`; returns the address (`IPNet.IP`).
fn unmarshal_ipnet(v: &Json) -> Result<Option<IpAddr>, String> {
    let mut s = gojson::custom_string(v)?;
    if s.is_empty() {
        return Ok(None);
    }
    match gojson::parse_ip(&s) {
        Some(ip) if gojson::is_ipv4(&ip) => s += "/32",
        Some(_) => s += "/128",
        None => {}
    }
    let (ip, _, _) = gojson::parse_cidr(&s)?;
    Ok(Some(ip))
}

/// Go `scw.Region.UnmarshalJSON` (only its errors matter here).
fn unmarshal_region(v: &Json) -> Result<(), String> {
    let s = gojson::custom_string(v)?;
    if s.is_empty() || s == "par1" || s == "ams1" || scw::is_region(&s) {
        return Ok(());
    }
    Err(
        "scaleway-sdk-go: bad region format, available regions are: fr-par, nl-ams, pl-waw, it-mil"
            .to_string(),
    )
}
