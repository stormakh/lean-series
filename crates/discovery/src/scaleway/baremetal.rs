//! Baremetal role (Go `baremetal.go`) with the scaleway-sdk-go baremetal calls it makes: `ListServers`,
//! `ListOffers` and `ListOS` (`/baremetal/v1/zones/<zone>/{servers,offers,os}`), all pages.
use std::net::IpAddr;
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::scw::{self, Body, decode_list, unmarshal_enum, unmarshal_zone};
use super::{SEPARATOR, SdConfig, new_client};
use crate::cancel::CancelToken;
use crate::gojson::{self, Dec, key_is, sub};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;

const P: &str = "__meta_scaleway_baremetal_";

/// Go `baremetalDiscovery`.
pub struct BaremetalDiscovery {
    client: scw::Client,
    port: i64,
    name_filter: String,
    tags_filter: Vec<String>,
}

/// Go `newBaremetalDiscovery`.
pub(crate) fn new_baremetal_discovery(conf: &SdConfig) -> Result<BaremetalDiscovery, String> {
    Ok(BaremetalDiscovery {
        client: new_client(conf)?,
        port: conf.port,
        name_filter: conf.name_filter.clone(),
        tags_filter: conf.tags_filter.clone(),
    })
}

/// The fields of `baremetal.Server` the discovery reads.
#[derive(Default)]
struct Server {
    id: String,
    name: String,
    zone: String,
    status: String,
    project_id: String,
    offer_id: String,
    tags: Vec<String>,
    /// (address, version) of the non-nil IPs
    ips: Vec<(Option<IpAddr>, String)>,
    /// `install.os_id` (`None`: no install)
    install_os_id: Option<String>,
}

fn decode_server(d: &mut Dec, v: &Json) -> Result<Option<Server>, String> {
    let Some(o) = d.obj(v, "ListServersResponse", "servers", "baremetal.Server") else {
        return Ok(None);
    };
    let mut s = Server::default();
    let st = "Server";
    for (k, e) in o {
        let p = sub("servers", k);
        let simple = [
            ("id", &mut s.id),
            ("name", &mut s.name),
            ("project_id", &mut s.project_id),
            ("offer_id", &mut s.offer_id),
        ];
        if let Some((_, dst)) = simple.into_iter().find(|(n, _)| key_is(k, n)) {
            d.str(e, dst, st, &p);
        } else if key_is(k, "zone") {
            unmarshal_zone(e, &mut s.zone)?;
        } else if key_is(k, "status") {
            unmarshal_enum(e, &mut s.status, "unknown")?;
        } else if key_is(k, "tags") {
            d.str_slice(e, &mut s.tags, st, &p);
        } else if key_is(k, "ips") {
            if let Some(a) = d.arr(e, st, &p, "[]*baremetal.IP") {
                s.ips.clear();
                for x in a {
                    if let Some(io) = d.obj(x, st, &p, "baremetal.IP") {
                        let mut ip = (None, String::new());
                        for (k2, y) in io {
                            if key_is(k2, "address") {
                                d.ip(y, &mut ip.0, "IP", &sub(&p, k2))?;
                            } else if key_is(k2, "version") {
                                unmarshal_enum(y, &mut ip.1, "IPv4")?;
                            }
                        }
                        s.ips.push(ip);
                    }
                }
            }
        } else if key_is(k, "install") {
            match d.obj(e, st, &p, "baremetal.ServerInstall") {
                Some(io) => {
                    let id = s.install_os_id.get_or_insert_with(String::new);
                    for (k2, y) in io {
                        if key_is(k2, "os_id") {
                            d.str(y, id, "ServerInstall", &sub(&p, k2));
                        }
                    }
                }
                None if e.is_null() => s.install_os_id = None,
                None => {}
            }
        }
    }
    Ok(Some(s))
}

/// Decodes the string fields `names` of the items of a list (offers, os).
fn decode_named(
    d: &mut Dec,
    v: &Json,
    root: &str,
    field: &str,
    ty: &str,
    names: &[&str],
) -> Result<Option<Vec<String>>, String> {
    let Some(o) = d.obj(v, root, field, &format!("baremetal.{ty}")) else {
        return Ok(None);
    };
    let mut out = vec![String::new(); names.len()];
    for (k, e) in o {
        for (i, n) in names.iter().enumerate() {
            if key_is(k, n) {
                d.str(e, &mut out[i], ty, &sub(field, k));
            }
        }
        if key_is(k, "zone") {
            unmarshal_zone(e, &mut String::new())?;
        }
    }
    Ok(Some(out))
}

impl BaremetalDiscovery {
    fn zone_path(&self, what: &str) -> Result<String, String> {
        let zone = &self.client.default_zone;
        if zone.is_empty() {
            return Err("scaleway-sdk-go: field Zone cannot be empty in request".to_string());
        }
        Ok(format!("/baremetal/v1/zones/{zone}/{what}"))
    }

    /// Lists every item of `field` (Go type `ty`) of `/baremetal/v1/zones/<zone>/<what>`.
    fn list_named(
        &self,
        what: &str,
        query: Vec<(String, String)>,
        root: &str,
        field: &str,
        ty: &str,
        names: &[&str],
    ) -> Result<Vec<Vec<String>>, String> {
        let mut out = Vec::new();
        self.client
            .list_all(&self.zone_path(what)?, query, false, |b: &Body| {
                decode_list(b, "baremetal", root, field, ty, "uint32", |d, x| {
                    if let Some(v) = decode_named(d, x, root, field, ty, names)? {
                        out.push(v);
                    }
                    Ok(())
                })
            })?;
        Ok(out)
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut q: Vec<(String, String)> = vec![("order_by".into(), "created_at_asc".into())];
        for t in &self.tags_filter {
            q.push(("tags".into(), t.clone()));
        }
        if !self.name_filter.is_empty() {
            q.push(("name".into(), self.name_filter.clone()));
        }
        let mut servers = Vec::new();
        self.client
            .list_all(&self.zone_path("servers")?, q, false, |b: &Body| {
                decode_list(
                    b,
                    "baremetal",
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
            })?;
        let offers = self.list_named(
            "offers",
            vec![(
                "subscription_period".into(),
                "unknown_subscription_period".into(),
            )],
            "ListOffersResponse",
            "offers",
            "Offer",
            &["id", "name"],
        )?;
        let os_list = self.list_named(
            "os",
            Vec::new(),
            "ListOSResponse",
            "os",
            "OS",
            &["id", "name", "version"],
        )?;

        let port = (self.port as u64).to_string();
        let mut targets = Vec::new();
        for s in servers {
            let mut labels = LabelSet::new();
            let l = |k: &str| format!("{P}{k}");
            labels.insert(l("id"), s.id.clone());
            labels.insert(l("name"), s.name.clone());
            labels.insert(l("zone"), s.zone.clone());
            labels.insert(l("status"), s.status.clone());
            labels.insert(l("project_id"), s.project_id.clone());
            if let Some(offer) = offers.iter().find(|o| o[0] == s.offer_id) {
                labels.insert(l("type"), offer[1].clone());
            }
            if let Some(os_id) = &s.install_os_id
                && let Some(os) = os_list.iter().find(|o| &o[0] == os_id)
            {
                labels.insert(l("os_name"), os[1].clone());
                labels.insert(l("os_version"), os[2].clone());
            }
            if !s.tags.is_empty() {
                labels.insert(l("tags"), join_tags(&s.tags, SEPARATOR));
            }
            for (addr, version) in &s.ips {
                let a = gojson::ip_string(addr);
                match version.as_str() {
                    // an IP decoded without a version has "" (IPVersion.String() is "IPv4")
                    "IPv4" | "" => {
                        if labels.contains_key(&l("public_ipv4")) {
                            // If the server has multiple IPv4, we only take the first one.
                            continue;
                        }
                        labels.insert(l("public_ipv4"), a.clone());
                        // We always default the __address__ to IPv4.
                        labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&a, &port));
                    }
                    "IPv6" => {
                        if labels.contains_key(&l("public_ipv6")) {
                            continue;
                        }
                        labels.insert(l("public_ipv6"), a.clone());
                        if !labels.contains_key(ADDRESS_LABEL) {
                            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&a, &port));
                        }
                    }
                    v => return Err(format!("unknown IP version: {v}")),
                }
            }
            targets.push(labels);
        }
        Ok(vec![Arc::new(Group {
            source: "scaleway".to_string(),
            targets,
            labels: LabelSet::new(),
        })])
    }
}
