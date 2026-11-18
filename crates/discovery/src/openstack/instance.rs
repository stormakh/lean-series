//! Port of `discovery/openstack/instance.go`: one target per fixed address of each Nova server
//! (`servers/detail`), with the floating IP from Neutron (`ports`, `floatingips`) as public IP.
//!
//! Go ranges over the server's address pools and metadata as maps (random order); here they are taken in the
//! order of the response.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::LABEL_PROJECT_ID;
use super::client::{AuthOptions, ProviderClient, Resource, Service, all_pages, each_page, field, map_field, str_field};
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::refresh::Refresher;
use crate::targetgroup::Group;
use crate::util::{join_host_port, sanitize_label_name};

pub const LABEL_ADDRESS_POOL: &str = "__meta_openstack_address_pool";
pub const LABEL_INSTANCE_FLAVOR: &str = "__meta_openstack_instance_flavor";
pub const LABEL_INSTANCE_ID: &str = "__meta_openstack_instance_id";
pub const LABEL_INSTANCE_IMAGE: &str = "__meta_openstack_instance_image";
pub const LABEL_INSTANCE_NAME: &str = "__meta_openstack_instance_name";
pub const LABEL_INSTANCE_STATUS: &str = "__meta_openstack_instance_status";
pub const LABEL_PRIVATE_IP: &str = "__meta_openstack_private_ip";
pub const LABEL_PUBLIC_IP: &str = "__meta_openstack_public_ip";
pub const LABEL_TAG_PREFIX: &str = "__meta_openstack_tag_";
pub const LABEL_USER_ID: &str = "__meta_openstack_user_id";

/// Go `InstanceDiscovery`.
pub struct InstanceDiscovery {
    provider: ProviderClient,
    auth_opts: AuthOptions,
    region: String,
    logger: Logger,
    port: i64,
    all_tenants: bool,
    availability: String,
}

impl InstanceDiscovery {
    pub fn new(provider: ProviderClient, auth_opts: AuthOptions, port: i64, region: String, all_tenants: bool, availability: String, logger: Logger) -> InstanceDiscovery {
        InstanceDiscovery { provider, auth_opts, region, logger, port, all_tenants, availability }
    }
}

/// Go `strconv.ParseBool`.
fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// The fields of gophercloud's `servers.Server` that the discovery reads, with its decode errors.
struct Server<'a> {
    id: String,
    tenant_id: String,
    user_id: String,
    name: String,
    status: String,
    image: Option<&'a Vec<(String, Json)>>,
    flavor: Option<&'a Vec<(String, Json)>>,
    addresses: Option<&'a Vec<(String, Json)>>,
    metadata: Vec<(String, String)>,
}

fn decode_server(s: &Json) -> Result<Server<'_>, String> {
    let ty = "Server";
    let metadata = match map_field(s, "metadata", ty)? {
        None => Vec::new(),
        Some(m) => m
            .iter()
            .map(|(k, v)| match v {
                Json::Null => Ok((k.clone(), String::new())),
                Json::Str(x) => Ok((k.clone(), x.clone())),
                _ => Err(format!("json: cannot unmarshal {} into Go struct field Server.metadata of type string", super::client::go_type_name(Some(v)).replace("float64", "number"))),
            })
            .collect::<Result<_, String>>()?,
    };
    let image = match field(s, "image") {
        Some(Json::Obj(m)) => Some(m),
        _ => None,
    };
    match field(s, "config_drive") {
        None | Some(Json::Null) | Some(Json::Bool(_)) => {}
        Some(Json::Str(t)) => {
            if !t.is_empty() && parse_bool(t).is_none() {
                return Err(format!("failed to parse ConfigDrive {}: strconv.ParseBool: parsing {}: invalid syntax", yaml::go_quote(t), yaml::go_quote(t)));
            }
        }
        Some(v) => return Err(format!("unknown type for ConfigDrive: {} (value: {})", super::client::go_type_name(Some(v)), v.stringify())),
    }
    Ok(Server {
        id: str_field(s, "id", ty)?,
        tenant_id: str_field(s, "tenant_id", ty)?,
        user_id: str_field(s, "user_id", ty)?,
        name: str_field(s, "name", ty)?,
        status: str_field(s, "status", ty)?,
        image,
        flavor: map_field(s, "flavor", ty)?,
        addresses: map_field(s, "addresses", ty)?,
        metadata,
    })
}

fn map_str<'a>(m: Option<&'a Vec<(String, Json)>>, k: &str) -> Option<&'a str> {
    m?.iter().rev().find(|(n, _)| n == k).and_then(|(_, v)| v.as_str())
}

impl Refresher for InstanceDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.provider.authenticate(ctx, &self.auth_opts).map_err(|e| format!("could not authenticate to OpenStack: {e}"))?;
        let client = self.provider.service_client(ctx, Service::Compute, &self.region, &self.availability).map_err(|e| format!("could not create OpenStack compute session: {e}"))?;
        let network = self.provider.service_client(ctx, Service::Network, &self.region, &self.availability).map_err(|e| format!("could not create OpenStack network session: {e}"))?;

        let ports = all_pages(&self.provider, ctx, &network.service_url(&["ports"]), &Resource { key: "ports", links_key: "ports_links", strict: true }).map_err(|e| format!("failed to list all ports: {e}"))?;
        let mut port_list: HashMap<String, String> = HashMap::new();
        for p in &ports {
            port_list.insert(str_field(p, "id", "Port").map_err(|e| format!("failed to list all ports: {e}"))?, str_field(p, "device_id", "Port").map_err(|e| format!("failed to list all ports: {e}"))?);
        }

        let mut floating_ip_list: HashMap<(String, String), String> = HashMap::new();
        let mut floating_ip_present: HashSet<String> = HashSet::new();
        let logger = self.logger.clone();
        each_page(&self.provider, ctx, &network.service_url(&["floatingips"]), &Resource { key: "floatingips", links_key: "floatingips_links", strict: false }, |items| {
            for ip in items {
                let ty = "FloatingIP";
                let (port_id, fixed, floating) = (str_field(ip, "port_id", ty)?, str_field(ip, "fixed_ip_address", ty)?, str_field(ip, "floating_ip_address", ty)?);
                if port_id.is_empty() || fixed.is_empty() {
                    continue;
                }
                let Some(device_id) = port_list.get(&port_id) else {
                    logger.warn("Floating IP PortID not found in portList", &[("PortID", &port_id)]);
                    continue;
                };
                floating_ip_list.insert((device_id.clone(), fixed), floating.clone());
                floating_ip_present.insert(floating);
            }
            Ok(true)
        })?;

        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: format!("OS_{}", self.region) };
        let mut url = client.service_url(&["servers", "detail"]);
        if self.all_tenants {
            url.push_str("?all_tenants=true");
        }
        let port = self.port.to_string();
        each_page(&self.provider, ctx, &url, &Resource { key: "servers", links_key: "servers_links", strict: true }, |items| {
            // Decode errors come from IsEmpty, before the handler, so they have no prefix.
            let mut servers = Vec::with_capacity(items.len());
            for s in items {
                servers.push(decode_server(s)?);
            }
            if ctx.is_cancelled() {
                return Err("could not extract instances: context canceled".to_string());
            }
            for s in servers {
                if s.addresses.is_none_or(|a| a.is_empty()) {
                    self.logger.info("Got no IP address", &[("instance", &s.id)]);
                    continue;
                }
                let mut labels = LabelSet::new();
                labels.insert(LABEL_INSTANCE_ID.to_string(), s.id.clone());
                labels.insert(LABEL_INSTANCE_STATUS.to_string(), s.status.clone());
                labels.insert(LABEL_INSTANCE_NAME.to_string(), s.name.clone());
                labels.insert(LABEL_PROJECT_ID.to_string(), s.tenant_id.clone());
                labels.insert(LABEL_USER_ID.to_string(), s.user_id.clone());
                // "original_name" is only available for microversion >= 2.47; "id" before.
                match map_str(s.flavor, "original_name") {
                    Some(name) => {
                        labels.insert(LABEL_INSTANCE_FLAVOR.to_string(), name.to_string());
                    }
                    None => match map_str(s.flavor, "id") {
                        Some(id) => {
                            labels.insert(LABEL_INSTANCE_FLAVOR.to_string(), id.to_string());
                        }
                        None => {
                            self.logger.warn("Invalid type for both flavor original_name and flavor id, expected string", &[]);
                            continue;
                        }
                    },
                }
                if let Some(id) = map_str(s.image, "id") {
                    labels.insert(LABEL_INSTANCE_IMAGE.to_string(), id.to_string());
                }
                for (k, v) in &s.metadata {
                    labels.insert(format!("{LABEL_TAG_PREFIX}{}", sanitize_label_name(k)), v.clone());
                }
                for (pool, address) in s.addresses.into_iter().flatten() {
                    let Json::Arr(md) = address else {
                        self.logger.warn("Invalid type for address, expected array", &[]);
                        continue;
                    };
                    if md.is_empty() {
                        self.logger.debug("Got no IP address", &[("instance", &s.id)]);
                        continue;
                    }
                    for address in md {
                        let Json::Obj(md1) = address else {
                            self.logger.warn("Invalid type for address, expected dict", &[]);
                            continue;
                        };
                        let Some(addr) = map_str(Some(md1), "addr") else {
                            self.logger.warn("Invalid type for address, expected string", &[]);
                            continue;
                        };
                        if floating_ip_present.contains(addr) {
                            continue;
                        }
                        let mut lbls = labels.clone();
                        lbls.insert(LABEL_ADDRESS_POOL.to_string(), pool.clone());
                        lbls.insert(LABEL_PRIVATE_IP.to_string(), addr.to_string());
                        if let Some(val) = floating_ip_list.get(&(s.id.clone(), addr.to_string())) {
                            lbls.insert(LABEL_PUBLIC_IP.to_string(), val.clone());
                        }
                        lbls.insert(ADDRESS_LABEL.to_string(), join_host_port(addr, &port));
                        tg.targets.push(lbls);
                    }
                }
            }
            Ok(true)
        })?;
        Ok(vec![Arc::new(tg)])
    }
}
