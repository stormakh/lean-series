//! Port of `discovery/ovhcloud/vps.go`.
use std::sync::Arc;

use json::Json;
use model::labelset::LabelSet;

use super::{Addr, SdConfig, create_client, join_path, meta_label_prefix, parse_ip_list, pick_ips};
use crate::Config;
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::sd_json::{self, INT, STRINGS, StructTy, Ty};
use crate::targetgroup::Group;

/// Go `vpsAPIPath`.
pub const VPS_API_PATH: &str = "/vps";

/// Go `vpsModel`.
const VPS_MODEL: StructTy = StructTy {
    go: "ovhcloud.vpsModel",
    fields: &[
        ("maximumAdditionnalIp", INT),
        ("offer", Ty::Str),
        ("datacenter", STRINGS),
        ("vcore", INT),
        ("version", Ty::Str),
        ("name", Ty::Str),
        ("disk", INT),
        ("memory", INT),
    ],
};

/// Go `virtualPrivateServer`.
const VIRTUAL_PRIVATE_SERVER: Ty = Ty::Struct(&StructTy {
    go: "ovhcloud.virtualPrivateServer",
    fields: &[
        ("keymap", STRINGS),
        ("zone", Ty::Str),
        ("model", Ty::Struct(&VPS_MODEL)),
        ("displayName", Ty::Str),
        ("monitoringIpBlocks", STRINGS),
        ("cluster", Ty::Str),
        ("state", Ty::Str),
        ("name", Ty::Str),
        ("netbootMode", Ty::Str),
        ("memoryLimit", INT),
        ("offerType", Ty::Str),
        ("vcore", INT),
    ],
});

/// Go `vpsModel`.
#[derive(Clone, Debug, Default)]
pub struct VpsModel {
    pub maximum_additional_ip: i64,
    pub offer: String,
    pub datacenter: Vec<String>,
    pub vcore: i64,
    pub version: String,
    pub name: String,
    pub disk: i64,
    pub memory: i64,
}

/// Go `virtualPrivateServer` (the API object plus the IPs fetched separately).
#[derive(Clone, Debug, Default)]
pub struct VirtualPrivateServer {
    pub ips: Vec<Addr>,
    pub keymap: Vec<String>,
    pub zone: String,
    pub model: VpsModel,
    pub display_name: String,
    pub monitoring_ip_blocks: Vec<String>,
    pub cluster: String,
    pub state: String,
    pub name: String,
    pub netboot_mode: String,
    pub memory_limit: i64,
    pub offer_type: String,
    pub vcore: i64,
}

/// Go `vpsDiscovery`.
pub struct VpsDiscovery {
    config: SdConfig,
    logger: Logger,
}

/// Go `getVpsDetails`.
pub fn get_vps_details(client: &mut super::client::Client, vps_name: &str) -> Result<VirtualPrivateServer, String> {
    let name_path = join_path(&[VPS_API_PATH, &yaml::url::query_escape(vps_name)]);
    let v = client.get(&name_path, &VIRTUAL_PRIVATE_SERVER)?.unwrap_or_default();
    let ips = client.get(&join_path(&[&name_path, "ips"]), &STRINGS)?;
    let ips: Vec<&str> = ips.as_ref().and_then(Json::as_arr).map(|a| a.iter().map(|s| s.as_str().unwrap_or("")).collect()).unwrap_or_default();
    let parsed = parse_ip_list(&ips)?;
    let m = sd_json::val(&v, "model").cloned().unwrap_or_default();
    Ok(VirtualPrivateServer {
        ips: parsed,
        keymap: sd_json::strings_of(&v, "keymap"),
        zone: sd_json::str_of(&v, "zone"),
        model: VpsModel {
            maximum_additional_ip: sd_json::int_of(&m, "maximumAdditionnalIp"),
            offer: sd_json::str_of(&m, "offer"),
            datacenter: sd_json::strings_of(&m, "datacenter"),
            vcore: sd_json::int_of(&m, "vcore"),
            version: sd_json::str_of(&m, "version"),
            name: sd_json::str_of(&m, "name"),
            disk: sd_json::int_of(&m, "disk"),
            memory: sd_json::int_of(&m, "memory"),
        },
        display_name: sd_json::str_of(&v, "displayName"),
        monitoring_ip_blocks: sd_json::strings_of(&v, "monitoringIpBlocks"),
        cluster: sd_json::str_of(&v, "cluster"),
        state: sd_json::str_of(&v, "state"),
        name: sd_json::str_of(&v, "name"),
        netboot_mode: sd_json::str_of(&v, "netbootMode"),
        memory_limit: sd_json::int_of(&v, "memoryLimit"),
        offer_type: sd_json::str_of(&v, "offerType"),
        vcore: sd_json::int_of(&v, "vcore"),
    })
}

/// Go `getVpsList`.
pub fn get_vps_list(client: &mut super::client::Client) -> Result<Vec<String>, String> {
    let v = client.get(VPS_API_PATH, &STRINGS)?;
    Ok(v.as_ref().and_then(Json::as_arr).map(|a| a.iter().map(|s| s.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default())
}

/// Go `fmt.Sprintf("%+v", []string)`: "[a b c]".
fn go_fmt_strings(v: &[String]) -> String {
    format!("[{}]", v.join(" "))
}

impl VpsDiscovery {
    /// Go `newVpsDiscovery`.
    pub fn new(conf: &SdConfig, logger: Logger) -> VpsDiscovery {
        VpsDiscovery { config: conf.clone(), logger }
    }

    /// Go `getService`.
    pub fn get_service(&self) -> &'static str {
        "vps"
    }

    /// Go `getSource`.
    pub fn get_source(&self) -> String {
        format!("{}_{}", self.config.name(), self.get_service())
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut client = create_client(&self.config)?;
        let list = get_vps_list(&mut client)?;
        let mut detailed = Vec::new();
        for name in &list {
            match get_vps_details(&mut client, name) {
                Ok(s) => detailed.push(s),
                Err(e) => {
                    self.logger.warn(&format!("{}: Could not get details of {}", self.get_source(), name), &[("err", &e)]);
                }
            }
        }
        let p = format!("{}vps_", meta_label_prefix());
        let mut targets = Vec::with_capacity(detailed.len());
        for server in &detailed {
            let (ipv4, ipv6, default_ip) = pick_ips(&server.ips);
            let mut l = LabelSet::new();
            l.insert("__address__".to_string(), default_ip);
            l.insert("instance".to_string(), server.name.clone());
            let mut add = |k: &str, v: String| {
                l.insert(format!("{p}{k}"), v);
            };
            add("offer", server.model.offer.clone());
            add("datacenter", go_fmt_strings(&server.model.datacenter));
            add("model_vcore", server.model.vcore.to_string());
            add("maximum_additional_ip", server.model.maximum_additional_ip.to_string());
            add("version", server.model.version.clone());
            add("model_name", server.model.name.clone());
            add("disk", server.model.disk.to_string());
            add("memory", server.model.memory.to_string());
            add("zone", server.zone.clone());
            add("display_name", server.display_name.clone());
            add("cluster", server.cluster.clone());
            add("state", server.state.clone());
            add("name", server.name.clone());
            add("netboot_mode", server.netboot_mode.clone());
            add("memory_limit", server.memory_limit.to_string());
            add("offer_type", server.offer_type.clone());
            add("vcore", server.vcore.to_string());
            add("ipv4", ipv4);
            add("ipv6", ipv6);
            targets.push(l);
        }
        Ok(vec![Arc::new(Group { source: self.get_source(), targets, labels: LabelSet::new() })])
    }
}
