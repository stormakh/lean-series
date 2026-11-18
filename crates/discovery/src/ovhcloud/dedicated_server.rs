//! Port of `discovery/ovhcloud/dedicated_server.go`.
use std::sync::Arc;

use json::Json;
use model::labelset::LabelSet;

use super::{Addr, SdConfig, create_client, join_path, meta_label_prefix, parse_ip_list, pick_ips};
use crate::Config;
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::sd_json::{self, INT, INT64, STRINGS, StructTy, Ty};
use crate::targetgroup::Group;

/// Go `dedicatedServerAPIPath`.
pub const DEDICATED_SERVER_API_PATH: &str = "/dedicated/server";

/// Go `dedicatedServer`.
const DEDICATED_SERVER: Ty = Ty::Struct(&StructTy {
    go: "ovhcloud.dedicatedServer",
    fields: &[
        ("state", Ty::Str),
        ("commercialRange", Ty::Str),
        ("linkSpeed", INT),
        ("rack", Ty::Str),
        ("noIntervention", Ty::Bool),
        ("os", Ty::Str),
        ("supportLevel", Ty::Str),
        ("serverId", INT64),
        ("reverse", Ty::Str),
        ("datacenter", Ty::Str),
        ("name", Ty::Str),
    ],
});

/// Go `dedicatedServer` (the API object plus the IPs fetched separately).
#[derive(Clone, Debug, Default)]
pub struct DedicatedServer {
    pub state: String,
    pub ips: Vec<Addr>,
    pub commercial_range: String,
    pub link_speed: i64,
    pub rack: String,
    pub no_intervention: bool,
    pub os: String,
    pub support_level: String,
    pub server_id: i64,
    pub reverse: String,
    pub datacenter: String,
    pub name: String,
}

/// Go `dedicatedServerDiscovery`.
pub struct DedicatedServerDiscovery {
    config: SdConfig,
    logger: Logger,
}

/// Go `getDedicatedServerList`.
pub fn get_dedicated_server_list(client: &mut super::client::Client) -> Result<Vec<String>, String> {
    let v = client.get(DEDICATED_SERVER_API_PATH, &STRINGS)?;
    Ok(v.as_ref().and_then(Json::as_arr).map(|a| a.iter().map(|s| s.as_str().unwrap_or("").to_string()).collect()).unwrap_or_default())
}

/// Go `getDedicatedServerDetails`.
pub fn get_dedicated_server_details(client: &mut super::client::Client, server_name: &str) -> Result<DedicatedServer, String> {
    let escaped = yaml::url::query_escape(server_name);
    let v = client.get(&join_path(&[DEDICATED_SERVER_API_PATH, &escaped]), &DEDICATED_SERVER)?.unwrap_or_default();
    let ips = client.get(&join_path(&[DEDICATED_SERVER_API_PATH, &escaped, "ips"]), &STRINGS)?;
    let ips: Vec<&str> = ips.as_ref().and_then(Json::as_arr).map(|a| a.iter().map(|s| s.as_str().unwrap_or("")).collect()).unwrap_or_default();
    let parsed = parse_ip_list(&ips)?;
    Ok(DedicatedServer {
        state: sd_json::str_of(&v, "state"),
        ips: parsed,
        commercial_range: sd_json::str_of(&v, "commercialRange"),
        link_speed: sd_json::int_of(&v, "linkSpeed"),
        rack: sd_json::str_of(&v, "rack"),
        no_intervention: sd_json::bool_of(&v, "noIntervention"),
        os: sd_json::str_of(&v, "os"),
        support_level: sd_json::str_of(&v, "supportLevel"),
        server_id: sd_json::int_of(&v, "serverId"),
        reverse: sd_json::str_of(&v, "reverse"),
        datacenter: sd_json::str_of(&v, "datacenter"),
        name: sd_json::str_of(&v, "name"),
    })
}

impl DedicatedServerDiscovery {
    /// Go `newDedicatedServerDiscovery`.
    pub fn new(conf: &SdConfig, logger: Logger) -> DedicatedServerDiscovery {
        DedicatedServerDiscovery { config: conf.clone(), logger }
    }

    /// Go `getService`.
    pub fn get_service(&self) -> &'static str {
        "dedicated_server"
    }

    /// Go `getSource`.
    pub fn get_source(&self) -> String {
        format!("{}_{}", self.config.name(), self.get_service())
    }

    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut client = create_client(&self.config)?;
        let list = get_dedicated_server_list(&mut client)?;
        let mut detailed = Vec::new();
        for name in &list {
            match get_dedicated_server_details(&mut client, name) {
                Ok(s) => detailed.push(s),
                Err(e) => {
                    self.logger.warn(&format!("{}: Could not get details of {}", self.get_source(), name), &[("err", &e)]);
                }
            }
        }
        let p = format!("{}dedicated_server_", meta_label_prefix());
        let mut targets = Vec::with_capacity(detailed.len());
        for server in &detailed {
            let (ipv4, ipv6, default_ip) = pick_ips(&server.ips);
            let mut l = LabelSet::new();
            l.insert("__address__".to_string(), default_ip);
            l.insert("instance".to_string(), server.name.clone());
            let mut add = |k: &str, v: String| {
                l.insert(format!("{p}{k}"), v);
            };
            add("state", server.state.clone());
            add("commercial_range", server.commercial_range.clone());
            add("link_speed", server.link_speed.to_string());
            add("rack", server.rack.clone());
            add("no_intervention", server.no_intervention.to_string());
            add("os", server.os.clone());
            add("support_level", server.support_level.clone());
            add("server_id", server.server_id.to_string());
            add("reverse", server.reverse.clone());
            add("datacenter", server.datacenter.clone());
            add("name", server.name.clone());
            add("ipv4", ipv4);
            add("ipv6", ipv6);
            targets.push(l);
        }
        Ok(vec![Arc::new(Group { source: self.get_source(), targets, labels: LabelSet::new() })])
    }
}
