//! Port of `discovery/stackit/server.go` and `types.go`: `GET <endpoint>/v1/projects/<project>/servers?details=true`.
use std::sync::Arc;

use json::Json;
use model::labelset::LabelSet;

use super::auth::{self, Configuration};
use super::{SdConfig, label_prefix};
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::refresh::Refresher;
use crate::sd_json::{self, StructTy, Ty};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

/// Go `stackitAPIEndpoint`.
pub fn stackit_api_endpoint(region: &str) -> String {
    format!("https://iaas.api.{region}.stackit.cloud")
}

const SERVER_NETWORK: StructTy = StructTy { go: "stackit.ServerNetwork", fields: &[("networkName", Ty::Str), ("ipv4", Ty::Str), ("publicIp", Ty::Str)] };
const SERVER: StructTy = StructTy {
    go: "stackit.Server",
    fields: &[
        ("availabilityZone", Ty::Str),
        ("id", Ty::Str),
        ("labels", Ty::Map(&Ty::Any)),
        ("machineType", Ty::Str),
        ("name", Ty::Str),
        ("nics", Ty::Slice(&Ty::Struct(&SERVER_NETWORK))),
        ("powerStatus", Ty::Str),
        ("status", Ty::Str),
    ],
};
/// Go `ServerListResponse`.
const SERVER_LIST_RESPONSE: Ty = Ty::Struct(&StructTy { go: "stackit.ServerListResponse", fields: &[("items", Ty::Slice(&Ty::Struct(&SERVER)))] });

/// Go `iaasDiscovery`.
pub struct IaasDiscovery {
    http: net::Client,
    logger: Logger,
    api_endpoint: String,
    project: String,
    port: i64,
}

/// Go `newServerDiscovery`.
pub fn new_server_discovery(conf: &SdConfig, logger: Logger) -> Result<IaasDiscovery, String> {
    let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
    let api_endpoint = if conf.endpoint.is_empty() { stackit_api_endpoint(&conf.region) } else { conf.endpoint.clone() };
    let mut cfg = Configuration {
        no_auth: conf.service_account_key.0.is_empty() && conf.service_account_key_path.is_empty(),
        service_account_key: conf.service_account_key.0.clone(),
        private_key: conf.private_key.0.clone(),
        service_account_key_path: conf.service_account_key_path.clone(),
        private_key_path: conf.private_key_path.clone(),
        credentials_file_path: conf.credentials_file_path.clone(),
        token_custom_url: conf.token_url.clone(),
    };
    let auth_rt = auth::setup_auth(&mut cfg, rt).map_err(|e| format!("setting up authentication: {e}"))?;
    let mut http = net::Client::new(auth_rt);
    http.timeout = Some(conf.refresh_interval.to_std());
    Ok(IaasDiscovery { http, logger, api_endpoint, project: conf.project.clone(), port: conf.port })
}

/// Go `path.Join`.
fn path_join(elems: &[&str]) -> String {
    let joined = elems.iter().filter(|e| !e.is_empty()).copied().collect::<Vec<_>>().join("/");
    if joined.is_empty() { String::new() } else { crate::ovhcloud::clean_path(&joined) }
}

/// Go `url.JoinPath(base, elem...)` where `base` is a URL path.
pub fn url_join_path(base: &str, elems: &[&str]) -> Result<String, String> {
    let u = yaml::url::parse(base)?;
    let first = u.escaped_path();
    let mut all: Vec<&str> = Vec::with_capacity(elems.len() + 1);
    let first_rooted = if first.starts_with('/') { first.clone() } else { format!("/{first}") };
    all.push(&first_rooted);
    all.extend_from_slice(elems);
    let mut p = path_join(&all);
    if !first.starts_with('/') {
        p = p[1..].to_string();
    }
    let last = elems.last().copied().unwrap_or(first.as_str());
    if last.ends_with('/') && !p.ends_with('/') {
        p.push('/');
    }
    // setPath + String: the path as Go escapes it
    let mut out = u;
    out.path = match yaml::url::parse(&p) {
        Ok(pu) => {
            out.raw_path = pu.raw_path;
            pu.path
        }
        Err(_) => {
            out.raw_path = String::new();
            p
        }
    };
    Ok(out.to_url_string())
}

impl IaasDiscovery {
    /// Go `iaasDiscovery.refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut api_url = yaml::url::parse(&self.api_endpoint).map_err(|e| format!("invalid API endpoint URL {}: {}", self.api_endpoint, e))?;
        api_url.path = url_join_path(&api_url.path, &["v1", "projects", &self.project, "servers"]).map_err(|e| format!("joining URL path: {e}"))?;
        let mut q = api_url.query_pairs();
        q.retain(|(k, _)| k != "details");
        q.push(("details".to_string(), "true".to_string()));
        api_url.raw_query = yaml::url::encode_query(&q);
        let mut req = net::Request::new("GET", &api_url.to_url_string(), Vec::new()).map_err(|e| format!("creating request: {e}"))?;
        req.header.set("Accept", "application/json");
        let res = self.http.do_request(req).map_err(|e| format!("sending request: {e}"))?;
        if res.status != 200 {
            return Err(format!("unexpected status code {}: {}", res.status, String::from_utf8_lossy(&res.body)));
        }
        let v = sd_json::decode_stream(&res.body, &SERVER_LIST_RESPONSE).map_err(|e| format!("decoding response: {e}"))?;
        let p = label_prefix();
        let items: &[Json] = sd_json::ptr(&v, "items").and_then(Json::as_arr).map(|a| a.as_slice()).unwrap_or(&[]);
        if items.is_empty() {
            return Ok(vec![Arc::new(Group { source: "stackit".to_string(), targets: Vec::new(), labels: LabelSet::new() })]);
        }
        let mut targets = Vec::with_capacity(items.len());
        for server in items {
            let id = sd_json::str_of(server, "id");
            let Some(nics) = sd_json::ptr(server, "nics").and_then(Json::as_arr) else {
                self.logger.debug("server has no network interfaces. Skipping", &[("server_id", &id)]);
                continue;
            };
            let mut labels = LabelSet::new();
            labels.insert(format!("{p}project"), self.project.clone());
            labels.insert(format!("{p}id"), id.clone());
            labels.insert(format!("{p}name"), sd_json::str_of(server, "name"));
            labels.insert(format!("{p}availability_zone"), sd_json::str_of(server, "availabilityZone"));
            labels.insert(format!("{p}status"), sd_json::str_of(server, "status"));
            labels.insert(format!("{p}power_status"), sd_json::str_of(server, "powerStatus"));
            labels.insert(format!("{p}type"), sd_json::str_of(server, "machineType"));
            let mut address = String::new();
            let mut public_ip = String::new();
            for nic in nics {
                if let Some(ip) = sd_json::opt_str(nic, "publicIp")
                    && !ip.is_empty()
                    && public_ip.is_empty()
                {
                    public_ip = ip.clone();
                    address = ip;
                }
                if let Some(ip) = sd_json::opt_str(nic, "ipv4")
                    && !ip.is_empty()
                {
                    labels.insert(format!("{p}private_ipv4_{}", sanitize_label_name(&sd_json::str_of(nic, "networkName"))), ip.clone());
                    if address.is_empty() {
                        address = ip;
                    }
                }
            }
            // Servers without IPs are skipped.
            if address.is_empty() {
                continue;
            }
            if !public_ip.is_empty() {
                labels.insert(format!("{p}public_ipv4"), public_ip);
            }
            labels.insert("__address__".to_string(), join_host_port(&address, self.port as u64));
            if let Some(Json::Obj(server_labels)) = sd_json::ptr(server, "labels") {
                for (k, v) in server_labels {
                    if let Json::Str(s) = v {
                        let name = sanitize_label_name(k);
                        labels.insert(format!("{p}labelpresent_{name}"), "true".to_string());
                        labels.insert(format!("{p}label_{name}"), s.clone());
                    }
                }
            }
            targets.push(labels);
        }
        Ok(vec![Arc::new(Group { source: "stackit".to_string(), targets, labels: LabelSet::new() })])
    }
}

impl Refresher for IaasDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        IaasDiscovery::refresh(self, ctx)
    }
}
