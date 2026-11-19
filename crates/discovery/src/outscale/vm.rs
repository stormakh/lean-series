//! Port of `discovery/outscale/vm.go` and the osc-sdk-go/v2 pieces it calls: `VmApi.ReadVms(ctx)
//! .ReadVmsRequest(...).Execute()` (POST `<endpoint>/ReadVms` with a JSON body, signed with the aws-sdk-go v1
//! SigV4 signer for service "oapi" and region "eu-west-2" as the SDK hard-codes, `GenericOpenAPIError` texts,
//! the JSON decoding of `ReadVmsResponse`) and the paging by `NextPageToken`.
//!
//! Differs from the SDK: XML responses are not decoded (an XML body is treated as an empty response); fields of
//! the SDK models that the SD does not read are not type-checked.
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use json::Json;
use model::labelset::LabelSet;

use super::SdConfig;
use crate::cancel::CancelToken;
use crate::refresh::Refresher;
use crate::sd_json::{self, StructTy, Ty};
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

pub const VM_LABEL_PREFIX: &str = "__meta_outscale_vm_";
pub const VM_LABEL_INSTANCE_ID: &str = "__meta_outscale_vm_instance_id";
pub const VM_LABEL_REGION: &str = "__meta_outscale_vm_region";
pub const VM_LABEL_SUBREGION: &str = "__meta_outscale_vm_subregion";
pub const VM_LABEL_STATE: &str = "__meta_outscale_vm_state";
pub const VM_LABEL_PRIVATE_IP: &str = "__meta_outscale_vm_private_ip";
pub const VM_LABEL_PUBLIC_IP: &str = "__meta_outscale_vm_public_ip";
pub const VM_LABEL_TAG: &str = "__meta_outscale_vm_tag_";

/// The service and region osc-sdk-go signs every request with.
const SIGNING_SERVICE: &str = "oapi";
const SIGNING_REGION: &str = "eu-west-2";

/// Go `osc.Placement`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Placement {
    pub subregion_name: Option<String>,
    pub tenancy: Option<String>,
}

/// Go `osc.ResourceTag`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceTag {
    pub key: String,
    pub value: String,
}

/// Go `osc.Vm` (the fields the SD reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Vm {
    pub placement: Option<Placement>,
    pub private_ip: Option<String>,
    pub public_ip: Option<String>,
    pub state: Option<String>,
    pub tags: Option<Vec<ResourceTag>>,
    pub vm_id: Option<String>,
}

const RESOURCE_TAG: StructTy = StructTy { go: "osc.ResourceTag", fields: &[("Key", Ty::Str), ("Value", Ty::Str)] };
const PLACEMENT: Ty = Ty::Struct(&StructTy { go: "osc.Placement", fields: &[("SubregionName", Ty::Str), ("Tenancy", Ty::Str)] });
const VM: StructTy = StructTy {
    go: "osc.Vm",
    fields: &[("Placement", PLACEMENT), ("PrivateIp", Ty::Str), ("PublicIp", Ty::Str), ("State", Ty::Str), ("Tags", Ty::Slice(&Ty::Struct(&RESOURCE_TAG))), ("VmId", Ty::Str)],
};
const RESPONSE_CONTEXT: Ty = Ty::Struct(&StructTy { go: "osc.ResponseContext", fields: &[("RequestId", Ty::Str)] });
/// Go `osc.ReadVmsResponse`.
const READ_VMS_RESPONSE: Ty = Ty::Struct(&StructTy { go: "osc.ReadVmsResponse", fields: &[("NextPageToken", Ty::Str), ("ResponseContext", RESPONSE_CONTEXT), ("Vms", Ty::Slice(&Ty::Struct(&VM)))] });
/// Go `osc.ErrorResponse`.
const ERROR_RESPONSE: Ty = Ty::Struct(&StructTy {
    go: "osc.ErrorResponse",
    fields: &[("Errors", Ty::Slice(&Ty::Struct(&StructTy { go: "osc.Errors", fields: &[("Code", Ty::Str), ("Details", Ty::Str), ("Type", Ty::Str)] }))), ("ResponseContext", RESPONSE_CONTEXT)],
});

fn vm_from_json(v: &Json) -> Vm {
    Vm {
        placement: sd_json::ptr(v, "Placement").map(|p| Placement { subregion_name: sd_json::opt_str(p, "SubregionName"), tenancy: sd_json::opt_str(p, "Tenancy") }),
        private_ip: sd_json::opt_str(v, "PrivateIp"),
        public_ip: sd_json::opt_str(v, "PublicIp"),
        state: sd_json::opt_str(v, "State"),
        tags: sd_json::ptr(v, "Tags").and_then(Json::as_arr).map(|a| a.iter().map(|t| ResourceTag { key: sd_json::str_of(t, "Key"), value: sd_json::str_of(t, "Value") }).collect()),
        vm_id: sd_json::opt_str(v, "VmId"),
    }
}

/// Go `vmDiscovery`.
pub struct VmDiscovery {
    http: net::Client,
    cfg: SdConfig,
}

/// Go `newVMDiscovery` (with `loadClient`).
pub fn new_vm_discovery(conf: &SdConfig) -> Result<VmDiscovery, String> {
    let rt = net::new_round_tripper_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| format!("outscale SD client config: {e}"))?;
    let mut http = net::Client::new(rt);
    http.timeout = Some(conf.refresh_interval.to_std());
    Ok(VmDiscovery { http, cfg: conf.clone() })
}

/// aws-sdk-go `rest.EscapePath(path, false)`.
fn escape_path(p: &str) -> String {
    let mut o = String::with_capacity(p.len());
    for &c in p.as_bytes() {
        if c.is_ascii_alphanumeric() || b"-_.~/".contains(&c) {
            o.push(c as char);
        } else {
            o.push_str(&format!("%{c:02X}"));
        }
    }
    o
}

/// aws-sdk-go `stripExcessSpaces` for one header line.
fn strip_excess_spaces(s: &str) -> String {
    let t = s.trim_matches(' ');
    let mut o = String::with_capacity(t.len());
    let mut prev_space = false;
    for c in t.chars() {
        if c == ' ' && prev_space {
            continue;
        }
        prev_space = c == ' ';
        o.push(c);
    }
    o
}

/// aws-sdk-go v1 `v4.Signer.Sign(req, body, service, region, time)`: sets X-Amz-Date and Authorization.
pub fn sign_v4(req: &mut net::Request, access_key: &str, secret_key: &str, service: &str, region: &str, unix_secs: u64) {
    // request.SanitizeHostForHeader: a default port is dropped from the Host.
    let mut host = if req.host.is_empty() { req.url.host.clone() } else { req.host.clone() };
    if let Some(i) = host.rfind(':')
        && !host[i..].contains(']')
    {
        let port = &host[i + 1..];
        if (req.url.scheme == "http" && port == "80") || (req.url.scheme == "https" && port == "443") {
            host.truncate(i);
            req.host = host.clone();
        }
    }
    let date = net::sigv4::amz_date(unix_secs);
    req.header.set("X-Amz-Date", &date);
    let scope = format!("{}/{}/{}/aws4_request", &date[..8], region, service);
    let mut names = vec!["host".to_string()];
    let mut values: Vec<(String, Vec<String>)> = Vec::new();
    for (k, v) in req.header.iter() {
        if ["Authorization", "User-Agent", "X-Amzn-Trace-Id"].contains(&k) {
            continue;
        }
        let lk = k.to_lowercase();
        if let Some((_, vs)) = values.iter_mut().find(|(n, _)| *n == lk) {
            vs.push(v.to_string());
            continue;
        }
        names.push(lk.clone());
        values.push((lk, vec![v.to_string()]));
    }
    names.sort();
    let signed_headers = names.join(";");
    let items: Vec<String> = names
        .iter()
        .map(|n| {
            if n == "host" {
                strip_excess_spaces(&format!("host:{host}"))
            } else {
                let vs = &values.iter().find(|(k, _)| k == n).unwrap().1;
                strip_excess_spaces(&format!("{n}:{}", vs.iter().map(|v| v.trim()).collect::<Vec<_>>().join(",")))
            }
        })
        .collect();
    let (mut q, _) = yaml::url::parse_query(&req.url.raw_query);
    q.sort_by(|a, b| a.0.cmp(&b.0));
    let query = yaml::url::encode_query(&q).replace('+', "%20");
    req.url.raw_query = query.clone();
    let uri = {
        let p = req.url.escaped_path();
        escape_path(if p.is_empty() { "/" } else { &p })
    };
    let body_digest = crypto::hex(&crypto::sha256(&req.body));
    let canonical = [req.method.as_str(), &uri, &query, &(items.join("\n") + "\n"), &signed_headers, &body_digest].join("\n");
    let to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, crypto::hex(&crypto::sha256(canonical.as_bytes())));
    let k_date = crypto::hmac_sha256(format!("AWS4{secret_key}").as_bytes(), &date.as_bytes()[..8]);
    let k_region = crypto::hmac_sha256(&k_date, region.as_bytes());
    let k_service = crypto::hmac_sha256(&k_region, service.as_bytes());
    let k_signing = crypto::hmac_sha256(&k_service, b"aws4_request");
    let sig = crypto::hex(&crypto::hmac_sha256(&k_signing, to_sign.as_bytes()));
    req.header.set("Authorization", &format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={sig}"));
}

/// osc-sdk-go `decode` for a model type.
fn decode(body: &[u8], content_type: &str, ty: &Ty) -> Result<Json, String> {
    if body.is_empty() || crate::ionos::server::xml_check(content_type) {
        return Ok(Json::Obj(Vec::new()));
    }
    if crate::ionos::server::json_check(content_type) {
        return sd_json::unmarshal(body, ty);
    }
    Err("undefined response type".to_string())
}

impl VmDiscovery {
    /// `ReadVms(ctx).ReadVmsRequest(osc.ReadVmsRequest{NextPageToken: token}).Execute()`.
    fn read_vms(&self, access_key: &str, secret_key: &str, next_page_token: Option<&str>) -> Result<Json, String> {
        let mut body = Json::obj();
        if let Some(t) = next_page_token {
            body.set("NextPageToken", t);
        }
        // json.NewEncoder(buf).Encode adds a newline.
        let body = format!("{}\n", body.stringify()).into_bytes();
        let path = format!("{}/ReadVms", self.cfg.endpoint);
        let u = yaml::url::parse(&path)?;
        let mut req = net::Request::new("POST", &u.to_url_string(), body).map_err(|e| e.to_string())?;
        req.header.set("Content-Type", "application/json");
        req.header.set("Accept", "application/json");
        req.header.add("User-Agent", crate::http::USER_AGENT);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        sign_v4(&mut req, access_key, secret_key, SIGNING_SERVICE, SIGNING_REGION, now);
        let resp = self.http.do_request(req).map_err(|e| e.to_string())?;
        let ct = resp.header.get_str("Content-Type");
        if resp.status >= 300 {
            if matches!(resp.status, 400 | 401 | 500)
                && let Err(e) = decode(&resp.body, ct, &ERROR_RESPONSE)
            {
                return Err(e);
            }
            return Err(resp.status_text.clone());
        }
        decode(&resp.body, ct, &READ_VMS_RESPONSE)
    }

    /// Go `vmDiscovery.refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let secret = self.cfg.secret_key_value()?;
        let mut all = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let resp = self.read_vms(&self.cfg.access_key, &secret.0, token.as_deref()).map_err(|e| format!("outscale ReadVms: {e}"))?;
            if let Some(vms) = sd_json::ptr(&resp, "Vms").and_then(Json::as_arr) {
                all.extend(vms.iter().map(vm_from_json));
            }
            match sd_json::opt_str(&resp, "NextPageToken") {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => break,
            }
        }
        let targets = vms_to_label_sets(&all, &self.cfg);
        Ok(vec![Arc::new(Group { source: self.cfg.region.clone(), targets, labels: LabelSet::new() })])
    }
}

impl Refresher for VmDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        VmDiscovery::refresh(self, ctx)
    }
}

/// Go `vmsToLabelSets`.
pub fn vms_to_label_sets(vms: &[Vm], cfg: &SdConfig) -> Vec<LabelSet> {
    let mut out = Vec::new();
    for vm in vms {
        let addr = match (vm.private_ip.as_deref(), vm.public_ip.as_deref()) {
            (Some(ip), _) if !ip.is_empty() => join_host_port(ip, cfg.port),
            (_, Some(ip)) if !ip.is_empty() => join_host_port(ip, cfg.port),
            _ => continue,
        };
        let mut labels = LabelSet::new();
        labels.insert("__address__".to_string(), addr);
        labels.insert(VM_LABEL_INSTANCE_ID.to_string(), vm.vm_id.clone().unwrap_or_default());
        labels.insert(VM_LABEL_REGION.to_string(), cfg.region.clone());
        labels.insert(VM_LABEL_STATE.to_string(), vm.state.clone().unwrap_or_default());
        if let Some(sr) = vm.placement.as_ref().and_then(|p| p.subregion_name.clone()) {
            labels.insert(VM_LABEL_SUBREGION.to_string(), sr);
        }
        if let Some(ip) = &vm.private_ip {
            labels.insert(VM_LABEL_PRIVATE_IP.to_string(), ip.clone());
        }
        if let Some(ip) = &vm.public_ip {
            labels.insert(VM_LABEL_PUBLIC_IP.to_string(), ip.clone());
        }
        for t in vm.tags.iter().flatten() {
            if t.key.is_empty() || t.value.is_empty() {
                continue;
            }
            labels.insert(format!("{VM_LABEL_TAG}{}", sanitize_label_name(&t.key)), t.value.clone());
        }
        out.push(labels);
    }
    out
}
