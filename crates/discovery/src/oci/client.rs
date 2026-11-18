//! The part of oci-go-sdk v65.127.0 that OCI SD uses, over blocking HTTPS + JSON: region endpoints
//! (`common/common.go`, `common/regions.go`), the request signer (`common/http_signer.go`), request building and
//! the default retry policy (`common/http.go`, `common/client.go`, `common/retry.go`), service errors
//! (`common/errors.go`) and the four operations Identity ListCompartments, Compute ListInstances and
//! ListVnicAttachments, VirtualNetwork GetVnic with their JSON models.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use json::Json;
use ring::signature::RsaKeyPair;

use super::regions;
use crate::cancel::CancelToken;

/// Go `common.Version()`.
pub const SDK_VERSION: &str = "65.127.0";
/// The API version path prefix of the Identity, Compute and VirtualNetwork clients.
pub const BASE_PATH: &str = "20160918";
/// Go `context.Canceled.Error()`.
pub const CONTEXT_CANCELED: &str = "context canceled";

// ---------------- regions and endpoints ----------------

fn lookup<'a>(t: &'a [(&'a str, &'a str)], k: &str) -> Option<&'a str> {
    t.iter().find(|(a, _)| *a == k).map(|(_, b)| *b)
}

/// Region metadata added at runtime (Go `addRegionSchema`): from `~/.oci/regions-config.json` or the
/// `OCI_REGION_METADATA` environment variable.
#[derive(Clone, Debug, Default)]
struct RegionSchema {
    region_identifier: String,
    realm_key: String,
    realm_domain_component: String,
    region_key: String,
}

fn schema_from_json(v: &Json) -> Option<RegionSchema> {
    let get = |k: &str| v.get(k).and_then(|x| x.as_str()).filter(|s| !s.is_empty()).map(|s| s.to_lowercase());
    Some(RegionSchema {
        region_identifier: get("regionIdentifier")?,
        realm_key: get("realmKey")?,
        realm_domain_component: get("realmDomainComponent")?,
        region_key: get("regionKey")?,
    })
}

fn home_dir() -> String {
    std::env::var("HOME").unwrap_or_default()
}

/// Go `checkAndAddRegionMetadata`: the region config file first, then the environment variable.
fn region_metadata(region: &str) -> Option<RegionSchema> {
    let file = format!("{}/.oci/regions-config.json", home_dir());
    if let Ok(b) = std::fs::read(&file)
        && let Ok(Json::Arr(items)) = Json::parse_bytes(&b)
    {
        for it in &items {
            if let Some(s) = schema_from_json(it)
                && (s.region_key == region || s.region_identifier == region)
            {
                return Some(s);
            }
        }
    }
    if let Ok(v) = std::env::var("OCI_REGION_METADATA")
        && let Ok(j) = Json::parse(&v)
        && let Some(s) = schema_from_json(&j)
        && (s.region_key == region || s.region_identifier == region)
    {
        return Some(s);
    }
    None
}

/// A resolved region (Go `common.Region` after `StringToRegion`) with its second level domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Region {
    pub name: String,
    pub second_level_domain: String,
}

/// Go `common.StringToRegion` followed by `Region.SecondLevelDomain`.
pub fn string_to_region(s: &str) -> Region {
    let lower = s.to_lowercase();
    let mut name = match lookup(regions::SHORT_NAME_REGION, &lower) {
        Some(r) => r.to_string(),
        None if lookup(regions::REGION_REALM, &lower).is_some() => lower.clone(),
        None => s.to_string(),
    };
    let mut domain = lookup(regions::REGION_REALM, &name).and_then(|realm| lookup(regions::REALM, realm)).map(|d| d.to_string());
    if domain.is_none()
        && let Some(m) = region_metadata(s)
    {
        name = m.region_identifier.clone();
        domain = Some(lookup(regions::REALM, &m.realm_key).map(|d| d.to_string()).unwrap_or(m.realm_domain_component));
    }
    let second_level_domain = domain.unwrap_or_else(|| std::env::var("OCI_DEFAULT_REALM").unwrap_or_else(|_| "oraclecloud.com".to_string()));
    Region { name, second_level_domain }
}

/// The service endpoints (scheme and host, no path) the discoverer calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoints {
    /// Identity (ListCompartments), for example `https://identity.us-ashburn-1.oci.oraclecloud.com`.
    pub identity: String,
    /// Compute and VirtualNetwork, for example `https://iaas.us-ashburn-1.oraclecloud.com`.
    pub iaas: String,
}

impl Endpoints {
    /// The endpoints oci-go-sdk builds for `region` (`IdentityClient.SetRegion`, `ComputeClient.SetRegion`,
    /// `VirtualNetworkClient.SetRegion`, then `UpdateEndpointTemplateForOptions` and
    /// `SetMissingTemplateParams` at call time).
    pub fn for_region(region: &str) -> Endpoints {
        let r = string_to_region(region);
        if r.name.contains('.') {
            // EndpointForTemplateDottedRegion: "https://{endpoint_service_name}.{region}"
            return Endpoints { identity: format!("https://identity.{}", r.name), iaas: format!("https://iaas.{}", r.name) };
        }
        let dual = std::env::var("OCI_DUAL_STACK_ENDPOINT_ENABLED").map(|v| v.to_lowercase() == "true").unwrap_or(false);
        let ds = if dual { "ds.oci." } else { "" };
        Endpoints {
            identity: format!("https://identity.{}.oci.{}", r.name, r.second_level_domain),
            iaas: format!("https://iaas.{}.{}{}", r.name, ds, r.second_level_domain),
        }
    }
}

// ---------------- rate limiter ----------------

/// Go `rate.Limiter` (token bucket, starts full) with `Wait(ctx)`.
pub struct Limiter {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl Limiter {
    pub fn new(rate: f64, burst: usize) -> Limiter {
        Limiter { rate, burst: burst as f64, state: Mutex::new((burst as f64, Instant::now())) }
    }

    /// Go `Limiter.Wait`: takes one token, waiting for it; fails with "context canceled" when `ctx` is.
    pub fn wait(&self, ctx: &CancelToken) -> Result<(), String> {
        if ctx.is_cancelled() {
            return Err(CONTEXT_CANCELED.to_string());
        }
        let delay = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let tokens = (s.0 + now.duration_since(s.1).as_secs_f64() * self.rate).min(self.burst) - 1.0;
            *s = (tokens, now);
            if tokens >= 0.0 { Duration::ZERO } else { Duration::from_secs_f64(-tokens / self.rate) }
        };
        if !delay.is_zero() && ctx.wait_timeout(delay) {
            // Go cancels the reservation: give the token back.
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            s.0 = (s.0 + 1.0).min(self.burst);
            return Err(CONTEXT_CANCELED.to_string());
        }
        Ok(())
    }
}

// ---------------- signer ----------------

/// The API key signer (Go `ociRequestSigner` with the default headers and a raw configuration provider).
pub struct Signer {
    /// "tenancy/user/fingerprint"
    pub key_id: String,
    pub key: RsaKeyPair,
}

const GENERIC_HEADERS: [&str; 3] = ["date", "(request-target)", "host"];
const BODY_HEADERS: [&str; 3] = ["content-length", "content-type", "x-content-sha256"];

impl Signer {
    /// Go `ociRequestSigner.Sign`: sets `Authorization` (and for POST, PUT and PATCH `X-Content-SHA256` and
    /// `Content-Length`).
    pub fn sign(&self, req: &mut net::Request) -> Result<(), String> {
        let hash_body = matches!(req.method.as_str(), "POST" | "PUT" | "PATCH");
        let mut headers: Vec<&str> = GENERIC_HEADERS.to_vec();
        if hash_body {
            let h = crypto::base64(&crypto::sha256(&req.body));
            req.header.set("X-Content-SHA256", &h);
            req.header.set("Content-Length", &req.body.len().to_string());
            headers.extend(BODY_HEADERS);
        }
        let signing_string = signing_string(req, &headers);
        let rng = ring::rand::SystemRandom::new();
        let mut sig = vec![0u8; self.key.public().modulus_len()];
        self.key
            .sign(&ring::signature::RSA_PKCS1_SHA256, &rng, signing_string.as_bytes(), &mut sig)
            .map_err(|e| format!("can not compute signature while signing the request {e}: "))?;
        let auth = format!(
            "Signature version=\"1\",headers=\"{}\",keyId=\"{}\",algorithm=\"rsa-sha256\",signature=\"{}\"",
            headers.join(" "),
            self.key_id,
            crypto::base64(&sig)
        );
        req.header.set("Authorization", &auth);
        Ok(())
    }
}

/// Go `ociRequestSigner.getSigningString`.
pub fn signing_string(req: &net::Request, headers: &[&str]) -> String {
    let parts: Vec<String> = headers
        .iter()
        .map(|h| {
            let v = match *h {
                "(request-target)" => format!("{} {}", req.method.to_lowercase(), req.url.request_uri()),
                "host" => {
                    if req.url.host.is_empty() {
                        req.host.clone()
                    } else {
                        req.url.host.clone()
                    }
                }
                _ => req.header.get_str(h).to_string(),
            };
            format!("{h}: {v}")
        })
        .collect();
    parts.join("\n")
}

// ---------------- time formatting ----------------

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// Go `t.UTC().Format(http.TimeFormat)`.
pub fn http_date(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT", WEEKDAYS[days.rem_euclid(7) as usize], d, MONTHS[(m - 1) as usize], y, rem / 3600, rem % 3600 / 60, rem % 60)
}

/// Parses an RFC 1123 date ("Mon, 02 Jan 2006 15:04:05 GMT") to Unix seconds.
fn parse_http_date(s: &str) -> Option<i64> {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() != 6 || p[5] != "GMT" {
        return None;
    }
    let d: u32 = p[1].parse().ok()?;
    let m = MONTHS.iter().position(|x| *x == p[2])? as u32 + 1;
    let y: i64 = p[3].parse().ok()?;
    let hms: Vec<i64> = p[4].split(':').map(|x| x.parse().ok()).collect::<Option<_>>()?;
    if hms.len() != 3 {
        return None;
    }
    Some(days_from_civil(y, m, d) * 86_400 + hms[0] * 3600 + hms[1] * 60 + hms[2])
}

/// Go `time.Format(time.RFC3339Nano)` for a UTC time.
fn rfc3339_nano(secs: i64, nanos: u32) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let mut s = format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60);
    if nanos > 0 {
        let f = format!("{nanos:09}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

// ---------------- errors ----------------

/// Go `common.servicefailure` after `PostProcessServiceError`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServiceError {
    pub status_code: u16,
    pub code: String,
    pub message: String,
    pub opc_request_id: String,
    pub target_service: String,
    pub operation_name: String,
    pub timestamp: String,
    pub client_version: String,
    pub request_target: String,
    pub error_troubleshooting_link: String,
    pub operation_reference_link: String,
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let reference = if self.operation_reference_link.is_empty() {
            String::new()
        } else {
            format!("\nAlso see {} for details on this operation's requirements.", self.operation_reference_link)
        };
        write!(
            f,
            "Error returned by {} Service. Http Status Code: {}. Error Code: {}. Opc request id: {}. Message: {}\nOperation Name: {}\nTimestamp: {}\nClient Version: {}\nRequest Endpoint: {}\nTroubleshooting Tips: See {} for more information about resolving this error.{}\nTo get more info on the failing request, you can set OCI_GO_SDK_DEBUG env var to info or higher level to log the request/response details.\nIf you are unable to resolve this {} issue, please contact Oracle support and provide them this full error message.",
            self.target_service,
            self.status_code,
            self.code,
            self.opc_request_id,
            self.message,
            self.operation_name,
            self.timestamp,
            self.client_version,
            self.request_target,
            self.error_troubleshooting_link,
            reference,
            self.target_service
        )
    }
}

/// Go `newServiceFailureFromResponse`.
fn service_failure(resp: &net::Response, method: &str, url: &str) -> ServiceError {
    let timestamp = match parse_http_date(resp.header.get_str("Date")) {
        Some(s) => rfc3339_nano(s, 0),
        None => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            rfc3339_nano(now.as_secs() as i64, now.subsec_nanos())
        }
    };
    let mut se = ServiceError {
        status_code: resp.status,
        code: "BadErrorResponse".to_string(),
        opc_request_id: resp.header.get_str("opc-request-id").to_string(),
        timestamp,
        client_version: format!("Oracle-GoSDK/{SDK_VERSION}"),
        request_target: format!("{method} {url}"),
        ..Default::default()
    };
    match Json::parse_bytes(&resp.body) {
        Ok(Json::Obj(fields)) => {
            for (k, v) in &fields {
                let Some(s) = v.as_str() else { continue };
                match k.to_ascii_lowercase().as_str() {
                    "code" => se.code = s.to_string(),
                    "message" => se.message = s.to_string(),
                    "opc-request-id" => se.opc_request_id = s.to_string(),
                    _ => {}
                }
            }
        }
        Ok(Json::Null) => {}
        Ok(other) => {
            se.message = format!("Failed to parse json from response body due to: json: cannot unmarshal {} into Go value of type common.servicefailure. With response body {}.", json_kind(&other), String::from_utf8_lossy(&resp.body));
        }
        Err(e) => {
            se.message = format!("Failed to parse json from response body due to: {}. With response body {}.", e.msg, String::from_utf8_lossy(&resp.body));
        }
    }
    se
}

/// The error of one API call.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError {
    Service(ServiceError),
    /// A transport or decoding error, or "context canceled".
    Other(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Service(s) => s.fmt(f),
            CallError::Other(s) => f.write_str(s),
        }
    }
}

/// Go `IsErrorRetryableByDefault`.
fn retryable(e: &CallError, timeout: bool) -> bool {
    match e {
        CallError::Service(s) => match (s.status_code, s.code.as_str()) {
            (409, "IncorrectState") | (409, "LockConflict") | (429, "TooManyRequests") => true,
            (501, "MethodNotImplemented") => false,
            (c, _) => (500..505).contains(&c),
        },
        CallError::Other(m) => timeout || m.contains("connection reset by peer") || m.contains("i/o timeout") || m.contains("transport connection broken"),
    }
}

// ---------------- models ----------------

/// Go `core.Instance` (the fields OCI SD reads).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Instance {
    pub id: Option<String>,
    pub display_name: Option<String>,
    pub shape: Option<String>,
    pub availability_domain: Option<String>,
    pub fault_domain: Option<String>,
    pub image_id: Option<String>,
    pub region: Option<String>,
    pub compartment_id: Option<String>,
    pub lifecycle_state: String,
    /// `freeformTags` in JSON order (a Go map; duplicate keys keep the last value).
    pub freeform_tags: Vec<(String, String)>,
    /// `definedTags`: namespace -> key -> scalar or other JSON value.
    pub defined_tags: Vec<(String, Vec<(String, Json)>)>,
}

/// Go `core.VnicAttachment`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VnicAttachment {
    pub id: Option<String>,
    pub instance_id: Option<String>,
    pub vnic_id: Option<String>,
    pub lifecycle_state: String,
}

/// Go `core.Vnic`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Vnic {
    pub id: Option<String>,
    pub is_primary: Option<bool>,
    pub private_ip: Option<String>,
    pub public_ip: Option<String>,
    pub ipv6_addresses: Vec<String>,
    pub hostname_label: Option<String>,
}

/// Go `identity.Compartment`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Compartment {
    pub id: Option<String>,
    pub lifecycle_state: String,
}

fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

fn type_err(v: &Json, field: &str, go_type: &str) -> String {
    format!("json: cannot unmarshal {} into Go struct field {} of type {}", json_kind(v), field, go_type)
}

/// Visits the members of a JSON object as Go's decoder matches struct fields: names compare without case,
/// later members win.
fn decode_object(v: &Json, go_type: &str, mut f: impl FnMut(&str, &Json) -> Result<(), String>) -> Result<(), String> {
    match v {
        Json::Obj(fields) => {
            for (k, x) in fields {
                f(&k.to_ascii_lowercase(), x)?;
            }
            Ok(())
        }
        Json::Null => Ok(()),
        other => Err(format!("json: cannot unmarshal {} into Go value of type {}", json_kind(other), go_type)),
    }
}

fn opt_string(v: &Json, field: &str) -> Result<Option<String>, String> {
    match v {
        Json::Null => Ok(None),
        Json::Str(s) => Ok(Some(s.clone())),
        o => Err(type_err(o, field, "string")),
    }
}

fn enum_string(v: &Json, field: &str, go_type: &str) -> Result<String, String> {
    match v {
        Json::Null => Ok(String::new()),
        Json::Str(s) => Ok(s.clone()),
        o => Err(type_err(o, field, go_type)),
    }
}

fn put<T>(m: &mut Vec<(String, T)>, k: &str, v: T) {
    match m.iter_mut().find(|(n, _)| n == k) {
        Some(e) => e.1 = v,
        None => m.push((k.to_string(), v)),
    }
}

impl Instance {
    pub fn from_json(v: &Json) -> Result<Instance, String> {
        let mut i = Instance::default();
        decode_object(v, "core.Instance", |k, x| {
            match k {
                "id" => i.id = opt_string(x, "Instance.id")?,
                "displayname" => i.display_name = opt_string(x, "Instance.displayName")?,
                "shape" => i.shape = opt_string(x, "Instance.shape")?,
                "availabilitydomain" => i.availability_domain = opt_string(x, "Instance.availabilityDomain")?,
                "faultdomain" => i.fault_domain = opt_string(x, "Instance.faultDomain")?,
                "imageid" => i.image_id = opt_string(x, "Instance.imageId")?,
                "region" => i.region = opt_string(x, "Instance.region")?,
                "compartmentid" => i.compartment_id = opt_string(x, "Instance.compartmentId")?,
                "lifecyclestate" => i.lifecycle_state = enum_string(x, "Instance.lifecycleState", "core.InstanceLifecycleStateEnum")?,
                "freeformtags" => match x {
                    Json::Null => i.freeform_tags.clear(),
                    Json::Obj(m) => {
                        for (tk, tv) in m {
                            match tv {
                                Json::Str(s) => put(&mut i.freeform_tags, tk, s.clone()),
                                Json::Null => put(&mut i.freeform_tags, tk, String::new()),
                                o => return Err(type_err(o, "Instance.freeformTags", "string")),
                            }
                        }
                    }
                    o => return Err(type_err(o, "Instance.freeformTags", "map[string]string")),
                },
                "definedtags" => match x {
                    Json::Null => i.defined_tags.clear(),
                    Json::Obj(m) => {
                        for (ns, tags) in m {
                            match tags {
                                Json::Null => put(&mut i.defined_tags, ns, Vec::new()),
                                Json::Obj(t) => {
                                    let mut cur = i.defined_tags.iter().find(|(n, _)| n == ns).map(|(_, v)| v.clone()).unwrap_or_default();
                                    for (tk, tv) in t {
                                        put(&mut cur, tk, tv.clone());
                                    }
                                    put(&mut i.defined_tags, ns, cur);
                                }
                                o => return Err(type_err(o, "Instance.definedTags", "map[string]interface {}")),
                            }
                        }
                    }
                    o => return Err(type_err(o, "Instance.definedTags", "map[string]map[string]interface {}")),
                },
                _ => {}
            }
            Ok(())
        })?;
        Ok(i)
    }
}

impl VnicAttachment {
    pub fn from_json(v: &Json) -> Result<VnicAttachment, String> {
        let mut a = VnicAttachment::default();
        decode_object(v, "core.VnicAttachment", |k, x| {
            match k {
                "id" => a.id = opt_string(x, "VnicAttachment.id")?,
                "instanceid" => a.instance_id = opt_string(x, "VnicAttachment.instanceId")?,
                "vnicid" => a.vnic_id = opt_string(x, "VnicAttachment.vnicId")?,
                "lifecyclestate" => a.lifecycle_state = enum_string(x, "VnicAttachment.lifecycleState", "core.VnicAttachmentLifecycleStateEnum")?,
                _ => {}
            }
            Ok(())
        })?;
        Ok(a)
    }
}

impl Vnic {
    pub fn from_json(v: &Json) -> Result<Vnic, String> {
        let mut n = Vnic::default();
        decode_object(v, "core.Vnic", |k, x| {
            match k {
                "id" => n.id = opt_string(x, "Vnic.id")?,
                "isprimary" => {
                    n.is_primary = match x {
                        Json::Null => None,
                        Json::Bool(b) => Some(*b),
                        o => return Err(type_err(o, "Vnic.isPrimary", "bool")),
                    }
                }
                "privateip" => n.private_ip = opt_string(x, "Vnic.privateIp")?,
                "publicip" => n.public_ip = opt_string(x, "Vnic.publicIp")?,
                "hostnamelabel" => n.hostname_label = opt_string(x, "Vnic.hostnameLabel")?,
                "ipv6addresses" => {
                    n.ipv6_addresses = match x {
                        Json::Null => Vec::new(),
                        Json::Arr(a) => a.iter().map(|e| opt_string(e, "Vnic.ipv6Addresses").map(Option::unwrap_or_default)).collect::<Result<_, _>>()?,
                        o => return Err(type_err(o, "Vnic.ipv6Addresses", "[]string")),
                    }
                }
                _ => {}
            }
            Ok(())
        })?;
        Ok(n)
    }
}

impl Compartment {
    pub fn from_json(v: &Json) -> Result<Compartment, String> {
        let mut c = Compartment::default();
        decode_object(v, "identity.Compartment", |k, x| {
            match k {
                "id" => c.id = opt_string(x, "Compartment.id")?,
                "lifecyclestate" => c.lifecycle_state = enum_string(x, "Compartment.lifecycleState", "identity.CompartmentLifecycleStateEnum")?,
                _ => {}
            }
            Ok(())
        })?;
        Ok(c)
    }
}

fn parse_list<T>(body: &[u8], go_type: &str, f: impl Fn(&Json) -> Result<T, String>) -> Result<Vec<T>, String> {
    match Json::parse_bytes(body).map_err(|e| format!("{} at offset {}", e.msg, e.offset))? {
        Json::Null => Ok(Vec::new()),
        Json::Arr(a) => a.iter().map(f).collect(),
        o => Err(format!("json: cannot unmarshal {} into Go value of type []{}", json_kind(&o), go_type)),
    }
}

// ---------------- the client ----------------

/// The OCI API client of one discoverer: the Prometheus HTTP client, the signer, the endpoints, the shared
/// rate limiter and the default retry policy.
pub struct Client {
    pub http: net::Client,
    pub signer: Arc<Signer>,
    pub endpoints: Endpoints,
    pub user_agent: String,
    pub limiter: Arc<Limiter>,
}

/// One successful call: the parsed body and the `opc-next-page` header.
struct Page {
    body: Vec<u8>,
    next: Option<String>,
}

/// Go `url.PathEscape`-style escaping of a path segment as `URL.EscapedPath` would leave it.
fn escape_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~!$&'()*+,;=:@".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl Client {
    /// One attempt of an operation (Go `client.<op>`: `MakeDefaultHTTPRequestWithTaggedStructAndExtraHeaders`,
    /// `prepareRequest`, `Sign`, `httpDo`, `checkForSuccessfulResponse`).
    fn attempt(&self, host: &str, path: &str, query: &[(String, String)], service: &str, op: &str, reference: &str) -> Result<Page, (CallError, bool)> {
        let raw_query = yaml::url::encode_query(query);
        let url = if raw_query.is_empty() { format!("{host}/{BASE_PATH}{path}") } else { format!("{host}/{BASE_PATH}{path}?{raw_query}") };
        let mut req = net::Request::get(&url).map_err(|e| (CallError::Other(format!("host is invalid. {}", e.msg)), false))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        req.header.set("Date", &http_date(now));
        req.header.set("Opc-Client-Info", &format!("Oracle-GoSDK/{SDK_VERSION}"));
        req.header.set("Accept", "*/*");
        req.header.set("Opc-Request-Id", &crypto::random_hex(16));
        req.header.set("Opc-Client-Retries", "true");
        req.header.set("User-Agent", &self.user_agent);
        self.signer.sign(&mut req).map_err(|e| (CallError::Other(e), false))?;
        let resp = match self.http.do_request(req) {
            Ok(r) => r,
            Err(e) => {
                let t = e.is_timeout();
                return Err((CallError::Other(e.msg), t));
            }
        };
        if resp.status / 100 == 4 || resp.status / 100 == 5 {
            let mut se = service_failure(&resp, "GET", &url);
            se.operation_name = op.to_string();
            se.target_service = service.to_string();
            se.error_troubleshooting_link = format!("https://docs.oracle.com/iaas/Content/API/References/apierrors.htm#apierrors_{0}__{0}_{1}", se.status_code, se.code.to_lowercase());
            se.operation_reference_link = reference.to_string();
            return Err((CallError::Service(se), false));
        }
        let next = resp.header.get("opc-next-page").filter(|s| !s.is_empty()).map(|s| s.to_string());
        Ok(Page { body: resp.body, next })
    }

    /// Go `common.Retry` with `common.DefaultRetryPolicy()` (no eventual-consistency window: up to 8 attempts,
    /// exponential backoff of min(2^(n-1), 30) seconds plus up to 1 second of jitter).
    fn call(&self, ctx: &CancelToken, host: &str, path: &str, query: &[(String, String)], service: &str, op: &str, reference: &str) -> Result<Page, CallError> {
        let mut attempt = 1u32;
        loop {
            if ctx.is_cancelled() {
                return Err(CallError::Other(CONTEXT_CANCELED.to_string()));
            }
            let (err, timeout) = match self.attempt(host, path, query, service, op, reference) {
                Ok(p) => return Ok(p),
                Err(e) => e,
            };
            if attempt >= 8 || !retryable(&err, timeout) {
                return Err(err);
            }
            let mut j = [0u8; 4];
            crypto::random_bytes(&mut j);
            let jitter = u32::from_le_bytes(j) as f64 / (u32::MAX as f64 + 1.0);
            let sleep = 2f64.powi(attempt as i32 - 1).min(30.0) + jitter;
            if ctx.wait_timeout(Duration::from_secs_f64(sleep)) {
                return Err(CallError::Other(CONTEXT_CANCELED.to_string()));
            }
            attempt += 1;
        }
    }

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// One page of direct child compartments (Go `IdentityClient.ListCompartments` with
    /// `accessLevel=ACCESSIBLE`, `compartmentIdInSubtree=false`, `limit=100`), after one rate limiter token.
    pub fn list_compartments_page(&self, ctx: &CancelToken, parent: &str, page: Option<&str>) -> Result<(Vec<Compartment>, Option<String>), String> {
        self.limiter.wait(ctx)?;
        let mut q = Self::q(&[("compartmentId", parent), ("accessLevel", "ACCESSIBLE"), ("compartmentIdInSubtree", "false"), ("limit", "100")]);
        if let Some(p) = page {
            q.push(("page".to_string(), p.to_string()));
        }
        let p = self
            .call(ctx, &self.endpoints.identity, "/compartments", &q, "Identity", "ListCompartments", "https://docs.oracle.com/iaas/api/#/en/identity/20160918/Compartment/ListCompartments")
            .map_err(|e| e.to_string())?;
        let items = parse_list(&p.body, "identity.Compartment", Compartment::from_json)?;
        Ok((items, p.next))
    }

    /// All instances of a compartment (Go `ComputeClient.ListInstances` with `limit=100`, every page).
    pub fn list_instances(&self, ctx: &CancelToken, compartment: &str) -> Result<Vec<Instance>, String> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            self.limiter.wait(ctx)?;
            let mut q = Self::q(&[("compartmentId", compartment), ("limit", "100")]);
            if let Some(p) = &page {
                q.push(("page".to_string(), p.clone()));
            }
            let p = self
                .call(ctx, &self.endpoints.iaas, "/instances", &q, "Compute", "ListInstances", "https://docs.oracle.com/iaas/api/#/en/iaas/20160918/Instance/ListInstances")
                .map_err(|e| e.to_string())?;
            out.extend(parse_list(&p.body, "core.Instance", Instance::from_json)?);
            match p.next {
                Some(n) => page = Some(n),
                None => return Ok(out),
            }
        }
    }

    /// All VNIC attachments of an instance (Go `ComputeClient.ListVnicAttachments` with `limit=100`).
    pub fn list_vnic_attachments(&self, ctx: &CancelToken, compartment: &str, instance: &str) -> Result<Vec<VnicAttachment>, String> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            self.limiter.wait(ctx)?;
            let mut q = Self::q(&[("compartmentId", compartment), ("instanceId", instance), ("limit", "100")]);
            if let Some(p) = &page {
                q.push(("page".to_string(), p.clone()));
            }
            let p = self
                .call(ctx, &self.endpoints.iaas, "/vnicAttachments", &q, "Compute", "ListVnicAttachments", "https://docs.oracle.com/iaas/api/#/en/iaas/20160918/VnicAttachment/ListVnicAttachments")
                .map_err(|e| e.to_string())?;
            out.extend(parse_list(&p.body, "core.VnicAttachment", VnicAttachment::from_json)?);
            match p.next {
                Some(n) => page = Some(n),
                None => return Ok(out),
            }
        }
    }

    /// One VNIC (Go `VirtualNetworkClient.GetVnic`).
    pub fn get_vnic(&self, ctx: &CancelToken, vnic_id: &str) -> Result<Vnic, String> {
        self.limiter.wait(ctx)?;
        let path = format!("/vnics/{}", escape_path_segment(vnic_id));
        let p = self
            .call(ctx, &self.endpoints.iaas, &path, &[], "VirtualNetwork", "GetVnic", "https://docs.oracle.com/iaas/api/#/en/iaas/20160918/Vnic/GetVnic")
            .map_err(|e| e.to_string())?;
        let v = Json::parse_bytes(&p.body).map_err(|e| format!("{} at offset {}", e.msg, e.offset))?;
        Vnic::from_json(&v)
    }
}
