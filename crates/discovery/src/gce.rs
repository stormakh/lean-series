//! Port of `discovery/gce`: the instances of a GCE project and zone (compute v1 `instances.list`), refreshed at
//! an interval.
//!
//! Auth as `google.DefaultClient(ctx, compute.readonly)` of golang.org/x/oauth2 v0.37.0:
//! 1. `GOOGLE_APPLICATION_CREDENTIALS`: a credentials JSON file;
//! 2. the gcloud file `$HOME/.config/gcloud/application_default_credentials.json`;
//! 3. on GCE (`GCE_METADATA_HOST` set, or the metadata server answers), the metadata server token.
//!
//! Credential files of type `service_account` (and `gdch_service_account`, which x/oauth2 treats the same: an
//! RS256 JWT bearer grant to `token_uri`) and `authorized_user` (refresh token grant) are supported.
//! `external_account` (workload identity federation), `external_account_authorized_user`,
//! `impersonated_service_account` and OAuth client files (`web` / `installed`) are not.
//!
//! Differences from Go:
//! - Requests have a timeout of the refresh interval (Go has none but aborts them when the discoverer stops; a
//!   request in flight here is not aborted on cancel).
//! - Response fields that Prometheus does not read are not type-checked; Go's JSON errors for the fields it reads
//!   have the same meaning but not always the same text.
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::util::sanitize_label_name;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const GCE_LABEL: &str = "__meta_gce_";
pub const LABEL_PROJECT: &str = "__meta_gce_project";
pub const LABEL_ZONE: &str = "__meta_gce_zone";
pub const LABEL_NETWORK: &str = "__meta_gce_network";
pub const LABEL_SUBNETWORK: &str = "__meta_gce_subnetwork";
pub const LABEL_PUBLIC_IP: &str = "__meta_gce_public_ip";
pub const LABEL_PRIVATE_IP: &str = "__meta_gce_private_ip";
pub const LABEL_INSTANCE_ID: &str = "__meta_gce_instance_id";
pub const LABEL_INSTANCE_NAME: &str = "__meta_gce_instance_name";
pub const LABEL_INSTANCE_STATUS: &str = "__meta_gce_instance_status";
pub const LABEL_TAGS: &str = "__meta_gce_tags";
pub const LABEL_METADATA: &str = "__meta_gce_metadata_";
pub const LABEL_LABEL: &str = "__meta_gce_label_";
pub const LABEL_MACHINE_TYPE: &str = "__meta_gce_machine_type";

/// Go `compute.ComputeReadonlyScope`.
pub const COMPUTE_READONLY_SCOPE: &str = "https://www.googleapis.com/auth/compute.readonly";
/// Go `google.JWTTokenURL` / `google.Endpoint.TokenURL`.
pub const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
/// Go compute `basePathTemplate` and `mtlsBasePath`.
const BASE_PATH_TEMPLATE: &str = "https://compute.UNIVERSE_DOMAIN/compute/v1/";
const MTLS_BASE_PATH: &str = "https://compute.mtls.googleapis.com/compute/v1/";
/// Go `googleapi.UserAgent`.
const API_USER_AGENT: &str = "google-api-go-client/0.5";
/// `gl-go/<Go version of the Prometheus 3.16.0 release build> gdcl/<google.golang.org/api version>`.
const API_CLIENT_HEADER: &str = "gl-go/1.27.3 gdcl/0.305.0";
/// The `x-goog-api-version` that compute v1 sends for `instances.list`.
const API_VERSION: &str = "2026-09-01";
/// Go metadata `metadataIP` and `userAgent`.
const METADATA_IP: &str = "169.254.169.254";
const METADATA_USER_AGENT: &str = "gcloud-golang/0.1";

/// Go `gce.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub project: String,
    pub zone: String,
    pub filter: String,
    pub refresh_interval: yaml::Duration,
    pub port: i64,
    pub tag_separator: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { port: 80, tag_separator: ",".to_string(), refresh_interval: yaml::Duration::from_secs(60), ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "gce.plain", [
    (project: "project")
    (zone: "zone")
    (filter: "filter", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (port: "port")
    (tag_separator: "tag_separator", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.project.is_empty() {
                return Err(YError::Msg("GCE SD configuration requires a project".into()));
            }
            if self.zone.is_empty() {
                return Err(YError::Msg("GCE SD configuration requires a zone".into()));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "gce.plain".to_string()
    }
}
impl yaml::Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "gce.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "gce"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(GceMetrics { refresh_metrics: rmi })
    }
}

/// Go `gceMetrics`.
pub struct GceMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for GceMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Non-YAML options (for tests and end-to-end runs).
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// The compute API base ("https://compute.googleapis.com/compute/v1/"); `None` resolves it as Go does
    /// (`GOOGLE_CLOUD_UNIVERSE_DOMAIN`, `GOOGLE_API_USE_MTLS_ENDPOINT`).
    pub base_path: Option<String>,
}

// ---------------- errors and small helpers ----------------

/// Go's `*fs.PathError` text of a failed read.
fn read_file(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| {
        let reason = match e.kind() {
            std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
            std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
            std::io::ErrorKind::IsADirectory => "is a directory".to_string(),
            _ => e.to_string(),
        };
        format!("open {path}: {reason}")
    })
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

/// A struct field as Go's decoder finds it: the last key that matches case-insensitively.
fn field<'a>(o: &'a Json, k: &str) -> Option<&'a Json> {
    o.as_obj()?.iter().rev().find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v)
}

/// A Go `string` struct field.
fn str_field(o: &Json, k: &str, ty: &str) -> Result<String, String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(String::new()),
        Some(Json::Str(s)) => Ok(s.clone()),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type string", json_kind(v))),
    }
}

/// A Go slice-of-structs field (`[]*T`): its elements, `Json::Null` for null elements.
fn arr_field<'a>(o: &'a Json, k: &str, ty: &str, elem: &str) -> Result<&'a [Json], String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(&[]),
        Some(Json::Arr(a)) => {
            for v in a {
                if !matches!(v, Json::Obj(_) | Json::Null) {
                    return Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type {elem}", json_kind(v)));
                }
            }
            Ok(a)
        }
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type []*{elem}", json_kind(v))),
    }
}

/// Go `url.QueryEscape`.
fn query_escape(s: &str) -> String {
    yaml::url::query_escape(s)
}

/// The escaping of googleapi.Expand for `{var}`: everything but the unreserved characters.
fn uri_template_escape(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn b64url(b: &[u8]) -> String {
    crypto::base64url(b).trim_end_matches('=').to_string()
}

// ---------------- the metadata server (cloud.google.com/go/compute/metadata v0.10.0) ----------------

fn metadata_client(timeout: bool) -> net::Client {
    // Go: its own transport, no proxy; 5s timeout for the default client.
    let mut c = net::new_client_from_config(&HttpClientConfig::default_config(), &net::HttpClientOptions::default()).expect("metadata client");
    c.timeout = if timeout { Some(Duration::from_secs(5)) } else { None };
    c
}

/// Go `metadata.OnGCE` (computed once per process).
pub fn on_gce() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        if !std::env::var("GCE_METADATA_HOST").unwrap_or_default().is_empty() {
            return true;
        }
        let (tx, rx) = mpsc::channel::<bool>();
        let tx2 = tx.clone();
        std::thread::spawn(move || {
            let mut req = match net::Request::get(&format!("http://{METADATA_IP}")) {
                Ok(r) => r,
                Err(_) => {
                    let _ = tx.send(false);
                    return;
                }
            };
            req.header.set("User-Agent", METADATA_USER_AGENT);
            let ok = metadata_client(true).do_request(req).map(|r| r.header.get_str("Metadata-Flavor") == "Google").unwrap_or(false);
            let _ = tx.send(ok);
        });
        std::thread::spawn(move || {
            use std::net::ToSocketAddrs;
            let ok = ("metadata.google.internal.", 0u16).to_socket_addrs().map(|a| a.into_iter().any(|a| a.ip().to_string() == METADATA_IP)).unwrap_or(false);
            let _ = tx2.send(ok);
        });
        let product = std::fs::read_to_string("/sys/class/dmi/id/product_name").unwrap_or_default();
        let try_harder = matches!(product.trim(), "Google" | "Google Compute Engine");
        let first = rx.recv().unwrap_or(false);
        if try_harder && !first {
            return rx.recv_timeout(Duration::from_secs(5)).unwrap_or(false);
        }
        first
    })
}

/// Go `metadata.Get(suffix)` with its retries (5xx, 429, refused or reset connections; at most 5 retries).
fn metadata_get(suffix: &str) -> Result<String, String> {
    let host = std::env::var("GCE_METADATA_HOST").ok().filter(|h| !h.is_empty()).unwrap_or_else(|| METADATA_IP.to_string());
    let suffix = suffix.trim_start_matches('/');
    let url = format!("http://{host}/computeMetadata/v1/{suffix}");
    let client = metadata_client(true);
    let mut attempts = 0;
    let mut cur = Duration::from_millis(100);
    loop {
        let mut req = net::Request::get(&url).map_err(|e| e.to_string())?;
        req.header.set("Metadata-Flavor", "Google");
        req.header.set("User-Agent", METADATA_USER_AGENT);
        req.decompress = true;
        let res = client.do_request(req);
        let retry = match &res {
            Ok(r) => r.status != 200 && ((500..=599).contains(&r.status) || r.status == 429),
            Err(e) => e.msg.contains("connection refused") || e.msg.contains("connection reset") || e.msg.contains("unexpected EOF"),
        };
        if retry && attempts < 5 {
            attempts += 1;
            let mut b = [0u8; 8];
            crypto::random_bytes(&mut b);
            let pause = 1 + u64::from_le_bytes(b) % (cur.as_nanos() as u64);
            std::thread::sleep(Duration::from_nanos(pause));
            cur = (cur * 2).min(Duration::from_secs(30));
            continue;
        }
        let r = res.map_err(|e| e.to_string())?;
        if r.status == 404 {
            return Err(format!("metadata: GCE metadata {} not defined", yaml::go_quote(suffix)));
        }
        if r.status != 200 {
            return Err(format!("compute: Received {} `{}`", r.status, String::from_utf8_lossy(&r.body)));
        }
        return Ok(String::from_utf8_lossy(&r.body).into_owned());
    }
}

// ---------------- token sources (golang.org/x/oauth2) ----------------

#[derive(Clone, Debug)]
struct Token {
    access_token: String,
    token_type: String,
    expiry: Option<Instant>,
}

impl Token {
    /// x/oauth2 `Token.Valid` with the default 10 s expiry delta.
    fn valid(&self) -> bool {
        !self.access_token.is_empty() && self.expiry.is_none_or(|e| e.checked_sub(Duration::from_secs(10)).is_some_and(|t| Instant::now() < t))
    }
    /// x/oauth2 `Token.Type`.
    fn type_(&self) -> String {
        let t = &self.token_type;
        if t.is_empty() || t.eq_ignore_ascii_case("bearer") {
            "Bearer".to_string()
        } else if t.eq_ignore_ascii_case("mac") {
            "MAC".to_string()
        } else if t.eq_ignore_ascii_case("basic") {
            "Basic".to_string()
        } else {
            t.clone()
        }
    }
}

/// How tokens are fetched.
#[derive(Clone, Debug)]
enum Source {
    /// `service_account` / `gdch_service_account`: jwt.Config.
    ServiceAccount { email: String, private_key: String, private_key_id: String, token_url: String, audience: String },
    /// `authorized_user`: oauth2.Config with a refresh token (AuthStyleInParams).
    AuthorizedUser { client_id: String, client_secret: String, token_url: String, refresh_token: String },
    /// The metadata server (`computeSource`).
    Compute,
}

/// A cached token source (x/oauth2 `ReuseTokenSource`).
struct TokenSource {
    source: Source,
    http: net::Client,
    token: Mutex<Option<Token>>,
}

fn expiry_from(secs: i64) -> Option<Instant> {
    if secs > 0 { Some(Instant::now() + Duration::from_secs(secs as u64)) } else { None }
}

fn expires_in(j: &Json) -> i64 {
    match field(j, "expires_in") {
        Some(Json::Int(i)) => *i,
        Some(Json::Float(f)) => *f as i64,
        Some(Json::Str(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// x/oauth2 `RetrieveError.Error`.
fn retrieve_error(status_text: &str, body: &[u8], code: &str, desc: &str, uri: &str) -> String {
    if !code.is_empty() {
        let mut s = format!("oauth2: {}", yaml::go_quote(code));
        if !desc.is_empty() {
            s.push_str(&format!(" {}", yaml::go_quote(desc)));
        }
        if !uri.is_empty() {
            s.push_str(&format!(" {}", yaml::go_quote(uri)));
        }
        return s;
    }
    format!("oauth2: cannot fetch token: {status_text}\nResponse: {}", String::from_utf8_lossy(body))
}

/// Go `jws.Encode` of the jwt.Config claim set, RS256.
fn sign_assertion(email: &str, private_key: &str, private_key_id: &str, aud: &str) -> Result<String, String> {
    let blocks = net::tls::pem_blocks(private_key.as_bytes());
    let der = match blocks.first() {
        Some(b) => b.der.clone(),
        None => private_key.as_bytes().to_vec(),
    };
    let kp = ring::signature::RsaKeyPair::from_pkcs8(&der)
        .or_else(|_| ring::signature::RsaKeyPair::from_der(&der))
        .map_err(|_| "private key should be a PEM or plain PKCS1 or PKCS8; parse error: x509: failed to parse private key".to_string())?;
    let mut header = vec![("alg".to_string(), Json::Str("RS256".into())), ("typ".to_string(), Json::Str("JWT".into()))];
    if !private_key_id.is_empty() {
        header.push(("kid".to_string(), Json::Str(private_key_id.to_string())));
    }
    // Go turns the clock back 10 s for machines whose time is ahead.
    let iat = now_unix() - 10;
    let claims = vec![
        ("iss".to_string(), Json::Str(email.to_string())),
        ("scope".to_string(), Json::Str(COMPUTE_READONLY_SCOPE.to_string())),
        ("aud".to_string(), Json::Str(aud.to_string())),
        ("exp".to_string(), Json::Int(iat + 3600)),
        ("iat".to_string(), Json::Int(iat)),
    ];
    let input = format!("{}.{}", b64url(Json::Obj(header).stringify().as_bytes()), b64url(Json::Obj(claims).stringify().as_bytes()));
    let mut sig = vec![0u8; kp.public().modulus_len()];
    kp.sign(&ring::signature::RSA_PKCS1_SHA256, &ring::rand::SystemRandom::new(), input.as_bytes(), &mut sig).map_err(|_| "crypto/rsa: signing failed".to_string())?;
    Ok(format!("{input}.{}", b64url(&sig)))
}

fn post_form(http: &net::Client, url: &str, form: &[(String, String)]) -> Result<net::Response, String> {
    let mut pairs = form.to_vec();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let body = yaml::url::encode_query(&pairs);
    let mut req = net::Request::new("POST", url, body.into_bytes()).map_err(|e| e.to_string())?;
    req.header.set("Content-Type", "application/x-www-form-urlencoded");
    req.decompress = true;
    http.do_request(req).map_err(|e| e.to_string())
}

impl TokenSource {
    fn token(&self) -> Result<Token, String> {
        let mut cached = self.token.lock().unwrap();
        if let Some(t) = cached.as_ref()
            && t.valid()
        {
            return Ok(t.clone());
        }
        let t = self.fetch()?;
        *cached = Some(t.clone());
        Ok(t)
    }

    fn fetch(&self) -> Result<Token, String> {
        match &self.source {
            Source::ServiceAccount { email, private_key, private_key_id, token_url, audience } => {
                let aud = if audience.is_empty() { token_url } else { audience };
                let assertion = sign_assertion(email, private_key, private_key_id, aud)?;
                let form = vec![("grant_type".to_string(), "urn:ietf:params:oauth:grant-type:jwt-bearer".to_string()), ("assertion".to_string(), assertion)];
                let resp = post_form(&self.http, token_url, &form).map_err(|e| format!("oauth2: cannot fetch token: {e}"))?;
                if resp.status < 200 || resp.status > 299 {
                    return Err(retrieve_error(&resp.status_text, &resp.body, "", "", ""));
                }
                let j = Json::parse_bytes(&resp.body).map_err(|e| format!("oauth2: cannot fetch token: {e}"))?;
                let mut tok = Token { access_token: str_field(&j, "access_token", "Token").unwrap_or_default(), token_type: str_field(&j, "token_type", "Token").unwrap_or_default(), expiry: expiry_from(expires_in(&j)) };
                // The expiry of an id_token wins.
                if let Ok(id) = str_field(&j, "id_token", "Token")
                    && !id.is_empty()
                {
                    let parts: Vec<&str> = id.split('.').collect();
                    let claims = (parts.len() == 3).then(|| crypto::base64_decode(parts[1])).flatten().and_then(|b| Json::parse_bytes(&b).ok()).ok_or_else(|| "oauth2: error decoding JWT token: jws: invalid token received".to_string())?;
                    let exp = claims.get("exp").and_then(Json::as_i64).unwrap_or(0);
                    tok.expiry = Some(Instant::now() + Duration::from_secs((exp - now_unix()).max(0) as u64));
                }
                Ok(tok)
            }
            Source::AuthorizedUser { client_id, client_secret, token_url, refresh_token } => {
                if refresh_token.is_empty() {
                    return Err("oauth2: token expired and refresh token is not set".to_string());
                }
                let mut form = vec![("grant_type".to_string(), "refresh_token".to_string()), ("refresh_token".to_string(), refresh_token.clone())];
                if !client_id.is_empty() {
                    form.push(("client_id".to_string(), client_id.clone()));
                }
                if !client_secret.is_empty() {
                    form.push(("client_secret".to_string(), client_secret.clone()));
                }
                let resp = post_form(&self.http, token_url, &form)?;
                parse_token_response(&resp)
            }
            Source::Compute => {
                if !on_gce() {
                    return Err("oauth2/google: can't get a token from the metadata service; not running on GCE".to_string());
                }
                let uri = format!("instance/service-accounts/default/token?scopes={}", query_escape(COMPUTE_READONLY_SCOPE));
                let body = metadata_get(&uri)?;
                let j = Json::parse(&body).map_err(|e| format!("oauth2/google: invalid token JSON from metadata: {e}"))?;
                let access_token = str_field(&j, "access_token", "Token").map_err(|e| format!("oauth2/google: invalid token JSON from metadata: {e}"))?;
                let secs = expires_in(&j);
                if secs == 0 || access_token.is_empty() {
                    return Err("oauth2/google: incomplete token received from metadata".to_string());
                }
                Ok(Token { access_token, token_type: str_field(&j, "token_type", "Token").unwrap_or_default(), expiry: expiry_from(secs) })
            }
        }
    }
}

/// x/oauth2 internal `doTokenRoundTrip`.
fn parse_token_response(resp: &net::Response) -> Result<Token, String> {
    let failure = resp.status < 200 || resp.status > 299;
    let ct = resp.header.get_str("Content-Type");
    let media = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    let body = String::from_utf8_lossy(&resp.body).into_owned();
    let (token, code, desc, uri);
    if media == "application/x-www-form-urlencoded" || media == "text/plain" {
        let (vals, err) = yaml::url::parse_query(&body);
        if let Some(e) = err {
            if failure {
                return Err(retrieve_error(&resp.status_text, &resp.body, "", "", ""));
            }
            return Err(format!("oauth2: cannot parse response: {e}"));
        }
        let get = |k: &str| vals.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()).unwrap_or_default();
        code = get("error");
        desc = get("error_description");
        uri = get("error_uri");
        token = Token { access_token: get("access_token"), token_type: get("token_type"), expiry: expiry_from(get("expires_in").parse().unwrap_or(0)) };
    } else {
        let j = match Json::parse(&body) {
            Ok(j) => j,
            Err(e) => {
                if failure {
                    return Err(retrieve_error(&resp.status_text, &resp.body, "", "", ""));
                }
                return Err(format!("oauth2: cannot parse json: {e}"));
            }
        };
        code = j.str_of("error").to_string();
        desc = j.str_of("error_description").to_string();
        uri = j.str_of("error_uri").to_string();
        token = Token { access_token: j.str_of("access_token").to_string(), token_type: j.str_of("token_type").to_string(), expiry: expiry_from(expires_in(&j).min(i32::MAX as i64)) };
    }
    if failure || !code.is_empty() {
        return Err(retrieve_error(&resp.status_text, &resp.body, &code, &desc, &uri));
    }
    if token.access_token.is_empty() {
        return Err("oauth2: server response missing access_token".to_string());
    }
    Ok(token)
}

/// Go `CredentialsFromJSONWithParams` (the supported file types).
fn credentials_from_json(data: &[u8], http: &net::Client) -> Result<TokenSource, String> {
    let f = Json::parse_bytes(data).map_err(|e| e.to_string())?;
    if !matches!(f, Json::Obj(_)) {
        return Err(format!("json: cannot unmarshal {} into Go value of type google.credentialsFile", json_kind(&f)));
    }
    // Go first tries a Google Developers Console client_credentials.json ("web" / "installed").
    if matches!(field(&f, "web"), Some(Json::Obj(_))) || matches!(field(&f, "installed"), Some(Json::Obj(_))) {
        return Err("google: OAuth client credentials (\"web\" / \"installed\") are not supported".to_string());
    }
    let sf = |k: &str| str_field(&f, k, "credentialsFile");
    let type_ = sf("type")?;
    let source = match type_.as_str() {
        "service_account" | "gdch_service_account" => {
            let token_url = sf("token_uri")?;
            Source::ServiceAccount {
                email: sf("client_email")?,
                private_key: sf("private_key")?,
                private_key_id: sf("private_key_id")?,
                token_url: if token_url.is_empty() { TOKEN_URL.to_string() } else { token_url },
                audience: sf("audience")?,
            }
        }
        "authorized_user" => {
            let token_url = sf("token_uri")?;
            Source::AuthorizedUser { client_id: sf("client_id")?, client_secret: sf("client_secret")?, token_url: if token_url.is_empty() { TOKEN_URL.to_string() } else { token_url }, refresh_token: sf("refresh_token")? }
        }
        "external_account" | "external_account_authorized_user" | "impersonated_service_account" => {
            return Err(format!("credential type {} is not supported", yaml::go_quote(&type_)));
        }
        "" => return Err("missing 'type' field in credentials".to_string()),
        t => return Err(format!("unknown credential type: {}", yaml::go_quote(t))),
    };
    Ok(TokenSource { source, http: http.clone(), token: Mutex::new(None) })
}

/// Go `google.FindDefaultCredentials` for the compute read-only scope.
fn find_default_credentials(http: &net::Client) -> Result<TokenSource, String> {
    const ENV: &str = "GOOGLE_APPLICATION_CREDENTIALS";
    if let Ok(filename) = std::env::var(ENV)
        && !filename.is_empty()
    {
        return read_file(&filename)
            .and_then(|b| credentials_from_json(&b, http))
            .map_err(|e| format!("google: error getting credentials using {ENV} environment variable: {e}"));
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let well_known = format!("{home}/.config/gcloud/application_default_credentials.json");
    if let Ok(b) = std::fs::read(&well_known) {
        return credentials_from_json(&b, http);
    }
    if on_gce() {
        // Go reads the project ID here (and ignores it).
        let _ = metadata_get("project/project-id");
        return Ok(TokenSource { source: Source::Compute, http: http.clone(), token: Mutex::new(None) });
    }
    Err("google: could not find default credentials. See https://cloud.google.com/docs/authentication/external/set-up-adc for more information".to_string())
}

/// The compute API base as google.golang.org/api resolves it for a client given with `WithHTTPClient`.
fn default_base_path() -> Result<String, String> {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let mut mode = env("GOOGLE_API_USE_MTLS_ENDPOINT");
    if mode.is_empty() {
        mode = env("GOOGLE_API_USE_MTLS");
    }
    let universe = match env("GOOGLE_CLOUD_UNIVERSE_DOMAIN") {
        u if u.is_empty() => "googleapis.com".to_string(),
        u => u,
    };
    if mode.to_lowercase() == "always" {
        if universe != "googleapis.com" {
            return Err("mTLS is not supported in any universe other than googleapis.com".to_string());
        }
        return Ok(MTLS_BASE_PATH.to_string());
    }
    Ok(BASE_PATH_TEMPLATE.replacen("UNIVERSE_DOMAIN", &universe, 1))
}

// ---------------- the discovery ----------------

/// The refresh function of the GCE discoverer (Go `Discovery.refresh` and its state).
pub struct GceRefresher {
    project: String,
    zone: String,
    filter: String,
    port: i64,
    tag_separator: String,
    base_path: String,
    http: net::Client,
    tokens: TokenSource,
}

/// Go `googleapi.CheckResponse` error text of a non-2xx response.
fn api_error(status: u16, body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body).into_owned();
    let parsed = if body.first() == Some(&b'[') {
        Json::parse_bytes(body).ok().and_then(|a| a.at(0).cloned())
    } else {
        Json::parse_bytes(body).ok()
    };
    let err = parsed.as_ref().and_then(|j| field(j, "error")).filter(|e| matches!(e, Json::Obj(_)));
    let Some(e) = err else {
        return format!("googleapi: got HTTP response code {status} with body: {text}");
    };
    let code = match field(e, "code") {
        Some(Json::Int(c)) if *c != 0 => *c,
        _ => status as i64,
    };
    let message = str_field(e, "message", "Error").unwrap_or_default();
    let errors: Vec<(String, String)> = match field(e, "errors") {
        Some(Json::Arr(a)) => a.iter().map(|i| (str_field(i, "reason", "ErrorItem").unwrap_or_default(), str_field(i, "message", "ErrorItem").unwrap_or_default())).collect(),
        _ => Vec::new(),
    };
    if errors.is_empty() && message.is_empty() {
        return format!("googleapi: got HTTP response code {code} with body: {text}");
    }
    let mut buf = format!("googleapi: Error {code}: {message}");
    if let Some(Json::Arr(d)) = field(e, "details")
        && !d.is_empty()
    {
        buf.push_str("\nDetails:\n");
        buf.push_str(&sorted(&Json::Arr(d.clone())).stringify_pretty("  "));
        buf.push('\n');
    }
    if errors.is_empty() {
        return buf.trim().to_string();
    }
    if errors.len() == 1 && errors[0].1 == message {
        buf.push_str(&format!(", {}", errors[0].0));
        return buf;
    }
    buf.push_str("\nMore details:\n");
    for (reason, msg) in &errors {
        buf.push_str(&format!("Reason: {reason}, Message: {msg}\n"));
    }
    buf
}

/// A JSON value with object keys sorted (Go marshals `map[string]any` so).
fn sorted(j: &Json) -> Json {
    match j {
        Json::Obj(m) => {
            let mut v: Vec<(String, Json)> = m.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            Json::Obj(v)
        }
        Json::Arr(a) => Json::Arr(a.iter().map(sorted).collect()),
        v => v.clone(),
    }
}

impl GceRefresher {
    /// One `instances.list` call (Go `InstancesListCall.Do`).
    fn list_page(&self, ctx: &CancelToken, page_token: &str) -> Result<Json, String> {
        let mut params = vec![("alt", "json".to_string())];
        if !self.filter.is_empty() {
            params.push(("filter", self.filter.clone()));
        }
        if !page_token.is_empty() {
            params.push(("pageToken", page_token.to_string()));
        }
        params.push(("prettyPrint", "false".to_string()));
        let query: Vec<String> = params.iter().map(|(k, v)| format!("{k}={}", query_escape(v))).collect();
        let url = format!("{}projects/{}/zones/{}/instances?{}", self.base_path, uri_template_escape(&self.project), uri_template_escape(&self.zone), query.join("&"));
        if ctx.is_cancelled() {
            return Err("context canceled".to_string());
        }
        // oauth2.Transport: the token, then the request; a token error is a url.Error of the request.
        let tok = self.tokens.token().map_err(|e| if ctx.is_cancelled() { "context canceled".to_string() } else { format!("Get {}: {e}", yaml::go_quote(&url)) })?;
        let mut req = net::Request::get(&url).map_err(|e| e.to_string())?;
        req.header.set("X-Goog-Api-Client", API_CLIENT_HEADER);
        req.header.set("X-Goog-Api-Version", API_VERSION);
        req.header.set("User-Agent", API_USER_AGENT);
        req.header.set("Authorization", &format!("{} {}", tok.type_(), tok.access_token));
        req.decompress = true;
        let resp = self.http.do_request(req).map_err(|e| if ctx.is_cancelled() { "context canceled".to_string() } else { e.to_string() })?;
        if resp.status == 304 {
            return Err(format!("googleapi: got HTTP response code {} with body: ", resp.status));
        }
        if !(200..=299).contains(&resp.status) {
            return Err(api_error(resp.status, &resp.body));
        }
        let j = Json::parse_bytes(&resp.body).map_err(|e| e.to_string())?;
        if !matches!(j, Json::Obj(_) | Json::Null) {
            return Err(format!("json: cannot unmarshal {} into Go value of type compute.InstanceList", json_kind(&j)));
        }
        Ok(j)
    }

    /// The target of one instance (Go: the body of the loop over `l.Items`).
    fn instance_labels(&self, inst: &Json) -> Result<Option<LabelSet>, String> {
        let ty = "Instance";
        let ifaces = arr_field(inst, "networkInterfaces", ty, "compute.NetworkInterface")?;
        if ifaces.is_empty() {
            return Ok(None);
        }
        let id: u64 = match field(inst, "id") {
            None | Some(Json::Null) => 0,
            Some(Json::Str(s)) => s.parse().map_err(|_| format!("json: invalid use of ,string struct tag, trying to unmarshal {} into uint64", json::quote(s)))?,
            Some(_) => return Err("json: invalid use of ,string struct tag, trying to unmarshal unquoted value into uint64".to_string()),
        };
        let mut labels = LabelSet::new();
        labels.insert(LABEL_PROJECT.to_string(), self.project.clone());
        labels.insert(LABEL_ZONE.to_string(), str_field(inst, "zone", ty)?);
        labels.insert(LABEL_INSTANCE_ID.to_string(), id.to_string());
        labels.insert(LABEL_INSTANCE_NAME.to_string(), str_field(inst, "name", ty)?);
        labels.insert(LABEL_INSTANCE_STATUS.to_string(), str_field(inst, "status", ty)?);
        labels.insert(LABEL_MACHINE_TYPE.to_string(), str_field(inst, "machineType", ty)?);
        let pri = &ifaces[0];
        let nty = "NetworkInterface";
        let pri_ip = str_field(pri, "networkIP", nty)?;
        labels.insert(LABEL_NETWORK.to_string(), str_field(pri, "network", nty)?);
        labels.insert(LABEL_SUBNETWORK.to_string(), str_field(pri, "subnetwork", nty)?);
        labels.insert(LABEL_PRIVATE_IP.to_string(), pri_ip.clone());
        labels.insert(ADDRESS_LABEL.to_string(), format!("{pri_ip}:{}", self.port));
        for iface in ifaces {
            labels.insert(format!("{GCE_LABEL}interface_ipv4_{}", sanitize_label_name(&str_field(iface, "name", nty)?)), str_field(iface, "networkIP", nty)?);
        }
        if let Some(tags @ Json::Obj(_)) = field(inst, "tags") {
            let items: Vec<String> = match field(tags, "items") {
                None | Some(Json::Null) => Vec::new(),
                Some(Json::Arr(a)) => a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect(),
                Some(v) => return Err(format!("json: cannot unmarshal {} into Go struct field Tags.tags.items of type []string", json_kind(v))),
            };
            if !items.is_empty() {
                let sep = &self.tag_separator;
                labels.insert(LABEL_TAGS.to_string(), format!("{sep}{}{sep}", items.join(sep)));
            }
        }
        if let Some(md @ Json::Obj(_)) = field(inst, "metadata") {
            for item in arr_field(md, "items", "Metadata", "compute.MetadataItems")? {
                // Protect against occasional nil pointers.
                match field(item, "value") {
                    None | Some(Json::Null) => continue,
                    Some(Json::Str(v)) => {
                        labels.insert(format!("{LABEL_METADATA}{}", sanitize_label_name(&str_field(item, "key", "MetadataItems")?)), v.clone());
                    }
                    Some(v) => return Err(format!("json: cannot unmarshal {} into Go struct field MetadataItems.metadata.items.value of type string", json_kind(v))),
                }
            }
        }
        match field(inst, "labels") {
            None | Some(Json::Null) => {}
            Some(Json::Obj(m)) => {
                for (k, v) in m {
                    let v = match v {
                        Json::Null => String::new(),
                        Json::Str(s) => s.clone(),
                        v => return Err(format!("json: cannot unmarshal {} into Go struct field Instance.labels of type string", json_kind(v))),
                    };
                    labels.insert(format!("{LABEL_LABEL}{}", sanitize_label_name(k)), v);
                }
            }
            Some(v) => return Err(format!("json: cannot unmarshal {} into Go struct field Instance.labels of type map[string]string", json_kind(v))),
        }
        let acs = arr_field(pri, "accessConfigs", nty, "compute.AccessConfig")?;
        if let Some(ac) = acs.first()
            && str_field(ac, "type", "AccessConfig")? == "ONE_TO_ONE_NAT"
        {
            labels.insert(LABEL_PUBLIC_IP.to_string(), str_field(ac, "natIP", "AccessConfig")?);
        }
        Ok(Some(labels))
    }

    fn list_instances(&self, ctx: &CancelToken) -> Result<Vec<LabelSet>, String> {
        let mut targets = Vec::new();
        let mut page_token = String::new();
        loop {
            let page = self.list_page(ctx, &page_token)?;
            for inst in arr_field(&page, "items", "InstanceList", "compute.Instance")? {
                if let Some(l) = self.instance_labels(inst)? {
                    targets.push(l);
                }
            }
            page_token = str_field(&page, "nextPageToken", "InstanceList")?;
            if page_token.is_empty() {
                return Ok(targets);
            }
        }
    }
}

impl Refresher for GceRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let targets = self.list_instances(ctx).map_err(|e| format!("error retrieving refresh targets from gce: {e}"))?;
        Ok(vec![Arc::new(Group { targets, labels: LabelSet::new(), source: format!("GCE_{}_{}", self.project, self.zone) })])
    }
}

/// Go `gce.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<GceRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        Discovery::with_options(conf, opts, Options::default())
    }

    /// `new` with non-YAML options.
    pub fn with_options(conf: &SdConfig, opts: DiscovererOptions, gopts: Options) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<GceMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        // Go: http.DefaultTransport (proxy from the environment, system roots).
        let mut hc = HttpClientConfig::default_config();
        hc.proxy_config.proxy_from_environment = true;
        let mut http = net::new_client_from_config(&hc, &net::HttpClientOptions::default()).map_err(|e| format!("error setting up communication with GCE service: {e}"))?;
        let interval = conf.refresh_interval.to_std();
        http.timeout = if interval.is_zero() { None } else { Some(interval) };
        let tokens = find_default_credentials(&http).map_err(|e| format!("error setting up communication with GCE service: {e}"))?;
        let base_path = match gopts.base_path {
            Some(b) => b,
            None => default_base_path().map_err(|e| format!("error setting up communication with GCE service: {e}"))?,
        };
        let refresher = GceRefresher {
            project: conf.project.clone(),
            zone: conf.zone.clone(),
            filter: conf.filter.clone(),
            port: conf.port,
            tag_separator: conf.tag_separator.clone(),
            base_path,
            http,
            tokens,
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "gce",
            set_name: &opts.set_name,
            interval,
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
