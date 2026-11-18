//! The part of github.com/ovh/go-ovh v1.9.0 that the OVHcloud SD uses: `NewClient` (endpoint aliases, the
//! `OVH_*` environment and the ovh.conf INI files, the auth method checks) and `Client.Get` (the `/auth/time`
//! server time call, `X-Ovh-*` headers with the SHA1 signature, API errors, JSON decoding).
//!
//! Differs from go-ovh: the OAuth2 client credentials flow (`client_id`/`client_secret` from the environment
//! or ovh.conf) is not built: Prometheus always sets an application key, so go-ovh refuses that mix with
//! "can't use multiple authentication methods" before it would be used.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use json::Json;

use crate::sd_json::{self, StructTy, Ty};

/// Go `DefaultTimeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(180);

pub const OVH_EU: &str = "https://eu.api.ovh.com/1.0";
pub const OVH_CA: &str = "https://ca.api.ovh.com/1.0";
pub const OVH_US: &str = "https://api.us.ovhcloud.com/1.0";
pub const KIMSUFI_EU: &str = "https://eu.api.kimsufi.com/1.0";
pub const KIMSUFI_CA: &str = "https://ca.api.kimsufi.com/1.0";
pub const SOYOUSTART_EU: &str = "https://eu.api.soyoustart.com/1.0";
pub const SOYOUSTART_CA: &str = "https://ca.api.soyoustart.com/1.0";

/// Go `Endpoints`.
pub fn endpoint_alias(name: &str) -> Option<&'static str> {
    Some(match name {
        "ovh-eu" => OVH_EU,
        "ovh-ca" => OVH_CA,
        "ovh-us" => OVH_US,
        "kimsufi-eu" => KIMSUFI_EU,
        "kimsufi-ca" => KIMSUFI_CA,
        "soyoustart-eu" => SOYOUSTART_EU,
        "soyoustart-ca" => SOYOUSTART_CA,
        _ => return None,
    })
}

/// Go `tokensURLs` (endpoints that support OAuth2).
fn has_token_url(endpoint: &str) -> bool {
    endpoint == OVH_EU || endpoint == OVH_CA || endpoint == OVH_US
}

/// Go `configPaths`.
const CONFIG_PATHS: [&str; 3] = ["/etc/ovh.conf", "~/.ovh.conf", "./ovh.conf"];

/// An INI file as go-ini loads it: sections of keys (later files override earlier ones).
#[derive(Default)]
struct Ini {
    sections: Vec<(String, Vec<(String, String)>)>,
}

impl Ini {
    fn section_mut(&mut self, name: &str) -> &mut Vec<(String, String)> {
        if let Some(i) = self.sections.iter().position(|(n, _)| n == name) {
            return &mut self.sections[i].1;
        }
        self.sections.push((name.to_string(), Vec::new()));
        &mut self.sections.last_mut().unwrap().1
    }

    fn parse_into(&mut self, text: &str) {
        let mut section = "DEFAULT".to_string();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            if let Some(rest) = line.strip_prefix('[') {
                if let Some(end) = rest.find(']') {
                    section = rest[..end].trim().to_string();
                    self.section_mut(&section);
                }
                continue;
            }
            let Some(i) = line.find(['=', ':']) else { continue };
            let key = line[..i].trim().to_string();
            let mut value = line[i + 1..].trim();
            let quoted = value.len() >= 2 && ((value.starts_with('"') && value.ends_with('"')) || (value.starts_with('\'') && value.ends_with('\'')) || (value.starts_with('`') && value.ends_with('`')));
            if quoted {
                value = &value[1..value.len() - 1];
            } else if let Some(c) = value.find(['#', ';']) {
                value = value[..c].trim_end();
            }
            let sec = self.section_mut(&section);
            match sec.iter_mut().find(|(k, _)| *k == key) {
                Some(kv) => kv.1 = value.to_string(),
                None => sec.push((key, value.to_string())),
            }
        }
    }

    fn get(&self, section: &str, key: &str) -> Option<&str> {
        let (_, kv) = self.sections.iter().find(|(n, _)| n == section)?;
        kv.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }
}

/// Go `currentUserHome`.
fn current_user_home() -> Option<String> {
    std::env::var("HOME").ok().filter(|h| !h.is_empty())
}

/// Go `loadINI` (`ini.LooseLoad`: missing files are skipped).
fn load_ini() -> Ini {
    let mut ini = Ini::default();
    let mut home: Option<Option<String>> = None;
    for p in CONFIG_PATHS {
        let path = if let Some(rest) = p.strip_prefix("~/") {
            let h = home.get_or_insert_with(current_user_home);
            match h {
                Some(h) => format!("{h}/{rest}"),
                None => continue,
            }
        } else {
            p.to_string()
        };
        if let Ok(text) = std::fs::read_to_string(&path) {
            ini.parse_into(&text);
        }
    }
    ini
}

/// Go `getConfigValue`: `OVH_<NAME>` from the environment, else the INI section, else the default.
fn get_config_value(cfg: &Ini, section: &str, name: &str, def: &str) -> String {
    if let Ok(v) = std::env::var(format!("OVH_{}", name.to_uppercase()))
        && !v.is_empty()
    {
        return v;
    }
    cfg.get(section, name).unwrap_or(def).to_string()
}

/// Go `ovh.Client`.
pub struct Client {
    pub access_token: String,
    pub app_key: String,
    pub app_secret: String,
    pub consumer_key: String,
    pub client_id: String,
    pub client_secret: String,
    endpoint: String,
    http: net::Client,
    /// Local time minus server time, in nanoseconds (Go `timeDelta`).
    time_delta: Option<i128>,
    pub timeout: Duration,
}

/// Go `ovh.NewClient`.
pub fn new_client(endpoint: &str, app_key: &str, app_secret: &str, consumer_key: &str) -> Result<Client, String> {
    let mut cfg = net::HttpClientConfig::default_config();
    // Go's http.DefaultTransport takes the proxy from the environment.
    cfg.proxy_config.proxy_from_environment = true;
    let http = net::new_client_from_config(&cfg, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
    let mut c = Client {
        access_token: String::new(),
        app_key: app_key.to_string(),
        app_secret: app_secret.to_string(),
        consumer_key: consumer_key.to_string(),
        client_id: String::new(),
        client_secret: String::new(),
        endpoint: String::new(),
        http,
        time_delta: None,
        timeout: DEFAULT_TIMEOUT,
    };
    c.load_config(endpoint)?;
    Ok(c)
}

impl Client {
    /// Go `Client.Endpoint`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Go `loadConfig`.
    fn load_config(&mut self, endpoint_name: &str) -> Result<(), String> {
        if endpoint_name.ends_with('/') {
            return Err("endpoint name cannot have a trailing slash".to_string());
        }
        let cfg = load_ini();
        let endpoint_name = if endpoint_name.is_empty() { get_config_value(&cfg, "default", "endpoint", "ovh-eu") } else { endpoint_name.to_string() };
        let fill = |v: &mut String, name: &str| {
            if v.is_empty() {
                *v = get_config_value(&cfg, &endpoint_name, name, "");
            }
        };
        fill(&mut self.access_token, "access_token");
        fill(&mut self.app_key, "application_key");
        fill(&mut self.app_secret, "application_secret");
        fill(&mut self.consumer_key, "consumer_key");
        fill(&mut self.client_id, "client_id");
        fill(&mut self.client_secret, "client_secret");

        let mut methods = Vec::new();
        if !self.app_key.is_empty() || !self.app_secret.is_empty() || !self.consumer_key.is_empty() {
            methods.push("application_key/application_secret");
        }
        if !self.client_id.is_empty() || !self.client_secret.is_empty() {
            methods.push("client_id/client_secret");
        }
        if !self.access_token.is_empty() {
            methods.push("access_token");
        }
        if methods.len() > 1 {
            return Err(format!("can't use multiple authentication methods: {}", methods.join(", ")));
        }
        if methods.is_empty() {
            return Err("missing authentication information, you need to provide one of the following: application_key/application_secret, client_id/client_secret, or access_token".to_string());
        }
        if self.client_id.is_empty() != self.client_secret.is_empty() {
            return Err("invalid oauth2 config, both client_id and client_secret must be given".to_string());
        }
        if self.app_key.is_empty() != self.app_secret.is_empty() {
            return Err("invalid authentication config, both application_key and application_secret must be given".to_string());
        }
        self.endpoint = if endpoint_name.contains('/') { endpoint_name.clone() } else { endpoint_alias(&endpoint_name).unwrap_or("").to_string() };
        if self.endpoint.is_empty() {
            return Err(format!("unknown endpoint '{endpoint_name}', consider checking 'Endpoints' list or using an URL"));
        }
        if !self.client_id.is_empty() && !has_token_url(&self.endpoint) {
            return Err(format!("oauth2 authentication is not compatible with endpoint {}", yaml::go_quote(&self.endpoint)));
        }
        Ok(())
    }

    /// Go `Client.Get` (an authenticated GET decoded into a value of type `ty`; `None` for an empty body).
    pub fn get(&mut self, path: &str, ty: &Ty) -> Result<Option<Json>, String> {
        self.call_api("GET", path, ty, true)
    }

    /// Go `getTimeDelta`.
    fn get_time_delta(&mut self) -> Result<i128, String> {
        if let Some(d) = self.time_delta {
            return Ok(d);
        }
        let server = self.call_api("GET", "/auth/time", &sd_json::INT64, false)?.and_then(|v| v.as_i64()).unwrap_or(0);
        let d = now_nanos() - server as i128 * 1_000_000_000;
        self.time_delta = Some(d);
        Ok(d)
    }

    /// Go `NewRequest` (without a body: the SD only reads).
    fn new_request(&mut self, method: &str, path: &str, need_auth: bool) -> Result<net::Request, String> {
        let target = get_target(&self.endpoint, path);
        let mut req = net::Request::new(method, &target, Vec::new()).map_err(|e| e.to_string())?;
        if !self.app_key.is_empty() {
            req.header.add("X-Ovh-Application", &self.app_key);
        }
        req.header.add("Accept", "application/json");
        if need_auth {
            if !self.app_key.is_empty() {
                let delta = self.get_time_delta()?;
                let timestamp = (now_nanos() - delta).div_euclid(1_000_000_000) as i64;
                req.header.add("X-Ovh-Timestamp", &timestamp.to_string());
                req.header.add("X-Ovh-Consumer", &self.consumer_key);
                let to_sign = format!("{}+{}+{}+{}+{}+{}", self.app_secret, self.consumer_key, method, target, "", timestamp);
                req.header.add("X-Ovh-Signature", &format!("$1${}", crypto::hex(&crypto::sha1(to_sign.as_bytes()))));
            } else if !self.access_token.is_empty() {
                req.header.set("Authorization", &format!("Bearer {}", self.access_token));
            }
        }
        req.header.set("User-Agent", "github.com/ovh/go-ovh");
        req.timeout = Some(self.timeout);
        Ok(req)
    }

    /// Go `CallAPI` + `UnmarshalResponse`.
    fn call_api(&mut self, method: &str, path: &str, ty: &Ty, need_auth: bool) -> Result<Option<Json>, String> {
        let req = self.new_request(method, path, need_auth)?;
        let resp = self.http.do_request(req).map_err(|e| e.to_string())?;
        if !(200..300).contains(&resp.status) {
            let mut e = ApiError { code: resp.status as i64, ..Default::default() };
            match sd_json::unmarshal(&resp.body, &API_ERROR) {
                Ok(v) => {
                    e.class = sd_json::str_of(&v, "class");
                    e.message = sd_json::str_of(&v, "message");
                    if let Some(Json::Obj(d)) = sd_json::val(&v, "details") {
                        e.details = d.iter().map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string())).collect();
                    }
                }
                Err(_) => e.message = String::from_utf8_lossy(&resp.body).into_owned(),
            }
            e.query_id = resp.header.get_str("X-Ovh-QueryID").to_string();
            return Err(e.to_string());
        }
        if resp.body.is_empty() {
            return Ok(None);
        }
        sd_json::decode_stream(&resp.body, ty).map(Some)
    }
}

const API_ERROR: Ty = Ty::Struct(&StructTy { go: "ovh.APIError", fields: &[("class", Ty::Str), ("message", Ty::Str), ("details", Ty::Map(&Ty::Str))] });

/// Go `ovh.APIError`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ApiError {
    pub class: String,
    pub message: String,
    pub details: Vec<(String, String)>,
    pub code: i64,
    pub query_id: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OVHcloud API error (status code {}): ", self.code)?;
        if !self.class.is_empty() {
            write!(f, "{}: ", self.class)?;
        }
        let detail = |k: &str| self.details.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str()).filter(|v| !v.is_empty());
        let missing: Vec<&str> = [detail("unauthorizedActionsByAuthentication"), detail("unauthorizedActionsByIAM")].into_iter().flatten().collect();
        let mut message = self.message.clone();
        if !missing.is_empty() {
            message.push_str(&format!(" (missing IAM permissions: {})", missing.join(", ")));
        }
        f.write_str(&yaml::go_quote(&message))?;
        if !self.query_id.is_empty() {
            write!(f, " (X-OVH-Query-Id: {})", self.query_id)?;
        }
        Ok(())
    }
}

/// Go `getTarget`.
pub fn get_target(endpoint: &str, path: &str) -> String {
    if endpoint.ends_with("/1.0") && (path.starts_with("/v1/") || path.starts_with("/v2/")) {
        return format!("{}{}", &endpoint[..endpoint.len() - 4], path);
    }
    format!("{endpoint}{path}")
}

fn now_nanos() -> i128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    }
}
