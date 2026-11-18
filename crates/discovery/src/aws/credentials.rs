//! The AWS credential chain of aws-sdk-go-v2 `config.LoadDefaultConfig` as the AWS SD clients use it, over plain
//! HTTP: static keys from the SD config, the environment, the shared config and credentials files (profiles with
//! static keys, `role_arn` + `source_profile` / `credential_source`, `web_identity_token_file`,
//! `credential_process`), web identity from the environment (EKS IRSA), ECS container credentials and EC2
//! instance metadata (IMDSv2 with v1 fallback). `role_arn` in the SD config assumes that role with STS on top.
//!
//! Not supported (a clear error): IAM Identity Center (SSO) profiles and `login_session` profiles.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use net::sigv4::{Credentials, EnvFn, sign_request};

use super::xml;

/// The environment lookup of the real process (empty values count as unset, as in the SDK).
pub fn std_env() -> EnvFn {
    Arc::new(|k: &str| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

/// Credentials with their expiry (None: never expire).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Creds {
    pub creds: Credentials,
    pub expires: Option<SystemTime>,
}

/// The shared config and credentials files.
#[derive(Clone, Debug, Default)]
pub struct SharedConfig {
    /// `[profile name]` / `[default]` sections of the config file, by profile name.
    pub config: BTreeMap<String, BTreeMap<String, String>>,
    /// `[name]` sections of the credentials file.
    pub credentials: BTreeMap<String, BTreeMap<String, String>>,
}

/// Parses an INI file into sections (keys lower case, values trimmed; `#`/`;` comments).
pub fn parse_ini(text: &str) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut cur = String::new();
    let mut last_key = String::new();
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') || l.starts_with(';') {
            continue;
        }
        if l.starts_with('[') && l.ends_with(']') {
            cur = l[1..l.len() - 1].trim().to_string();
            out.entry(cur.clone()).or_default();
            continue;
        }
        if (line.starts_with(' ') || line.starts_with('\t')) && !last_key.is_empty() && !l.contains('=') {
            continue; // nested sub-property values are ignored
        }
        if let Some((k, v)) = l.split_once('=') {
            last_key = k.trim().to_lowercase();
            out.entry(cur.clone()).or_default().insert(last_key.clone(), v.trim().to_string());
        }
    }
    out
}

impl SharedConfig {
    pub fn load(env: &EnvFn) -> SharedConfig {
        let home = env("HOME").unwrap_or_default();
        let cfg_file = env("AWS_CONFIG_FILE").unwrap_or_else(|| format!("{home}/.aws/config"));
        let cred_file = env("AWS_SHARED_CREDENTIALS_FILE").unwrap_or_else(|| format!("{home}/.aws/credentials"));
        let mut config = BTreeMap::new();
        for (name, sec) in std::fs::read_to_string(&cfg_file).map(|t| parse_ini(&t)).unwrap_or_default() {
            if name == "default" {
                config.insert(name, sec);
            } else if let Some(p) = name.strip_prefix("profile ") {
                config.insert(p.trim().to_string(), sec);
            }
        }
        let credentials = std::fs::read_to_string(&cred_file).map(|t| parse_ini(&t)).unwrap_or_default();
        SharedConfig { config, credentials }
    }

    pub fn has_profile(&self, p: &str) -> bool {
        self.config.contains_key(p) || self.credentials.contains_key(p)
    }

    /// A profile's keys: the credentials file wins over the config file.
    pub fn get(&self, profile: &str, key: &str) -> Option<String> {
        self.credentials.get(profile).and_then(|s| s.get(key)).or_else(|| self.config.get(profile).and_then(|s| s.get(key))).filter(|v| !v.is_empty()).cloned()
    }
}

/// The profile LoadDefaultConfig uses when none is given (AWS_PROFILE, AWS_DEFAULT_PROFILE, "default").
pub fn env_profile(env: &EnvFn) -> String {
    env("AWS_PROFILE").or_else(|| env("AWS_DEFAULT_PROFILE")).unwrap_or_else(|| "default".to_string())
}

fn now_unix() -> SystemTime {
    SystemTime::now()
}

/// Where credentials come from (one link of the chain).
#[derive(Clone, Debug)]
pub enum Source {
    Static(Credentials),
    /// STS AssumeRole with the credentials of `base`.
    AssumeRole { base: Box<Source>, role_arn: String, external_id: String, session_name: String, region: String },
    /// STS AssumeRoleWithWebIdentity.
    WebIdentity { token_file: String, role_arn: String, session_name: String, region: String },
    /// `credential_process`.
    Process(String),
    /// ECS / container credentials endpoint.
    Container { url: String, auth: String, auth_file: String },
    /// EC2 instance metadata.
    Imds,
    /// A source that cannot be used (the error is returned when credentials are needed).
    Error(String),
}

/// The plain-HTTP side of the chain: the client (Go: the SD's HTTP client set in the AWS config) and the
/// endpoints, which tests and the environment can override.
#[derive(Clone)]
pub struct Endpoints {
    pub http: net::Client,
    pub env: EnvFn,
}

impl Endpoints {
    /// The STS endpoint: AWS_ENDPOINT_URL_STS, AWS_ENDPOINT_URL, or the regional endpoint.
    pub fn sts(&self, region: &str) -> String {
        if self.env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS").as_deref() != Some("true")
            && let Some(u) = self.env("AWS_ENDPOINT_URL_STS").or_else(|| self.env("AWS_ENDPOINT_URL"))
        {
            return u;
        }
        format!("https://sts.{region}.{}", super::client::dns_suffix(region))
    }

    pub fn imds(&self) -> String {
        if let Some(e) = self.env("AWS_EC2_METADATA_SERVICE_ENDPOINT") {
            return e.trim_end_matches('/').to_string();
        }
        if self.env("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE").is_some_and(|m| m.eq_ignore_ascii_case("ipv6")) {
            return "http://[fd00:ec2::254]".to_string();
        }
        "http://169.254.169.254".to_string()
    }

    fn env(&self, k: &str) -> Option<String> {
        (self.env)(k)
    }
}

/// Resolves the base chain (before the SD config's `role_arn`), as aws-sdk-go-v2 `resolveCredentialChain`.
/// `profile` is the SD config's profile ("" = not set programmatically). Errors that LoadDefaultConfig returns
/// (for example a missing profile) are returned as Err; errors that appear only when credentials are fetched
/// become `Source::Error`.
pub fn resolve_chain(access_key: &str, secret_key: &str, profile: &str, region: &str, shared: &SharedConfig, env: &EnvFn) -> Result<Source, String> {
    if !access_key.is_empty() && !secret_key.is_empty() {
        return Ok(Source::Static(Credentials { access_key_id: access_key.to_string(), secret_access_key: secret_key.to_string(), session_token: String::new() }));
    }
    if !profile.is_empty() {
        if !shared.has_profile(profile) {
            return Err(format!("failed to get shared config profile, {profile}"));
        }
        return resolve_profile(profile, region, shared, env, 0);
    }
    if let (Some(a), Some(s)) = (env("AWS_ACCESS_KEY_ID").or_else(|| env("AWS_ACCESS_KEY")), env("AWS_SECRET_ACCESS_KEY").or_else(|| env("AWS_SECRET_KEY"))) {
        return Ok(Source::Static(Credentials { access_key_id: a, secret_access_key: s, session_token: env("AWS_SESSION_TOKEN").unwrap_or_default() }));
    }
    if let Some(tf) = env("AWS_WEB_IDENTITY_TOKEN_FILE") {
        let Some(role) = env("AWS_ROLE_ARN") else { return Ok(Source::Error("role ARN is not set".to_string())) };
        return Ok(Source::WebIdentity { token_file: tf, role_arn: role, session_name: env("AWS_ROLE_SESSION_NAME").unwrap_or_default(), region: region.to_string() });
    }
    let p = env_profile(env);
    if p != "default" && !shared.has_profile(&p) {
        return Err(format!("failed to get shared config profile, {p}"));
    }
    resolve_profile(&p, region, shared, env, 0)
}

fn resolve_profile(profile: &str, region: &str, shared: &SharedConfig, env: &EnvFn, depth: usize) -> Result<Source, String> {
    if depth > 10 {
        return Err(format!("infinite loop in credential configuration, profile {profile}"));
    }
    let g = |k: &str| shared.get(profile, k);
    let static_keys = match (g("aws_access_key_id"), g("aws_secret_access_key")) {
        (Some(a), Some(s)) => Some(Credentials { access_key_id: a, secret_access_key: s, session_token: g("aws_session_token").unwrap_or_default() }),
        _ => None,
    };
    if let Some(role) = g("role_arn") {
        let session = g("role_session_name").unwrap_or_default();
        if let Some(tf) = g("web_identity_token_file") {
            return Ok(Source::WebIdentity { token_file: tf, role_arn: role, session_name: session, region: region.to_string() });
        }
        let base = if let Some(src) = g("source_profile") {
            if src == profile {
                match static_keys {
                    Some(c) => Source::Static(c),
                    None => return Err(format!("failed to get shared config profile, {src}")),
                }
            } else {
                if !shared.has_profile(&src) {
                    return Err(format!("failed to get shared config profile, {src}"));
                }
                resolve_profile(&src, region, shared, env, depth + 1)?
            }
        } else if let Some(cs) = g("credential_source") {
            match cs.as_str() {
                "Environment" => match (env("AWS_ACCESS_KEY_ID").or_else(|| env("AWS_ACCESS_KEY")), env("AWS_SECRET_ACCESS_KEY").or_else(|| env("AWS_SECRET_KEY"))) {
                    (Some(a), Some(s)) => Source::Static(Credentials { access_key_id: a, secret_access_key: s, session_token: env("AWS_SESSION_TOKEN").unwrap_or_default() }),
                    _ => return Err("credential source Environment: no credentials in the environment".to_string()),
                },
                "Ec2InstanceMetadata" => Source::Imds,
                "EcsContainer" => container_source(env).unwrap_or_else(|| Source::Error("container credentials endpoint is not configured".to_string())),
                other => return Err(format!("unsupported credential source {}", yaml::go_quote(other))),
            }
        } else {
            return Err(format!("profile {}: role_arn requires source_profile, credential_source or web_identity_token_file", yaml::go_quote(profile)));
        };
        return Ok(Source::AssumeRole { base: Box::new(base), role_arn: role, external_id: g("external_id").unwrap_or_default(), session_name: session, region: region.to_string() });
    }
    if g("sso_session").is_some() || g("sso_start_url").is_some() || g("sso_account_id").is_some() {
        return Ok(Source::Error(format!("profile {}: IAM Identity Center (SSO) credentials are not supported by prometheus-rs", yaml::go_quote(profile))));
    }
    if g("login_session").is_some() {
        return Ok(Source::Error(format!("profile {}: login_session credentials are not supported by prometheus-rs", yaml::go_quote(profile))));
    }
    if let Some(c) = static_keys {
        return Ok(Source::Static(c));
    }
    if let Some(p) = g("credential_process") {
        return Ok(Source::Process(p));
    }
    if let Some(s) = container_source(env) {
        return Ok(s);
    }
    Ok(Source::Imds)
}

fn container_source(env: &EnvFn) -> Option<Source> {
    let url = if let Some(r) = env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
        format!("http://169.254.170.2{r}")
    } else {
        env("AWS_CONTAINER_CREDENTIALS_FULL_URI")?
    };
    Some(Source::Container { url, auth: env("AWS_CONTAINER_AUTHORIZATION_TOKEN").unwrap_or_default(), auth_file: env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE").unwrap_or_default() })
}

/// A cached credentials provider (Go `aws.CredentialsCache` around the chain).
pub struct Provider {
    source: Source,
    ep: Endpoints,
    cache: Mutex<Option<Creds>>,
}

impl Provider {
    pub fn new(source: Source, ep: Endpoints) -> Provider {
        Provider { source, ep, cache: Mutex::new(None) }
    }

    /// The credentials, fetched again 1 minute before they expire. Errors read as Go's
    /// "failed to refresh cached credentials, ..." chain.
    pub fn retrieve(&self) -> Result<Credentials, String> {
        let mut g = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = g.as_ref()
            && c.expires.is_none_or(|e| now_unix() + Duration::from_secs(60) < e)
        {
            return Ok(c.creds.clone());
        }
        let c = fetch(&self.source, &self.ep).map_err(|e| format!("failed to refresh cached credentials, {e}"))?;
        let out = c.creds.clone();
        *g = Some(c);
        Ok(out)
    }
}

fn fetch(src: &Source, ep: &Endpoints) -> Result<Creds, String> {
    match src {
        Source::Static(c) => {
            if c.access_key_id.is_empty() || c.secret_access_key.is_empty() {
                return Err("static credentials are empty".to_string());
            }
            Ok(Creds { creds: c.clone(), expires: None })
        }
        Source::Error(e) => Err(e.clone()),
        Source::AssumeRole { base, role_arn, external_id, session_name, region } => {
            let b = fetch(base, ep)?;
            assume_role(ep, &b.creds, role_arn, external_id, session_name, region)
        }
        Source::WebIdentity { token_file, role_arn, session_name, region } => web_identity(ep, token_file, role_arn, session_name, region),
        Source::Process(cmd) => process(cmd),
        Source::Container { url, auth, auth_file } => container(ep, url, auth, auth_file),
        Source::Imds => imds_credentials(ep),
    }
}

fn session_name_default() -> String {
    format!("aws-go-sdk-{}", SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0))
}

fn sts_call(ep: &Endpoints, region: &str, op: &str, form: Vec<(String, String)>, sign_with: Option<&Credentials>) -> Result<xml::Element, String> {
    let body = yaml::url::encode_query(&form);
    let url = ep.sts(region);
    let mut req = net::Request::new("POST", &url, body.into_bytes()).map_err(|e| format!("operation error STS: {op}, {}", e.msg))?;
    if req.url.path.is_empty() {
        req.url.path = "/".to_string();
    }
    req.header.set("Content-Type", "application/x-www-form-urlencoded");
    req.header.set("User-Agent", super::client::USER_AGENT);
    if let Some(c) = sign_with {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        sign_request(&mut req, c, "sts", region, now);
    }
    let resp = ep.http.do_request(req).map_err(|e| format!("operation error STS: {op}, https response error StatusCode: 0, RequestID: , request send failed, {}", e.msg))?;
    let text = String::from_utf8_lossy(&resp.body).into_owned();
    if resp.status / 100 != 2 {
        let (code, msg, rid) = match xml::parse(&text) {
            Ok(r) => (r.text_at("Error/Code").unwrap_or("").to_string(), r.text_at("Error/Message").unwrap_or("").to_string(), r.text_at("RequestId").unwrap_or("").to_string()),
            Err(_) => (String::new(), String::new(), String::new()),
        };
        return Err(format!("operation error STS: {op}, https response error StatusCode: {}, RequestID: {rid}, api error {code}: {msg}", resp.status));
    }
    xml::parse(&text).map_err(|e| format!("operation error STS: {op}, deserialization failed, {e}"))
}

fn sts_creds(root: &xml::Element, result: &str) -> Result<Creds, String> {
    let c = root.path(&format!("{result}/Credentials")).ok_or_else(|| "STS response has no credentials".to_string())?;
    Ok(Creds {
        creds: Credentials {
            access_key_id: c.text_at("AccessKeyId").unwrap_or("").to_string(),
            secret_access_key: c.text_at("SecretAccessKey").unwrap_or("").to_string(),
            session_token: c.text_at("SessionToken").unwrap_or("").to_string(),
        },
        expires: c.text_at("Expiration").and_then(crate::util::parse_rfc3339),
    })
}

/// STS AssumeRole (stscreds.AssumeRoleProvider).
pub fn assume_role(ep: &Endpoints, base: &Credentials, role_arn: &str, external_id: &str, session_name: &str, region: &str) -> Result<Creds, String> {
    let session = if session_name.is_empty() { session_name_default() } else { session_name.to_string() };
    let mut form: Vec<(String, String)> = vec![("Action".into(), "AssumeRole".into()), ("Version".into(), "2011-06-15".into()), ("RoleArn".into(), role_arn.to_string()), ("RoleSessionName".into(), session), ("DurationSeconds".into(), "900".into())];
    if !external_id.is_empty() {
        form.push(("ExternalId".into(), external_id.to_string()));
    }
    let root = sts_call(ep, region, "AssumeRole", form, Some(base))?;
    sts_creds(&root, "AssumeRoleResult")
}

fn web_identity(ep: &Endpoints, token_file: &str, role_arn: &str, session_name: &str, region: &str) -> Result<Creds, String> {
    let token = std::fs::read_to_string(token_file).map_err(|e| format!("failed to retrieve jwt from provide source, unable to read file at {token_file}: {}", net::types::go_io_error(&e)))?;
    let session = if session_name.is_empty() { session_name_default() } else { session_name.to_string() };
    let form: Vec<(String, String)> = vec![
        ("Action".into(), "AssumeRoleWithWebIdentity".into()),
        ("Version".into(), "2011-06-15".into()),
        ("RoleArn".into(), role_arn.to_string()),
        ("RoleSessionName".into(), session),
        ("WebIdentityToken".into(), token.trim().to_string()),
    ];
    let root = sts_call(ep, region, "AssumeRoleWithWebIdentity", form, None)?;
    sts_creds(&root, "AssumeRoleWithWebIdentityResult")
}

fn creds_from_json(j: &json::Json, token_key: &str) -> Result<Creds, String> {
    let a = j.str_of("AccessKeyId");
    let s = j.str_of("SecretAccessKey");
    if a.is_empty() || s.is_empty() {
        return Err("credentials response is missing AccessKeyId or SecretAccessKey".to_string());
    }
    Ok(Creds {
        creds: Credentials { access_key_id: a.to_string(), secret_access_key: s.to_string(), session_token: j.str_of(token_key).to_string() },
        expires: j.get("Expiration").and_then(|e| e.as_str()).and_then(crate::util::parse_rfc3339),
    })
}

fn process(cmd: &str) -> Result<Creds, String> {
    let out = std::process::Command::new("sh").arg("-c").arg(cmd).stdin(std::process::Stdio::null()).stderr(std::process::Stdio::inherit()).output().map_err(|e| format!("error in credential_process, {e}"))?;
    if !out.status.success() {
        return Err(format!("error in credential_process, {}", out.status));
    }
    let j = json::Json::parse(&String::from_utf8_lossy(&out.stdout)).map_err(|e| format!("parse failed of process output: {e}"))?;
    if j.i64_of("Version") != 1 {
        return Err("wrong version in process output (not 1)".to_string());
    }
    creds_from_json(&j, "SessionToken")
}

fn container(ep: &Endpoints, url: &str, auth: &str, auth_file: &str) -> Result<Creds, String> {
    let mut req = net::Request::get(url).map_err(|e| e.msg)?;
    let mut token = auth.to_string();
    if !auth_file.is_empty() {
        token = std::fs::read_to_string(auth_file).map_err(|e| format!("failed to read authorization token from {auth_file}: {}", net::types::go_io_error(&e)))?.trim().to_string();
    }
    if !token.is_empty() {
        req.header.set("Authorization", &token);
    }
    req.header.set("Accept", "application/json");
    req.timeout = Some(Duration::from_secs(5));
    let resp = ep.http.do_request(req).map_err(|e| format!("failed to load credentials, {}", e.msg))?;
    if resp.status != 200 {
        return Err(format!("failed to load credentials, StatusCode: {}", resp.status));
    }
    let j = json::Json::parse_bytes(&resp.body).map_err(|e| format!("failed to load credentials, {e}"))?;
    creds_from_json(&j, "Token")
}

/// IMDSv2 session token (None: fall back to IMDSv1 for this request, as the SDK does on token errors).
fn imds_token(ep: &Endpoints) -> Result<Option<String>, String> {
    let mut req = net::Request::new("PUT", &format!("{}/latest/api/token", ep.imds()), Vec::new()).map_err(|e| e.msg)?;
    req.header.set("X-Aws-Ec2-Metadata-Token-Ttl-Seconds", "21600");
    req.timeout = Some(Duration::from_secs(5));
    match ep.http.do_request(req) {
        Ok(r) if r.status == 200 => Ok(Some(String::from_utf8_lossy(&r.body).into_owned())),
        Ok(r) if r.status == 401 => Err("cannot get API token, StatusCode: 401".to_string()),
        r => {
            if ep.env("AWS_EC2_METADATA_V1_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
                return Err(match r {
                    Ok(r) => format!("failed to get API token, StatusCode: {}", r.status),
                    Err(e) => format!("failed to get API token, {}", e.msg),
                });
            }
            Ok(None)
        }
    }
}

/// GET of an IMDS path (Go `imds.Client.GetMetadata` and friends). `op` names the operation in errors.
pub fn imds_get(ep: &Endpoints, op: &str, path: &str) -> Result<String, String> {
    if ep.env("AWS_EC2_METADATA_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return Err(format!("operation error ec2imds: {op}, access disabled to EC2 IMDS via client option, or \"AWS_EC2_METADATA_DISABLED\" environment variable"));
    }
    let token = imds_token(ep).map_err(|e| format!("operation error ec2imds: {op}, {e}"))?;
    let mut req = net::Request::get(&format!("{}{path}", ep.imds())).map_err(|e| e.msg)?;
    if let Some(t) = token {
        req.header.set("X-Aws-Ec2-Metadata-Token", &t);
    }
    req.timeout = Some(Duration::from_secs(5));
    let resp = ep.http.do_request(req).map_err(|e| format!("operation error ec2imds: {op}, request canceled, {}", e.msg))?;
    if resp.status != 200 {
        return Err(format!("operation error ec2imds: {op}, http response error StatusCode: {}, request to EC2 IMDS failed", resp.status));
    }
    Ok(String::from_utf8_lossy(&resp.body).into_owned())
}

fn imds_credentials(ep: &Endpoints) -> Result<Creds, String> {
    let roles = imds_get(ep, "GetMetadata", "/latest/meta-data/iam/security-credentials/").map_err(|e| format!("no EC2 IMDS role found, {e}"))?;
    let role = roles.lines().map(str::trim).find(|l| !l.is_empty()).ok_or_else(|| "no EC2 IMDS role found, empty EC2 IMDS role list".to_string())?;
    let body = imds_get(ep, "GetMetadata", &format!("/latest/meta-data/iam/security-credentials/{role}")).map_err(|e| format!("failed to get {role} EC2 IMDS role credentials, {e}"))?;
    let j = json::Json::parse(&body).map_err(|e| format!("failed to decode {role} EC2 IMDS role credentials, {e}"))?;
    if !j.str_of("Code").is_empty() && j.str_of("Code") != "Success" {
        return Err(format!("failed to get {role} EC2 IMDS role credentials, {}: {}", j.str_of("Code"), j.str_of("Message")));
    }
    creds_from_json(&j, "Token")
}
