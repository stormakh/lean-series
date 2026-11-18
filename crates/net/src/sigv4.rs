//! AWS Signature Version 4 request signing (github.com/prometheus/sigv4 with the aws-sdk-go-v2 signer), without
//! an AWS SDK: static keys, the environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN), the
//! shared credentials and config files (profile), and STS AssumeRole for role_arn.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use yaml::{Decoder, Error as YError, Marshal, Node, Secret, Struct, Unmarshal, Value};

use crate::roundtrip::RoundTripper;
use crate::types::{Error, Request, Response};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SigV4Config {
    pub region: String,
    pub access_key: String,
    pub secret_key: Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub session_name: String,
    pub tags: BTreeMap<String, String>,
    pub use_fips_sts_endpoint: bool,
    pub service_name: String,
}
yaml::yaml_struct!(SigV4Config, "sigv4.plain", [
    (region: "region", omitempty)
    (access_key: "access_key", omitempty)
    (secret_key: "secret_key", omitempty)
    (profile: "profile", omitempty)
    (role_arn: "role_arn", omitempty)
    (external_id: "external_id", omitempty)
    (session_name: "session_name", omitempty)
    (tags: "tags", omitempty)
    (use_fips_sts_endpoint: "use_fips_sts_endpoint", omitempty)
    (service_name: "service_name", omitempty)
]);
impl Unmarshal for SigV4Config {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = SigV4Config::default();
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "sigv4.plain".to_string()
    }
}
impl Marshal for SigV4Config {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

/// `^[\w+=,.@-]{2,64}$`
fn session_name_ok(s: &str) -> bool {
    (2..=64).contains(&s.len()) && s.bytes().all(|c| c.is_ascii_alphanumeric() || b"_+=,.@-".contains(&c))
}

impl SigV4Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.access_key.is_empty() != self.secret_key.is_empty() {
            return Err("must provide a AWS SigV4 Access key and Secret Key if credentials are specified in the SigV4 config".to_string());
        }
        if !self.external_id.is_empty() && self.role_arn.is_empty() {
            return Err("external_id can only be used with role_arn".to_string());
        }
        if !self.session_name.is_empty() && self.role_arn.is_empty() {
            return Err("session_name can only be used with role_arn".to_string());
        }
        if !self.session_name.is_empty() && !session_name_ok(&self.session_name) {
            return Err(r"session_name must match ^[\w+=,.@-]{2,64}$ (2-64 alphanumeric and +=,.@- characters)".to_string());
        }
        if !self.tags.is_empty() && self.role_arn.is_empty() {
            return Err("tags can only be used with role_arn".to_string());
        }
        for (k, v) in &self.tags {
            if k.is_empty() {
                return Err("tag key must not be empty".to_string());
            }
            if k.len() > 128 {
                return Err(format!("tag key {} exceeds maximum length of 128", yaml::go_quote(k)));
            }
            if v.len() > 256 {
                return Err(format!("tag value for key {} exceeds maximum length of 256", yaml::go_quote(k)));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
}

/// Where the environment comes from (tests replace it).
pub type EnvFn = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

fn std_env() -> EnvFn {
    Arc::new(|k: &str| std::env::var(k).ok().filter(|v| !v.is_empty()))
}

/// Parses an INI file into sections.
fn ini(text: &str) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut cur = String::new();
    for line in text.lines() {
        let l = line.trim();
        if l.is_empty() || l.starts_with('#') || l.starts_with(';') {
            continue;
        }
        if l.starts_with('[') && l.ends_with(']') {
            cur = l[1..l.len() - 1].trim().to_string();
            continue;
        }
        if let Some((k, v)) = l.split_once('=') {
            out.entry(cur.clone()).or_default().insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    out
}

fn home(env: &EnvFn) -> String {
    env("HOME").unwrap_or_default()
}

/// The default chain of aws-sdk-go-v2 that does not need the network: env keys, then the shared credentials
/// file, then the shared config file (for the profile).
fn resolve_credentials(cfg: &SigV4Config, env: &EnvFn) -> Result<(Credentials, String), String> {
    let profile = if !cfg.profile.is_empty() { cfg.profile.clone() } else { env("AWS_PROFILE").unwrap_or_else(|| "default".to_string()) };
    let cfg_file = env("AWS_CONFIG_FILE").unwrap_or_else(|| format!("{}/.aws/config", home(env)));
    let cred_file = env("AWS_SHARED_CREDENTIALS_FILE").unwrap_or_else(|| format!("{}/.aws/credentials", home(env)));
    let config_ini = std::fs::read_to_string(&cfg_file).map(|t| ini(&t)).unwrap_or_default();
    let cred_ini = std::fs::read_to_string(&cred_file).map(|t| ini(&t)).unwrap_or_default();
    let cfg_section = if profile == "default" { config_ini.get("default") } else { config_ini.get(&format!("profile {profile}")).or_else(|| config_ini.get(&profile)) };
    let mut region = cfg.region.clone();
    if region.is_empty() {
        region = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")).or_else(|| cfg_section.and_then(|s| s.get("region").cloned())).unwrap_or_default();
    }
    if !cfg.access_key.is_empty() && !cfg.secret_key.is_empty() {
        return Ok((Credentials { access_key_id: cfg.access_key.clone(), secret_access_key: cfg.secret_key.0.clone(), session_token: String::new() }, region));
    }
    if cfg.profile.is_empty()
        && let (Some(a), Some(s)) = (env("AWS_ACCESS_KEY_ID").or_else(|| env("AWS_ACCESS_KEY")), env("AWS_SECRET_ACCESS_KEY").or_else(|| env("AWS_SECRET_KEY"))) {
            return Ok((Credentials { access_key_id: a, secret_access_key: s, session_token: env("AWS_SESSION_TOKEN").unwrap_or_default() }, region));
        }
    for sec in [cred_ini.get(&profile), cfg_section].into_iter().flatten() {
        if let (Some(a), Some(s)) = (sec.get("aws_access_key_id"), sec.get("aws_secret_access_key")) {
            return Ok((Credentials { access_key_id: a.clone(), secret_access_key: s.clone(), session_token: sec.get("aws_session_token").cloned().unwrap_or_default() }, region));
        }
    }
    Err("failed to refresh cached credentials, no EC2 IMDS role found, operation error ec2imds: GetMetadata, access disabled to EC2 IMDS via client option, or \"AWS_EC2_METADATA_DISABLED\" environment variable".to_string())
}

/// UTC civil time from Unix seconds: (y, m, d, hh, mm, ss).
fn civil(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d, (rem / 3600) as u32, (rem % 3600 / 60) as u32, (rem % 60) as u32)
}

/// "20060102T150405Z"
pub fn amz_date(secs: u64) -> String {
    let (y, m, d, hh, mm, ss) = civil(secs);
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

/// aws-sdk-go-v2 httpbinding.EscapePath(path, false): escapes all but unreserved characters and '/'.
fn escape_path(p: &str) -> String {
    let mut o = String::new();
    for &c in p.as_bytes() {
        if c.is_ascii_alphanumeric() || b"-_.~/".contains(&c) {
            o.push(c as char);
        } else {
            o.push_str(&format!("%{c:02X}"));
        }
    }
    o
}

fn strip_excess_spaces(s: &str) -> String {
    let t = s.trim_matches(' ');
    let mut o = String::with_capacity(t.len());
    let mut sp = false;
    for c in t.chars() {
        if c == ' ' {
            if !sp {
                o.push(c);
            }
            sp = true;
        } else {
            sp = false;
            o.push(c);
        }
    }
    o
}

const IGNORED_HEADERS: [&str; 5] = ["authorization", "user-agent", "x-amzn-trace-id", "expect", "transfer-encoding"];

/// Signs `req` in place (aws-sdk-go-v2 `Signer.SignHTTP`): sets X-Amz-Date, X-Amz-Security-Token and
/// Authorization.
pub fn sign_request(req: &mut Request, creds: &Credentials, service: &str, region: &str, unix_secs: u64) {
    let date = amz_date(unix_secs);
    req.header.set("X-Amz-Date", &date);
    if !creds.session_token.is_empty() {
        req.header.set("X-Amz-Security-Token", &creds.session_token);
    }
    // SanitizeHostForHeader: a default port is dropped from the signed host
    let mut host = if req.host.is_empty() { req.url.host.clone() } else { req.host.clone() };
    if let Some(i) = host.rfind(':') {
        let port = &host[i + 1..];
        let has_port = !host[i..].contains(']');
        let default = (req.url.scheme == "http" && port == "80") || (req.url.scheme == "https" && port == "443");
        if has_port && default {
            host.truncate(i);
            req.host = host.clone();
        }
    }
    let scope = format!("{}/{}/{}/aws4_request", &date[..8], region, service);
    let mut names: Vec<String> = vec!["host".to_string()];
    let mut signed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    signed.insert("host".to_string(), vec![host.clone()]);
    if !req.body.is_empty() {
        names.push("content-length".to_string());
        signed.insert("content-length".to_string(), vec![req.body.len().to_string()]);
    }
    for (k, v) in req.header.iter() {
        let lk = k.to_lowercase();
        if IGNORED_HEADERS.contains(&lk.as_str()) || lk == "content-length" {
            continue;
        }
        if let Some(vs) = signed.get_mut(&lk) {
            vs.push(v.to_string());
            continue;
        }
        names.push(lk.clone());
        signed.insert(lk, vec![v.to_string()]);
    }
    names.sort();
    let signed_headers = names.join(";");
    let mut canon_headers = String::new();
    for n in &names {
        canon_headers.push_str(n);
        canon_headers.push(':');
        if n == "host" {
            canon_headers.push_str(&strip_excess_spaces(&host));
        } else {
            let vs: Vec<String> = signed[n].iter().map(|v| strip_excess_spaces(v).trim().to_string()).collect();
            canon_headers.push_str(&vs.join(","));
        }
        canon_headers.push('\n');
    }
    let (mut q, _) = yaml::url::parse_query(&req.url.raw_query);
    q.sort();
    let query = yaml::url::encode_query(&q).replace('+', "%20");
    let uri = {
        let p = req.url.escaped_path();
        escape_path(if p.is_empty() { "/" } else { &p })
    };
    let payload_hash = crypto::hex(&crypto::sha256(&req.body));
    let canonical = [req.method.as_str(), &uri, &query, &canon_headers, &signed_headers, &payload_hash].join("\n");
    let to_sign = format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", date, scope, crypto::hex(&crypto::sha256(canonical.as_bytes())));
    let k_date = crypto::hmac_sha256(format!("AWS4{}", creds.secret_access_key).as_bytes(), date[..8].as_bytes());
    let k_region = crypto::hmac_sha256(&k_date, region.as_bytes());
    let k_service = crypto::hmac_sha256(&k_region, service.as_bytes());
    let k_signing = crypto::hmac_sha256(&k_service, b"aws4_request");
    let sig = crypto::hex(&crypto::hmac_sha256(&k_signing, to_sign.as_bytes()));
    req.header.set("Authorization", &format!("AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}", creds.access_key_id, scope, signed_headers, sig));
}

/// Go `path.Clean` as the sigv4 round tripper applies it (keeps "" and a trailing slash).
fn clean_url_path(p: &str) -> String {
    if p.is_empty() {
        return String::new();
    }
    let mut c = crate::config::clean_path(p);
    if c == "." {
        return p.to_string();
    }
    if c != "/" && p.ends_with('/') {
        c.push('/');
    }
    c
}

struct Cached {
    creds: Credentials,
    expires: Option<Instant>,
}

pub struct SigV4RoundTripper {
    pub region: String,
    pub service_name: String,
    base: Credentials,
    role: Option<SigV4Config>,
    sts_endpoint: String,
    sts_client: Option<crate::client::Client>,
    cache: Mutex<Option<Cached>>,
    pub next: Arc<dyn RoundTripper>,
}

/// Options for `SigV4RoundTripper::new`.
#[derive(Clone, Default)]
pub struct SigV4Options {
    /// Overrides the STS endpoint (tests).
    pub sts_endpoint: String,
    /// Overrides the environment lookup (tests).
    pub env: Option<EnvFn>,
}

impl SigV4RoundTripper {
    /// Go `NewSigV4RoundTripper`.
    pub fn new(cfg: &SigV4Config, next: Arc<dyn RoundTripper>, opts: SigV4Options) -> Result<SigV4RoundTripper, Error> {
        let env = opts.env.clone().unwrap_or_else(std_env);
        let (creds, region) = resolve_credentials(cfg, &env).map_err(|e| Error::other(format!("could not get SigV4 credentials: {e}")))?;
        if region.is_empty() {
            return Err(Error::other("region not configured in sigv4 or in default credentials chain"));
        }
        let service_name = if cfg.service_name.is_empty() { "aps".to_string() } else { cfg.service_name.clone() };
        let (role, sts_endpoint, sts_client) = if cfg.role_arn.is_empty() {
            (None, String::new(), None)
        } else {
            let ep = if !opts.sts_endpoint.is_empty() {
                opts.sts_endpoint.clone()
            } else if cfg.use_fips_sts_endpoint {
                format!("https://sts-fips.{region}.amazonaws.com")
            } else {
                format!("https://sts.{region}.amazonaws.com")
            };
            (Some(cfg.clone()), ep, Some(crate::client::Client::new(Arc::new(crate::transport::Transport::default_transport()))))
        };
        Ok(SigV4RoundTripper { region, service_name, base: creds, role, sts_endpoint, sts_client, cache: Mutex::new(None), next })
    }

    /// The credentials to sign with (assumed role credentials are cached until 30 s before they expire).
    pub fn credentials(&self) -> Result<Credentials, Error> {
        let Some(role) = &self.role else { return Ok(self.base.clone()) };
        let mut g = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = g.as_ref()
            && c.expires.is_none_or(|e| Instant::now() + Duration::from_secs(30) < e) {
                return Ok(c.creds.clone());
            }
        let (creds, expires) = self.assume_role(role)?;
        *g = Some(Cached { creds: creds.clone(), expires });
        Ok(creds)
    }

    fn assume_role(&self, role: &SigV4Config) -> Result<(Credentials, Option<Instant>), Error> {
        let session = if role.session_name.is_empty() { format!("{}", SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)) } else { role.session_name.clone() };
        let mut form: Vec<(String, String)> = vec![("Action".into(), "AssumeRole".into()), ("Version".into(), "2011-06-15".into()), ("RoleArn".into(), role.role_arn.clone()), ("RoleSessionName".into(), session)];
        if !role.external_id.is_empty() {
            form.push(("ExternalId".into(), role.external_id.clone()));
        }
        for (i, (k, v)) in role.tags.iter().enumerate() {
            form.push((format!("Tags.member.{}.Key", i + 1), k.clone()));
            form.push((format!("Tags.member.{}.Value", i + 1), v.clone()));
        }
        let body = yaml::url::encode_query(&form);
        let mut req = Request::new("POST", &self.sts_endpoint, body.into_bytes())?;
        if req.url.path.is_empty() {
            req.url.path = "/".to_string();
        }
        req.header.set("Content-Type", "application/x-www-form-urlencoded");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        sign_request(&mut req, &self.base, "sts", &self.region, now);
        let client = self.sts_client.as_ref().ok_or_else(|| Error::other("no STS client"))?;
        let resp = client.do_request(req).map_err(|e| e.wrap("error retrieving credentials: operation error STS: AssumeRole"))?;
        let text = String::from_utf8_lossy(&resp.body).into_owned();
        if resp.status != 200 {
            return Err(Error::other(format!("error retrieving credentials: operation error STS: AssumeRole, https response error StatusCode: {}, {}", resp.status, text.trim())));
        }
        let tag = |n: &str| -> String {
            let (o, c) = (format!("<{n}>"), format!("</{n}>"));
            text.find(&o).and_then(|i| text[i + o.len()..].find(&c).map(|j| text[i + o.len()..i + o.len() + j].to_string())).unwrap_or_default()
        };
        let creds = Credentials { access_key_id: tag("AccessKeyId"), secret_access_key: tag("SecretAccessKey"), session_token: tag("SessionToken") };
        // credentials are valid for at least 15 minutes; refresh after that to keep it simple
        Ok((creds, Some(Instant::now() + Duration::from_secs(900))))
    }
}

impl RoundTripper for SigV4RoundTripper {
    fn round_trip(&self, req: Request) -> Result<Response, Error> {
        let mut sreq = req.clone();
        sreq.url.path = clean_url_path(&req.url.path);
        sreq.url.raw_path.clear();
        let trace = req.header.get("Uber-Trace-Id").map(|s| s.to_string());
        sreq.header.del("Uber-Trace-Id");
        let creds = self.credentials().map_err(|e| if e.msg.starts_with("error retrieving credentials") { e } else { e.wrap("error retrieving credentials") })?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        sign_request(&mut sreq, &creds, &self.service_name, &self.region, now);
        if let Some(t) = trace
            && !t.is_empty() {
                sreq.header.set("Uber-Trace-Id", &t);
            }
        self.next.round_trip(sreq)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}
