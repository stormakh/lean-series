//! The credentials Prometheus builds with azidentity v1.15.0 (and MSAL for Go v1.10.1 under it), ported to
//! blocking HTTP: client secret, client assertion (workload identity), client certificate (PEM), managed identity
//! (IMDS and App Service), Azure CLI and Azure Developer CLI (the same shell commands Go runs), and the
//! `DefaultAzureCredential` chain. Request shapes, headers and error texts follow the Go modules.
//!
//! Not supported (a clear error instead): PKCS#12 certificates, username/password (ROPC), Azure PowerShell, the
//! managed identity sources Azure Arc, Azure ML, Cloud Shell and Service Fabric, regional authorities
//! (`AZURE_REGIONAL_AUTHORITY_NAME`) and AAD instance discovery for non-public authority hosts.
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use json::Json;

use super::arm::{
    NonRetriable, RetryOptions, do_with_retry, go_arch, indent_body, telemetry_value,
};
use crate::cancel::CancelToken;

/// An access token (Go `azcore.AccessToken`).
#[derive(Clone, Debug, PartialEq)]
pub struct AccessToken {
    pub token: String,
    pub expires_on: SystemTime,
    pub refresh_on: Option<SystemTime>,
}

/// Go `azcore.TokenCredential`.
pub trait TokenCredential: Send + Sync {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError>;
}

pub const CRED_NAME_SECRET: &str = "ClientSecretCredential";
pub const CRED_NAME_ASSERTION: &str = "ClientAssertionCredential";
pub const CRED_NAME_CERT: &str = "ClientCertificateCredential";
pub const CRED_NAME_WORKLOAD_IDENTITY: &str = "WorkloadIdentityCredential";
pub const CRED_NAME_MANAGED_IDENTITY: &str = "ManagedIdentityCredential";
pub const CRED_NAME_AZURE_CLI: &str = "AzureCLICredential";
pub const CRED_NAME_AZURE_DEVELOPER_CLI: &str = "AzureDeveloperCLICredential";
pub const CRED_NAME_AZURE_POWERSHELL: &str = "AzurePowerShellCredential";
pub const CRED_NAME_ENVIRONMENT: &str = "EnvironmentCredential";
pub const CRED_NAME_USERNAME_PASSWORD: &str = "UsernamePasswordCredential";

/// The default IMDS token endpoint (MSAL `imdsDefaultEndpoint`).
pub const IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";

const SEPARATOR: &str =
    "--------------------------------------------------------------------------------";
const MSAL_SKU: &str = "MSAL.Go";
const MSAL_VERSION: &str = "1.10.1";
const AZIDENTITY_MODULE: &str = "github.com/Azure/azure-sdk-for-go/sdk/azidentity";
const AZIDENTITY_VERSION: &str = "v1.15.0";
/// The CAE client capability every ARM token request carries (`EnableCAE: true` -> `CP1`).
pub const CP1_CLAIMS: &str = r#"{"access_token":{"xms_cc":{"values":["CP1"]}}}"#;

fn tsg_anchor(cred_type: &str) -> Option<&'static str> {
    Some(match cred_type {
        CRED_NAME_AZURE_CLI => "azure-cli",
        CRED_NAME_AZURE_DEVELOPER_CLI => "azd",
        CRED_NAME_AZURE_POWERSHELL => "azure-pwsh",
        CRED_NAME_CERT => "client-cert",
        CRED_NAME_MANAGED_IDENTITY => "managed-id",
        CRED_NAME_SECRET => "client-secret",
        CRED_NAME_WORKLOAD_IDENTITY => "workload",
        _ => return None,
    })
}

/// What Go's `AuthenticationFailedError` prints of the response.
#[derive(Clone, Debug, PartialEq)]
pub struct RawResponse {
    pub method: String,
    pub scheme: String,
    pub host: String,
    pub path: String,
    pub status: u16,
    pub status_text: String,
    pub body: Vec<u8>,
}

impl RawResponse {
    fn from(resp: &net::Response) -> RawResponse {
        let u = &resp.request.url;
        RawResponse {
            method: resp.request.method.clone(),
            scheme: u.scheme.clone(),
            host: u.host.clone(),
            path: u.path.clone(),
            status: resp.status,
            status_text: resp.status_text.clone(),
            body: resp.body.clone(),
        }
    }
}

/// Go azidentity `AuthenticationFailedError`.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthFailedError {
    pub cred_type: String,
    pub message: String,
    pub response: Option<Box<RawResponse>>,
    pub omit_response: bool,
}

impl std::fmt::Display for AuthFailedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut link = tsg_anchor(&self.cred_type)
            .map(|a| {
                format!("To troubleshoot, visit https://aka.ms/azsdk/go/identity/troubleshoot#{a}")
            })
            .unwrap_or_default();
        let resp = match &self.response {
            Some(r) if !self.omit_response => r,
            _ => {
                if !link.is_empty() {
                    link = if self.message.ends_with('.') {
                        format!(" {link}")
                    } else {
                        format!(". {link}")
                    };
                }
                return write!(f, "{}: {}{}", self.cred_type, self.message, link);
            }
        };
        let mut msg = format!(
            "{} authentication failed. {}\n",
            self.cred_type, self.message
        );
        msg.push_str(&format!(
            "{} {}://{}{}\n",
            resp.method, resp.scheme, resp.host, resp.path
        ));
        msg.push_str(SEPARATOR);
        msg.push('\n');
        msg.push_str(&format!("RESPONSE {}: {}\n", resp.status, resp.status_text));
        msg.push_str(SEPARATOR);
        msg.push('\n');
        if resp.body.is_empty() {
            msg.push_str("Response contained no body");
        } else {
            msg.push_str(&indent_body(&resp.body));
        }
        if !link.is_empty() {
            link = format!("\n{link}");
        }
        msg.push_str(&format!("\n{SEPARATOR}{link}"));
        f.write_str(&msg)
    }
}

/// A credential error: Go `credentialUnavailableError` (the chain tries the next credential),
/// `AuthenticationFailedError`, or any other error.
#[derive(Clone, Debug, PartialEq)]
pub enum CredError {
    Unavailable(String),
    AuthFailed(Box<AuthFailedError>),
    Other(String),
}

impl std::fmt::Display for CredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredError::Unavailable(m) | CredError::Other(m) => f.write_str(m),
            CredError::AuthFailed(a) => a.fmt(f),
        }
    }
}

fn auth_failed(
    cred_type: &str,
    message: impl Into<String>,
    response: Option<RawResponse>,
) -> CredError {
    CredError::AuthFailed(Box::new(AuthFailedError {
        cred_type: cred_type.to_string(),
        message: message.into(),
        response: response.map(Box::new),
        omit_response: false,
    }))
}

fn unavailable(cred_type: &str, message: &str) -> CredError {
    CredError::Unavailable(format!("{cred_type}: {message}"))
}

/// An MSAL-level error: a failed call with its response (Go `errors.CallErr`), a JSON decode error (Go
/// `errors.InvalidJsonErr`), or another error.
enum MsalError {
    Call(Box<RawResponse>),
    InvalidJson(String),
    Other(String),
}

/// Go `newAuthenticationFailedErrorFromMSAL`.
fn from_msal(cred_type: &str, e: MsalError) -> CredError {
    match e {
        MsalError::Call(r) => auth_failed(cred_type, "", Some(*r)),
        MsalError::InvalidJson(m) | MsalError::Other(m) => auth_failed(cred_type, m, None),
    }
}

/// A random UUID (version 4) as Go `uuid.New().String()` prints it.
pub fn new_uuid() -> String {
    let mut b = [0u8; 16];
    crypto::random_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = crypto::hex(&b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// Go azidentity `validTenantID`.
pub fn valid_tenant_id(t: &str) -> bool {
    !t.is_empty()
        && t.chars()
            .all(|r| r.is_ascii_alphanumeric() || r == '.' || r == '-')
}

const ERR_INVALID_TENANT_ID: &str = "invalid tenantID. You can locate your tenantID by following the instructions listed here: https://learn.microsoft.com/partner-center/find-ids-and-domain-names";

/// The HTTP side of the azidentity clients: the azcore default transport (proxy from the environment, system
/// roots) behind a pipeline with the azidentity telemetry user agent and the retry policy.
#[derive(Clone)]
pub struct IdentityHttp {
    pub client: net::Client,
    pub user_agent: String,
}

impl IdentityHttp {
    /// The azcore default HTTP client (Go `transport.DefaultHTTPClient`: `http.ProxyFromEnvironment`).
    pub fn new(application_id: &str) -> Result<IdentityHttp, String> {
        let mut cfg = net::HttpClientConfig::default_config();
        cfg.proxy_config.proxy_from_environment = true;
        let client = net::new_client_from_config(&cfg, &net::HttpClientOptions::default())
            .map_err(|e| e.to_string())?;
        Ok(IdentityHttp {
            client,
            user_agent: telemetry_value(application_id, AZIDENTITY_MODULE, AZIDENTITY_VERSION),
        })
    }

    /// Sends `req` through the pipeline; transport errors come back as Go's client errors.
    fn send(
        &self,
        ctx: &CancelToken,
        mut req: net::Request,
        retry: &RetryOptions,
        timeout: Option<Duration>,
    ) -> Result<net::Response, String> {
        req.decompress = true;
        let ua = match req.header.get("User-Agent") {
            Some(u) if !u.is_empty() => format!("{} {u}", self.user_agent),
            _ => self.user_agent.clone(),
        };
        req.header.set("User-Agent", &ua);
        let deadline = timeout.map(|t| Instant::now() + t);
        do_with_retry(ctx, retry, |try_timeout| {
            let mut r = req.clone();
            let left = deadline.map(|d| d.saturating_duration_since(Instant::now()));
            r.timeout = match (left, try_timeout) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if r.timeout == Some(Duration::ZERO) {
                return Err(NonRetriable("context deadline exceeded".to_string()));
            }
            Ok(self.client.do_request(r).map_err(|e| e.to_string()))
        })
    }
}

fn msal_headers(h: &mut net::Header) {
    h.set("client-request-id", &new_uuid());
    h.set("Return-Client-Request-Id", "false");
    h.set("x-client-sku", MSAL_SKU);
    h.set("x-client-os", std::env::consts::OS);
    h.set("x-client-cpu", go_arch());
    h.set("x-client-ver", MSAL_VERSION);
}

/// MSAL `comm.do`: the body of a 200/201 response, else a call error.
fn msal_do(
    http: &IdentityHttp,
    ctx: &CancelToken,
    req: net::Request,
) -> Result<Vec<u8>, MsalError> {
    let resp = http
        .send(
            ctx,
            req,
            &RetryOptions::default(),
            Some(Duration::from_secs(30)),
        )
        .map_err(|e| MsalError::Other(format!("server response error:\n {e}")))?;
    match resp.status {
        200 | 201 => Ok(resp.body),
        _ => Err(MsalError::Call(Box::new(RawResponse::from(&resp)))),
    }
}

fn now() -> SystemTime {
    SystemTime::now()
}

fn json_secs(v: Option<&Json>) -> Option<i64> {
    match v? {
        Json::Int(i) => Some(*i),
        Json::Float(f) => Some(*f as i64),
        Json::Str(s) => s.trim().parse::<f64>().ok().map(|f| f as i64),
        _ => None,
    }
}

/// MSAL `TokenResponse` (unmarshal, `Validate`, expiry): `expires_in`/`expires_on` as numbers or strings,
/// `refresh_in`; tokens living longer than two hours refresh at half-life (managed identity).
fn parse_token_response(data: &[u8], half_life_refresh: bool) -> Result<AccessToken, MsalError> {
    let v = Json::parse_bytes(data).map_err(|e| {
        MsalError::InvalidJson(format!(
            "json decode error: {e}\nraw message was: {}",
            String::from_utf8_lossy(data)
        ))
    })?;
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let err = s("error");
    if !err.is_empty() {
        return Err(MsalError::Other(format!(
            "{err}: {}",
            s("error_description")
        )));
    }
    let token = s("access_token");
    if token.is_empty() {
        return Err(MsalError::Other(
            "response is missing access_token".to_string(),
        ));
    }
    let n = now();
    let expires_on = match json_secs(v.get("expires_in")) {
        Some(secs) if secs > 0 => n + Duration::from_secs(secs as u64),
        _ => match json_secs(v.get("expires_on")) {
            Some(secs) if secs > 0 => UNIX_EPOCH + Duration::from_secs(secs as u64),
            _ => n,
        },
    };
    let mut refresh_on = json_secs(v.get("refresh_in"))
        .filter(|s| *s > 0)
        .map(|secs| n + Duration::from_secs(secs as u64));
    if refresh_on.is_none() && half_life_refresh {
        let lifetime = expires_on.duration_since(n).unwrap_or_default();
        if lifetime > Duration::from_secs(7200) {
            refresh_on = Some(n + lifetime / 2);
        }
    }
    Ok(AccessToken {
        token,
        expires_on,
        refresh_on,
    })
}

/// An MSAL cache hit: not within five minutes of expiry and not past `refresh_on`.
fn cached_valid(t: &AccessToken) -> bool {
    let n = now();
    t.expires_on > n + Duration::from_secs(300) && t.refresh_on.is_none_or(|r| r > n)
}

// ---------------- confidential client (secret, assertion, certificate) ----------------

/// How a confidential client proves its identity.
pub enum ClientCred {
    Secret(String),
    /// Called for each token request (Go `NewCredFromAssertionCallback`).
    Assertion(Box<dyn Fn() -> Result<String, String> + Send + Sync>),
    Certificate(Box<CertCred>),
}

/// A client certificate and its RSA key (PEM).
pub struct CertCred {
    /// DER of the certificates, the signing certificate first.
    pub certs: Vec<Vec<u8>>,
    pub key: ring::rsa::KeyPair,
    pub send_x5c: bool,
}

/// Go azidentity `confidentialClient` over MSAL `confidential.Client`.
pub struct ConfidentialClient {
    name: String,
    tenant_id: String,
    client_id: String,
    cred: ClientCred,
    authority: String,
    http: IdentityHttp,
    endpoints: Mutex<Option<String>>,
    cache: Mutex<Option<AccessToken>>,
}

/// Go azidentity `setAuthorityHost` (the cloud configuration always names the host in Prometheus).
fn check_authority_host(host: &str, allow_http: bool) -> Result<(), String> {
    let u = yaml::url::parse(host)?;
    if u.scheme != "https" && !allow_http {
        return Err("cannot use an authority host without https".to_string());
    }
    Ok(())
}

impl ConfidentialClient {
    /// Go `newConfidentialClient`.
    pub fn new(
        tenant_id: &str,
        client_id: &str,
        name: &str,
        cred: ClientCred,
        opts: &IdentityOptions,
    ) -> Result<ConfidentialClient, String> {
        if !valid_tenant_id(tenant_id) {
            return Err(ERR_INVALID_TENANT_ID.to_string());
        }
        check_authority_host(&opts.authority_host, opts.allow_http)?;
        let authority = super::arm::join_paths(&opts.authority_host, tenant_id);
        Ok(ConfidentialClient {
            name: name.to_string(),
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            cred,
            authority,
            http: opts.http.clone(),
            endpoints: Mutex::new(None),
            cache: Mutex::new(None),
        })
    }

    /// MSAL `ResolveEndpoints` for an AAD authority on a trusted host: the OpenID configuration of the tenant.
    fn resolve_endpoints(&self, ctx: &CancelToken) -> Result<String, MsalError> {
        if let Some(e) = self
            .endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return Ok(e);
        }
        let mut canonical = self.authority.clone();
        if !canonical.ends_with('/') {
            canonical.push('/');
        }
        let lower = yaml::url::parse(&canonical.to_lowercase())
            .map_err(|e| MsalError::Other(format!("couldn't parse authority url: {e}")))?;
        if lower.scheme != "https" && !self.http_allowed() {
            return Err(MsalError::Other(
                "authority url scheme must be https".to_string(),
            ));
        }
        let parts: Vec<&str> = lower.path.split('/').collect();
        if parts.len() < 3 {
            return Err(MsalError::Other(r#"authority must be an URL such as "https://login.microsoftonline.com/<your tenant>""#.to_string()));
        }
        let tenant = parts[1].to_string();
        let endpoint = if tenant == "adfs" {
            format!(
                "https://{}/adfs/.well-known/openid-configuration",
                lower.host
            )
        } else {
            format!("{canonical}v2.0/.well-known/openid-configuration")
        };
        let mut req = net::Request::get(&format!("{endpoint}?"))
            .map_err(|e| MsalError::Other(format!("could not create request: {e}")))?;
        msal_headers(&mut req.header);
        let data = msal_do(&self.http, ctx, req)?;
        let v = Json::parse_bytes(&data).map_err(|e| {
            MsalError::InvalidJson(format!(
                "json decode error: {e}\njson message bytes were: {}",
                String::from_utf8_lossy(&data)
            ))
        })?;
        let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
        let (authz, token, issuer) = (
            s("authorization_endpoint"),
            s("token_endpoint"),
            s("issuer"),
        );
        let invalid =
            |m: &str| MsalError::Other(format!("ResolveEndpoints(): TenantDiscoveryResponse: {m}"));
        if authz.is_empty() {
            return Err(invalid(
                "authorize endpoint was not found in the openid configuration",
            ));
        }
        if token.is_empty() {
            return Err(invalid(
                "token endpoint was not found in the openid configuration",
            ));
        }
        if issuer.is_empty() {
            return Err(invalid("issuer was not found in the openid configuration"));
        }
        let issuer = issuer.replace("{tenant}", &tenant);
        let token = token.replace("{tenant}", &tenant);
        // ValidateIssuerMatchesAuthority
        let iu = yaml::url::parse(&issuer)
            .map_err(|e| invalid(&format!("failed to parse issuer URL: {e}")))?;
        let au = yaml::url::parse(&canonical)
            .map_err(|e| invalid(&format!("failed to parse authority URL: {e}")))?;
        let ih = iu.host.to_lowercase();
        let ah = au.host.to_lowercase();
        let ok = (iu.scheme == au.scheme && ih == ah)
            || is_trusted_host(&ih)
            || ah.ends_with(&format!(".{ih}"));
        if !ok {
            return Err(MsalError::Other(format!(
                "ResolveEndpoints(): TenantDiscoveryResponse: issuer '{issuer}' does not match authority '{canonical}' or any trusted/alias rule"
            )));
        }
        *self.endpoints.lock().unwrap_or_else(|e| e.into_inner()) = Some(token.clone());
        Ok(token)
    }

    fn http_allowed(&self) -> bool {
        !self.authority.starts_with("https:")
    }

    fn client_assertion_jwt(&self, c: &CertCred, token_endpoint: &str) -> Result<String, String> {
        let thumb = crypto::base64(&crypto::sha256(&c.certs[0]));
        let mut header = Json::obj().with("alg", "PS256").with("typ", "JWT");
        if c.send_x5c {
            header.set(
                "x5c",
                Json::Arr(
                    c.certs
                        .iter()
                        .map(|d| Json::from(crypto::base64(d)))
                        .collect(),
                ),
            );
        }
        header.set("x5t#S256", thumb);
        let n = now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let claims = Json::obj()
            .with("aud", token_endpoint)
            .with("exp", n + 600)
            .with("iss", self.client_id.as_str())
            .with("jti", new_uuid())
            .with("nbf", n)
            .with("sub", self.client_id.as_str());
        let signing_input = format!(
            "{}.{}",
            crypto::base64url(header.stringify().as_bytes()),
            crypto::base64url(claims.stringify().as_bytes())
        );
        let mut sig = vec![0u8; c.key.public().modulus_len()];
        c.key
            .sign(
                &ring::signature::RSA_PSS_SHA256,
                &ring::rand::SystemRandom::new(),
                signing_input.as_bytes(),
                &mut sig,
            )
            .map_err(|_| "unable to sign JWT token: crypto/rsa: signing failed".to_string())?;
        Ok(format!("{signing_input}.{}", crypto::base64url(&sig)))
    }

    fn acquire(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, MsalError> {
        let token_endpoint = self.resolve_endpoints(ctx)?;
        let mut scope: Vec<&str> = scopes
            .iter()
            .map(|s| s.as_str())
            .filter(|s| {
                !s.trim().is_empty() && !matches!(*s, "openid" | "offline_access" | "profile")
            })
            .collect();
        scope.extend(["openid", "offline_access", "profile"]);
        let mut form: Vec<(String, String)> = vec![
            ("claims".into(), CP1_CLAIMS.into()),
            ("grant_type".into(), "client_credentials".into()),
        ];
        match &self.cred {
            ClientCred::Secret(s) => {
                form.push(("client_secret".into(), s.clone()));
            }
            ClientCred::Assertion(f) => {
                let a = f().map_err(MsalError::Other)?;
                form.push((
                    "client_assertion_type".into(),
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into(),
                ));
                form.push(("client_assertion".into(), a));
                form.push(("client_info".into(), "1".into()));
            }
            ClientCred::Certificate(c) => {
                let a = self
                    .client_assertion_jwt(c, &token_endpoint)
                    .map_err(MsalError::Other)?;
                form.push((
                    "client_assertion_type".into(),
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into(),
                ));
                form.push(("client_assertion".into(), a));
                form.push(("client_info".into(), "1".into()));
            }
        }
        form.push(("client_id".into(), self.client_id.clone()));
        form.push(("scope".into(), scope.join(" ")));
        let body = yaml::url::encode_query(&form);
        let mut req =
            net::Request::new("POST", &token_endpoint, body.into_bytes()).map_err(|e| {
                MsalError::Other(format!("could not parse path URL({token_endpoint}): {e}"))
            })?;
        req.header.set(
            "Content-Type",
            "application/x-www-form-urlencoded; charset=utf-8",
        );
        msal_headers(&mut req.header);
        let data = msal_do(&self.http, ctx, req)?;
        parse_token_response(&data, false)
    }
}

fn is_trusted_host(h: &str) -> bool {
    matches!(
        h,
        "login.windows.net"
            | "login.partner.microsoftonline.cn"
            | "login.chinacloudapi.cn"
            | "login.microsoftonline.de"
            | "login-us.microsoftonline.com"
            | "login.microsoftonline.us"
            | "login.microsoftonline.com"
            | "login.microsoft.com"
            | "sts.windows.net"
            | "login.usgovcloudapi.net"
            | "login.sovcloud-identity.fr"
            | "login.sovcloud-identity.de"
            | "login.sovcloud-identity.sg"
    )
}

impl TokenCredential for ConfidentialClient {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError> {
        if scopes.is_empty() {
            return Err(CredError::Other(format!(
                "{}.GetToken() requires at least one scope",
                self.name
            )));
        }
        let _ = &self.tenant_id;
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = cache.as_ref().filter(|t| cached_valid(t)) {
            return Ok(t.clone());
        }
        let t = self
            .acquire(ctx, scopes)
            .map_err(|e| from_msal(&self.name, e))?;
        *cache = Some(t.clone());
        Ok(t)
    }
}

/// Options shared by the credentials: the authority host of the cloud, the IMDS endpoint and the HTTP client.
#[derive(Clone)]
pub struct IdentityOptions {
    /// Go `cloud.Configuration.ActiveDirectoryAuthorityHost`.
    pub authority_host: String,
    /// The IMDS token endpoint (only tests change it).
    pub imds_endpoint: String,
    /// Allow plain-HTTP authority hosts (only tests).
    pub allow_http: bool,
    pub http: IdentityHttp,
}

/// Go `azidentity.NewClientSecretCredential`.
pub fn new_client_secret_credential(
    tenant_id: &str,
    client_id: &str,
    secret: &str,
    opts: &IdentityOptions,
) -> Result<ConfidentialClient, String> {
    if secret.is_empty() {
        return Err("secret can't be empty string".to_string());
    }
    ConfidentialClient::new(
        tenant_id,
        client_id,
        CRED_NAME_SECRET,
        ClientCred::Secret(secret.to_string()),
        opts,
    )
}

/// Go `azidentity.NewWorkloadIdentityCredential` (client ID, token file and tenant from the arguments or the
/// `AZURE_CLIENT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_TENANT_ID` environment variables). The token file is
/// read again at most every ten minutes.
pub fn new_workload_identity_credential(
    client_id: &str,
    token_file: &str,
    tenant_id: &str,
    opts: &IdentityOptions,
) -> Result<ConfidentialClient, String> {
    let client_id = if client_id.is_empty() {
        std::env::var("AZURE_CLIENT_ID").map_err(|_| {
            "no client ID specified. Check pod configuration or set ClientID in the options"
                .to_string()
        })?
    } else {
        client_id.to_string()
    };
    let file = if token_file.is_empty() {
        std::env::var("AZURE_FEDERATED_TOKEN_FILE").map_err(|_| "no token file specified. Check pod configuration or set TokenFilePath in the options".to_string())?
    } else {
        token_file.to_string()
    };
    let tenant_id = if tenant_id.is_empty() {
        std::env::var("AZURE_TENANT_ID").map_err(|_| {
            "no tenant ID specified. Check pod configuration or set TenantID in the options"
                .to_string()
        })?
    } else {
        tenant_id.to_string()
    };
    let state: Mutex<(String, Option<Instant>)> = Mutex::new((String::new(), None));
    let get = move || -> Result<String, String> {
        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
        if s.1.is_none_or(|t| t <= Instant::now()) {
            let content = net::types::read_file(&file)?;
            *s = (
                String::from_utf8_lossy(&content).into_owned(),
                Some(Instant::now() + Duration::from_secs(600)),
            );
        }
        Ok(s.0.clone())
    };
    ConfidentialClient::new(
        &tenant_id,
        &client_id,
        CRED_NAME_WORKLOAD_IDENTITY,
        ClientCred::Assertion(Box::new(get)),
        opts,
    )
}

/// Go `azidentity.ParseCertificates` for PEM data (certificates and an RSA key in PKCS#8 or PKCS#1).
pub fn parse_pem_certificates(
    data: &[u8],
    password: &str,
) -> Result<(Vec<Vec<u8>>, ring::rsa::KeyPair), String> {
    let blocks = net::tls::pem_blocks(data);
    if !password.is_empty() || blocks.is_empty() {
        return Err(
            "PKCS#12 certificates are not supported by prometheus-rs; use a PEM file".to_string(),
        );
    }
    let mut certs = Vec::new();
    let mut key: Option<ring::rsa::KeyPair> = None;
    for b in blocks {
        match b.type_.as_str() {
            "CERTIFICATE" => certs.push(b.der),
            "PRIVATE KEY" | "RSA PRIVATE KEY" => {
                if key.is_some() {
                    return Err("certData contains multiple private keys".to_string());
                }
                let k = if b.type_ == "PRIVATE KEY" {
                    ring::rsa::KeyPair::from_pkcs8(&b.der)
                        .or_else(|_| ring::rsa::KeyPair::from_der(&b.der))
                } else {
                    ring::rsa::KeyPair::from_der(&b.der)
                };
                key = Some(k.map_err(|e| format!("x509: failed to parse private key: {e}"))?);
            }
            _ => {}
        }
    }
    if certs.is_empty() {
        return Err("found no certificate".to_string());
    }
    let key = key.ok_or_else(|| "found no private key".to_string())?;
    Ok((certs, key))
}

// ---------------- managed identity ----------------

/// MSAL `managedidentity.Source`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MiSource {
    Imds,
    AppService,
    CloudShell,
    AzureMl,
    AzureArc,
    ServiceFabric,
}

impl MiSource {
    fn name(self) -> &'static str {
        match self {
            MiSource::Imds => "IMDS",
            MiSource::AppService => "AppService",
            MiSource::CloudShell => "CloudShell",
            MiSource::AzureMl => "AzureML",
            MiSource::AzureArc => "AzureArc",
            MiSource::ServiceFabric => "ServiceFabric",
        }
    }
}

/// MSAL `managedidentity.GetSource`.
pub fn get_mi_source() -> MiSource {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let (ie, ih, thumb, msi, secret, imds) = (
        env("IDENTITY_ENDPOINT"),
        env("IDENTITY_HEADER"),
        env("IDENTITY_SERVER_THUMBPRINT"),
        env("MSI_ENDPOINT"),
        env("MSI_SECRET"),
        env("IMDS_ENDPOINT"),
    );
    if !ie.is_empty() && !ih.is_empty() {
        if !thumb.is_empty() {
            MiSource::ServiceFabric
        } else {
            MiSource::AppService
        }
    } else if !msi.is_empty() {
        if !secret.is_empty() {
            MiSource::AzureMl
        } else {
            MiSource::CloudShell
        }
    } else if (!ie.is_empty() && !imds.is_empty())
        || std::path::Path::new("/opt/azcmagent/bin/himds").exists()
    {
        MiSource::AzureArc
    } else {
        MiSource::Imds
    }
}

/// The MSAL managed identity token cache is process wide.
fn mi_cache() -> &'static Mutex<HashMap<String, AccessToken>> {
    static C: OnceLock<Mutex<HashMap<String, AccessToken>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Go azidentity `ManagedIdentityCredential` (system assigned, or user assigned by client ID).
pub struct ManagedIdentityCredential {
    source: MiSource,
    client_id: Option<String>,
    http: IdentityHttp,
    imds_endpoint: String,
    probe: AtomicBool,
    chained: AtomicBool,
}

/// Go `setIMDSRetryOptionDefaults`.
fn imds_retry() -> RetryOptions {
    RetryOptions {
        max_retries: 6,
        retry_delay: Duration::from_secs(2),
        max_retry_delay: Duration::from_secs(25),
        status_codes: vec![
            404, 410, 429, 500, 501, 502, 503, 504, 505, 506, 507, 508, 510, 511,
        ],
        try_timeout: Some(Duration::from_secs(60)),
    }
}

impl ManagedIdentityCredential {
    /// Go `azidentity.NewManagedIdentityCredential`; `dac`: probe IMDS first (inside `DefaultAzureCredential`).
    pub fn new(
        client_id: Option<String>,
        dac: bool,
        opts: &IdentityOptions,
    ) -> ManagedIdentityCredential {
        let source = get_mi_source();
        ManagedIdentityCredential {
            source,
            client_id,
            http: opts.http.clone(),
            imds_endpoint: opts.imds_endpoint.clone(),
            probe: AtomicBool::new(dac && source == MiSource::Imds),
            chained: AtomicBool::new(false),
        }
    }

    fn request(&self, resource: &str) -> Result<(net::Request, RetryOptions), String> {
        let mut q: Vec<(String, String)> = Vec::new();
        match self.source {
            MiSource::Imds => {
                q.push(("api-version".into(), "2018-02-01".into()));
                q.push(("resource".into(), resource.into()));
                if let Some(c) = &self.client_id {
                    q.push(("client_id".into(), c.clone()));
                }
                let mut req = net::Request::get(&format!(
                    "{}?{}",
                    self.imds_endpoint,
                    yaml::url::encode_query(&q)
                ))
                .map_err(|e| format!("error creating http request {e}"))?;
                req.header.set("Metadata", "true");
                req.header.set("x-client-SKU", MSAL_SKU);
                req.header.set("x-client-Ver", MSAL_VERSION);
                req.header.set("x-ms-client-request-id", &new_uuid());
                Ok((req, imds_retry()))
            }
            MiSource::AppService => {
                let ep = std::env::var("IDENTITY_ENDPOINT").unwrap_or_default();
                let mut u = yaml::url::parse(&ep)?;
                let mut pairs = u.query_pairs();
                pairs.retain(|(k, _)| k != "api-version" && k != "resource" && k != "client_id");
                pairs.push(("api-version".into(), "2019-08-01".into()));
                pairs.push(("resource".into(), resource.into()));
                if let Some(c) = &self.client_id {
                    pairs.push(("client_id".into(), c.clone()));
                }
                u.raw_query = yaml::url::encode_query(&pairs);
                let mut req = net::Request::get(&u.to_url_string()).map_err(|e| e.to_string())?;
                req.header.set(
                    "X-IDENTITY-HEADER",
                    &std::env::var("IDENTITY_HEADER").unwrap_or_default(),
                );
                Ok((req, RetryOptions::default()))
            }
            s => Err(format!(
                "{} managed identity is not supported by prometheus-rs",
                s.name()
            )),
        }
    }

    fn cache_key(&self, resource: &str) -> String {
        format!(
            "{}|{}|{}",
            self.source.name(),
            self.client_id.as_deref().unwrap_or(""),
            resource
        )
    }
}

impl TokenCredential for ManagedIdentityCredential {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError> {
        let name = CRED_NAME_MANAGED_IDENTITY;
        if scopes.len() != 1 {
            return Err(CredError::Other(format!(
                "{name}.GetToken() requires exactly one scope"
            )));
        }
        // azidentity and then MSAL each trim one "/.default"
        let resource = scopes[0].strip_suffix("/.default").unwrap_or(&scopes[0]);
        let resource = resource
            .strip_suffix("/.default")
            .unwrap_or(resource)
            .to_string();
        if self.probe.load(Ordering::SeqCst) {
            let req = net::Request::get(&self.imds_endpoint).map_err(|e| {
                CredError::Other(format!("failed to create IMDS probe request: {e}"))
            })?;
            let no_retry = RetryOptions {
                max_retries: 0,
                ..RetryOptions::default()
            };
            if let Err(e) = self
                .http
                .send(ctx, req, &no_retry, Some(Duration::from_secs(1)))
            {
                let msg = if e.contains("Client.Timeout")
                    || e.contains("timeout")
                    || e.contains("deadline exceeded")
                {
                    "managed identity timed out. See https://aka.ms/azsdk/go/identity/troubleshoot#dac for more information".to_string()
                } else {
                    e
                };
                return Err(unavailable(name, &msg));
            }
            self.probe.store(false, Ordering::SeqCst);
        }
        let key = self.cache_key(&resource);
        if let Some(t) = mi_cache()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .filter(|t| cached_valid(t))
        {
            return Ok(t.clone());
        }
        let (req, retry) = self.request(&resource).map_err(|e| unavailable(name, &e))?;
        let r = match self.http.send(ctx, req, &retry, None) {
            Err(e) => Err(MsalError::Other(e)),
            Ok(resp) if resp.status == 200 || resp.status == 202 => {
                parse_token_response(&resp.body, true)
            }
            Ok(resp) => Err(MsalError::Call(Box::new(RawResponse::from(&resp)))),
        };
        match r {
            Ok(t) => {
                mi_cache()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(key, t.clone());
                Ok(t)
            }
            Err(e) => {
                if self.source == MiSource::Imds {
                    match &e {
                        MsalError::InvalidJson(m) if self.chained.load(Ordering::SeqCst) => {
                            return Err(unavailable(name, m));
                        }
                        MsalError::Call(r) if r.status == 400 => {
                            if self.client_id.is_some() {
                                return Err(auth_failed(
                                    name,
                                    "the requested identity isn't assigned to this resource",
                                    Some((**r).clone()),
                                ));
                            }
                            let mut msg =
                                "failed to authenticate a system assigned identity".to_string();
                            if !r.body.is_empty() {
                                msg.push_str(&format!(
                                    ". The endpoint responded with {}",
                                    String::from_utf8_lossy(&r.body)
                                ));
                            }
                            return Err(unavailable(name, &msg));
                        }
                        MsalError::Call(r)
                            if r.status == 403
                                && String::from_utf8_lossy(&r.body).contains("unreachable") =>
                        {
                            return Err(unavailable(
                                name,
                                &format!(
                                    "unexpected response {}",
                                    yaml::go_quote(&String::from_utf8_lossy(&r.body))
                                ),
                            ));
                        }
                        _ => {}
                    }
                }
                Err(from_msal(name, e))
            }
        }
    }
}

// ---------------- developer CLIs ----------------

/// Go `shellExec`: `/bin/sh -c <command>` in /bin with a ten second timeout.
fn shell_exec(ctx: &CancelToken, cred_name: &str, command: &str) -> Result<Vec<u8>, CredError> {
    let mut child = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(command)
        .current_dir("/bin")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| auth_failed(cred_name, e.to_string(), None))?;
    let mut out = child.stdout.take();
    let mut err = child.stderr.take();
    let t_out = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(o) = out.as_mut() {
            let _ = o.read_to_end(&mut b);
        }
        b
    });
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        if let Some(o) = err.as_mut() {
            let _ = o.read_to_end(&mut b);
        }
        b
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break Some(s),
            Ok(None) => {}
            Err(_) => break None,
        }
        if Instant::now() >= deadline || ctx.wait_timeout(Duration::from_millis(20)) {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
    };
    let stdout = t_out.join().unwrap_or_default();
    let stderr = t_err.join().unwrap_or_default();
    match status {
        Some(s) if s.success() => Ok(stdout),
        s => {
            let msg = String::from_utf8_lossy(&stderr)
                .trim_matches(|c| c == '\r' || c == '\n')
                .to_string();
            let code = s.and_then(|s| s.code());
            if code == Some(127) || msg.contains("' is not recognized") {
                return Err(unavailable(cred_name, "executable not found on path"));
            }
            let msg = if !msg.is_empty() {
                msg
            } else {
                match code {
                    Some(c) => format!("exit status {c}"),
                    None => "signal: killed".to_string(),
                }
            };
            Err(auth_failed(cred_name, msg, None))
        }
    }
}

/// Go `unavailableIfInDAC`.
fn unavailable_if_in_dac(e: CredError, in_dac: bool) -> CredError {
    match e {
        CredError::Unavailable(_) => e,
        e if in_dac => CredError::Unavailable(e.to_string()),
        e => e,
    }
}

fn valid_scope(s: &str) -> bool {
    s.chars()
        .all(|r| r.is_ascii_alphanumeric() || matches!(r, '.' | '-' | '_' | '/' | ':'))
}

/// Parses "2006-01-02 15:04:05.999999" (as UTC).
fn parse_cli_time(s: &str) -> Option<SystemTime> {
    let (date, time) = s.split_once(' ')?;
    let t = crate::util::parse_rfc3339(&format!("{date}T{time}Z"))?;
    Some(t)
}

/// Go azidentity `AzureCLICredential` (`az account get-access-token`).
pub struct AzureCliCredential {
    tenant_id: String,
    in_dac: bool,
    mu: Mutex<()>,
}

impl AzureCliCredential {
    pub fn new(tenant_id: &str, in_dac: bool) -> Result<AzureCliCredential, String> {
        if !tenant_id.is_empty() && !valid_tenant_id(tenant_id) {
            return Err(ERR_INVALID_TENANT_ID.to_string());
        }
        Ok(AzureCliCredential {
            tenant_id: tenant_id.to_string(),
            in_dac,
            mu: Mutex::new(()),
        })
    }
}

impl TokenCredential for AzureCliCredential {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError> {
        let name = CRED_NAME_AZURE_CLI;
        if scopes.len() != 1 {
            return Err(CredError::Other(format!(
                "{name}: GetToken() requires exactly one scope"
            )));
        }
        if !valid_scope(&scopes[0]) {
            return Err(CredError::Other(format!(
                "{name}.GetToken(): invalid scope {}",
                yaml::go_quote(&scopes[0])
            )));
        }
        let resource = scopes[0].strip_suffix("/.default").unwrap_or(&scopes[0]);
        let mut command = format!("az account get-access-token -o json --resource {resource}");
        if !self.tenant_id.is_empty() {
            command.push_str(&format!(" --tenant {}", self.tenant_id));
        }
        let _g = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        let r = shell_exec(ctx, name, &command).and_then(|b| {
            let v = Json::parse_bytes(&b).map_err(|e| CredError::Other(e.to_string()))?;
            let token = v.get("accessToken").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let exp = v.get("expires_on").and_then(|x| x.as_i64()).unwrap_or(0);
            let expires_on = if exp != 0 {
                UNIX_EPOCH + Duration::from_secs(exp.max(0) as u64)
            } else {
                let s = v.get("expiresOn").and_then(|x| x.as_str()).unwrap_or("");
                parse_cli_time(s).ok_or_else(|| CredError::Other(format!("{name}: error parsing token expiration time {}: parsing time {} as \"2006-01-02 15:04:05.999999\": cannot parse", yaml::go_quote(s), yaml::go_quote(s))))?
            };
            Ok(AccessToken { token, expires_on, refresh_on: None })
        });
        r.map_err(|e| unavailable_if_in_dac(e, self.in_dac))
    }
}

/// Go azidentity `AzureDeveloperCLICredential` (`azd auth token`).
pub struct AzureDeveloperCliCredential {
    tenant_id: String,
    in_dac: bool,
    mu: Mutex<()>,
}

impl AzureDeveloperCliCredential {
    pub fn new(tenant_id: &str, in_dac: bool) -> Result<AzureDeveloperCliCredential, String> {
        if !tenant_id.is_empty() && !valid_tenant_id(tenant_id) {
            return Err(ERR_INVALID_TENANT_ID.to_string());
        }
        Ok(AzureDeveloperCliCredential {
            tenant_id: tenant_id.to_string(),
            in_dac,
            mu: Mutex::new(()),
        })
    }
}

impl TokenCredential for AzureDeveloperCliCredential {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError> {
        let name = CRED_NAME_AZURE_DEVELOPER_CLI;
        if scopes.is_empty() {
            return Err(CredError::Other(format!(
                "{name}: GetToken() requires at least one scope"
            )));
        }
        let mut command = "azd auth token -o json --no-prompt".to_string();
        for s in scopes {
            if !valid_scope(s) {
                return Err(CredError::Other(format!(
                    "{name}.GetToken(): invalid scope {}",
                    yaml::go_quote(s)
                )));
            }
            command.push_str(&format!(" --scope {s}"));
        }
        if !self.tenant_id.is_empty() {
            command.push_str(&format!(" --tenant-id {}", self.tenant_id));
        }
        let _g = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        let r = shell_exec(ctx, name, &command).and_then(|b| {
            let v = Json::parse_bytes(&b).map_err(|e| CredError::Other(e.to_string()))?;
            let token = v.get("token").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let s = v.get("expiresOn").and_then(|x| x.as_str()).unwrap_or("");
            let expires_on = crate::util::parse_rfc3339(s).ok_or_else(|| CredError::Other(format!("error parsing token expiration time {}: parsing time {} as \"2006-01-02T15:04:05Z\": cannot parse", yaml::go_quote(s), yaml::go_quote(s))))?;
            Ok(AccessToken { token, expires_on, refresh_on: None })
        });
        r.map_err(|e| unavailable_if_in_dac(e, self.in_dac))
    }
}

/// A credential that always fails with a fixed error (Go `defaultCredentialErrorReporter`, and the auth paths
/// prometheus-rs does not support).
pub struct ErrorCredential {
    pub err: CredError,
}

impl TokenCredential for ErrorCredential {
    fn get_token(&self, _ctx: &CancelToken, _scopes: &[String]) -> Result<AccessToken, CredError> {
        Err(self.err.clone())
    }
}

/// Go azidentity `NewEnvironmentCredential`.
pub fn new_environment_credential(
    opts: &IdentityOptions,
) -> Result<Arc<dyn TokenCredential>, String> {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let tenant_id = env("AZURE_TENANT_ID");
    if tenant_id.is_empty() {
        return Err("missing environment variable AZURE_TENANT_ID".to_string());
    }
    let client_id = env("AZURE_CLIENT_ID");
    if client_id.is_empty() {
        return Err("missing environment variable AZURE_CLIENT_ID".to_string());
    }
    let secret = env("AZURE_CLIENT_SECRET");
    if !secret.is_empty() {
        return Ok(Arc::new(new_client_secret_credential(
            &tenant_id, &client_id, &secret, opts,
        )?));
    }
    let cert_path = env("AZURE_CLIENT_CERTIFICATE_PATH");
    if !cert_path.is_empty() {
        let data = std::fs::read(&cert_path).map_err(|e| {
            format!(
                r#"failed to read certificate file "{cert_path}": open {cert_path}: {}"#,
                net::types::go_io_error(&e)
            )
        })?;
        let (certs, key) = parse_pem_certificates(&data, &env("AZURE_CLIENT_CERTIFICATE_PASSWORD")).map_err(|e| {
            format!("failed to parse {} due to error {}. This may be due to a limitation of this module's certificate loader. Consider calling NewClientCertificateCredential instead", yaml::go_quote(&cert_path), yaml::go_quote(&e))
        })?;
        let send_x5c = std::env::var("AZURE_CLIENT_SEND_CERTIFICATE_CHAIN")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);
        return Ok(Arc::new(ConfidentialClient::new(
            &tenant_id,
            &client_id,
            CRED_NAME_CERT,
            ClientCred::Certificate(Box::new(CertCred {
                certs,
                key,
                send_x5c,
            })),
            opts,
        )?));
    }
    let username = env("AZURE_USERNAME");
    if !username.is_empty() {
        if !env("AZURE_PASSWORD").is_empty() {
            return Ok(Arc::new(ErrorCredential {
                err: auth_failed(
                    CRED_NAME_USERNAME_PASSWORD,
                    "username/password authentication is not supported by prometheus-rs",
                    None,
                ),
            }));
        }
        return Err("no value for AZURE_PASSWORD".to_string());
    }
    Err("incomplete environment variable configuration. Only AZURE_TENANT_ID and AZURE_CLIENT_ID are set".to_string())
}

/// Go azidentity `ChainedTokenCredential` (as `DefaultAzureCredential` names it).
pub struct ChainedTokenCredential {
    name: String,
    sources: Vec<Arc<dyn TokenCredential>>,
    successful: Mutex<Option<Arc<dyn TokenCredential>>>,
}

impl TokenCredential for ChainedTokenCredential {
    fn get_token(&self, ctx: &CancelToken, scopes: &[String]) -> Result<AccessToken, CredError> {
        let mut succ = self.successful.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = succ.as_ref() {
            return c.get_token(ctx, scopes);
        }
        let mut errs: Vec<CredError> = Vec::new();
        for c in &self.sources {
            match c.get_token(ctx, scopes) {
                Ok(t) => {
                    *succ = Some(c.clone());
                    return Ok(t);
                }
                Err(e) => {
                    let stop = !matches!(e, CredError::Unavailable(_));
                    errs.push(e);
                    if stop {
                        break;
                    }
                }
            }
        }
        let mut msg = "failed to acquire a token.\nAttempted credentials:".to_string();
        for e in &errs {
            msg.push_str(&format!("\n\t{}", e.to_string().replace('\n', "\n\t\t")));
        }
        Err(match errs.last() {
            Some(CredError::AuthFailed(a)) => CredError::AuthFailed(Box::new(AuthFailedError {
                cred_type: self.name.clone(),
                message: msg,
                response: a.response.clone(),
                omit_response: true,
            })),
            Some(CredError::Unavailable(_)) | None => unavailable(&self.name, &msg),
            Some(CredError::Other(_)) => auth_failed(&self.name, msg, None),
        })
    }
}

/// Go azidentity `NewDefaultAzureCredential` (with `AZURE_TOKEN_CREDENTIALS` selection). The chain:
/// environment, workload identity, managed identity, Azure CLI, Azure Developer CLI, Azure PowerShell.
pub fn new_default_azure_credential(
    tenant_id: &str,
    opts: &IdentityOptions,
) -> Result<Arc<dyn TokenCredential>, String> {
    const ENV: u8 = 1;
    const WI: u8 = 2;
    const MI: u8 = 4;
    const AZ: u8 = 8;
    const AZD: u8 = 16;
    const PS: u8 = 32;
    let mut selected = ENV | WI | MI | AZ | AZD | PS;
    if let Ok(atc) = std::env::var("AZURE_TOKEN_CREDENTIALS") {
        selected = match atc.as_str() {
            "dev" => AZ | AZD | PS,
            "prod" => ENV | WI | MI,
            a if a.eq_ignore_ascii_case(CRED_NAME_ENVIRONMENT) => ENV,
            a if a.eq_ignore_ascii_case(CRED_NAME_WORKLOAD_IDENTITY) => WI,
            a if a.eq_ignore_ascii_case(CRED_NAME_MANAGED_IDENTITY) => MI,
            a if a.eq_ignore_ascii_case(CRED_NAME_AZURE_CLI) => AZ,
            a if a.eq_ignore_ascii_case(CRED_NAME_AZURE_DEVELOPER_CLI) => AZD,
            a if a.eq_ignore_ascii_case(CRED_NAME_AZURE_POWERSHELL) => PS,
            _ => {
                return Err(format!(
                    r#"invalid AZURE_TOKEN_CREDENTIALS value {}. Valid values are "dev", "prod", or the name of any credential type in the default chain. See https://aka.ms/azsdk/go/identity/docs#DefaultAzureCredential for more information"#,
                    yaml::go_quote(&atc)
                ));
            }
        };
    }
    let mut creds: Vec<Arc<dyn TokenCredential>> = Vec::new();
    let reporter = |name: &str, e: String| -> Arc<dyn TokenCredential> {
        Arc::new(ErrorCredential {
            err: unavailable(name, &e),
        })
    };
    if selected & ENV != 0 {
        creds.push(
            new_environment_credential(opts).unwrap_or_else(|e| reporter(CRED_NAME_ENVIRONMENT, e)),
        );
    }
    if selected & WI != 0 {
        creds.push(
            match new_workload_identity_credential("", "", tenant_id, opts) {
                Ok(c) => Arc::new(c),
                Err(e) => reporter(CRED_NAME_WORKLOAD_IDENTITY, e),
            },
        );
    }
    if selected & MI != 0 {
        let mi = ManagedIdentityCredential::new(
            std::env::var("AZURE_CLIENT_ID").ok(),
            selected ^ MI != 0,
            opts,
        );
        mi.chained.store(true, Ordering::SeqCst);
        creds.push(Arc::new(mi));
    }
    if selected & AZ != 0 {
        creds.push(match AzureCliCredential::new(tenant_id, true) {
            Ok(c) => Arc::new(c),
            Err(e) => reporter(CRED_NAME_AZURE_CLI, e),
        });
    }
    if selected & AZD != 0 {
        creds.push(match AzureDeveloperCliCredential::new(tenant_id, true) {
            Ok(c) => Arc::new(c),
            Err(e) => reporter(CRED_NAME_AZURE_DEVELOPER_CLI, e),
        });
    }
    if selected & PS != 0 {
        creds.push(reporter(
            CRED_NAME_AZURE_POWERSHELL,
            "Azure PowerShell authentication is not supported by prometheus-rs".to_string(),
        ));
    }
    Ok(Arc::new(ChainedTokenCredential {
        name: "DefaultAzureCredential".to_string(),
        sources: creds,
        successful: Mutex::new(None),
    }))
}
