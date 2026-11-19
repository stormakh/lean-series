//! Port of `storage/remote/azuread/azuread.go`: the Azure AD round tripper for remote write. It adds
//! `Authorization: Bearer <token>` to each request; the token comes from a [`TokenProvider`] that caches it and
//! fetches a new one at half of its lifetime.
//!
//! The config types and their validation live in `config::azuread` (Go `AzureADConfig.Validate`). The
//! credentials are the blocking ports of azidentity v1.15.0 in `discovery::azure::identity` (shared with Azure
//! service discovery): managed identity (IMDS, App Service), OAuth client secret, workload identity (federated
//! token file), client certificate (PEM) and the SDK chain (`DefaultAzureCredential`: environment, workload
//! identity, managed identity, Azure CLI, Azure Developer CLI). Not possible there: PKCS#12 (PFX) certificates,
//! Azure PowerShell, username/password and the managed identity sources Azure Arc, Azure ML, Cloud Shell and
//! Service Fabric (each fails with a clear error).
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use config::azuread::{AZURE_CHINA, AZURE_GOVERNMENT, AZURE_PUBLIC, AzureAdConfig, CertificateConfig};
use discovery::azure::CloudConfiguration;
use discovery::azure::identity::{
    self as azidentity, CRED_NAME_CERT, CertCred, ClientCred, ConfidentialClient, IdentityHttp, IdentityOptions,
    ManagedIdentityCredential,
};
use discovery::cancel::CancelToken;
use net::{Error, Request, Response, RoundTripper};

pub use config::azuread::{
    AZURE_FEDERATED_TOKEN_FILE_ENV_VAR, DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH, ManagedIdentityConfig, OAuthConfig,
    SdkConfig, WorkloadIdentityConfig,
};

// Audiences.
pub const INGESTION_CHINA_AUDIENCE: &str = "https://monitor.azure.cn//.default";
pub const INGESTION_GOVERNMENT_AUDIENCE: &str = "https://monitor.azure.us//.default";
pub const INGESTION_PUBLIC_AUDIENCE: &str = "https://monitor.azure.com//.default";

/// Go `azcore.AccessToken`.
#[derive(Clone, Debug, PartialEq)]
pub struct AccessToken {
    pub token: String,
    pub expires_on: SystemTime,
}

/// Go `policy.TokenRequestOptions` (only the scopes are used here).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TokenRequestOptions {
    pub scopes: Vec<String>,
}

/// Go `azcore.TokenCredential`: the token source behind the provider (tests mock it).
pub trait TokenCredential: Send + Sync {
    fn get_token(&self, options: &TokenRequestOptions) -> Result<AccessToken, String>;
}

/// Adapts an azidentity credential to [`TokenCredential`].
struct SdkCredential(Arc<dyn azidentity::TokenCredential>);

impl TokenCredential for SdkCredential {
    fn get_token(&self, options: &TokenRequestOptions) -> Result<AccessToken, String> {
        let t = self.0.get_token(&CancelToken::new(), &options.scopes).map_err(|e| e.to_string())?;
        Ok(AccessToken { token: t.token, expires_on: t.expires_on })
    }
}

/// Go `tokenProvider`: stores the current token and its refresh time.
pub struct TokenProvider {
    /// The token and its refresh time (Go `token`, `refreshTime`, guarded by `mu`).
    state: Mutex<(String, SystemTime)>,
    credential_client: Arc<dyn TokenCredential>,
    options: TokenRequestOptions,
}

/// Go `newTokenProvider`: the custom scope if set, else the audience of the cloud.
pub fn new_token_provider(cfg: &AzureAdConfig, cred: Arc<dyn TokenCredential>) -> Result<TokenProvider, String> {
    let scope = if !cfg.scope.is_empty() { cfg.scope.clone() } else { get_audience(&cfg.cloud)?.to_string() };
    Ok(TokenProvider { state: Mutex::new((String::new(), SystemTime::UNIX_EPOCH)), credential_client: cred, options: TokenRequestOptions { scopes: vec![scope] } })
}

impl TokenProvider {
    /// Go `getAccessToken`: the cached token while valid, else a new one.
    pub fn get_access_token(&self) -> Result<String, String> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !st.0.is_empty() && st.1 > SystemTime::now() {
            return Ok(st.0.clone());
        }
        self.get_token(&mut st).map_err(|e| format!("Failed to get access token: {e}"))?;
        Ok(st.0.clone())
    }

    /// Go `getToken` and `updateRefreshTime`: the refresh time is half of the remaining lifetime.
    fn get_token(&self, st: &mut (String, SystemTime)) -> Result<(), String> {
        let t = self.credential_client.get_token(&self.options)?;
        if t.token.is_empty() {
            return Err("access token is empty".to_string());
        }
        st.0 = t.token;
        let now = SystemTime::now();
        let half = t.expires_on.duration_since(now).unwrap_or(Duration::ZERO) / 2;
        let refresh = now + half;
        if refresh <= SystemTime::now() {
            return Err("access token expiry is less than the current time".to_string());
        }
        st.1 = refresh;
        Ok(())
    }
}

/// Go `azureADRoundTripper`.
pub struct AzureAdRoundTripper {
    pub next: Arc<dyn RoundTripper>,
    pub token_provider: TokenProvider,
}

impl RoundTripper for AzureAdRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        let token = self.token_provider.get_access_token().map_err(Error::other)?;
        req.header.set("Authorization", &format!("Bearer {token}"));
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections();
    }
}

/// Go `NewAzureADRoundTripper`: `cfg` must be validated (as YAML loading does). `next` defaults to the default
/// transport.
pub fn new_azure_ad_round_tripper(cfg: &AzureAdConfig, next: Option<Arc<dyn RoundTripper>>) -> Result<Arc<dyn RoundTripper>, String> {
    let next = match next {
        Some(n) => n,
        None => Arc::new(net::Transport::default_transport()),
    };
    let cred = new_token_credential(cfg)?;
    let token_provider = new_token_provider(cfg, cred)?;
    Ok(Arc::new(AzureAdRoundTripper { next, token_provider }))
}

/// Go `newTokenCredential`: the credential of the configured kind for the configured cloud.
pub fn new_token_credential(cfg: &AzureAdConfig) -> Result<Arc<dyn TokenCredential>, String> {
    let cloud = get_cloud_configuration(&cfg.cloud)?;
    let opts = IdentityOptions {
        authority_host: cloud.active_directory_authority_host,
        imds_endpoint: azidentity::IMDS_ENDPOINT.to_string(),
        allow_http: false,
        http: IdentityHttp::new("")?,
    };
    let mut cred: Option<Arc<dyn azidentity::TokenCredential>> = None;
    if let Some(m) = &cfg.managed_identity {
        let id = if m.client_id.is_empty() { None } else { Some(m.client_id.clone()) };
        cred = Some(Arc::new(ManagedIdentityCredential::new(id, false, &opts)));
    }
    if let Some(w) = &cfg.workload_identity {
        cred = Some(Arc::new(azidentity::new_workload_identity_credential(&w.client_id, &w.token_file_path, &w.tenant_id, &opts)?));
    }
    if let Some(o) = &cfg.oauth {
        cred = Some(Arc::new(azidentity::new_client_secret_credential(&o.tenant_id, &o.client_id, o.client_secret.as_str(), &opts)?));
    }
    if let Some(s) = &cfg.sdk {
        cred = Some(azidentity::new_default_azure_credential(&s.tenant_id, &opts)?);
    }
    if let Some(c) = &cfg.certificate {
        cred = Some(Arc::new(new_certificate_token_credential(c, &opts)?));
    }
    match cred {
        Some(c) => Ok(Arc::new(SdkCredential(c))),
        // Go returns a nil credential here; a validated config always has one.
        None => Err("must provide an Azure Managed Identity, Azure Workload Identity, Azure OAuth, Azure Certificate or Azure SDK in the Azure AD config".to_string()),
    }
}

/// Go `newCertificateTokenCredential` (PEM only: PKCS#12 data fails in `parse_pem_certificates`).
fn new_certificate_token_credential(c: &CertificateConfig, opts: &IdentityOptions) -> Result<ConfidentialClient, String> {
    let mut data = std::fs::read(&c.certificate_path)
        .map_err(|e| format!("failed to read certificate file {}: open {}: {}", c.certificate_path, c.certificate_path, net::types::go_io_error(&e)))?;
    if !c.certificate_key_path.is_empty() {
        let key = std::fs::read(&c.certificate_key_path)
            .map_err(|e| format!("failed to read private key file {}: open {}: {}", c.certificate_key_path, c.certificate_key_path, net::types::go_io_error(&e)))?;
        data.push(b'\n');
        data.extend_from_slice(&key);
    }
    let (certs, key) = azidentity::parse_pem_certificates(&data, c.certificate_password.as_str()).map_err(|e| format!("failed to parse certificate data: {e}"))?;
    ConfidentialClient::new(&c.tenant_id, &c.client_id, CRED_NAME_CERT, ClientCred::Certificate(Box::new(CertCred { certs, key, send_x5c: c.send_certificate_chain })), opts)
}

/// Go `getAudience`.
pub fn get_audience(cloud: &str) -> Result<&'static str, String> {
    let c = cloud.to_lowercase();
    if c == AZURE_CHINA.to_lowercase() {
        Ok(INGESTION_CHINA_AUDIENCE)
    } else if c == AZURE_GOVERNMENT.to_lowercase() {
        Ok(INGESTION_GOVERNMENT_AUDIENCE)
    } else if c == AZURE_PUBLIC.to_lowercase() {
        Ok(INGESTION_PUBLIC_AUDIENCE)
    } else {
        Err(format!("Cloud is not specified or is incorrect: {cloud}"))
    }
}

/// Go `getCloudConfiguration`.
pub fn get_cloud_configuration(cloud: &str) -> Result<CloudConfiguration, String> {
    let c = cloud.to_lowercase();
    if c == AZURE_CHINA.to_lowercase() {
        Ok(CloudConfiguration::azure_china())
    } else if c == AZURE_GOVERNMENT.to_lowercase() {
        Ok(CloudConfiguration::azure_government())
    } else if c == AZURE_PUBLIC.to_lowercase() {
        Ok(CloudConfiguration::azure_public())
    } else {
        Err(format!("Cloud is not specified or is incorrect: {cloud}"))
    }
}
