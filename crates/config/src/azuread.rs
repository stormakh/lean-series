//! The configuration part of `storage/remote/azuread` and `storage/remote/googleiam` (the `azuread` and
//! `google_iam` sections of `remote_write`). The round trippers belong to the remote write client.
use yaml::{Decoder, Error as YError, Node, Secret, Unmarshal};

pub const AZURE_CHINA: &str = "AzureChina";
pub const AZURE_GOVERNMENT: &str = "AzureGovernment";
pub const AZURE_PUBLIC: &str = "AzurePublic";
pub const DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH: &str = "/var/run/secrets/azure/tokens/azure-identity-token";
pub const AZURE_FEDERATED_TOKEN_FILE_ENV_VAR: &str = "AZURE_FEDERATED_TOKEN_FILE";

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ManagedIdentityConfig {
    pub client_id: String,
}
yaml::yaml_struct!(ManagedIdentityConfig, "azuread.ManagedIdentityConfig", [
    (client_id: "client_id", omitempty)
]);
yaml::impl_plain_struct!(ManagedIdentityConfig);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WorkloadIdentityConfig {
    pub client_id: String,
    pub tenant_id: String,
    pub token_file_path: String,
}
yaml::yaml_struct!(WorkloadIdentityConfig, "azuread.WorkloadIdentityConfig", [
    (client_id: "client_id", omitempty)
    (tenant_id: "tenant_id", omitempty)
    (token_file_path: "token_file_path", omitempty)
]);
yaml::impl_plain_struct!(WorkloadIdentityConfig);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: Secret,
    pub tenant_id: String,
}
yaml::yaml_struct!(OAuthConfig, "azuread.OAuthConfig", [
    (client_id: "client_id", omitempty)
    (client_secret: "client_secret", omitempty)
    (tenant_id: "tenant_id", omitempty)
]);
yaml::impl_plain_struct!(OAuthConfig);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdkConfig {
    pub tenant_id: String,
}
yaml::yaml_struct!(SdkConfig, "azuread.SDKConfig", [
    (tenant_id: "tenant_id", omitempty)
]);
yaml::impl_plain_struct!(SdkConfig);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CertificateConfig {
    pub client_id: String,
    pub tenant_id: String,
    pub certificate_path: String,
    pub certificate_key_path: String,
    pub certificate_password: Secret,
    pub send_certificate_chain: bool,
}
yaml::yaml_struct!(CertificateConfig, "azuread.CertificateConfig", [
    (client_id: "client_id", omitempty)
    (tenant_id: "tenant_id", omitempty)
    (certificate_path: "certificate_path", omitempty)
    (certificate_key_path: "certificate_key_path", omitempty)
    (certificate_password: "certificate_password", omitempty)
    (send_certificate_chain: "send_certificate_chain", omitempty)
]);
yaml::impl_plain_struct!(CertificateConfig);

/// Go `azuread.AzureADConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AzureAdConfig {
    pub managed_identity: Option<ManagedIdentityConfig>,
    pub workload_identity: Option<WorkloadIdentityConfig>,
    pub oauth: Option<OAuthConfig>,
    pub sdk: Option<SdkConfig>,
    pub certificate: Option<CertificateConfig>,
    pub cloud: String,
    pub scope: String,
}
yaml::yaml_struct!(AzureAdConfig, "azuread.plain", [
    (managed_identity: "managed_identity", omitempty)
    (workload_identity: "workload_identity", omitempty)
    (oauth: "oauth", omitempty)
    (sdk: "sdk", omitempty)
    (certificate: "certificate", omitempty)
    (cloud: "cloud", omitempty)
    (scope: "scope", omitempty)
]);

impl Unmarshal for AzureAdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = AzureAdConfig::default();
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "azuread.plain".to_string()
    }
}
impl yaml::Marshal for AzureAdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

/// google/uuid `Parse`: 36 chars with dashes, 38 with braces, 45 with `urn:uuid:`, or 32 hex digits.
pub fn uuid_parse(s: &str) -> bool {
    let b = s.as_bytes();
    let core: &[u8] = match b.len() {
        36 => b,
        45 => {
            if !s[..9].eq_ignore_ascii_case("urn:uuid:") {
                return false;
            }
            &b[9..]
        }
        38 => {
            if b[0] != b'{' || b[37] != b'}' {
                return false;
            }
            &b[1..37]
        }
        32 => return b.iter().all(|c| c.is_ascii_hexdigit()),
        _ => return false,
    };
    if core[8] != b'-' || core[13] != b'-' || core[18] != b'-' || core[23] != b'-' {
        return false;
    }
    core.iter().enumerate().all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) || c.is_ascii_hexdigit())
}

/// `^[\w\s:/.\-]+$` with Go's ASCII classes.
fn scope_ok(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'\t' | b'\n' | b'\x0c' | b'\r' | b' ' | b':' | b'/' | b'.' | b'-'))
}

impl AzureAdConfig {
    /// Go `Validate` (also fills in the default cloud and token path).
    pub fn validate(&mut self) -> Result<(), String> {
        if self.cloud.is_empty() {
            self.cloud = AZURE_PUBLIC.to_string();
        }
        if self.cloud != AZURE_CHINA && self.cloud != AZURE_GOVERNMENT && self.cloud != AZURE_PUBLIC {
            return Err("must provide a cloud in the Azure AD config".into());
        }
        let n = [self.managed_identity.is_some(), self.workload_identity.is_some(), self.oauth.is_some(), self.sdk.is_some(), self.certificate.is_some()].iter().filter(|b| **b).count();
        if n == 0 {
            return Err("must provide an Azure Managed Identity, Azure Workload Identity, Azure OAuth, Azure Certificate or Azure SDK in the Azure AD config".into());
        }
        if n > 1 {
            return Err("cannot provide multiple authentication methods in the Azure AD config".into());
        }
        if let Some(m) = &self.managed_identity
            && !m.client_id.is_empty()
            && !uuid_parse(&m.client_id)
        {
            return Err("the provided Azure Managed Identity client_id is invalid".into());
        }
        if let Some(w) = self.workload_identity.as_mut() {
            if w.client_id.is_empty() {
                return Err("must provide an Azure Workload Identity client_id in the Azure AD config".into());
            }
            if w.tenant_id.is_empty() {
                return Err("must provide an Azure Workload Identity tenant_id in the Azure AD config".into());
            }
            if !uuid_parse(&w.client_id) {
                return Err("the provided Azure Workload Identity client_id is invalid".into());
            }
            if !uuid_parse(&w.tenant_id) {
                return Err("the provided Azure Workload Identity tenant_id is invalid".into());
            }
            if w.token_file_path.is_empty() {
                w.token_file_path = match std::env::var(AZURE_FEDERATED_TOKEN_FILE_ENV_VAR) {
                    Ok(p) if !p.is_empty() => p,
                    _ => DEFAULT_WORKLOAD_IDENTITY_TOKEN_PATH.to_string(),
                };
            }
        }
        if let Some(o) = &self.oauth {
            if o.client_id.is_empty() {
                return Err("must provide an Azure OAuth client_id in the Azure AD config".into());
            }
            if o.client_secret.is_empty() {
                return Err("must provide an Azure OAuth client_secret in the Azure AD config".into());
            }
            if o.tenant_id.is_empty() {
                return Err("must provide an Azure OAuth tenant_id in the Azure AD config".into());
            }
            if !uuid_parse(&o.client_id) {
                return Err("the provided Azure OAuth client_id is invalid".into());
            }
            // Go checks only the error of regexp.MatchString, which is never set: any tenant_id passes.
        }
        if let Some(c) = &self.certificate {
            if c.client_id.is_empty() {
                return Err("must provide an Azure Certificate client_id in the Azure AD config".into());
            }
            if c.tenant_id.is_empty() {
                return Err("must provide an Azure Certificate tenant_id in the Azure AD config".into());
            }
            if c.certificate_path.is_empty() {
                return Err("must provide an Azure Certificate certificate_path in the Azure AD config".into());
            }
            if !uuid_parse(&c.client_id) {
                return Err("the provided Azure Certificate client_id is invalid".into());
            }
        }
        if !self.scope.is_empty() && !scope_ok(&self.scope) {
            return Err("the provided scope contains invalid characters".into());
        }
        Ok(())
    }
}

/// Go `googleiam.Config`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GoogleIamConfig {
    pub credentials_file: String,
}
yaml::yaml_struct!(GoogleIamConfig, "googleiam.Config", [
    (credentials_file: "credentials_file", omitempty)
]);
yaml::impl_plain_struct!(GoogleIamConfig);
