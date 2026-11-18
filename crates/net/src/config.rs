//! prometheus/common config/http_config.go and headers.go: the HTTP client configuration, its YAML form,
//! validation (same messages) and relative path resolution.
use std::collections::BTreeMap;

use yaml::{Decoder, Error as YError, Marshal, Node, Secret, Struct, Unmarshal, Url, Value};

pub const GRANT_TYPE_JWT_BEARER: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
pub const VALID_SIGNATURE_ALGORITHMS: [&str; 3] = ["RS256", "RS384", "RS512"];

/// Go `config.JoinDir`.
pub fn join_dir(dir: &str, path: &str) -> String {
    if path.is_empty() || path.starts_with('/') {
        return path.to_string();
    }
    clean_path(&format!("{dir}/{path}"))
}

/// Go `filepath.Clean` for slash paths.
pub fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            x => out.push(x),
        }
    }
    let s = out.join("/");
    if rooted {
        format!("/{s}")
    } else if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}

/// Go `filepath.Dir`.
pub fn dir_of(p: &str) -> String {
    match p.rfind('/') {
        Some(0) => "/".to_string(),
        Some(i) => clean_path(&p[..i]),
        None => ".".to_string(),
    }
}

// ---------------- TLSVersion ----------------

pub const TLS10: u16 = 0x0301;
pub const TLS11: u16 = 0x0302;
pub const TLS12: u16 = 0x0303;
pub const TLS13: u16 = 0x0304;

/// config.TLSVersion (a Go tls.VersionTLS1x value, 0 when unset).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TlsVersion(pub u16);

impl TlsVersion {
    pub fn from_name(s: &str) -> Option<TlsVersion> {
        Some(TlsVersion(match s {
            "TLS13" => TLS13,
            "TLS12" => TLS12,
            "TLS11" => TLS11,
            "TLS10" => TLS10,
            _ => return None,
        }))
    }
    pub fn name(&self) -> Option<&'static str> {
        Some(match self.0 {
            TLS13 => "TLS13",
            TLS12 => "TLS12",
            TLS11 => "TLS11",
            TLS10 => "TLS10",
            _ => return None,
        })
    }
    /// Go `(*TLSVersion).String`.
    pub fn go_string(&self) -> String {
        if self.0 == 0 {
            return String::new();
        }
        self.name().map(|s| s.to_string()).unwrap_or_else(|| self.0.to_string())
    }
}

impl Unmarshal for TlsVersion {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            match TlsVersion::from_name(&s) {
                Some(v) => {
                    *self = v;
                    Ok(())
                }
                None => Err(YError::Msg(format!("unknown TLS version: {s}"))),
            }
        })
    }
    fn go_type() -> String {
        "config.TLSVersion".to_string()
    }
}
impl Marshal for TlsVersion {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match self.name() {
            Some(s) => Ok(Value::Str(s.to_string())),
            None => Err(format!("unknown TLS version: {}", self.0)),
        }
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

// ---------------- BasicAuth, Authorization ----------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicAuth {
    pub username: String,
    pub username_file: String,
    pub username_ref: String,
    pub password: Secret,
    pub password_file: String,
    pub password_ref: String,
}
yaml::yaml_struct!(BasicAuth, "config.plain", [
    (username: "username", omitempty)
    (username_file: "username_file", omitempty)
    (username_ref: "username_ref", omitempty)
    (password: "password", omitempty)
    (password_file: "password_file", omitempty)
    (password_ref: "password_ref", omitempty)
]);
impl Unmarshal for BasicAuth {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| d.inner_struct(n, self))
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for BasicAuth {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}
impl BasicAuth {
    pub fn set_directory(&mut self, dir: &str) {
        self.password_file = join_dir(dir, &self.password_file);
        self.username_file = join_dir(dir, &self.username_file);
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Authorization {
    pub type_: String,
    pub credentials: Secret,
    pub credentials_file: String,
    pub credentials_ref: String,
}
yaml::yaml_struct!(Authorization, "config.Authorization", [
    (type_: "type", omitempty)
    (credentials: "credentials", omitempty)
    (credentials_file: "credentials_file", omitempty)
    (credentials_ref: "credentials_ref", omitempty)
]);
yaml::impl_plain_struct!(Authorization);
impl Authorization {
    pub fn set_directory(&mut self, dir: &str) {
        self.credentials_file = join_dir(dir, &self.credentials_file);
    }
}

// ---------------- TLSConfig ----------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TlsConfig {
    pub ca: String,
    pub cert: String,
    pub key: Secret,
    pub ca_file: String,
    pub cert_file: String,
    pub key_file: String,
    pub ca_ref: String,
    pub cert_ref: String,
    pub key_ref: String,
    pub server_name: String,
    pub insecure_skip_verify: bool,
    pub min_version: TlsVersion,
    pub max_version: TlsVersion,
}
yaml::yaml_struct!(TlsConfig, "config.plain", [
    (ca: "ca", omitempty)
    (cert: "cert", omitempty)
    (key: "key", omitempty)
    (ca_file: "ca_file", omitempty)
    (cert_file: "cert_file", omitempty)
    (key_file: "key_file", omitempty)
    (ca_ref: "ca_ref", omitempty)
    (cert_ref: "cert_ref", omitempty)
    (key_ref: "key_ref", omitempty)
    (server_name: "server_name", omitempty)
    (insecure_skip_verify: "insecure_skip_verify")
    (min_version: "min_version", omitempty)
    (max_version: "max_version", omitempty)
]);
impl Unmarshal for TlsConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for TlsConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

fn count(v: &[bool]) -> usize {
    v.iter().filter(|b| **b).count()
}

impl TlsConfig {
    pub fn set_directory(&mut self, dir: &str) {
        self.ca_file = join_dir(dir, &self.ca_file);
        self.cert_file = join_dir(dir, &self.cert_file);
        self.key_file = join_dir(dir, &self.key_file);
    }
    pub fn using_client_cert(&self) -> bool {
        !self.cert.is_empty() || !self.cert_file.is_empty() || !self.cert_ref.is_empty()
    }
    pub fn using_client_key(&self) -> bool {
        !self.key.is_empty() || !self.key_file.is_empty() || !self.key_ref.is_empty()
    }
    pub fn validate(&self) -> Result<(), String> {
        if count(&[!self.ca.is_empty(), !self.ca_file.is_empty(), !self.ca_ref.is_empty()]) > 1 {
            return Err("at most one of ca, ca_file & ca_ref must be configured".to_string());
        }
        if count(&[!self.cert.is_empty(), !self.cert_file.is_empty(), !self.cert_ref.is_empty()]) > 1 {
            return Err("at most one of cert, cert_file & cert_ref must be configured".to_string());
        }
        if count(&[!self.key.is_empty(), !self.key_file.is_empty(), !self.key_ref.is_empty()]) > 1 {
            return Err("at most one of key and key_file must be configured".to_string());
        }
        if self.using_client_cert() && !self.using_client_key() {
            return Err("exactly one of key or key_file must be configured when a client certificate is configured".to_string());
        } else if self.using_client_key() && !self.using_client_cert() {
            return Err("exactly one of cert or cert_file must be configured when a client key is configured".to_string());
        }
        Ok(())
    }
}

// ---------------- ProxyConfig ----------------

/// config.ProxyHeader: header name to secret values.
pub type ProxyHeader = BTreeMap<String, Vec<Secret>>;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProxyConfig {
    pub proxy_url: Url,
    pub no_proxy: String,
    pub proxy_from_environment: bool,
    pub proxy_connect_header: ProxyHeader,
}
yaml::yaml_struct!(ProxyConfig, "config.ProxyConfig", [
    (proxy_url: "proxy_url", omitempty)
    (no_proxy: "no_proxy", omitempty)
    (proxy_from_environment: "proxy_from_environment", omitempty)
    (proxy_connect_header: "proxy_connect_header", omitempty)
]);
yaml::impl_plain_struct!(ProxyConfig);

impl ProxyConfig {
    fn proxy_url_set(&self) -> bool {
        self.proxy_url.0.as_ref().is_some_and(|u| !u.to_url_string().is_empty())
    }
    pub fn validate(&self) -> Result<(), String> {
        if !self.proxy_connect_header.is_empty() && !self.proxy_from_environment && !self.proxy_url_set() {
            return Err("if proxy_connect_header is configured, proxy_url or proxy_from_environment must also be configured".to_string());
        }
        if self.proxy_from_environment && self.proxy_url_set() {
            return Err("if proxy_from_environment is configured, proxy_url must not be configured".to_string());
        }
        if self.proxy_from_environment && !self.no_proxy.is_empty() {
            return Err("if proxy_from_environment is configured, no_proxy must not be configured".to_string());
        }
        if self.proxy_url.0.is_none() && !self.no_proxy.is_empty() {
            return Err("if no_proxy is configured, proxy_url must also be configured".to_string());
        }
        Ok(())
    }
    /// Go `ProxyConfig.Proxy()`: the proxy function, or None for direct connections.
    pub fn proxy(&self) -> Option<crate::proxy::ProxyFunc> {
        crate::proxy::from_config(self)
    }
    /// Go `GetProxyConnectHeader`.
    pub fn get_proxy_connect_header(&self) -> crate::Header {
        let mut h = crate::Header::new();
        for (k, vs) in &self.proxy_connect_header {
            for v in vs {
                h.0.push((k.clone(), v.0.clone()));
            }
        }
        h
    }
}

// ---------------- OAuth2 ----------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OAuth2 {
    pub client_id: String,
    pub client_secret: Secret,
    pub client_secret_file: String,
    pub client_secret_ref: String,
    pub client_certificate_key_id: String,
    pub client_certificate_key: Secret,
    pub client_certificate_key_file: String,
    pub client_certificate_key_ref: String,
    pub grant_type: String,
    pub signature_algorithm: String,
    pub iss: String,
    pub audience: String,
    /// `map[string]any`, as decoded YAML values (keys sorted).
    pub claims: BTreeMap<String, Value>,
    pub scopes: Vec<String>,
    pub token_url: String,
    pub endpoint_params: BTreeMap<String, String>,
    pub tls_config: TlsConfig,
    pub proxy_config: ProxyConfig,
}
yaml::yaml_struct!(OAuth2, "config.plain", [
    (client_id: "client_id", omitempty)
    (client_secret: "client_secret", omitempty)
    (client_secret_file: "client_secret_file", omitempty)
    (client_secret_ref: "client_secret_ref", omitempty)
    (client_certificate_key_id: "client_certificate_key_id", omitempty)
    (client_certificate_key: "client_certificate_key", omitempty)
    (client_certificate_key_file: "client_certificate_key_file", omitempty)
    (client_certificate_key_ref: "client_certificate_key_ref", omitempty)
    (grant_type: "grant_type", omitempty)
    (signature_algorithm: "signature_algorithm", omitempty)
    (iss: "iss", omitempty)
    (audience: "audience", omitempty)
    (claims: "claims", omitempty)
    (scopes: "scopes", omitempty)
    (token_url: "token_url", omitempty)
    (endpoint_params: "endpoint_params", omitempty)
    (tls_config: "tls_config", omitempty)
    (inline proxy_config)
]);
impl Unmarshal for OAuth2 {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            d.inner_struct(n, self)?;
            self.proxy_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for OAuth2 {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}
impl OAuth2 {
    pub fn set_directory(&mut self, dir: &str) {
        self.client_secret_file = join_dir(dir, &self.client_secret_file);
        self.tls_config.set_directory(dir);
    }
}

// ---------------- Headers ----------------

/// Reserved header names that `http_headers` may not set (config.ReservedHeaders).
pub const RESERVED_HEADERS: [&str; 18] = [
    "Authorization",
    "Host",
    "Content-Encoding",
    "Content-Length",
    "Content-Type",
    "User-Agent",
    "Connection",
    "Keep-Alive",
    "Proxy-Authenticate",
    "Proxy-Authorization",
    "Www-Authenticate",
    "Accept-Encoding",
    "X-Prometheus-Remote-Write-Version",
    "X-Prometheus-Remote-Read-Version",
    "X-Prometheus-Scrape-Timeout-Seconds",
    "X-Amz-Date",
    "X-Amz-Security-Token",
    "X-Amz-Content-Sha256",
];

/// config.Header: values, secrets and files (read on every request).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Header {
    pub values: Vec<String>,
    pub secrets: Vec<Secret>,
    pub files: Vec<String>,
}
yaml::yaml_struct!(Header, "config.Header", [
    (values: "values", omitempty)
    (secrets: "secrets", omitempty)
    (files: "files", omitempty)
]);
yaml::impl_plain_struct!(Header);
impl Header {
    pub fn set_directory(&mut self, dir: &str) {
        for f in self.files.iter_mut() {
            *f = join_dir(dir, f);
        }
    }
}

/// config.Headers: header name to Header (an inline map in Go).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Headers {
    pub headers: BTreeMap<String, Header>,
}
impl Unmarshal for Headers {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        match n.kind {
            yaml::Kind::Mapping => {
                // the inline map is reset, then unknown keys (all of them) go into it
                self.headers = BTreeMap::new();
                d.mapping_into_map(n, &mut self.headers)?;
                Ok(true)
            }
            yaml::Kind::Sequence => {
                d.terror(n, yaml::node::SEQ_TAG, "config.Headers");
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, "config.Headers");
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        "config.Headers".to_string()
    }
}
impl Marshal for Headers {
    fn marshal_yaml(&self) -> Result<Value, String> {
        self.headers.marshal_yaml()
    }
    fn is_zero(&self) -> bool {
        self.headers.is_empty()
    }
}
impl Headers {
    pub fn set_directory(&mut self, dir: &str) {
        for h in self.headers.values_mut() {
            h.set_directory(dir);
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        for n in self.headers.keys() {
            let c = crate::types::canonical_header_key(n);
            if RESERVED_HEADERS.contains(&c.as_str()) {
                return Err(format!("setting header {} is not allowed", yaml::go_quote(&c)));
            }
        }
        Ok(())
    }
}

// ---------------- HTTPClientConfig ----------------

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HttpClientConfig {
    pub basic_auth: Option<BasicAuth>,
    pub authorization: Option<Authorization>,
    pub oauth2: Option<OAuth2>,
    pub bearer_token: Secret,
    pub bearer_token_file: String,
    pub tls_config: TlsConfig,
    pub follow_redirects: bool,
    pub enable_http2: bool,
    pub proxy_config: ProxyConfig,
    pub http_headers: Option<Headers>,
}
yaml::yaml_struct!(HttpClientConfig, "config.plain", [
    (basic_auth: "basic_auth", omitempty)
    (authorization: "authorization", omitempty)
    (oauth2: "oauth2", omitempty)
    (bearer_token: "bearer_token", omitempty)
    (bearer_token_file: "bearer_token_file", omitempty)
    (tls_config: "tls_config", omitempty)
    (follow_redirects: "follow_redirects")
    (enable_http2: "enable_http2")
    (inline proxy_config)
    (http_headers: "http_headers", omitempty)
]);

impl Unmarshal for HttpClientConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = HttpClientConfig::default_config();
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "config.plain".to_string()
    }
}
impl Marshal for HttpClientConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        self.fields_zero()
    }
}

impl HttpClientConfig {
    /// config.DefaultHTTPClientConfig.
    pub fn default_config() -> HttpClientConfig {
        HttpClientConfig { follow_redirects: true, enable_http2: true, ..Default::default() }
    }

    pub fn set_directory(&mut self, dir: &str) {
        self.tls_config.set_directory(dir);
        if let Some(b) = self.basic_auth.as_mut() {
            b.set_directory(dir);
        }
        if let Some(a) = self.authorization.as_mut() {
            a.set_directory(dir);
        }
        if let Some(o) = self.oauth2.as_mut() {
            o.set_directory(dir);
        }
        if let Some(h) = self.http_headers.as_mut() {
            h.set_directory(dir);
        }
        self.bearer_token_file = join_dir(dir, &self.bearer_token_file);
    }

    /// Go `HTTPClientConfig.Validate` (also moves bearer_token(_file) into authorization, as Go does).
    pub fn validate(&mut self) -> Result<(), String> {
        let bt = !self.bearer_token.is_empty();
        let btf = !self.bearer_token_file.is_empty();
        if bt && btf {
            return Err("at most one of bearer_token & bearer_token_file must be configured".to_string());
        }
        if (self.basic_auth.is_some() || self.oauth2.is_some()) && (bt || btf) {
            return Err("at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured".to_string());
        }
        if let Some(b) = &self.basic_auth {
            if count(&[!b.username.is_empty(), !b.username_file.is_empty(), !b.username_ref.is_empty()]) > 1 {
                return Err("at most one of basic_auth username, username_file & username_ref must be configured".to_string());
            }
            if count(&[!b.password.is_empty(), !b.password_file.is_empty(), !b.password_ref.is_empty()]) > 1 {
                return Err("at most one of basic_auth password, password_file & password_ref must be configured".to_string());
            }
        }
        if let Some(a) = self.authorization.as_mut() {
            if bt || btf {
                return Err("authorization is not compatible with bearer_token & bearer_token_file".to_string());
            }
            if count(&[!a.credentials.is_empty(), !a.credentials_file.is_empty(), !a.credentials_ref.is_empty()]) > 1 {
                return Err("at most one of authorization credentials & credentials_file must be configured".to_string());
            }
            a.type_ = a.type_.trim().to_string();
            if a.type_.is_empty() {
                a.type_ = "Bearer".to_string();
            }
            if a.type_.to_lowercase() == "basic" {
                return Err(r#"authorization type cannot be set to "basic", use "basic_auth" instead"#.to_string());
            }
            if self.basic_auth.is_some() || self.oauth2.is_some() {
                return Err("at most one of basic_auth, oauth2 & authorization must be configured".to_string());
            }
        } else {
            if bt {
                self.authorization = Some(Authorization { type_: "Bearer".to_string(), credentials: std::mem::take(&mut self.bearer_token), ..Default::default() });
            }
            if btf {
                self.authorization = Some(Authorization { type_: "Bearer".to_string(), credentials_file: std::mem::take(&mut self.bearer_token_file), ..Default::default() });
            }
        }
        if let Some(o) = &self.oauth2 {
            if self.basic_auth.is_some() {
                return Err("at most one of basic_auth, oauth2 & authorization must be configured".to_string());
            }
            if o.client_id.is_empty() {
                return Err("oauth2 client_id must be configured".to_string());
            }
            if o.token_url.is_empty() {
                return Err("oauth2 token_url must be configured".to_string());
            }
            if o.grant_type == GRANT_TYPE_JWT_BEARER {
                if count(&[!o.client_certificate_key.is_empty(), !o.client_certificate_key_file.is_empty(), !o.client_certificate_key_ref.is_empty()]) > 1 {
                    return Err("at most one of oauth2 client_certificate_key, client_certificate_key_file & client_certificate_key_ref must be configured using grant-type=urn:ietf:params:oauth:grant-type:jwt-bearer".to_string());
                }
                if !o.signature_algorithm.is_empty() && !VALID_SIGNATURE_ALGORITHMS.contains(&o.signature_algorithm.as_str()) {
                    return Err("valid signature algorithms are RS256, RS384 and RS512".to_string());
                }
            } else if count(&[!o.client_secret.is_empty(), !o.client_secret_file.is_empty(), !o.client_secret_ref.is_empty()]) > 1 {
                return Err("at most one of oauth2 client_secret, client_secret_file & client_secret_ref must be configured using grant-type=client_credentials".to_string());
            }
        }
        self.proxy_config.validate()?;
        if let Some(h) = &self.http_headers {
            h.validate()?;
        }
        Ok(())
    }

    /// Go `HTTPClientConfig.String()` (YAML, secrets hidden).
    pub fn to_yaml_string(&self) -> String {
        match yaml::marshal(self) {
            Ok(s) => s,
            Err(e) => format!("<error creating http client config string: {e}>"),
        }
    }
}

/// Go `config.LoadHTTPConfig`: strict YAML decode and validation.
pub fn load_http_config(s: &str) -> Result<HttpClientConfig, YError> {
    yaml::unmarshal_strict(s)
}

/// Go `config.LoadHTTPConfigFile`: also resolves relative file paths against the file's directory.
pub fn load_http_config_file(filename: &str) -> Result<(HttpClientConfig, Vec<u8>), YError> {
    let content = crate::types::read_file(filename).map_err(YError::Msg)?;
    let mut cfg = load_http_config(&String::from_utf8_lossy(&content))?;
    cfg.set_directory(&dir_of(filename));
    Ok((cfg, content))
}
