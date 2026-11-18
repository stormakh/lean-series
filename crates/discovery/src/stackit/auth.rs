//! The part of github.com/stackitcloud/stackit-sdk-go/core v0.27.1 (`auth`, `clients`) that the STACKIT SD
//! uses: `auth.SetupAuth` with the configuration Prometheus builds, so either the no-auth flow (no service
//! account key: the HTTP client config authenticates) or the key flow (a service account key from the config,
//! a file, the `STACKIT_*` environment or the credentials file; a private key from the same places or from the
//! key itself; a self-signed RS512 JWT exchanged for an access token at the token endpoint, refreshed with the
//! refresh token). JWTs are read as golang-jwt v5 `ParseUnverified` reads them.
//!
//! Differs from upstream: an access token without `exp` is an error here (Go dereferences a nil pointer and
//! panics); a private key that `ring` cannot load (not RSA, or RSA smaller than 2048 bits, which Go accepts) gives
//! "key is not a valid RSA private key" (Go gives x509's parse error for malformed keys).
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use json::Json;
use net::{Error, Request, Response, RoundTripper};

use crate::sd_json::{self, StructTy, Ty};

/// Go `tokenAPI`.
pub const TOKEN_API: &str = "https://service-account.api.stackit.cloud/token";
/// Go `DefaultClientTimeout`.
pub const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_secs(60);
/// Go `defaultTokenExpirationLeeway`.
const TOKEN_EXPIRATION_LEEWAY: Duration = Duration::from_secs(5);
/// Go `credentialsFilePath`.
const CREDENTIALS_FILE_PATH: &str = ".stackit/credentials.json";

/// The fields of Go `config.Configuration` that `SetupAuth` reads.
#[derive(Clone, Default)]
pub struct Configuration {
    pub no_auth: bool,
    pub service_account_key: String,
    pub private_key: String,
    pub service_account_key_path: String,
    pub private_key_path: String,
    pub credentials_file_path: String,
    pub token_custom_url: String,
}

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

/// Go `auth.SetupAuth`: the round tripper that authenticates requests sent through `transport`.
pub fn setup_auth(cfg: &mut Configuration, transport: Arc<dyn RoundTripper>) -> Result<Arc<dyn RoundTripper>, String> {
    if cfg.no_auth {
        // Go NoAuthFlow: the transport as is.
        return Ok(Arc::new(NoAuthFlow { rt: transport }));
    }
    if !cfg.service_account_key.is_empty() || !cfg.service_account_key_path.is_empty() {
        return key_auth(cfg, transport).map(|k| k as Arc<dyn RoundTripper>).map_err(|e| format!("configuring key authentication: {e}"));
    }
    // Unreachable from Prometheus: NoAuth is set exactly when there is no service account key.
    key_auth(cfg, transport).map(|k| k as Arc<dyn RoundTripper>).map_err(|e| format!("configuring default authentication: no valid credentials were found: trying key flow: {e}"))
}

/// Go `clients.NoAuthFlow`.
pub struct NoAuthFlow {
    rt: Arc<dyn RoundTripper>,
}

impl RoundTripper for NoAuthFlow {
    fn round_trip(&self, req: Request) -> Result<Response, Error> {
        self.rt.round_trip(req)
    }
}

/// Go `auth.Credentials` (the credentials file).
const CREDENTIALS: Ty = Ty::Struct(&StructTy {
    go: "auth.Credentials",
    fields: &[
        ("STACKIT_SERVICE_ACCOUNT_EMAIL", Ty::Str),
        ("STACKIT_SERVICE_ACCOUNT_TOKEN", Ty::Str),
        ("STACKIT_SERVICE_ACCOUNT_KEY_PATH", Ty::Str),
        ("STACKIT_PRIVATE_KEY_PATH", Ty::Str),
        ("STACKIT_SERVICE_ACCOUNT_KEY", Ty::Str),
        ("STACKIT_PRIVATE_KEY", Ty::Str),
    ],
});

/// Go `readCredentialsFile`.
fn read_credentials_file(path: &str) -> Result<Json, String> {
    let path = if !path.is_empty() {
        path.to_string()
    } else {
        match env("STACKIT_CREDENTIALS_PATH").filter(|p| !p.is_empty()) {
            Some(p) => p,
            None => {
                let home = env("HOME").filter(|h| !h.is_empty()).ok_or_else(|| "getting home directory: $HOME is not defined".to_string())?;
                format!("{home}/{CREDENTIALS_FILE_PATH}")
            }
        }
    };
    let raw = net::types::read_file(&path).map_err(|e| format!("opening file: {e}"))?;
    sd_json::unmarshal(&raw, &CREDENTIALS).map_err(|e| format!("unmarshalling credentials: {e}"))
}

/// Go `readCredential`.
fn read_credential(field: &str, what: &str, credentials: &Json) -> Result<String, String> {
    let v = sd_json::str_of(credentials, field);
    if v.is_empty() {
        return Err(format!("{what} is empty or not set"));
    }
    Ok(v)
}

/// Go `getKey`.
#[allow(clippy::too_many_arguments)]
fn get_key(cfg_key: &mut String, cfg_key_path: &mut String, env_key_path: &str, env_key: &str, cred_path: (&str, &str), cred_key: (&str, &str), cred_file_path: &str) -> Result<(), String> {
    if !cfg_key.is_empty() {
        return Ok(());
    }
    if cfg_key_path.is_empty() {
        let key_path = env(env_key_path).unwrap_or_default();
        let key = env(env_key).unwrap_or_default();
        if key_path.is_empty() && key.is_empty() {
            let credentials = read_credentials_file(cred_file_path).map_err(|e| format!("reading from credentials file: {e}"))?;
            match read_credential(cred_path.0, cred_path.1, &credentials) {
                Ok(p) => *cfg_key_path = p,
                Err(_) => {
                    let k = read_credential(cred_key.0, cred_key.1, &credentials).map_err(|e| format!("neither key nor path is provided in the configuration, environment variable, or credentials file: {e}"))?;
                    *cfg_key = k;
                    return Ok(());
                }
            }
        } else if key_path.is_empty() {
            *cfg_key = key;
            return Ok(());
        } else {
            *cfg_key_path = key_path;
        }
    }
    let b = net::types::read_file(cfg_key_path).map_err(|e| format!("reading key from file path: {e}"))?;
    if b.is_empty() {
        return Err("key path points to an empty file".to_string());
    }
    *cfg_key = String::from_utf8_lossy(&b).into_owned();
    Ok(())
}

/// google/uuid `Parse` (the canonical lower-case form on success).
pub fn parse_uuid(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let hex = |s: &[u8]| -> Result<Vec<u8>, String> { crypto::hex_decode(std::str::from_utf8(s).map_err(|_| "invalid UUID format".to_string())?).ok_or_else(|| "invalid UUID format".to_string()) };
    let body: &[u8] = match b.len() {
        36 => b,
        45 => {
            if !s[..9].eq_ignore_ascii_case("urn:uuid:") {
                return Err(format!("invalid urn prefix: {}", yaml::go_quote(&s[..9])));
            }
            &b[9..]
        }
        38 => &b[1..37],
        32 => {
            if !b.iter().all(u8::is_ascii_hexdigit) {
                return Err("invalid UUID format".to_string());
            }
            return Ok(format_uuid(&hex(b)?));
        }
        n => return Err(format!("invalid UUID length: {n}")),
    };
    if body[8] != b'-' || body[13] != b'-' || body[18] != b'-' || body[23] != b'-' {
        return Err("invalid UUID format".to_string());
    }
    let digits: Vec<u8> = body.iter().enumerate().filter(|(i, _)| ![8, 13, 18, 23].contains(i)).map(|(_, c)| *c).collect();
    if !digits.iter().all(u8::is_ascii_hexdigit) {
        return Err("invalid UUID format".to_string());
    }
    Ok(format_uuid(&hex(&digits)?))
}

fn format_uuid(u: &[u8]) -> String {
    let h = crypto::hex(u);
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

fn check_uuid(s: &str) -> Result<(), String> {
    parse_uuid(s).map(|_| ())
}

/// google/uuid `New` (version 4).
fn new_uuid() -> String {
    let mut b = [0u8; 16];
    crypto::random_bytes(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format_uuid(&b)
}

const UUID: Ty = Ty::Text("uuid.UUID", check_uuid);

/// Go `clients.ServiceAccountKeyResponse`.
const SERVICE_ACCOUNT_KEY_RESPONSE: Ty = Ty::Struct(&StructTy {
    go: "clients.ServiceAccountKeyResponse",
    fields: &[
        ("active", Ty::Bool),
        ("createdAt", Ty::Unmarshaler(sd_json::check_time)),
        (
            "credentials",
            Ty::Struct(&StructTy {
                go: "clients.ServiceAccountKeyCredentials",
                fields: &[("aud", Ty::Str), ("iss", Ty::Str), ("kid", Ty::Str), ("privateKey", Ty::Str), ("sub", UUID), ("tokenEndpoint", Ty::Str)],
            }),
        ),
        ("id", UUID),
        ("keyAlgorithm", Ty::Str),
        ("keyOrigin", Ty::Str),
        ("keyType", Ty::Str),
        ("publicKey", Ty::Str),
        ("validUntil", Ty::Unmarshaler(sd_json::check_time)),
    ],
});

/// Go `ServiceAccountKeyCredentials` (the fields the key flow uses).
#[derive(Clone, Debug, Default)]
pub struct ServiceAccountKeyCredentials {
    pub aud: String,
    pub iss: String,
    pub kid: String,
    pub private_key: Option<String>,
    /// The `sub` UUID in canonical form.
    pub sub: String,
    pub token_endpoint: String,
}

/// Go `KeyAuth`.
fn key_auth(cfg: &mut Configuration, transport: Arc<dyn RoundTripper>) -> Result<Arc<KeyFlow>, String> {
    let cred_file = cfg.credentials_file_path.clone();
    get_key(
        &mut cfg.service_account_key,
        &mut cfg.service_account_key_path,
        "STACKIT_SERVICE_ACCOUNT_KEY_PATH",
        "STACKIT_SERVICE_ACCOUNT_KEY",
        ("STACKIT_SERVICE_ACCOUNT_KEY_PATH", "service account key path"),
        ("STACKIT_SERVICE_ACCOUNT_KEY", "service account key"),
        &cred_file,
    )
    .map_err(|e| format!("configuring key authentication: service account key could not be found: {e}"))?;
    let key = sd_json::unmarshal(cfg.service_account_key.as_bytes(), &SERVICE_ACCOUNT_KEY_RESPONSE).map_err(|e| {
        let suffix = if cfg.service_account_key.starts_with("-----BEGIN") { " - it seems like the provided service account key is in PEM format. Please provide it in JSON format." } else { "" };
        format!("unmarshalling service account key: {e}{suffix}")
    })?;
    let credentials = sd_json::ptr(&key, "credentials").map(|c| ServiceAccountKeyCredentials {
        aud: sd_json::str_of(c, "aud"),
        iss: sd_json::str_of(c, "iss"),
        kid: sd_json::str_of(c, "kid"),
        private_key: sd_json::opt_str(c, "privateKey"),
        sub: sd_json::val(c, "sub").and_then(Json::as_str).and_then(|s| parse_uuid(s).ok()).unwrap_or_else(|| format_uuid(&[0; 16])),
        token_endpoint: sd_json::str_of(c, "tokenEndpoint"),
    });
    if let Err(e) = get_key(
        &mut cfg.private_key,
        &mut cfg.private_key_path,
        "STACKIT_PRIVATE_KEY_PATH",
        "STACKIT_PRIVATE_KEY",
        ("STACKIT_PRIVATE_KEY_PATH", "private key path"),
        ("STACKIT_PRIVATE_KEY", "private key"),
        &cred_file,
    ) {
        let extracted = credentials.as_ref().and_then(|c| c.private_key.clone()).unwrap_or_default();
        if extracted.is_empty() {
            return Err(format!("configuring key authentication: private key is not part of the service account key and could not be found: {e}"));
        }
        cfg.private_key = extracted;
    }
    if cfg.token_custom_url.is_empty()
        && let Some(u) = env("STACKIT_TOKEN_BASEURL")
    {
        cfg.token_custom_url = u;
    }
    KeyFlow::init(credentials, &cfg.private_key, &cfg.token_custom_url, transport).map(Arc::new).map_err(|e| format!("error initializing client: {e}"))
}

/// Go `TokenResponseBody`.
#[derive(Clone, Debug, Default)]
pub struct TokenResponseBody {
    pub access_token: String,
    pub expires_in: i64,
    pub refresh_token: String,
    pub scope: String,
    pub token_type: String,
}

const TOKEN_RESPONSE_BODY: Ty = Ty::Struct(&StructTy {
    go: "clients.TokenResponseBody",
    fields: &[("access_token", Ty::Str), ("expires_in", sd_json::INT), ("refresh_token", Ty::Str), ("scope", Ty::Str), ("token_type", Ty::Str)],
});

/// Go `clients.KeyFlow`.
pub struct KeyFlow {
    rt: Arc<dyn RoundTripper>,
    auth_client: net::Client,
    token_url: String,
    credentials: ServiceAccountKeyCredentials,
    key_pair: ring::signature::RsaKeyPair,
    token: Mutex<TokenResponseBody>,
}

/// golang-jwt `ParseRSAPrivateKeyFromPEM` (as an RSA key pair for signing).
pub fn parse_rsa_private_key_from_pem(key: &str) -> Result<ring::signature::RsaKeyPair, String> {
    let blocks = net::tls::pem_blocks(key.as_bytes());
    let Some(b) = blocks.first() else {
        return Err("invalid key: Key must be a PEM encoded PKCS1 or PKCS8 key".to_string());
    };
    ring::signature::RsaKeyPair::from_der(&b.der).or_else(|_| ring::signature::RsaKeyPair::from_pkcs8(&b.der)).map_err(|_| "key is not a valid RSA private key".to_string())
}

impl KeyFlow {
    /// Go `KeyFlow.Init` (with `validate`).
    fn init(credentials: Option<ServiceAccountKeyCredentials>, private_key: &str, token_url: &str, rt: Arc<dyn RoundTripper>) -> Result<KeyFlow, String> {
        let token_url = if !token_url.is_empty() {
            token_url.to_string()
        } else {
            match credentials.as_ref().map(|c| c.token_endpoint.as_str()) {
                Some(e) if !e.is_empty() => e.to_string(),
                _ => TOKEN_API.to_string(),
            }
        };
        let mut auth_client = net::Client::new(rt.clone());
        auth_client.timeout = Some(DEFAULT_CLIENT_TIMEOUT);
        if private_key.is_empty() {
            return Err("private key cannot be empty".to_string());
        }
        let key_pair = parse_rsa_private_key_from_pem(private_key).map_err(|e| format!("parse private key from PEM file: {e}"))?;
        Ok(KeyFlow { rt, auth_client, token_url, credentials: credentials.unwrap_or_default(), key_pair, token: Mutex::new(TokenResponseBody::default()) })
    }

    /// Go `GetAccessToken`.
    pub fn get_access_token(&self) -> Result<String, String> {
        let access = self.token.lock().unwrap_or_else(|p| p.into_inner()).access_token.clone();
        let expired = token_expired(&access, TOKEN_EXPIRATION_LEEWAY).map_err(|e| format!("check access token is expired: {e}"))?;
        if !expired {
            return Ok(access);
        }
        if let Err(e) = self.recreate_access_token() {
            let msg = match e {
                TokenError::Api { status, body } => {
                    let text = generic_open_api_error(status, &body);
                    if key_not_found(&body) { format!("check if your configured key is valid and if the token endpoint is configured correct: {text}") } else { text }
                }
                TokenError::Other(s) => s,
            };
            return Err(format!("get new access token: {msg}"));
        }
        Ok(self.token.lock().unwrap_or_else(|p| p.into_inner()).access_token.clone())
    }

    /// Go `recreateAccessToken`.
    fn recreate_access_token(&self) -> Result<(), TokenError> {
        let refresh = self.token.lock().unwrap_or_else(|p| p.into_inner()).refresh_token.clone();
        let refresh_expired = token_expired(&refresh, TOKEN_EXPIRATION_LEEWAY).map_err(TokenError::Other)?;
        let token = if !refresh_expired { self.request_token("refresh_token", &refresh)? } else { self.request_token("urn:ietf:params:oauth:grant-type:jwt-bearer", &self.generate_self_signed_jwt().map_err(TokenError::Other)?)? };
        *self.token.lock().unwrap_or_else(|p| p.into_inner()) = token;
        Ok(())
    }

    /// Go `generateSelfSignedJWT`.
    fn generate_self_signed_jwt(&self) -> Result<String, String> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
        let c = &self.credentials;
        let header = Json::obj().with("alg", "RS512").with("kid", c.kid.as_str()).with("typ", "JWT");
        let claims = Json::obj().with("aud", c.aud.as_str()).with("exp", now + 3600).with("iat", now).with("iss", c.iss.as_str()).with("jti", new_uuid()).with("sub", c.sub.as_str());
        let signing_input = format!("{}.{}", b64url(header.stringify().as_bytes()), b64url(claims.stringify().as_bytes()));
        let mut sig = vec![0u8; self.key_pair.public().modulus_len()];
        self.key_pair.sign(&ring::signature::RSA_PKCS1_SHA512, &ring::rand::SystemRandom::new(), signing_input.as_bytes(), &mut sig).map_err(|_| "crypto/rsa: signing failed".to_string())?;
        Ok(format!("{signing_input}.{}", b64url(&sig)))
    }

    /// Go `requestToken` + `parseTokenResponse`.
    fn request_token(&self, grant: &str, assertion: &str) -> Result<TokenResponseBody, TokenError> {
        let mut form = vec![("grant_type".to_string(), grant.to_string())];
        let field = if grant == "refresh_token" { "refresh_token" } else { "assertion" };
        form.push((field.to_string(), assertion.to_string()));
        let body = yaml::url::encode_query(&form);
        let mut req = Request::new("POST", &self.token_url, body.into_bytes()).map_err(|e| TokenError::Other(e.to_string()))?;
        req.header.add("Content-Type", "application/x-www-form-urlencoded");
        let res = self.auth_client.do_request(req).map_err(|e| TokenError::Other(e.to_string()))?;
        if res.status != 200 {
            return Err(TokenError::Api { status: res.status, body: res.body });
        }
        let v = sd_json::unmarshal(&res.body, &TOKEN_RESPONSE_BODY).map_err(|e| TokenError::Other(format!("unmarshal token response: {e}")))?;
        Ok(TokenResponseBody {
            access_token: sd_json::str_of(&v, "access_token"),
            expires_in: sd_json::int_of(&v, "expires_in"),
            refresh_token: sd_json::str_of(&v, "refresh_token"),
            scope: sd_json::str_of(&v, "scope"),
            token_type: sd_json::str_of(&v, "token_type"),
        })
    }
}

impl RoundTripper for KeyFlow {
    /// Go `KeyFlow.RoundTrip`.
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        let token = self.get_access_token().map_err(Error::other)?;
        req.header.set("Authorization", &format!("Bearer {token}"));
        self.rt.round_trip(req)
    }
}

enum TokenError {
    /// Go `*oapierror.GenericOpenAPIError` from a token response that is not 200.
    Api { status: u16, body: Vec<u8> },
    Other(String),
}

/// Go `oapierror.GenericOpenAPIError.Error()` with an empty message.
fn generic_open_api_error(status: u16, body: &[u8]) -> String {
    const LIMIT: usize = 500;
    if body.len() <= LIMIT {
        return format!(", status code {status}, Body: {}\n", String::from_utf8_lossy(body));
    }
    let start = LIMIT / 2;
    let end = body.len() - LIMIT / 2;
    format!(", status code {status}, Body: {} [...truncated {} characters...] {}", String::from_utf8_lossy(&body[..start]), end - start, String::from_utf8_lossy(&body[end..]))
}

/// `regexp.MustCompile("Key with kid .*? was not found").Match(body)`.
fn key_not_found(body: &[u8]) -> bool {
    let s = String::from_utf8_lossy(body);
    s.split('\n').any(|line| line.find("Key with kid ").is_some_and(|i| line[i + 13..].contains(" was not found")))
}

fn b64url(b: &[u8]) -> String {
    crypto::base64url(b).trim_end_matches('=').to_string()
}

/// Go `base64.RawURLEncoding.DecodeString` (CR and LF are skipped).
pub fn raw_url_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut n = 0;
    let mut last = 0;
    for (i, &c) in s.as_bytes().iter().enumerate() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'\r' | b'\n' => continue,
            _ => return Err(format!("illegal base64 data at input byte {i}")),
        };
        last = i;
        acc = (acc << 6) | v as u32;
        n += 1;
        if n == 4 {
            out.extend_from_slice(&[(acc >> 16) as u8, (acc >> 8) as u8, acc as u8]);
            acc = 0;
            n = 0;
        }
    }
    match n {
        0 => {}
        1 => return Err(format!("illegal base64 data at input byte {last}")),
        2 => out.push((acc >> 4) as u8),
        _ => out.extend_from_slice(&[(acc >> 10) as u8, (acc >> 2) as u8]),
    }
    Ok(out)
}

/// golang-jwt v5 `SigningMethod` names.
const SIGNING_METHODS: [&str; 14] = ["HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA", "none"];

/// jwt.NumericDate `UnmarshalJSON` as a check.
fn check_numeric_date(v: &Json) -> Result<(), String> {
    match v {
        Json::Null | Json::Int(_) | Json::Float(_) => Ok(()),
        other => Err(format!("json: cannot unmarshal {} into Go value of type json.Number", match other {
            Json::Bool(_) => "bool",
            Json::Str(_) => "string",
            Json::Arr(_) => "array",
            _ => "object",
        })),
    }
}

/// jwt.ClaimStrings `UnmarshalJSON` as a check.
fn check_claim_strings(v: &Json) -> Result<(), String> {
    match v {
        Json::Null | Json::Str(_) => Ok(()),
        Json::Arr(a) if a.iter().all(|x| matches!(x, Json::Str(_))) => Ok(()),
        _ => Err("invalid type for claim".to_string()),
    }
}

const REGISTERED_CLAIMS: Ty = Ty::Struct(&StructTy {
    go: "jwt.RegisteredClaims",
    fields: &[
        ("iss", Ty::Str),
        ("sub", Ty::Str),
        ("aud", Ty::Unmarshaler(check_claim_strings)),
        ("exp", Ty::Unmarshaler(check_numeric_date)),
        ("nbf", Ty::Unmarshaler(check_numeric_date)),
        ("iat", Ty::Unmarshaler(check_numeric_date)),
        ("jti", Ty::Str),
    ],
});

/// golang-jwt v5 `Parser.ParseUnverified` with `RegisteredClaims`: the `exp` claim (seconds, fractional).
pub fn parse_unverified_exp(token: &str) -> Result<Option<f64>, String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("token is malformed: token contains an invalid number of segments".to_string());
    }
    let header = raw_url_decode(parts[0]).map_err(|e| format!("token is malformed: could not base64 decode header: {e}"))?;
    let header = sd_json::unmarshal(&header, &Ty::Map(&Ty::Any)).map_err(|e| format!("token is malformed: could not JSON decode header: {e}"))?;
    let claims = raw_url_decode(parts[1]).map_err(|e| format!("token is malformed: could not base64 decode claim: {e}"))?;
    let claims = sd_json::unmarshal(&claims, &REGISTERED_CLAIMS).map_err(|e| format!("token is malformed: could not JSON decode claim: {e}"))?;
    // a map: the key matches exactly
    match header.as_obj().and_then(|o| o.iter().find(|(k, _)| k == "alg")).map(|(_, v)| v) {
        Some(Json::Str(alg)) => {
            if !SIGNING_METHODS.contains(&alg.as_str()) {
                return Err("token is unverifiable: signing method (alg) is unavailable".to_string());
            }
        }
        _ => return Err("token is unverifiable: signing method (alg) is unspecified".to_string()),
    }
    raw_url_decode(parts[2]).map_err(|e| format!("token is malformed: could not base64 decode signature: {e}"))?;
    Ok(header_exp(&claims))
}

fn header_exp(claims: &Json) -> Option<f64> {
    sd_json::ptr(claims, "exp").and_then(Json::as_f64)
}

/// Go `tokenExpired`.
pub fn token_expired(token: &str, leeway: Duration) -> Result<bool, String> {
    if token.is_empty() {
        return Ok(true);
    }
    let exp = parse_unverified_exp(token).map_err(|e| format!("parse token: {e}"))?;
    let Some(exp) = exp else {
        return Err("get expiration timestamp: token has no exp claim".to_string());
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or(0.0) + leeway.as_secs_f64();
    // NumericDate keeps whole seconds (jwt.TimePrecision).
    Ok(now >= exp.trunc())
}
