//! `promtool check web-config` (main.go `CheckWebConfig`) with a port of exporter-toolkit (v0.20.0) `web.Validate`:
//! the web configuration file (TLS server config, HTTP server config, rate limit, basic auth users) parsed with
//! yaml strict unmarshal, and the checks of `validateUsers` and `ConfigToTLSConfig`, with Go's error texts.

use std::collections::BTreeMap;

use yaml::decode::Error as YError;
use yaml::node::{Kind, MAP_TAG, SEQ_TAG};
use yaml::resolve::Resolved;
use yaml::{Decoder, Marshal, Node, Secret, Unmarshal, Value};

use crate::{FAILURE_EXIT_CODE, Io, SUCCESS_EXIT_CODE};

/// Go `CheckWebConfig(files...)`.
pub fn check_web_config(files: &[String], io: &mut Io) -> i32 {
    let mut failed = false;
    for f in files {
        if let Err(e) = validate(f) {
            let _ = writeln!(io.stderr, "{f} FAILED: {e}");
            failed = true;
            continue;
        }
        let _ = writeln!(io.stderr, "{f} SUCCESS");
    }
    if failed {
        return FAILURE_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

/// Go `web.Config`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Config {
    pub tls_config: TlsConfig,
    pub http_config: HttpConfig,
    pub rate_limiter_config: RateLimiterConfig,
    pub users: BTreeMap<String, Secret>,
}

yaml::yaml_struct!(Config, "web.Config", [
    (tls_config: "tls_server_config")
    (http_config: "http_server_config")
    (rate_limiter_config: "rate_limit")
    (users: "basic_auth_users")
]);
yaml::impl_plain_struct!(Config);

/// Go `web.TLSConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TlsConfig {
    pub tls_cert: String,
    pub tls_key: Secret,
    pub client_cas_text: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub client_auth: String,
    pub client_cas: String,
    pub cipher_suites: Vec<Cipher>,
    pub curve_preferences: Vec<Curve>,
    pub min_version: TlsVersion,
    pub max_version: TlsVersion,
    /// Parsed and ignored.
    pub prefer_server_cipher_suites: bool,
    pub client_allowed_sans: Vec<String>,
}

yaml::yaml_struct!(TlsConfig, "web.TLSConfig", [
    (tls_cert: "cert")
    (tls_key: "key")
    (client_cas_text: "client_ca")
    (tls_cert_path: "cert_file")
    (tls_key_path: "key_file")
    (client_auth: "client_auth_type")
    (client_cas: "client_ca_file")
    (cipher_suites: "cipher_suites")
    (curve_preferences: "curve_preferences")
    (min_version: "min_version")
    (max_version: "max_version")
    (prefer_server_cipher_suites: "prefer_server_cipher_suites")
    (client_allowed_sans: "client_allowed_sans")
]);
yaml::impl_plain_struct!(TlsConfig);

/// Go `web.HTTPConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HttpConfig {
    pub http2: bool,
    pub header: BTreeMap<String, String>,
}

yaml::yaml_struct!(HttpConfig, "web.HTTPConfig", [
    (http2: "http2")
    (header: "headers", omitempty)
]);
yaml::impl_plain_struct!(HttpConfig);

/// Go `web.RateLimiterConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RateLimiterConfig {
    pub burst: i64,
    pub interval: GoDuration,
}

yaml::yaml_struct!(RateLimiterConfig, "web.RateLimiterConfig", [
    (burst: "burst")
    (interval: "interval")
]);
yaml::impl_plain_struct!(RateLimiterConfig);

/// A Go `time.Duration` field as go-yaml v2 decodes it: an integer (nanoseconds) or a `time.ParseDuration`
/// string.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GoDuration(pub i64);

impl Unmarshal for GoDuration {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        match n.kind {
            Kind::Scalar => {
                let (tag, r) = d.scalar_tag(n)?;
                let v = match r {
                    Resolved::Int(i) => Some(i),
                    Resolved::Uint(u) => i64::try_from(u).ok(),
                    Resolved::Float(f) if f <= i64::MAX as f64 && f >= i64::MIN as f64 => Some(f as i64),
                    Resolved::Str(s) => discovery::ionos::server::parse_go_duration(&s)
                        .ok()
                        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)),
                    _ => None,
                };
                match v {
                    Some(v) => {
                        self.0 = v;
                        Ok(true)
                    }
                    None => {
                        d.terror(n, &tag, "time.Duration");
                        Ok(false)
                    }
                }
            }
            Kind::Mapping => {
                d.terror(n, MAP_TAG, "time.Duration");
                Ok(false)
            }
            _ => {
                d.terror(n, SEQ_TAG, "time.Duration");
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        "time.Duration".to_string()
    }
}

impl Marshal for GoDuration {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Int(self.0))
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

/// Decodes a string with a custom `UnmarshalYAML` that maps it to a value or an error text.
fn unmarshal_named<T>(
    out: &mut T,
    n: &Node,
    d: &mut Decoder,
    lookup: impl FnOnce(&str) -> Result<T, String>,
) -> yaml::Result<bool> {
    d.custom(|d| {
        let mut s = String::new();
        d.inner(n, &mut s)?;
        *out = lookup(&s).map_err(YError::Msg)?;
        Ok(())
    })
}

/// Go `tls.CipherSuites()` (Go 1.26): the secure suites by name.
const CIPHER_SUITES: &[(&str, u16)] = &[
    ("TLS_AES_128_GCM_SHA256", 0x1301),
    ("TLS_AES_256_GCM_SHA384", 0x1302),
    ("TLS_CHACHA20_POLY1305_SHA256", 0x1303),
    ("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", 0xc009),
    ("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", 0xc00a),
    ("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", 0xc013),
    ("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", 0xc014),
    ("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", 0xc02b),
    ("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", 0xc02c),
    ("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", 0xc02f),
    ("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", 0xc030),
    ("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256", 0xcca8),
    ("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256", 0xcca9),
];

/// Go `web.Cipher`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cipher(pub u16);

impl Unmarshal for Cipher {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        unmarshal_named(self, n, d, |s| {
            CIPHER_SUITES
                .iter()
                .find(|(name, _)| *name == s)
                .map(|(_, id)| Cipher(*id))
                .ok_or_else(|| format!("unknown cipher: {s}"))
        })
    }
    fn go_type() -> String {
        "web.Cipher".to_string()
    }
}

impl Marshal for Cipher {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match CIPHER_SUITES.iter().find(|(_, id)| *id == self.0) {
            Some((name, _)) => Ok(Value::Str(name.to_string())),
            None => Ok(Value::Str(format!("0x{:04X}", self.0))),
        }
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

const CURVES: &[(&str, u16)] = &[("CurveP256", 23), ("CurveP384", 24), ("CurveP521", 25), ("X25519", 29)];

/// Go `web.Curve`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Curve(pub u16);

impl Unmarshal for Curve {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        unmarshal_named(self, n, d, |s| {
            CURVES
                .iter()
                .find(|(name, _)| *name == s)
                .map(|(_, id)| Curve(*id))
                .ok_or_else(|| format!("unknown curve: {s}"))
        })
    }
    fn go_type() -> String {
        "web.Curve".to_string()
    }
}

impl Marshal for Curve {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match CURVES.iter().find(|(_, id)| *id == self.0) {
            Some((name, _)) => Ok(Value::Str(name.to_string())),
            None => Ok(Value::Str(self.0.to_string())),
        }
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

const TLS_VERSIONS: &[(&str, u16)] = &[("TLS13", 0x0304), ("TLS12", 0x0303), ("TLS11", 0x0302), ("TLS10", 0x0301)];

/// Go `web.TLSVersion`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TlsVersion(pub u16);

impl Unmarshal for TlsVersion {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        unmarshal_named(self, n, d, |s| {
            TLS_VERSIONS
                .iter()
                .find(|(name, _)| *name == s)
                .map(|(_, id)| TlsVersion(*id))
                .ok_or_else(|| format!("unknown TLS version: {s}"))
        })
    }
    fn go_type() -> String {
        "web.TLSVersion".to_string()
    }
}

impl Marshal for TlsVersion {
    fn marshal_yaml(&self) -> Result<Value, String> {
        match TLS_VERSIONS.iter().find(|(_, id)| *id == self.0) {
            Some((name, _)) => Ok(Value::Str(name.to_string())),
            None => Ok(Value::Str(self.0.to_string())),
        }
    }
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl TlsConfig {
    /// Go `IsEnabled`.
    pub fn is_enabled(&self) -> bool {
        !self.tls_cert_path.is_empty()
            || !self.tls_cert.is_empty()
            || !self.tls_key_path.is_empty()
            || !self.tls_key.0.is_empty()
            || !self.client_cas.is_empty()
            || !self.client_cas_text.is_empty()
            || !self.client_auth.is_empty()
    }

    /// Go `SetDirectory`.
    pub fn set_directory(&mut self, dir: &str) {
        self.tls_cert_path = join_dir(dir, &self.tls_cert_path);
        self.tls_key_path = join_dir(dir, &self.tls_key_path);
        self.client_cas = join_dir(dir, &self.client_cas);
    }
}

/// Go `config.JoinDir`.
fn join_dir(dir: &str, path: &str) -> String {
    if path.is_empty() || path.starts_with('/') {
        return path.to_string();
    }
    net::config::clean_path(&format!("{dir}/{path}"))
}

/// Go `filepath.Dir`.
fn file_dir(path: &str) -> String {
    let dir = match path.rfind('/') {
        Some(i) => &path[..=i],
        None => "",
    };
    net::config::clean_path(dir)
}

/// Go `os.ReadFile` error texts.
fn read_file(path: &str) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path).map_err(|e| format!("open {path}: {}", net::types::go_io_error(&e)))?;
    let mut data = Vec::new();
    f.read_to_end(&mut data).map_err(|e| {
        let msg = if e.raw_os_error() == Some(21) { "is a directory".to_string() } else { net::types::go_io_error(&e) };
        format!("read {path}: {msg}")
    })?;
    Ok(data)
}

/// The header values that may be configured (Go `extraHTTPHeaders`).
const EXTRA_HTTP_HEADERS: &[(&str, &[&str])] = &[
    ("Strict-Transport-Security", &[]),
    ("X-Content-Type-Options", &["nosniff"]),
    ("X-Frame-Options", &["deny", "sameorigin"]),
    ("X-XSS-Protection", &[]),
    ("Content-Security-Policy", &[]),
];

fn go_q(s: &str) -> String {
    textparse::strconv::quote_bytes(s.as_bytes())
}

/// Go `validateHeaderConfig` (Go walks the map in random order; this checks in name order).
fn validate_header_config(headers: &BTreeMap<String, String>) -> Result<(), String> {
    for (k, v) in headers {
        let Some((_, values)) = EXTRA_HTTP_HEADERS.iter().find(|(name, _)| name == k) else {
            return Err(format!("HTTP header {} can not be configured", go_q(k)));
        };
        if values.iter().any(|a| a == v) {
            continue;
        }
        if !values.is_empty() {
            let list: Vec<String> = values.iter().map(|s| go_q(s)).collect();
            return Err(format!(
                "invalid value for {k}. Expected one of: [{}], but got: {}",
                list.join(" "),
                go_q(v)
            ));
        }
    }
    Ok(())
}

/// Go `getConfig`.
pub fn get_config(config_path: &str) -> Result<Config, String> {
    let content = read_file(config_path)?;
    let mut c = Config {
        tls_config: TlsConfig {
            min_version: TlsVersion(0x0303),
            max_version: TlsVersion(0x0304),
            prefer_server_cipher_suites: true,
            ..Default::default()
        },
        http_config: HttpConfig { http2: true, ..Default::default() },
        ..Default::default()
    };
    let text = String::from_utf8_lossy(&content);
    let mut err = yaml::unmarshal_into(&text, &mut c, true).map_err(|e| e.to_string()).err();
    if err.is_none() {
        err = validate_header_config(&c.http_config.header).err();
    }
    c.tls_config.set_directory(&file_dir(config_path));
    match err {
        Some(e) => Err(e),
        None => Ok(c),
    }
}

/// Go `bcrypt.Cost` (golang.org/x/crypto v0.58.0): parses the version and cost of a bcrypt hash.
pub fn bcrypt_cost(hashed: &[u8]) -> Result<i64, String> {
    const MIN_HASH_SIZE: usize = 59;
    const MAJOR_VERSION: u8 = b'2';
    if hashed.len() < MIN_HASH_SIZE {
        return Err("crypto/bcrypt: hashedSecret too short to be a bcrypted password".to_string());
    }
    if hashed[0] != b'$' {
        return Err(format!(
            "crypto/bcrypt: bcrypt hashes must start with '$', but hashedSecret started with '{}'",
            char::from(hashed[0])
        ));
    }
    if hashed[1] > MAJOR_VERSION {
        return Err(format!(
            "crypto/bcrypt: bcrypt algorithm version '{}' requested is newer than current version '{}'",
            char::from(hashed[1]),
            char::from(MAJOR_VERSION)
        ));
    }
    let n = if hashed[2] != b'$' { 4 } else { 3 };
    let cost_bytes = &hashed[n..n + 2];
    let cost = atoi(cost_bytes)?;
    if !(4..=31).contains(&cost) {
        return Err(format!("crypto/bcrypt: cost {cost} is outside allowed inclusive range 4..31"));
    }
    Ok(cost)
}

/// Go `strconv.Atoi` of a short string.
fn atoi(s: &[u8]) -> Result<i64, String> {
    let err = || format!("strconv.Atoi: parsing {}: invalid syntax", textparse::strconv::quote_bytes(s));
    let (neg, digits) = match s.first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.iter().all(|c| c.is_ascii_digit()) {
        return Err(err());
    }
    let mut v: i64 = 0;
    for &c in digits {
        v = v * 10 + (c - b'0') as i64;
    }
    Ok(if neg { -v } else { v })
}

/// Go `validateUsers`.
fn validate_users(config_path: &str) -> Result<(), String> {
    let c = get_config(config_path)?;
    // Go walks the users map in random order; this checks them in name order.
    for p in c.users.values() {
        bcrypt_cost(p.0.as_bytes())?;
    }
    Ok(())
}

const ERR_NO_TLS_CONFIG: &str = "TLS config is not present";

/// Go `validateTLSPaths`.
fn validate_tls_paths(c: &TlsConfig) -> Result<(), String> {
    if !c.is_enabled() {
        return Err(ERR_NO_TLS_CONFIG.to_string());
    }
    if c.tls_cert_path.is_empty() && c.tls_cert.is_empty() {
        return Err("missing one of cert or cert_file".to_string());
    }
    if c.tls_key_path.is_empty() && c.tls_key.0.is_empty() {
        return Err("missing one of key or key_file".to_string());
    }
    Ok(())
}

/// The checks of Go `ConfigToTLSConfig` (the `tls.Config` itself is not built). `Err(None)` is
/// `errNoTLSConfig`.
pub fn config_to_tls_config(c: &TlsConfig) -> Result<(), Option<String>> {
    if let Err(e) = validate_tls_paths(c) {
        return Err(if e == ERR_NO_TLS_CONFIG { None } else { Some(e) });
    }

    // Confirm that certificate and key paths are valid (Go loadCert).
    let cert_data = if !c.tls_cert_path.is_empty() {
        read_file(&c.tls_cert_path)
            .map_err(|e| Some(format!("failed to read cert_file ({}): {}", c.tls_cert_path, e)))?
    } else {
        c.tls_cert.as_bytes().to_vec()
    };
    let key_data = if !c.tls_key_path.is_empty() {
        read_file(&c.tls_key_path).map_err(|e| Some(format!("failed to read key_file ({}): {}", c.tls_key_path, e)))?
    } else {
        c.tls_key.0.as_bytes().to_vec()
    };
    net::tls::x509_key_pair(&cert_data, &key_data).map_err(|e| Some(format!("failed to load X509KeyPair: {e}")))?;

    if !c.client_cas.is_empty() {
        // Go appends the PEM certificates to a pool and ignores bad ones; only a read error fails.
        read_file(&c.client_cas).map_err(Some)?;
    }

    let no_client_cert = match c.client_auth.as_str() {
        "RequestClientCert" | "RequireAnyClientCert" | "RequireClientCert" | "VerifyClientCertIfGiven"
        | "RequireAndVerifyClientCert" => false,
        "" | "NoClientCert" => true,
        _ => return Err(Some(format!("invalid ClientAuth: {}", c.client_auth))),
    };

    if (!c.client_cas.is_empty() || !c.client_cas_text.is_empty()) && no_client_cert {
        return Err(Some("client CA's have been configured without a Client Auth Policy".to_string()));
    }
    Ok(())
}

/// Go `web.Validate`.
pub fn validate(tls_config_path: &str) -> Result<(), String> {
    if tls_config_path.is_empty() {
        return Ok(());
    }
    validate_users(tls_config_path)?;
    let c = get_config(tls_config_path)?;
    match config_to_tls_config(&c.tls_config) {
        Ok(()) | Err(None) => Ok(()),
        Err(Some(e)) => Err(e),
    }
}
