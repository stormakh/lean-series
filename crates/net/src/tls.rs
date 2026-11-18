//! TLS settings from a TLSConfig (Go `NewTLSConfig`): CA pool, server name, versions, a client certificate
//! read again at every handshake (Go `GetClientCertificate`), and the rustls client config built from them.
//! Errors carry Go's text ("tls: failed to find any PEM data in key input", "x509: certificate signed by unknown
//! authority", "remote error: tls: bad certificate").
use std::sync::{Arc, Mutex, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer, ServerName, UnixTime};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::config::{TLS12, TLS13, TlsConfig};
use crate::secret::{SecretManager, SecretReader, to_secret};

pub fn provider() -> Arc<CryptoProvider> {
    static P: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    P.get_or_init(|| Arc::new(rustls::crypto::ring::default_provider())).clone()
}

/// One PEM block.
#[derive(Debug, Clone, PartialEq)]
pub struct PemBlock {
    pub type_: String,
    pub der: Vec<u8>,
}

/// Go `pem.Decode` in a loop: every well-formed block, in order.
pub fn pem_blocks(data: &[u8]) -> Vec<PemBlock> {
    let text = String::from_utf8_lossy(data);
    let mut out = Vec::new();
    let mut rest: &str = &text;
    while let Some(i) = rest.find("-----BEGIN ") {
        let after = &rest[i + 11..];
        let Some(j) = after.find("-----") else { break };
        let type_ = after[..j].to_string();
        let body_start = &after[j + 5..];
        let end_marker = format!("-----END {type_}-----");
        let Some(k) = body_start.find(&end_marker) else {
            rest = body_start;
            continue;
        };
        let body = &body_start[..k];
        // skip RFC 1421 headers ("Key: value" lines before an empty line)
        let body = if body.contains(':') { body.split("\n\n").last().unwrap_or(body) } else { body };
        let b64: String = body.chars().filter(|c| !c.is_whitespace()).collect();
        if let Some(der) = crypto::base64_decode(&b64) {
            out.push(PemBlock { type_, der });
        }
        rest = &body_start[k + end_marker.len()..];
    }
    out
}

/// Go `tls.X509KeyPair`, with the same error texts.
pub fn x509_key_pair(cert_pem: &[u8], key_pem: &[u8]) -> Result<CertifiedKey, String> {
    let mut certs: Vec<CertificateDer<'static>> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    for b in pem_blocks(cert_pem) {
        if b.type_ == "CERTIFICATE" {
            certs.push(CertificateDer::from(b.der));
        } else {
            skipped.push(b.type_);
        }
    }
    if certs.is_empty() {
        if skipped.is_empty() {
            return Err("tls: failed to find any PEM data in certificate input".to_string());
        }
        if skipped.len() == 1 && skipped[0].ends_with("PRIVATE KEY") {
            return Err("tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched".to_string());
        }
        return Err(format!("tls: failed to find \"CERTIFICATE\" PEM block in certificate input after skipping PEM blocks of the following types: [{}]", skipped.join(" ")));
    }
    let mut skipped: Vec<String> = Vec::new();
    let mut key_block = None;
    for b in pem_blocks(key_pem) {
        if b.type_ == "PRIVATE KEY" || b.type_.ends_with(" PRIVATE KEY") {
            key_block = Some(b);
            break;
        }
        skipped.push(b.type_);
    }
    let Some(kb) = key_block else {
        if skipped.is_empty() {
            return Err("tls: failed to find any PEM data in key input".to_string());
        }
        if skipped.len() == 1 && skipped[0] == "CERTIFICATE" {
            return Err("tls: found a certificate rather than a key in the PEM for the private key".to_string());
        }
        return Err(format!("tls: failed to find PEM block with type ending in \"PRIVATE KEY\" in key input after skipping PEM blocks of the following types: [{}]", skipped.join(" ")));
    };
    let p = provider();
    // Go tries PKCS#1, PKCS#8 and SEC 1 whatever the block type says
    let candidates: [PrivateKeyDer<'static>; 3] = [
        PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(kb.der.clone())),
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(kb.der.clone())),
        PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(kb.der.clone())),
    ];
    let mut signing = None;
    for c in candidates {
        if let Ok(k) = p.key_provider.load_private_key(c) {
            signing = Some(k);
            break;
        }
    }
    let Some(signing) = signing else { return Err("tls: failed to parse private key".to_string()) };
    let ck = CertifiedKey::new(certs, signing);
    match ck.keys_match() {
        Ok(()) | Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => Ok(ck),
        Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::KeyMismatch)) => Err("tls: private key does not match public key".to_string()),
        Err(e) => Err(format!("x509: {}", tls_error_text(&e))),
    }
}

/// The client certificate, read from its sources at every use (Go `TLSConfig.getClientCertificate`).
pub struct ClientCertSource {
    pub cfg: TlsConfig,
    pub manager: Option<Arc<dyn SecretManager>>,
    last_error: Mutex<Option<String>>,
}

impl std::fmt::Debug for ClientCertSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientCertSource")
    }
}

impl ClientCertSource {
    pub fn new(cfg: &TlsConfig, manager: Option<Arc<dyn SecretManager>>) -> ClientCertSource {
        ClientCertSource { cfg: cfg.clone(), manager, last_error: Mutex::new(None) }
    }

    pub fn get(&self) -> Result<CertifiedKey, String> {
        let c = &self.cfg;
        let cert_secret = to_secret(self.manager.as_ref(), &yaml::Secret(c.cert.clone()), &c.cert_file, &c.cert_ref).map_err(|e| format!("unable to use client cert: {e}"))?;
        let mut cert_data = String::new();
        if let Some(s) = &cert_secret {
            cert_data = s.fetch().map_err(|e| format!("unable to read specified client cert: {e}"))?;
        }
        let key_secret = to_secret(self.manager.as_ref(), &c.key, &c.key_file, &c.key_ref).map_err(|e| format!("unable to use client key: {e}"))?;
        let mut key_data = String::new();
        if let Some(s) = &key_secret {
            key_data = s.fetch().map_err(|e| format!("unable to read specified client key: {e}"))?;
        }
        let desc = |s: &Option<Arc<dyn SecretReader>>| s.as_ref().map(|s| s.description()).unwrap_or_default();
        x509_key_pair(cert_data.as_bytes(), key_data.as_bytes()).map_err(|e| format!("unable to use specified client cert ({}) & key ({}): {}", desc(&cert_secret), desc(&key_secret), e))
    }

    /// The error of the last failed handshake-time load (the handshake then went on without a certificate).
    pub fn take_error(&self) -> Option<String> {
        self.last_error.lock().map(|mut g| g.take()).unwrap_or(None)
    }
}

impl rustls::client::ResolvesClientCert for ClientCertSource {
    fn resolve(&self, _root_hint_subjects: &[&[u8]], _sigschemes: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        match self.get() {
            Ok(k) => Some(Arc::new(k)),
            Err(e) => {
                if let Ok(mut g) = self.last_error.lock() {
                    *g = Some(e);
                }
                None
            }
        }
    }
    fn has_certs(&self) -> bool {
        true
    }
}

/// What Go keeps in a *tls.Config for a client.
#[derive(Debug, Clone, Default)]
pub struct TlsClientConfig {
    /// None = the system roots.
    pub root_cas: Option<Vec<CertificateDer<'static>>>,
    pub server_name: String,
    pub insecure_skip_verify: bool,
    pub min_version: u16,
    pub max_version: u16,
    pub client_cert: Option<Arc<ClientCertSource>>,
}

/// Go `x509.CertPool.AppendCertsFromPEM`: the parsable certificates, None when there are none.
pub fn parse_ca_pem(data: &[u8]) -> Option<Vec<CertificateDer<'static>>> {
    let mut out = Vec::new();
    for b in pem_blocks(data) {
        if b.type_ != "CERTIFICATE" || !b.der.is_empty() && b.der[0] != 0x30 {
            continue;
        }
        let der = CertificateDer::from(b.der);
        let mut store = rustls::RootCertStore::empty();
        if store.add(der.clone()).is_ok() {
            out.push(der);
        }
    }
    if out.is_empty() { None } else { Some(out) }
}

/// Go `NewTLSConfigWithContext`.
pub fn new_tls_config(cfg: &TlsConfig, manager: Option<&Arc<dyn SecretManager>>) -> Result<TlsClientConfig, String> {
    cfg.validate()?;
    let mut t = TlsClientConfig { insecure_skip_verify: cfg.insecure_skip_verify, min_version: cfg.min_version.0, max_version: cfg.max_version.0, ..Default::default() };
    if cfg.max_version.0 != 0 && cfg.min_version.0 != 0 && cfg.max_version.0 < cfg.min_version.0 {
        return Err("tls_config.max_version must be greater than or equal to tls_config.min_version if both are specified".to_string());
    }
    let ca = to_secret(manager, &yaml::Secret(cfg.ca.clone()), &cfg.ca_file, &cfg.ca_ref).map_err(|e| format!("unable to use CA cert: {e}"))?;
    if let Some(ca) = ca {
        let data = ca.fetch().map_err(|e| format!("unable to read CA cert: {e}"))?;
        match parse_ca_pem(data.as_bytes()) {
            Some(roots) => t.root_cas = Some(roots),
            None => return Err(format!("unable to use specified CA cert {}", ca.description())),
        }
    }
    if !cfg.server_name.is_empty() {
        t.server_name = cfg.server_name.clone();
    }
    if cfg.using_client_cert() && cfg.using_client_key() {
        let src = ClientCertSource::new(cfg, manager.cloned());
        src.get()?;
        t.client_cert = Some(Arc::new(src));
    }
    Ok(t)
}

fn system_roots() -> Arc<rustls::RootCertStore> {
    static R: OnceLock<Arc<rustls::RootCertStore>> = OnceLock::new();
    R.get_or_init(|| {
        let mut store = rustls::RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        for c in native.certs {
            let _ = store.add(c);
        }
        if store.is_empty() {
            store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        Arc::new(store)
    })
    .clone()
}

/// Accepts any server certificate (Go InsecureSkipVerify); handshake signatures are still checked.
#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(&self, _: &CertificateDer<'_>, _: &[CertificateDer<'_>], _: &ServerName<'_>, _: &[u8], _: UnixTime) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

impl TlsClientConfig {
    /// The rustls config. TLS 1.0 and 1.1 are not available: a minimum below 1.2 means 1.2.
    pub fn to_rustls(&self) -> Result<Arc<rustls::ClientConfig>, String> {
        let p = provider();
        let lo = if self.min_version == 0 { TLS12 } else { self.min_version.max(TLS12) };
        let hi = if self.max_version == 0 { TLS13 } else { self.max_version };
        let mut versions: Vec<&'static rustls::SupportedProtocolVersion> = Vec::new();
        if lo <= TLS13 && hi >= TLS13 {
            versions.push(&rustls::version::TLS13);
        }
        if lo <= TLS12 && hi >= TLS12 {
            versions.push(&rustls::version::TLS12);
        }
        if versions.is_empty() {
            return Err("tls: no supported versions satisfy MinVersion and MaxVersion".to_string());
        }
        let b = rustls::ClientConfig::builder_with_provider(p.clone()).with_protocol_versions(&versions).map_err(|e| e.to_string())?;
        let b = if self.insecure_skip_verify {
            b.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify(p.clone())))
        } else {
            let roots = match &self.root_cas {
                Some(certs) => {
                    let mut s = rustls::RootCertStore::empty();
                    for c in certs {
                        let _ = s.add(c.clone());
                    }
                    Arc::new(s)
                }
                None => system_roots(),
            };
            b.with_root_certificates(roots)
        };
        let c = match &self.client_cert {
            Some(src) => b.with_client_cert_resolver(src.clone()),
            None => b.with_no_client_auth(),
        };
        Ok(Arc::new(c))
    }

    /// The server name to verify and send: server_name, else the URL host.
    pub fn server_name_for(&self, host: &str) -> Result<ServerName<'static>, String> {
        let name = if self.server_name.is_empty() { host } else { self.server_name.as_str() };
        let name = name.trim_start_matches('[').trim_end_matches(']');
        ServerName::try_from(name.to_string()).map_err(|_| format!("tls: invalid server name {}", yaml::go_quote(name)))
    }
}

fn alert_text(a: &rustls::AlertDescription) -> String {
    use rustls::AlertDescription as A;
    match a {
        A::CloseNotify => "close notify",
        A::UnexpectedMessage => "unexpected message",
        A::BadRecordMac => "bad record MAC",
        A::RecordOverflow => "record overflow",
        A::HandshakeFailure => "handshake failure",
        A::BadCertificate => "bad certificate",
        A::UnsupportedCertificate => "unsupported certificate",
        A::CertificateRevoked => "revoked certificate",
        A::CertificateExpired => "expired certificate",
        A::CertificateUnknown => "unknown certificate",
        A::IllegalParameter => "illegal parameter",
        A::UnknownCA => "unknown certificate authority",
        A::AccessDenied => "access denied",
        A::DecodeError => "error decoding message",
        A::DecryptError => "error decrypting message",
        A::ProtocolVersion => "protocol version not supported",
        A::InsufficientSecurity => "insufficient security level",
        A::InternalError => "internal error",
        A::UserCanceled => "user canceled",
        A::NoRenegotiation => "no renegotiation",
        A::MissingExtension => "missing extension",
        A::UnsupportedExtension => "unsupported extension",
        A::UnrecognisedName => "unrecognized name",
        A::CertificateRequired => "certificate required",
        A::NoApplicationProtocol => "no application protocol",
        other => return format!("alert({other:?})"),
    }
    .to_string()
}

/// Go's text for a TLS error.
pub fn tls_error_text(e: &rustls::Error) -> String {
    use rustls::CertificateError as C;
    match e {
        rustls::Error::AlertReceived(a) => format!("remote error: tls: {}", alert_text(a)),
        rustls::Error::InvalidCertificate(c) => {
            let x = match c {
                C::UnknownIssuer => "x509: certificate signed by unknown authority".to_string(),
                C::Expired | C::ExpiredContext { .. } | C::NotValidYet | C::NotValidYetContext { .. } => "x509: certificate has expired or is not yet valid".to_string(),
                C::NotValidForName | C::NotValidForNameContext { .. } => "x509: certificate is not valid for the requested name".to_string(),
                C::BadSignature => "x509: invalid signature".to_string(),
                C::BadEncoding => "x509: malformed certificate".to_string(),
                C::InvalidPurpose | C::InvalidPurposeContext { .. } => "x509: certificate specifies an incompatible key usage".to_string(),
                other => format!("x509: {other:?}"),
            };
            format!("tls: failed to verify certificate: {x}")
        }
        other => format!("tls: {other}"),
    }
}

/// Go's text for an I/O error that may carry a TLS error.
pub fn io_error_text(e: &std::io::Error) -> String {
    if let Some(inner) = e.get_ref()
        && let Some(t) = inner.downcast_ref::<rustls::Error>() {
            return tls_error_text(t);
        }
    crate::types::go_io_error(e)
}
