//! Round trippers (Go http.RoundTripper) and the layers http_config.go stacks on the transport: authorization,
//! basic auth, extra headers, user agent, host, sensitive header stripping on cross-host redirects, and the TLS
//! round tripper that rebuilds the transport when the CA, certificate or key files change.
use std::any::Any;
use std::sync::{Arc, Mutex, RwLock};

use crate::config::TlsConfig;
use crate::secret::{SecretManager, SecretReader, to_secret};
use crate::tls::TlsClientConfig;
use crate::types::{Error, Request, Response};

pub trait RoundTripper: Send + Sync + Any {
    fn round_trip(&self, req: Request) -> Result<Response, Error>;
    fn close_idle_connections(&self) {}
}

/// Go `isDomainOrSubdomain` (net/http/client.go).
pub fn is_domain_or_subdomain(sub: &str, parent: &str) -> bool {
    if parent.is_empty() {
        return false;
    }
    if sub == parent {
        return true;
    }
    if sub.contains(':') || sub.contains('%') {
        return false;
    }
    if !sub.ends_with(parent) {
        return false;
    }
    sub.len() > parent.len() && sub.as_bytes()[sub.len() - parent.len() - 1] == b'.'
}

/// Go `isCrossHostRedirect`: some hop of the redirect chain left the original host (sticky).
pub fn is_cross_host_redirect(req: &Request) -> bool {
    if req.redirect_hosts.is_empty() {
        return false;
    }
    let original = req.redirect_hosts[0].to_lowercase();
    let current = req.url.hostname().to_lowercase();
    if !is_domain_or_subdomain(&current, &original) {
        return true;
    }
    req.redirect_hosts[1..].iter().any(|h| !is_domain_or_subdomain(&h.to_lowercase(), &original))
}

pub struct AuthorizationCredentialsRoundTripper {
    pub auth_type: String,
    pub credentials: Option<Arc<dyn SecretReader>>,
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for AuthorizationCredentialsRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        if req.header.get("Authorization").is_some_and(|v| !v.is_empty()) || is_cross_host_redirect(&req) {
            return self.next.round_trip(req);
        }
        let mut creds = String::new();
        if let Some(c) = &self.credentials {
            creds = c.fetch().map_err(|e| Error::other(format!("unable to read authorization credentials: {e}")))?;
        }
        req.header.set("Authorization", &format!("{} {}", self.auth_type, creds));
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

pub struct BasicAuthRoundTripper {
    pub username: Option<Arc<dyn SecretReader>>,
    pub password: Option<Arc<dyn SecretReader>>,
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for BasicAuthRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        if req.header.get("Authorization").is_some_and(|v| !v.is_empty()) || is_cross_host_redirect(&req) {
            return self.next.round_trip(req);
        }
        let mut user = String::new();
        let mut pass = String::new();
        if let Some(u) = &self.username {
            user = u.fetch().map_err(|e| Error::other(format!("unable to read basic auth username: {e}")))?;
        }
        if let Some(p) = &self.password {
            pass = p.fetch().map_err(|e| Error::other(format!("unable to read basic auth password: {e}")))?;
        }
        req.set_basic_auth(&user, &pass);
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

/// Adds the configured `http_headers` (values, secrets, files read on every request).
pub struct HeadersRoundTripper {
    pub config: crate::config::Headers,
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for HeadersRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        for (n, h) in &self.config.headers {
            for v in &h.values {
                req.header.add(n, v);
            }
            for v in &h.secrets {
                req.header.add(n, v.as_str());
            }
            for f in &h.files {
                let b = crate::types::read_file(f).map_err(|e| Error::other(format!("unable to read headers file {f}: {e}")))?;
                req.header.add(n, String::from_utf8_lossy(&b).trim());
            }
        }
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

pub const SENSITIVE_HEADERS_ON_REDIRECT: [&str; 6] = ["Authorization", "Www-Authenticate", "Cookie", "Cookie2", "Proxy-Authorization", "Proxy-Authenticate"];

pub struct SensitiveHeadersStripRoundTripper {
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for SensitiveHeadersStripRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        if is_cross_host_redirect(&req) {
            for h in SENSITIVE_HEADERS_ON_REDIRECT {
                req.header.del(h);
            }
        }
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

pub struct UserAgentRoundTripper {
    pub user_agent: String,
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for UserAgentRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        req.header.set("User-Agent", &self.user_agent);
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

pub struct HostRoundTripper {
    pub host: String,
    pub next: Arc<dyn RoundTripper>,
}
impl RoundTripper for HostRoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        req.host = self.host.clone();
        req.header.set("Host", &self.host);
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        self.next.close_idle_connections()
    }
}

/// Builds the round tripper stack for a TLS config (Go `newRT`).
pub type NewRt = Arc<dyn Fn(TlsClientConfig) -> Result<Arc<dyn RoundTripper>, Error> + Send + Sync>;

/// The CA, cert and key sources of a TLSConfig (Go `TLSRoundTripperSettings`).
pub struct TlsRoundTripperSettings {
    pub ca: Option<Arc<dyn SecretReader>>,
    pub cert: Option<Arc<dyn SecretReader>>,
    pub key: Option<Arc<dyn SecretReader>>,
}
impl TlsRoundTripperSettings {
    pub fn from_config(c: &TlsConfig, manager: Option<&Arc<dyn SecretManager>>) -> Result<TlsRoundTripperSettings, String> {
        Ok(TlsRoundTripperSettings {
            ca: to_secret(manager, &yaml::Secret(c.ca.clone()), &c.ca_file, &c.ca_ref)?,
            cert: to_secret(manager, &yaml::Secret(c.cert.clone()), &c.cert_file, &c.cert_ref)?,
            key: to_secret(manager, &c.key, &c.key_file, &c.key_ref)?,
        })
    }
    pub fn immutable(&self) -> bool {
        [&self.ca, &self.cert, &self.key].iter().all(|s| s.as_ref().is_none_or(|s| s.immutable()))
    }
}

struct TlsState {
    rt: Arc<dyn RoundTripper>,
    hashes: [[u8; 32]; 3],
}

/// Rebuilds its round tripper when the CA, certificate or key data change (files are read on every request).
pub struct TlsRoundTripper {
    settings: TlsRoundTripperSettings,
    new_rt: NewRt,
    tls_config: TlsClientConfig,
    state: RwLock<TlsState>,
    rebuild: Mutex<()>,
}

impl TlsRoundTripper {
    pub fn new(cfg: TlsClientConfig, settings: TlsRoundTripperSettings, new_rt: NewRt) -> Result<TlsRoundTripper, Error> {
        let rt = new_rt(cfg.clone())?;
        let t = TlsRoundTripper { settings, new_rt, tls_config: cfg, state: RwLock::new(TlsState { rt, hashes: [[0; 32]; 3] }), rebuild: Mutex::new(()) };
        let (_, h) = t.data_with_hash()?;
        t.state.write().unwrap_or_else(|p| p.into_inner()).hashes = h;
        Ok(t)
    }

    fn data_with_hash(&self) -> Result<(Vec<u8>, [[u8; 32]; 3]), Error> {
        let mut ca = Vec::new();
        let mut cert = Vec::new();
        let mut key = Vec::new();
        if let Some(s) = &self.settings.ca {
            ca = s.fetch().map_err(|e| Error::other(format!("unable to read CA cert: {e}")))?.into_bytes();
        }
        if let Some(s) = &self.settings.cert {
            cert = s.fetch().map_err(|e| Error::other(format!("unable to read client cert: {e}")))?.into_bytes();
        }
        if let Some(s) = &self.settings.key {
            key = s.fetch().map_err(|e| Error::other(format!("unable to read client key: {e}")))?.into_bytes();
        }
        let h = |b: &[u8]| if b.is_empty() { [0u8; 32] } else { crypto::sha256(b) };
        let hashes = [h(&ca), h(&cert), h(&key)];
        Ok((ca, hashes))
    }
}

impl RoundTripper for TlsRoundTripper {
    fn round_trip(&self, req: Request) -> Result<Response, Error> {
        let (ca, hashes) = self.data_with_hash()?;
        let rt = {
            let st = self.state.read().unwrap_or_else(|p| p.into_inner());
            if st.hashes == hashes { Some(st.rt.clone()) } else { None }
        };
        if let Some(rt) = rt {
            return rt.round_trip(req);
        }
        let _g = self.rebuild.lock().unwrap_or_else(|p| p.into_inner());
        let mut cfg = self.tls_config.clone();
        if let Some(s) = &self.settings.ca {
            match crate::tls::parse_ca_pem(&ca) {
                Some(roots) => cfg.root_cas = Some(roots),
                None => return Err(Error::other(format!("unable to use specified CA cert {}", s.description()))),
            }
        }
        let rt = (self.new_rt)(cfg)?;
        self.close_idle_connections();
        {
            let mut st = self.state.write().unwrap_or_else(|p| p.into_inner());
            st.rt = rt.clone();
            st.hashes = hashes;
        }
        rt.round_trip(req)
    }
    fn close_idle_connections(&self) {
        let rt = self.state.read().unwrap_or_else(|p| p.into_inner()).rt.clone();
        rt.close_idle_connections();
    }
}

/// A round tripper from a closure (tests and small adapters).
pub struct FnRoundTripper<F: Fn(Request) -> Result<Response, Error> + Send + Sync + 'static>(pub F);
impl<F: Fn(Request) -> Result<Response, Error> + Send + Sync + 'static> RoundTripper for FnRoundTripper<F> {
    fn round_trip(&self, req: Request) -> Result<Response, Error> {
        (self.0)(req)
    }
}

/// Downcasts a round tripper to its concrete type.
pub fn downcast<T: RoundTripper>(rt: &dyn RoundTripper) -> Option<&T> {
    (rt as &dyn Any).downcast_ref::<T>()
}
