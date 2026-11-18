//! Clients from an HTTPClientConfig (Go `NewClientFromConfig`, `NewRoundTripperFromConfig`) and the client loop
//! (Go `http.Client.Do`): redirects with Go's rules and errors wrapped as `Get "url": reason`.
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::{GRANT_TYPE_JWT_BEARER, HttpClientConfig};
use crate::oauth2::OAuth2RoundTripper;
use crate::roundtrip::*;
use crate::secret::{SecretManager, to_secret};
use crate::tls::new_tls_config;
use crate::transport::{DialFunc, Transport, TransportOptions};
use crate::types::{Error, Request, Response};

/// Go `httpClientOptions` (set with the `with_*` methods).
#[derive(Clone)]
pub struct HttpClientOptions {
    pub dial: Option<DialFunc>,
    pub keep_alives: bool,
    pub http2: bool,
    pub idle_conn_timeout: Duration,
    pub user_agent: String,
    pub host: String,
    pub secret_manager: Option<Arc<dyn SecretManager>>,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        HttpClientOptions { dial: None, keep_alives: true, http2: true, idle_conn_timeout: Duration::from_secs(300), user_agent: String::new(), host: String::new(), secret_manager: None }
    }
}

impl HttpClientOptions {
    pub fn with_dial(mut self, d: DialFunc) -> Self {
        self.dial = Some(d);
        self
    }
    pub fn with_keep_alives_disabled(mut self) -> Self {
        self.keep_alives = false;
        self
    }
    pub fn with_http2_disabled(mut self) -> Self {
        self.http2 = false;
        self
    }
    pub fn with_idle_conn_timeout(mut self, t: Duration) -> Self {
        self.idle_conn_timeout = t;
        self
    }
    pub fn with_user_agent(mut self, ua: &str) -> Self {
        self.user_agent = ua.to_string();
        self
    }
    pub fn with_host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }
    pub fn with_secret_manager(mut self, m: Arc<dyn SecretManager>) -> Self {
        self.secret_manager = Some(m);
        self
    }
}

/// Go `NewRoundTripperFromConfig`: transport plus the layers the config asks for.
pub fn new_round_tripper_from_config(cfg: &HttpClientConfig, opts: &HttpClientOptions) -> Result<Arc<dyn RoundTripper>, Error> {
    let mgr = opts.secret_manager.clone();
    let cfg2 = cfg.clone();
    let opts2 = opts.clone();
    let new_rt: NewRt = Arc::new(move |tls| {
        let cfg = &cfg2;
        let opts = &opts2;
        let mgr = opts.secret_manager.as_ref();
        let topts = TransportOptions { keep_alives: opts.keep_alives, idle_conn_timeout: opts.idle_conn_timeout, max_idle_per_host: 1000, dial: opts.dial.clone() };
        let mut rt: Arc<dyn RoundTripper> = Arc::new(Transport::new(tls, cfg.proxy_config.proxy(), cfg.proxy_config.get_proxy_connect_header(), topts).map_err(Error::other)?);
        if let Some(a) = &cfg.authorization {
            let s = to_secret(mgr, &a.credentials, &a.credentials_file, &a.credentials_ref).map_err(|e| Error::other(format!("unable to use credentials: {e}")))?;
            rt = Arc::new(AuthorizationCredentialsRoundTripper { auth_type: a.type_.clone(), credentials: s, next: rt });
        }
        if !cfg.bearer_token.is_empty() || !cfg.bearer_token_file.is_empty() {
            let s = to_secret(mgr, &cfg.bearer_token, &cfg.bearer_token_file, "").map_err(|e| Error::other(format!("unable to use bearer token: {e}")))?;
            rt = Arc::new(AuthorizationCredentialsRoundTripper { auth_type: "Bearer".to_string(), credentials: s, next: rt });
        }
        if let Some(b) = &cfg.basic_auth {
            let u = to_secret(mgr, &yaml::Secret(b.username.clone()), &b.username_file, &b.username_ref).map_err(|e| Error::other(format!("unable to use username: {e}")))?;
            let p = to_secret(mgr, &b.password, &b.password_file, &b.password_ref).map_err(|e| Error::other(format!("unable to use password: {e}")))?;
            rt = Arc::new(BasicAuthRoundTripper { username: u, password: p, next: rt });
        }
        if let Some(o) = &cfg.oauth2 {
            let cred = if o.grant_type == GRANT_TYPE_JWT_BEARER {
                to_secret(mgr, &o.client_certificate_key, &o.client_certificate_key_file, &o.client_certificate_key_ref).map_err(|e| Error::other(format!("unable to use client certificate: {e}")))?
            } else {
                to_secret(mgr, &o.client_secret, &o.client_secret_file, &o.client_secret_ref).map_err(|e| Error::other(format!("unable to use client secret: {e}")))?
            };
            rt = Arc::new(OAuth2RoundTripper::new(cred, o, rt, opts.clone()));
        }
        if let Some(h) = &cfg.http_headers {
            if cfg.follow_redirects {
                rt = Arc::new(SensitiveHeadersStripRoundTripper { next: rt });
            }
            if !h.headers.is_empty() {
                rt = Arc::new(HeadersRoundTripper { config: h.clone(), next: rt });
            }
        }
        if !opts.user_agent.is_empty() {
            rt = Arc::new(UserAgentRoundTripper { user_agent: opts.user_agent.clone(), next: rt });
        }
        if !opts.host.is_empty() {
            rt = Arc::new(HostRoundTripper { host: opts.host.clone(), next: rt });
        }
        Ok(rt)
    });
    let tls = new_tls_config(&cfg.tls_config, mgr.as_ref()).map_err(Error::other)?;
    let settings = TlsRoundTripperSettings::from_config(&cfg.tls_config, mgr.as_ref()).map_err(Error::other)?;
    if settings.immutable() {
        return new_rt(tls);
    }
    Ok(Arc::new(TlsRoundTripper::new(tls, settings, new_rt)?))
}

/// Go `NewClientFromConfig`.
pub fn new_client_from_config(cfg: &HttpClientConfig, opts: &HttpClientOptions) -> Result<Client, Error> {
    let rt = new_round_tripper_from_config(cfg, opts)?;
    Ok(Client { transport: rt, follow_redirects: cfg.follow_redirects, timeout: None })
}

/// A Go http.Client: a round tripper, the redirect policy and an optional timeout per request.
#[derive(Clone)]
pub struct Client {
    pub transport: Arc<dyn RoundTripper>,
    pub follow_redirects: bool,
    /// Used when the request has no timeout of its own.
    pub timeout: Option<Duration>,
}

/// Go `url.Error.Error` with the password hidden ("Get \"http://a:***@h/\": reason").
fn url_error(method: &str, u: &yaml::url::GoUrl, e: Error) -> Error {
    let op = {
        let m = method.to_ascii_lowercase();
        let mut c = m.chars();
        match c.next() {
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            None => String::new(),
        }
    };
    let mut su = u.clone();
    if let Some(ui) = su.user.as_mut()
        && ui.password_set {
            ui.password = "***".to_string();
        }
    Error { kind: e.kind, msg: format!("{} {}: {}", op, yaml::go_quote(&su.to_url_string()), e.msg) }
}

impl Client {
    pub fn new(transport: Arc<dyn RoundTripper>) -> Client {
        Client { transport, follow_redirects: true, timeout: None }
    }

    pub fn get(&self, url: &str) -> Result<Response, Error> {
        self.do_request(Request::get(url)?)
    }

    /// Go `Client.Do`: sends the request and follows redirects (at most 10) when enabled.
    pub fn do_request(&self, mut req: Request) -> Result<Response, Error> {
        if req.timeout.is_none() {
            req.timeout = self.timeout;
        }
        let deadline = req.timeout.map(|t| Instant::now() + t);
        let initial = req.clone();
        let mut hops = 0;
        loop {
            if let Some(d) = deadline {
                let now = Instant::now();
                if now >= d {
                    return Err(url_error(&req.method, &req.url, Error::timeout()));
                }
                req.timeout = Some(d - now);
            }
            let method = req.method.clone();
            let url = req.url.clone();
            let resp = self.transport.round_trip(req.clone()).map_err(|e| url_error(&method, &url, e))?;
            let (redirect_method, include_body) = match resp.status {
                301..=303 => (if method != "GET" && method != "HEAD" { "GET".to_string() } else { method.clone() }, false),
                307 | 308 => (method.clone(), true),
                _ => return Ok(resp),
            };
            let loc = resp.header.get_str("Location").to_string();
            if loc.is_empty() {
                return Ok(resp);
            }
            if !self.follow_redirects {
                return Ok(resp);
            }
            let next_url = url.join(&loc).map_err(|e| url_error(&method, &url, Error::other(format!("failed to parse Location header {}: {}", yaml::go_quote(&loc), e))))?;
            hops += 1;
            if hops > 10 {
                return Err(url_error(&redirect_method, &next_url, Error::other("stopped after 10 redirects")));
            }
            // headers of the first request, minus credentials when leaving the initial host
            let mut header = initial.header.clone();
            let ihost = initial.url.hostname().to_lowercase();
            let dhost = next_url.hostname().to_lowercase();
            if !is_domain_or_subdomain(&dhost, &ihost) {
                for h in ["Authorization", "Www-Authenticate", "Cookie", "Cookie2", "Proxy-Authorization", "Proxy-Authenticate"] {
                    header.del(h);
                }
            }
            if !(url.scheme == "https" && next_url.scheme == "http") {
                let mut r = url.clone();
                r.user = None;
                r.fragment.clear();
                r.raw_fragment.clear();
                header.set("Referer", &r.to_url_string());
            }
            let mut host = String::new();
            if !req.host.is_empty() && req.host != req.url.host
                && let Ok(u) = yaml::url::parse(&loc)
                    && !u.is_abs() {
                        host = req.host.clone();
                    }
            let mut redirect_hosts = std::mem::take(&mut req.redirect_hosts);
            redirect_hosts.push(url.hostname().to_string());
            let body = if include_body { std::mem::take(&mut req.body) } else { Vec::new() };
            if !include_body {
                header.del("Content-Type");
                header.del("Content-Length");
            }
            req = Request { method: redirect_method, url: next_url, header, host, body, timeout: req.timeout, max_body_bytes: req.max_body_bytes, decompress: req.decompress, redirect_hosts };
        }
    }
}
