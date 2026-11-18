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
    /// Dial this unix socket instead of the request host.
    pub unix_socket: Option<String>,
}

impl Default for HttpClientOptions {
    fn default() -> Self {
        HttpClientOptions { dial: None, keep_alives: true, http2: true, idle_conn_timeout: Duration::from_secs(300), user_agent: String::new(), host: String::new(), secret_manager: None, unix_socket: None }
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
    pub fn with_unix_socket(mut self, path: &str) -> Self {
        self.unix_socket = Some(path.to_string());
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
        let topts = TransportOptions { keep_alives: opts.keep_alives, idle_conn_timeout: opts.idle_conn_timeout, max_idle_per_host: 1000, dial: opts.dial.clone(), unix_socket: opts.unix_socket.clone() };
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

    /// Go `Client.Do`: sends the request and follows redirects (at most 10) when enabled, with Go's rules:
    /// 301/302/303 turn a non-GET/HEAD into GET without body, 307/308 keep method and body; credentials are
    /// dropped for good once a hop leaves the initial host; URL user info becomes Basic auth.
    pub fn do_request(&self, mut req: Request) -> Result<Response, Error> {
        if req.timeout.is_none() {
            req.timeout = self.timeout;
        }
        let deadline = req.timeout.map(|t| Instant::now() + t);
        let initial = req.clone();
        let op_method = initial.method.clone();
        let mut via = 0usize;
        let mut include_body = true;
        let mut strip_sensitive = false;
        loop {
            if let Some(d) = deadline {
                let now = Instant::now();
                if now >= d {
                    return Err(url_error(&op_method, &req.url, Error::timeout()));
                }
                req.timeout = Some(d - now);
            }
            let method = req.method.clone();
            let url = req.url.clone();
            let mut sent = req.clone();
            if let Some(ui) = &url.user
                && sent.header.get_str("Authorization").is_empty()
            {
                sent.set_basic_auth(&ui.username, &ui.password);
            }
            let resp = self.transport.round_trip(sent).map_err(|e| url_error(&op_method, &url, e))?;
            via += 1;
            let (redirect_method, include_on_hop) = match resp.status {
                301..=303 => (if method != "GET" && method != "HEAD" { "GET".to_string() } else { method.clone() }, false),
                307 | 308 => (method.clone(), true),
                _ => return Ok(resp),
            };
            if !include_on_hop {
                include_body = false;
            }
            let loc = resp.header.get_str("Location").to_string();
            if loc.is_empty() {
                return Ok(resp);
            }
            let next_url = url.join(&loc).map_err(|e| url_error(&op_method, &url, Error::other(format!("failed to parse Location header {}: {}", yaml::go_quote(&loc), e))))?;
            let mut host = String::new();
            if !req.host.is_empty()
                && req.host != req.url.host
                && let Ok(u) = yaml::url::parse(&loc)
                && !u.is_abs()
            {
                host = req.host.clone();
            }
            // headers of the first request (Go's makeHeadersCopier); stripping is sticky
            if !strip_sensitive && initial.url.host != next_url.host && !is_domain_or_subdomain(&next_url.hostname().to_lowercase(), &initial.url.hostname().to_lowercase()) {
                strip_sensitive = true;
            }
            let mut header = crate::Header::new();
            for (k, v) in initial.header.iter() {
                let sensitive = matches!(k, "Authorization" | "Www-Authenticate" | "Cookie" | "Cookie2" | "Proxy-Authorization" | "Proxy-Authenticate");
                let body = matches!(k, "Content-Encoding" | "Content-Language" | "Content-Location" | "Content-Type");
                if !(sensitive && strip_sensitive) && !(body && !include_body) {
                    header.0.push((k.to_string(), v.to_string()));
                }
            }
            if !(url.scheme == "https" && next_url.scheme == "http") {
                let explicit = header.get_str("Referer").to_string();
                let referer = if !explicit.is_empty() {
                    explicit
                } else {
                    let mut r = url.clone();
                    r.user = None;
                    r.to_url_string()
                };
                header.set("Referer", &referer);
            }
            if !self.follow_redirects {
                return Ok(resp);
            }
            if via >= 10 {
                return Err(Error::other(format!("{} {}: stopped after 10 redirects", op_upper(&op_method), yaml::go_quote(&loc))));
            }
            let mut redirect_hosts = std::mem::take(&mut req.redirect_hosts);
            redirect_hosts.push(url.hostname().to_string());
            let body = if include_body { initial.body.clone() } else { Vec::new() };
            req = Request { method: redirect_method, url: next_url, header, host, body, timeout: req.timeout, max_body_bytes: req.max_body_bytes, decompress: req.decompress, redirect_hosts, truncate_body: req.truncate_body, cancel: req.cancel.clone() };
        }
    }
}

fn op_upper(method: &str) -> String {
    let m = method.to_ascii_lowercase();
    let mut c = m.chars();
    match c.next() {
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}
