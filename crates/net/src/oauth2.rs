//! OAuth2 round tripper (http_config.go oauth2RoundTripper, golang.org/x/oauth2 clientcredentials and the
//! jwt-bearer grant of oauth_assertion.go): tokens are cached until 10 s before expiry and the token source is
//! rebuilt when the client secret (or key) changes.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use json::Json;
use yaml::Value;
use yaml::url::{encode_query, query_escape};

use crate::client::{Client, HttpClientOptions};
use crate::config::{GRANT_TYPE_JWT_BEARER, OAuth2};
use crate::roundtrip::{NewRt, RoundTripper, TlsRoundTripper, TlsRoundTripperSettings, UserAgentRoundTripper, is_cross_host_redirect};
use crate::secret::SecretReader;
use crate::tls::new_tls_config;
use crate::transport::{Transport, TransportOptions};
use crate::types::{Error, Request, Response};

#[derive(Debug, Clone)]
pub struct Token {
    pub access_token: String,
    pub token_type: String,
    pub expiry: Option<Instant>,
}

impl Token {
    /// x/oauth2 `Token.Valid` with the 10 s expiry delta.
    fn valid(&self) -> bool {
        !self.access_token.is_empty() && self.expiry.is_none_or(|e| Instant::now() + Duration::from_secs(10) < e)
    }
    /// x/oauth2 `Token.Type`.
    pub fn type_(&self) -> String {
        if self.token_type.eq_ignore_ascii_case("bearer") || self.token_type.is_empty() {
            return "Bearer".to_string();
        }
        if self.token_type.eq_ignore_ascii_case("mac") {
            return "MAC".to_string();
        }
        if self.token_type.eq_ignore_ascii_case("basic") {
            return "Basic".to_string();
        }
        self.token_type.clone()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum AuthStyle {
    Unknown,
    InHeader,
    InParams,
}

/// A token source with a cached token (x/oauth2 ReuseTokenSource).
pub struct TokenSource {
    client: Client,
    config: OAuth2,
    credential: String,
    token: Mutex<Option<Token>>,
    auth_style: Mutex<AuthStyle>,
}

fn retrieve_error(resp: &Response, code: &str, desc: &str, uri: &str) -> Error {
    if !code.is_empty() {
        let mut s = format!("oauth2: {}", yaml::go_quote(code));
        if !desc.is_empty() {
            s.push_str(&format!(" {}", yaml::go_quote(desc)));
        }
        if !uri.is_empty() {
            s.push_str(&format!(" {}", yaml::go_quote(uri)));
        }
        return Error::other(s);
    }
    Error::other(format!("oauth2: cannot fetch token: {}\nResponse: {}", resp.status_text, String::from_utf8_lossy(&resp.body)))
}

fn post_form(client: &Client, url: &str, form: &str, basic: Option<(&str, &str)>, timeout: Option<Duration>) -> Result<Response, Error> {
    let mut req = Request::new("POST", url, form.as_bytes().to_vec())?;
    req.header.set("Content-Type", "application/x-www-form-urlencoded");
    if let Some((u, p)) = basic {
        req.set_basic_auth(&query_escape(u), &query_escape(p));
    }
    req.timeout = timeout;
    client.do_request(req)
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Parses a token endpoint response (internal.doTokenRoundTrip).
fn parse_token_response(resp: &Response) -> Result<Token, Error> {
    let failure = resp.status < 200 || resp.status > 299;
    let ct = resp.header.get_str("Content-Type");
    let media = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    let body = String::from_utf8_lossy(&resp.body).into_owned();
    let (token, code, desc, uri);
    if media == "application/x-www-form-urlencoded" || media == "text/plain" {
        let (vals, err) = yaml::url::parse_query(&body);
        if let Some(e) = err {
            if failure {
                return Err(retrieve_error(resp, "", "", ""));
            }
            return Err(Error::other(format!("oauth2: cannot parse response: {e}")));
        }
        let get = |k: &str| vals.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()).unwrap_or_default();
        code = get("error");
        desc = get("error_description");
        uri = get("error_uri");
        let expires: i64 = get("expires_in").parse().unwrap_or(0);
        token = Token { access_token: get("access_token"), token_type: get("token_type"), expiry: if expires != 0 { Some(Instant::now() + Duration::from_secs(expires.max(0) as u64)) } else { None } };
    } else {
        let j = match Json::parse(&body) {
            Ok(j) => j,
            Err(e) => {
                if failure {
                    return Err(retrieve_error(resp, "", "", ""));
                }
                return Err(Error::other(format!("oauth2: cannot parse json: {e}")));
            }
        };
        code = j.str_of("error").to_string();
        desc = j.str_of("error_description").to_string();
        uri = j.str_of("error_uri").to_string();
        let expires = match j.get("expires_in") {
            Some(Json::Int(i)) => *i,
            Some(Json::Float(f)) => *f as i64,
            Some(Json::Str(s)) => s.parse().unwrap_or(0),
            _ => 0,
        };
        let expires = expires.min(i32::MAX as i64);
        token = Token { access_token: j.str_of("access_token").to_string(), token_type: j.str_of("token_type").to_string(), expiry: if expires != 0 { Some(Instant::now() + Duration::from_secs(expires.max(0) as u64)) } else { None } };
    }
    if failure || !code.is_empty() {
        return Err(retrieve_error(resp, &code, &desc, &uri));
    }
    if token.access_token.is_empty() {
        return Err(Error::other("oauth2: server response missing access_token"));
    }
    Ok(token)
}

fn value_to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => Json::Int(*i),
        Value::Uint(u) => Json::Float(*u as f64),
        Value::Float(f) => Json::Float(*f),
        Value::Str(s) => Json::Str(s.clone()),
        Value::Seq(items) | Value::FlowSeq(items) => Json::Arr(items.iter().map(value_to_json).collect()),
        Value::Map(pairs) => {
            let mut o: Vec<(String, Json)> = pairs.iter().map(|(k, v)| (k.as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("{k:?}")), value_to_json(v))).collect();
            o.sort_by(|a, b| a.0.cmp(&b.0));
            Json::Obj(o)
        }
    }
}

fn b64url(b: &[u8]) -> String {
    crypto::base64url(b).trim_end_matches('=').to_string()
}

/// Signs the jwt-bearer assertion (RS256/384/512) with the PEM key (PKCS#1 or PKCS#8).
fn sign_jwt(cfg: &OAuth2, key_pem: &str) -> Result<String, Error> {
    let blocks = crate::tls::pem_blocks(key_pem.as_bytes());
    let Some(b) = blocks.first() else { return Err(Error::other("invalid key: Key must be a PEM encoded PKCS1 or PKCS8 key")) };
    let kp = ring::signature::RsaKeyPair::from_der(&b.der).or_else(|_| ring::signature::RsaKeyPair::from_pkcs8(&b.der)).map_err(|_| Error::other("key is not a valid RSA private key"))?;
    let (alg, enc): (&str, &'static dyn ring::signature::RsaEncoding) = match cfg.signature_algorithm.as_str() {
        "RS384" => ("RS384", &ring::signature::RSA_PKCS1_SHA384),
        "RS512" => ("RS512", &ring::signature::RSA_PKCS1_SHA512),
        _ => ("RS256", &ring::signature::RSA_PKCS1_SHA256),
    };
    let mut header = vec![("alg".to_string(), Json::Str(alg.to_string()))];
    if !cfg.client_certificate_key_id.is_empty() {
        header.push(("kid".to_string(), Json::Str(cfg.client_certificate_key_id.clone())));
    }
    header.push(("typ".to_string(), Json::Str("JWT".to_string())));
    let iss = if cfg.iss.is_empty() { cfg.client_id.clone() } else { cfg.iss.clone() };
    let audience = if cfg.audience.is_empty() { cfg.token_url.clone() } else { cfg.audience.clone() };
    let now = now_unix();
    let mut jti = [0u8; 16];
    crypto::random_bytes(&mut jti);
    jti[6] = (jti[6] & 0x0f) | 0x40;
    jti[8] = (jti[8] & 0x3f) | 0x80;
    let h = crypto::hex(&jti);
    let jti = format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32]);
    let mut claims: Vec<(String, Json)> = vec![
        ("iss".to_string(), Json::Str(iss)),
        ("sub".to_string(), Json::Str(cfg.client_id.clone())),
        ("jti".to_string(), Json::Str(jti)),
        ("aud".to_string(), Json::Str(audience)),
        ("iat".to_string(), Json::Int(now as i64)),
        ("exp".to_string(), Json::Int(now as i64 + 600)),
    ];
    if !cfg.scopes.is_empty() {
        claims.push(("scope".to_string(), Json::Str(cfg.scopes.join(" "))));
    }
    for (k, v) in &cfg.claims {
        claims.retain(|(n, _)| n != k);
        claims.push((k.clone(), value_to_json(v)));
    }
    claims.sort_by(|a, b| a.0.cmp(&b.0));
    let signing_input = format!("{}.{}", b64url(Json::Obj(header).stringify().as_bytes()), b64url(Json::Obj(claims).stringify().as_bytes()));
    let mut sig = vec![0u8; kp.public().modulus_len()];
    kp.sign(enc, &ring::rand::SystemRandom::new(), signing_input.as_bytes(), &mut sig).map_err(|_| Error::other("crypto/rsa: signing failed"))?;
    Ok(format!("{}.{}", signing_input, b64url(&sig)))
}

impl TokenSource {
    /// The cached token, or a new one fetched within `timeout`.
    pub fn token(&self, timeout: Option<Duration>) -> Result<Token, Error> {
        let mut g = self.token.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(t) = g.as_ref()
            && t.valid() {
                return Ok(t.clone());
            }
        let t = if self.config.grant_type == GRANT_TYPE_JWT_BEARER { self.fetch_jwt(timeout)? } else { self.fetch_client_credentials(timeout)? };
        *g = Some(t.clone());
        Ok(t)
    }

    fn params(&self, base: Vec<(String, String)>) -> Result<Vec<(String, String)>, Error> {
        let mut v = base;
        for (k, p) in &self.config.endpoint_params {
            if v.iter().any(|(n, _)| n == k) && k != "grant_type" {
                return Err(Error::other(format!("oauth2: cannot overwrite parameter {}", yaml::go_quote(k))));
            }
            v.retain(|(n, _)| n != k);
            v.push((k.clone(), p.clone()));
        }
        Ok(v)
    }

    fn fetch_client_credentials(&self, timeout: Option<Duration>) -> Result<Token, Error> {
        let mut v = vec![("grant_type".to_string(), "client_credentials".to_string())];
        if !self.config.scopes.is_empty() {
            v.push(("scope".to_string(), self.config.scopes.join(" ")));
        }
        let v = self.params(v)?;
        let id = self.config.client_id.as_str();
        let secret = self.credential.as_str();
        let mut style = *self.auth_style.lock().unwrap_or_else(|p| p.into_inner());
        let probe = style == AuthStyle::Unknown;
        if probe {
            style = AuthStyle::InHeader;
        }
        let attempt = |style: AuthStyle| -> Result<Token, Error> {
            let resp = if style == AuthStyle::InParams {
                let mut p = v.clone();
                if !id.is_empty() {
                    p.push(("client_id".to_string(), id.to_string()));
                }
                if !secret.is_empty() {
                    p.push(("client_secret".to_string(), secret.to_string()));
                }
                post_form(&self.client, &self.config.token_url, &encode_query(&p), None, timeout)?
            } else {
                post_form(&self.client, &self.config.token_url, &encode_query(&v), Some((id, secret)), timeout)?
            };
            parse_token_response(&resp)
        };
        let mut r = attempt(style);
        if r.is_err() && probe {
            style = AuthStyle::InParams;
            r = attempt(style);
        }
        if probe && r.is_ok() {
            *self.auth_style.lock().unwrap_or_else(|p| p.into_inner()) = style;
        }
        r
    }

    fn fetch_jwt(&self, timeout: Option<Duration>) -> Result<Token, Error> {
        let assertion = sign_jwt(&self.config, &self.credential)?;
        let mut v = vec![("grant_type".to_string(), GRANT_TYPE_JWT_BEARER.to_string()), ("assertion".to_string(), assertion)];
        if !self.config.scopes.is_empty() {
            v.push(("scope".to_string(), self.config.scopes.join(" ")));
        }
        let v = self.params(v)?;
        let resp = post_form(&self.client, &self.config.token_url, &encode_query(&v), None, timeout).map_err(|e| e.wrap("oauth2: cannot fetch token"))?;
        if resp.status < 200 || resp.status > 299 {
            return Err(retrieve_error(&resp, "", "", ""));
        }
        let j = Json::parse(&String::from_utf8_lossy(&resp.body)).map_err(|e| Error::other(format!("oauth2: cannot fetch token: {e}")))?;
        let secs = j.get("expires_in").and_then(|x| x.as_i64()).unwrap_or(0);
        Ok(Token { access_token: j.str_of("access_token").to_string(), token_type: j.str_of("token_type").to_string(), expiry: if secs > 0 { Some(Instant::now() + Duration::from_secs(secs as u64)) } else { None } })
    }
}

struct State {
    last_secret: Option<String>,
    source: Option<Arc<TokenSource>>,
}

pub struct OAuth2RoundTripper {
    pub config: OAuth2,
    pub credential: Arc<dyn SecretReader>,
    pub next: Arc<dyn RoundTripper>,
    opts: HttpClientOptions,
    state: Mutex<State>,
}

impl OAuth2RoundTripper {
    /// Go `NewOAuth2RoundTripper` (a missing credential is an empty inline secret).
    pub fn new(credential: Option<Arc<dyn SecretReader>>, config: &OAuth2, next: Arc<dyn RoundTripper>, opts: HttpClientOptions) -> OAuth2RoundTripper {
        OAuth2RoundTripper {
            config: config.clone(),
            credential: credential.unwrap_or_else(|| crate::secret::new_inline_secret("")),
            next,
            opts,
            state: Mutex::new(State { last_secret: None, source: None }),
        }
    }

    fn new_token_source(&self, req: &Request, secret: String) -> Result<Arc<TokenSource>, Error> {
        let mgr = self.opts.secret_manager.as_ref();
        let tls = new_tls_config(&self.config.tls_config, mgr).map_err(Error::other)?;
        let proxy = self.config.proxy_config.proxy();
        let pch = self.config.proxy_config.get_proxy_connect_header();
        let topts = TransportOptions { keep_alives: self.opts.keep_alives, idle_conn_timeout: Duration::from_secs(10), max_idle_per_host: 1, dial: self.opts.dial.clone(), unix_socket: self.opts.unix_socket.clone() };
        let new_rt: NewRt = Arc::new(move |t| Ok(Arc::new(Transport::new(t, proxy.clone(), pch.clone(), topts.clone()).map_err(Error::other)?) as Arc<dyn RoundTripper>));
        let settings = TlsRoundTripperSettings::from_config(&self.config.tls_config, mgr).map_err(Error::other)?;
        let mut t: Arc<dyn RoundTripper> = if settings.immutable() { new_rt(tls)? } else { Arc::new(TlsRoundTripper::new(tls, settings, new_rt)?) };
        let ua = req.user_agent();
        if !ua.is_empty() {
            t = Arc::new(UserAgentRoundTripper { user_agent: ua.to_string(), next: t });
        }
        let client = Client { transport: t, follow_redirects: true, timeout: None };
        Ok(Arc::new(TokenSource { client, config: self.config.clone(), credential: secret, token: Mutex::new(None), auth_style: Mutex::new(AuthStyle::Unknown) }))
    }

    /// The current token source (for tests: a token fetched once is reused).
    pub fn token_source(&self) -> Option<Arc<TokenSource>> {
        self.state.lock().ok().and_then(|s| s.source.clone())
    }
}

impl RoundTripper for OAuth2RoundTripper {
    fn round_trip(&self, mut req: Request) -> Result<Response, Error> {
        if is_cross_host_redirect(&req) {
            return self.next.round_trip(req);
        }
        let (secret, needs_init) = {
            let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (s.last_secret.clone(), s.source.is_none())
        };
        if !self.credential.immutable() || needs_init {
            let new_secret = self.credential.fetch().map_err(|e| Error::other(format!("unable to read oauth2 client secret: {e}")))?;
            if Some(&new_secret) != secret.as_ref() || needs_init {
                let src = self.new_token_source(&req, new_secret.clone())?;
                let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(old) = &s.source {
                    old.client.transport.close_idle_connections();
                }
                s.last_secret = Some(new_secret);
                s.source = Some(src);
            }
        }
        let src = self.state.lock().unwrap_or_else(|p| p.into_inner()).source.clone().ok_or_else(|| Error::other("oauth2: no token source"))?;
        // the token fetch spends the request's time (Go shares the request context)
        let deadline = req.timeout.map(|t| Instant::now() + t);
        let tok = src.token(req.timeout)?;
        if let Some(d) = deadline {
            let now = Instant::now();
            if now >= d {
                return Err(Error::timeout());
            }
            req.timeout = Some(d - now);
        }
        req.header.set("Authorization", &format!("{} {}", tok.type_(), tok.access_token));
        self.next.round_trip(req)
    }
    fn close_idle_connections(&self) {
        if let Some(s) = self.token_source() {
            s.client.transport.close_idle_connections();
        }
        self.next.close_idle_connections();
    }
}
