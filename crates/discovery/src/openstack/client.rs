//! The parts of gophercloud v2.15.0 that the OpenStack discovery uses, over blocking HTTP + JSON:
//! `openstack.NewClient`, `openstack.Authenticate` (version choice, Keystone v3 token create with password,
//! TOTP passcode or application credential, project / domain / system scope), the service catalog lookup with
//! endpoint version discovery (`NewComputeV2`, `NewNetworkV2`, `NewLoadBalancerV2`), `ProviderClient.Request`
//! and linked paging (`<resource>_links` with rel "next").
//!
//! Prometheus does not set `AllowReauth`, so gophercloud never re-authenticates on 401 (the request fails with
//! `ErrUnexpectedResponseCode`); every refresh authenticates again instead. Neither is there a retry function.
use json::Json;

use crate::cancel::CancelToken;

/// Go `gophercloud.DefaultUserAgent`.
pub const USER_AGENT: &str = "gophercloud/v2.15.0";

/// Go `gophercloud.AuthOptions` (the fields Prometheus fills).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AuthOptions {
    pub identity_endpoint: String,
    pub username: String,
    pub user_id: String,
    pub password: String,
    pub passcode: String,
    pub tenant_id: String,
    pub tenant_name: String,
    pub domain_id: String,
    pub domain_name: String,
    pub application_credential_id: String,
    pub application_credential_name: String,
    pub application_credential_secret: String,
    /// `Scope{System: true}` (OS_SYSTEM_SCOPE=all).
    pub system_scope: bool,
}

/// Go `openstack.AuthOptionsFromEnv`.
pub fn auth_options_from_env() -> Result<AuthOptions, String> {
    let env = |k: &str| std::env::var(k).unwrap_or_default();
    let auth_url = env("OS_AUTH_URL");
    let username = env("OS_USERNAME");
    let user_id = env("OS_USERID");
    let password = env("OS_PASSWORD");
    let passcode = env("OS_PASSCODE");
    let mut tenant_id = env("OS_TENANT_ID");
    let mut tenant_name = env("OS_TENANT_NAME");
    let domain_id = env("OS_DOMAIN_ID");
    let domain_name = env("OS_DOMAIN_NAME");
    let ac_id = env("OS_APPLICATION_CREDENTIAL_ID");
    let ac_name = env("OS_APPLICATION_CREDENTIAL_NAME");
    let ac_secret = env("OS_APPLICATION_CREDENTIAL_SECRET");
    let system_scope = env("OS_SYSTEM_SCOPE");
    let v = env("OS_PROJECT_ID");
    if !v.is_empty() {
        tenant_id = v;
    }
    let v = env("OS_PROJECT_NAME");
    if !v.is_empty() {
        tenant_name = v;
    }
    let missing = |v: &str| format!("Missing environment variable [{v}]");
    let missing_any = |v: &[&str]| format!("Missing one of the following environment variables [{}]", v.join(", "));
    if auth_url.is_empty() {
        return Err(missing("OS_AUTH_URL"));
    }
    if user_id.is_empty() && username.is_empty() && ac_id.is_empty() && ac_secret.is_empty() {
        return Err(missing_any(&["OS_USERID", "OS_USERNAME"]));
    }
    if password.is_empty() && passcode.is_empty() && ac_id.is_empty() && ac_name.is_empty() {
        return Err(missing("OS_PASSWORD"));
    }
    if (!ac_id.is_empty() || !ac_name.is_empty()) && ac_secret.is_empty() {
        return Err(missing("OS_APPLICATION_CREDENTIAL_SECRET"));
    }
    if domain_id.is_empty() && domain_name.is_empty() && tenant_id.is_empty() && !tenant_name.is_empty() {
        return Err(missing("OS_PROJECT_ID"));
    }
    if ac_id.is_empty() && !ac_name.is_empty() && !ac_secret.is_empty() {
        if user_id.is_empty() && username.is_empty() {
            return Err(missing_any(&["OS_USERID", "OS_USERNAME"]));
        }
        if !username.is_empty() && domain_id.is_empty() && domain_name.is_empty() {
            return Err(missing_any(&["OS_DOMAIN_ID", "OS_DOMAIN_NAME"]));
        }
    }
    Ok(AuthOptions {
        identity_endpoint: auth_url,
        username,
        user_id,
        password,
        passcode,
        tenant_id,
        tenant_name,
        domain_id,
        domain_name,
        application_credential_id: ac_id,
        application_credential_name: ac_name,
        application_credential_secret: ac_secret,
        system_scope: system_scope == "all",
    })
}

fn obj(pairs: Vec<(&str, Json)>) -> Json {
    // Go marshals these bodies from map[string]any: keys sorted.
    let mut v: Vec<(String, Json)> = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    Json::Obj(v)
}

fn s(v: &str) -> Json {
    Json::Str(v.to_string())
}

impl AuthOptions {
    /// Go `AuthOptions.ToTokenV3ScopeMap`.
    fn scope_map(&self) -> Result<Option<Json>, String> {
        if !self.application_credential_id.is_empty() || !self.application_credential_name.is_empty() {
            return Ok(None);
        }
        if self.system_scope {
            return Ok(Some(obj(vec![("system", obj(vec![("all", Json::Bool(true))]))])));
        }
        let (mut project_id, mut project_name, mut domain_id, mut domain_name) = (String::new(), String::new(), String::new(), String::new());
        if !self.tenant_id.is_empty() {
            project_id = self.tenant_id.clone();
        } else if !self.tenant_name.is_empty() {
            project_name = self.tenant_name.clone();
            domain_id = self.domain_id.clone();
            domain_name = self.domain_name.clone();
        }
        if !project_name.is_empty() {
            if domain_id.is_empty() && domain_name.is_empty() {
                return Err("You must provide exactly one of DomainID or DomainName in a Scope with ProjectName".into());
            }
            if !project_id.is_empty() {
                return Err("You must provide at most one of ProjectID or ProjectName in a Scope".into());
            }
            let domain = if !domain_id.is_empty() { obj(vec![("id", s(&domain_id))]) } else { obj(vec![("name", s(&domain_name))]) };
            return Ok(Some(obj(vec![("project", obj(vec![("name", s(&project_name)), ("domain", domain)]))])));
        }
        if !project_id.is_empty() {
            return Ok(Some(obj(vec![("project", obj(vec![("id", s(&project_id))]))])));
        }
        Ok(None)
    }

    /// Go `AuthOptions.ToTokenV3CreateMap` (without the token method, which Prometheus cannot configure).
    fn create_map(&self, scope: Option<Json>) -> Result<Json, String> {
        const USER_OR_ID: &str = "Exactly one of Username and UserID must be provided for password authentication";
        const DOMAIN_ID_OR_NAME: &str = "You must provide exactly one of DomainID or DomainName to authenticate by Username";
        let mut identity: Vec<(&str, Json)> = Vec::new();
        if self.password.is_empty() && self.passcode.is_empty() {
            if !self.application_credential_id.is_empty() {
                if self.application_credential_secret.is_empty() {
                    return Err("You must provide an Application Credential Secret".into());
                }
                identity.push(("methods", Json::Arr(vec![s("application_credential")])));
                identity.push(("application_credential", obj(vec![("id", s(&self.application_credential_id)), ("secret", s(&self.application_credential_secret))])));
            } else if !self.application_credential_name.is_empty() {
                if self.application_credential_secret.is_empty() {
                    return Err("You must provide an Application Credential Secret".into());
                }
                let user = if !self.user_id.is_empty() {
                    obj(vec![("id", s(&self.user_id))])
                } else if self.username.is_empty() {
                    return Err(USER_OR_ID.into());
                } else if !self.domain_id.is_empty() {
                    obj(vec![("name", s(&self.username)), ("domain", obj(vec![("id", s(&self.domain_id))]))])
                } else if !self.domain_name.is_empty() {
                    obj(vec![("name", s(&self.username)), ("domain", obj(vec![("name", s(&self.domain_name))]))])
                } else {
                    return Err(DOMAIN_ID_OR_NAME.into());
                };
                identity.push(("methods", Json::Arr(vec![s("application_credential")])));
                identity.push(("application_credential", obj(vec![("name", s(&self.application_credential_name)), ("user", user), ("secret", s(&self.application_credential_secret))])));
            } else {
                return Err("You must provide a password to authenticate".into());
            }
        } else {
            let mut methods = Vec::new();
            if !self.password.is_empty() {
                methods.push(s("password"));
            }
            if !self.passcode.is_empty() {
                methods.push(s("totp"));
            }
            identity.push(("methods", Json::Arr(methods)));
            if self.username.is_empty() && self.user_id.is_empty() {
                return Err(USER_OR_ID.into());
            }
            let user_with = |secret_key: &str, secret: &str| -> Json {
                let mut u = vec![(secret_key, s(secret))];
                if !self.user_id.is_empty() {
                    u.push(("id", s(&self.user_id)));
                } else {
                    u.push(("name", s(&self.username)));
                    let d = if !self.domain_id.is_empty() { obj(vec![("id", s(&self.domain_id))]) } else { obj(vec![("name", s(&self.domain_name))]) };
                    u.push(("domain", d));
                }
                obj(u)
            };
            if !self.username.is_empty() {
                if !self.user_id.is_empty() {
                    return Err(USER_OR_ID.into());
                }
                if self.domain_id.is_empty() && self.domain_name.is_empty() {
                    return Err(DOMAIN_ID_OR_NAME.into());
                }
                if !self.domain_id.is_empty() && !self.domain_name.is_empty() {
                    return Err(DOMAIN_ID_OR_NAME.into());
                }
            }
            if !self.user_id.is_empty() {
                if !self.domain_id.is_empty() {
                    return Err("DomainID may not be provided when authenticating with a UserID".into());
                }
                if !self.domain_name.is_empty() {
                    return Err("DomainName may not be provided when authenticating with a UserID".into());
                }
            }
            if !self.password.is_empty() {
                identity.push(("password", obj(vec![("user", user_with("password", &self.password))])));
            }
            if !self.passcode.is_empty() {
                identity.push(("totp", obj(vec![("user", user_with("passcode", &self.passcode))])));
            }
        }
        let mut auth = vec![("identity", obj(identity))];
        if let Some(sc) = scope {
            auth.push(("scope", sc));
        }
        Ok(obj(vec![("auth", obj(auth))]))
    }
}

/// Go `utils.BaseEndpoint` / `BaseVersionedEndpoint`: the URL without query and fragment, cut before (or after)
/// the first `v[0-9.]+/?` of the path.
fn parse_endpoint(endpoint: &str, include_version: bool) -> Result<String, String> {
    let mut u = yaml::url::parse(endpoint)?;
    u.raw_query.clear();
    u.fragment.clear();
    u.raw_fragment.clear();
    u.force_query = false;
    let path = u.path.clone();
    let b = path.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'v' {
            let mut j = i + 1;
            while j < b.len() && (b[j].is_ascii_digit() || b[j] == b'.') {
                j += 1;
            }
            if j > i + 1 {
                if j < b.len() && b[j] == b'/' {
                    j += 1;
                }
                u.path = if include_version { path[..j].to_string() } else { path[..i].to_string() };
                u.raw_path.clear();
                break;
            }
        }
        i += 1;
    }
    Ok(u.to_url_string())
}

/// Go `gophercloud.NormalizeURL`.
fn normalize_url(u: &str) -> String {
    if u.ends_with('/') { u.to_string() } else { format!("{u}/") }
}

/// Go `utils.ParseVersion`.
fn parse_version(v: &str) -> Result<(i64, i64), String> {
    if v.is_empty() {
        return Err("empty version provided".into());
    }
    let t = v.strip_prefix('v').unwrap_or(v);
    let (maj, min) = match t.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (t, None),
    };
    let digits = |x: &str| !x.is_empty() && x.bytes().all(|c| c.is_ascii_digit());
    if !digits(maj) || min.is_some_and(|m| !digits(m)) {
        return Err(format!("invalid format: {}", yaml::go_quote(v)));
    }
    let atoi = |x: &str| x.parse::<i64>().map_err(|_| format!("strconv.Atoi: parsing {}: value out of range", yaml::go_quote(x)));
    Ok((atoi(maj)?, match min {
        Some(m) => atoi(m)?,
        None => 0,
    }))
}

/// Go `utils.ParseMicroversion`.
fn parse_microversion(v: &str) -> Result<(), String> {
    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() != 2 {
        return Err(format!("invalid microversion format: {}", yaml::go_quote(v)));
    }
    for p in parts {
        if p.parse::<i64>().is_err() {
            return Err(format!("strconv.Atoi: parsing {}: invalid syntax", yaml::go_quote(p)));
        }
    }
    Ok(())
}

/// Go `utils.extractVersion`: the major version from the last path segment.
fn extract_version(endpoint: &str) -> Option<i64> {
    let u = yaml::url::parse(endpoint).ok()?;
    let p = u.path.trim_end_matches('/');
    let last = p.rsplit('/').next().unwrap_or("");
    parse_version(last).ok().map(|v| v.0)
}

/// The kind name Go's `%T` prints for a value decoded into `any`.
pub(crate) fn go_type_name(v: Option<&Json>) -> &'static str {
    match v {
        None | Some(Json::Null) => "<nil>",
        Some(Json::Bool(_)) => "bool",
        Some(Json::Int(_)) | Some(Json::Float(_)) => "float64",
        Some(Json::Str(_)) => "string",
        Some(Json::Arr(_)) => "[]interface {}",
        Some(Json::Obj(_)) => "map[string]interface {}",
    }
}

fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

/// A struct field as Go's decoder finds it: the last key that matches case-insensitively.
pub(crate) fn field<'a>(o: &'a Json, k: &str) -> Option<&'a Json> {
    o.as_obj()?.iter().rev().find(|(n, _)| n.eq_ignore_ascii_case(k)).map(|(_, v)| v)
}

/// A Go `string` struct field: missing or null is "".
pub(crate) fn str_field(o: &Json, k: &str, ty: &str) -> Result<String, String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(String::new()),
        Some(Json::Str(s)) => Ok(s.clone()),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type string", json_kind(v))),
    }
}

/// A Go `int` struct field.
pub(crate) fn int_field(o: &Json, k: &str, ty: &str) -> Result<i64, String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(0),
        Some(Json::Int(i)) => Ok(*i),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type int", json_kind(v))),
    }
}

/// A Go `[]string` struct field.
pub(crate) fn str_list_field(o: &Json, k: &str, ty: &str) -> Result<Vec<String>, String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(Vec::new()),
        Some(Json::Arr(a)) => a
            .iter()
            .map(|v| match v {
                Json::Null => Ok(String::new()),
                Json::Str(s) => Ok(s.clone()),
                v => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type string", json_kind(v))),
            })
            .collect(),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type []string", json_kind(v))),
    }
}

/// A Go `map[string]any` struct field (`None` for a nil map).
pub(crate) fn map_field<'a>(o: &'a Json, k: &str, ty: &str) -> Result<Option<&'a Vec<(String, Json)>>, String> {
    match field(o, k) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Obj(m)) => Ok(Some(m)),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type map[string]interface {{}}", json_kind(v))),
    }
}

/// The `url.Error` text of a request that was not sent because the context is done.
fn canceled(method: &str, url: &str) -> String {
    let op = format!("{}{}", &method[..1], method[1..].to_ascii_lowercase());
    format!("{op} {}: context canceled", yaml::go_quote(url))
}

/// A catalog endpoint (Go `tokens.Endpoint`).
#[derive(Clone, Debug, Default)]
struct CatalogEndpoint {
    region: String,
    region_id: String,
    interface: String,
    url: String,
}

/// Go `tokens.CatalogEntry`.
#[derive(Clone, Debug, Default)]
struct CatalogEntry {
    type_: String,
    endpoints: Vec<CatalogEndpoint>,
}

/// Go `gophercloud.ProviderClient` with the v3 endpoint locator.
pub struct ProviderClient {
    identity_base: String,
    identity_endpoint: String,
    token_id: String,
    catalog: Vec<CatalogEntry>,
    http: net::Client,
}

/// A response of `request`: status, the `Content-Type` and the body.
pub(crate) struct Resp {
    pub status: u16,
    pub content_type: String,
    pub subject_token: String,
    pub body: Vec<u8>,
}

/// A service client: `endpoint` from the catalog and the base of its resource URLs.
#[derive(Clone, Debug)]
pub struct ServiceClient {
    pub endpoint: String,
    pub resource_base: String,
}

impl ServiceClient {
    /// Go `ServiceClient.ServiceURL`.
    pub fn service_url(&self, parts: &[&str]) -> String {
        format!("{}{}", self.resource_base, parts.join("/"))
    }
}

/// The services the discovery uses (type, major version).
#[derive(Clone, Copy, Debug)]
pub enum Service {
    Compute,
    Network,
    LoadBalancer,
}

impl ProviderClient {
    /// Go `openstack.NewClient`.
    pub fn new(endpoint: &str, http: net::Client) -> Result<ProviderClient, String> {
        let base = parse_endpoint(endpoint, false)?;
        Ok(ProviderClient { identity_base: normalize_url(&base), identity_endpoint: normalize_url(endpoint), token_id: String::new(), catalog: Vec::new(), http })
    }

    /// Go `ProviderClient.Request` (no reauth, no retry function): `ok` are the accepted status codes.
    pub(crate) fn request(&self, ctx: &CancelToken, method: &str, url: &str, body: Option<&Json>, ok: &[u16]) -> Result<Resp, String> {
        if ctx.is_cancelled() {
            return Err(canceled(method, url));
        }
        let data = body.map(|b| b.stringify().into_bytes()).unwrap_or_default();
        let mut req = net::Request::new(method, url, data).map_err(|e| e.to_string())?;
        // Go's transport asks for gzip and decompresses.
        req.decompress = true;
        if body.is_some() {
            req.header.set("Content-Type", "application/json");
        }
        req.header.set("Accept", "application/json");
        req.header.set("User-Agent", USER_AGENT);
        if !self.token_id.is_empty() {
            req.header.set("X-Auth-Token", &self.token_id);
        }
        let resp = match self.http.do_request(req) {
            Ok(r) => r,
            Err(e) => {
                if ctx.is_cancelled() {
                    return Err(canceled(method, url));
                }
                return Err(e.to_string());
            }
        };
        if !ok.contains(&resp.status) {
            let codes: Vec<String> = ok.iter().map(|c| c.to_string()).collect();
            let body = String::from_utf8_lossy(&resp.body);
            return Err(format!("Expected HTTP response code [{}] when accessing [{method} {url}], but got {} instead: {}", codes.join(" "), resp.status, body.trim()));
        }
        Ok(Resp { status: resp.status, content_type: resp.header.get_str("Content-Type").to_string(), subject_token: resp.header.get_str("X-Subject-Token").to_string(), body: resp.body })
    }

    /// `request` with `JSONResponse`: the body decoded (a 204 has none).
    fn request_json(&self, ctx: &CancelToken, method: &str, url: &str, body: Option<&Json>, ok: &[u16]) -> Result<(Resp, Json), String> {
        let r = self.request(ctx, method, url, body, ok)?;
        if r.status == 204 {
            return Ok((r, Json::Null));
        }
        let j = Json::parse_bytes(&r.body).map_err(|e| e.to_string())?;
        Ok((r, j))
    }

    /// Go `openstack.Authenticate`: chooses the identity version and creates a token.
    pub fn authenticate(&mut self, ctx: &CancelToken, opts: &AuthOptions) -> Result<(), String> {
        let (version, endpoint) = self.choose_version(ctx)?;
        if version == "v2.0" {
            return Err("identity v2.0 is not supported, use a Keystone v3 endpoint".to_string());
        }
        self.v3_auth(ctx, &endpoint, opts)
    }

    /// Go `utils.ChooseVersion` with v2.0 (priority 20) and v3 (priority 30).
    fn choose_version(&self, ctx: &CancelToken) -> Result<(&'static str, String), String> {
        const RECOGNIZED: [(&str, &str, i32); 2] = [("v2.0", "/v2.0/", 20), ("v3", "/v3/", 30)];
        let identity_endpoint = normalize_url(&self.identity_endpoint);
        for (id, suffix, _) in RECOGNIZED {
            if identity_endpoint.ends_with(suffix) {
                return Ok((id, identity_endpoint));
            }
        }
        let (_, resp) = self.request_json(ctx, "GET", &self.identity_base, None, &[200, 300])?;
        let values = resp.get("versions").and_then(|v| field(v, "values")).and_then(Json::as_arr).cloned().unwrap_or_default();
        let mut highest: Option<(&'static str, i32)> = None;
        let mut endpoint = String::new();
        for value in &values {
            let vid = str_field(value, "id", "valueResp")?;
            let status = str_field(value, "status", "valueResp")?;
            let mut href = String::new();
            if let Some(Json::Arr(links)) = field(value, "links") {
                for l in links {
                    if str_field(l, "rel", "linkResp")? == "self" {
                        href = normalize_url(&str_field(l, "href", "linkResp")?);
                    }
                }
            }
            for (id, _, prio) in RECOGNIZED {
                if vid.contains(id) {
                    if href == identity_endpoint {
                        return Ok((id, href));
                    }
                    if matches!(status.to_lowercase().as_str(), "current" | "supported" | "stable") && highest.is_none_or(|h| prio > h.1) {
                        highest = Some((id, prio));
                        endpoint = href.clone();
                    }
                }
            }
        }
        let Some((id, _)) = highest else {
            return Err(format!("no supported version available from endpoint {}", self.identity_base));
        };
        if endpoint.is_empty() {
            return Err(format!("endpoint missing in version {id} response from {}", self.identity_base));
        }
        Ok((id, endpoint))
    }

    /// Go `openstack.v3auth` with `tokens.Create`: stores the token and the service catalog.
    fn v3_auth(&mut self, ctx: &CancelToken, endpoint: &str, opts: &AuthOptions) -> Result<(), String> {
        let ep = if endpoint.is_empty() { format!("{}v3/", self.identity_base) } else { endpoint.to_string() };
        let scope = opts.scope_map()?;
        let body = opts.create_map(scope)?;
        let url = format!("{ep}auth/tokens");
        let result = self.request_json(ctx, "POST", &url, Some(&body), &[201, 202]);
        let (resp, j) = match result {
            Ok(x) => x,
            Err(e) => {
                self.token_id.clear();
                return Err(e);
            }
        };
        self.token_id = resp.subject_token.clone();
        // ExtractServiceCatalog: the "token" key of the body.
        let token = match &j {
            Json::Obj(m) => match m.iter().rev().find(|(k, _)| k == "token") {
                Some((_, v)) => v.clone(),
                None if m.is_empty() => Json::Null,
                None => return Err("expected response key \"token\" not found in response".to_string()),
            },
            Json::Null => Json::Null,
            v => return Err(format!("json: cannot unmarshal {} into Go value of type map[string]interface {{}}", json_kind(v))),
        };
        let mut catalog = Vec::new();
        if let Some(Json::Arr(entries)) = field(&token, "catalog") {
            for e in entries {
                let mut entry = CatalogEntry { type_: str_field(e, "type", "CatalogEntry")?, endpoints: Vec::new() };
                if let Some(Json::Arr(eps)) = field(e, "endpoints") {
                    for ep in eps {
                        entry.endpoints.push(CatalogEndpoint {
                            region: str_field(ep, "region", "Endpoint")?,
                            region_id: str_field(ep, "region_id", "Endpoint")?,
                            interface: str_field(ep, "interface", "Endpoint")?,
                            url: str_field(ep, "url", "Endpoint")?,
                        });
                    }
                }
                catalog.push(entry);
            }
        }
        self.catalog = catalog;
        Ok(())
    }

    /// Go `endpointSupportsVersion` with `utils.GetServiceVersions(..., false)`.
    fn endpoint_supports_version(&self, ctx: &CancelToken, endpoint_url: &str, expected: i64) -> Result<bool, String> {
        let url = parse_endpoint(endpoint_url, true)?;
        if let Some(major) = extract_version(&url) {
            return Ok(major == expected);
        }
        let (_, j) = self.request_json(ctx, "GET", &url, None, &[200, 300])?;
        let versions = parse_versions_document(&j)?;
        for v in versions {
            let (major, _) = parse_version(&str_field(&v, "id", "version")?)?;
            let status = str_field(&v, "status", "version")?;
            match status.to_uppercase().as_str() {
                "CURRENT" | "STABLE" | "SUPPORTED" | "DEPRECATED" | "" => {}
                _ => return Err(format!("invalid status: {}", yaml::go_quote(&status))),
            }
            let (ver, max, min) = (str_field(&v, "version", "version")?, str_field(&v, "max_version", "version")?, str_field(&v, "min_version", "version")?);
            if (!ver.is_empty() || !max.is_empty()) && !min.is_empty() {
                parse_microversion(&min)?;
                parse_microversion(if ver.is_empty() { &max } else { &ver })?;
            }
            if major == expected {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Go `NewComputeV2` / `NewNetworkV2` / `NewLoadBalancerV2`: the endpoint from the catalog (`V3Endpoint`).
    pub fn service_client(&self, ctx: &CancelToken, service: Service, region: &str, availability: &str) -> Result<ServiceClient, String> {
        let type_ = match service {
            Service::Compute => "compute",
            Service::Network => "network",
            Service::LoadBalancer => "load-balancer",
        };
        let availability = if availability.is_empty() { "public" } else { availability };
        if !matches!(availability, "admin" | "public" | "internal") {
            return Err(format!("Unexpected availability in endpoint query: {availability}"));
        }
        let mut found = None;
        'outer: for entry in &self.catalog {
            if entry.type_ != type_ {
                continue;
            }
            for ep in &entry.endpoints {
                if ep.interface != availability {
                    continue;
                }
                if !region.is_empty() && ep.region != region && ep.region_id != region {
                    continue;
                }
                let url = normalize_url(&ep.url);
                if !self.endpoint_supports_version(ctx, &url, 2)? {
                    continue;
                }
                found = Some(url);
                break 'outer;
            }
        }
        let Some(endpoint) = found else {
            return Err("No suitable endpoint could be found in the service catalog.".to_string());
        };
        let resource_base = match service {
            Service::Compute => endpoint.clone(),
            Service::Network => format!("{endpoint}v2.0/"),
            Service::LoadBalancer => format!("{}v2.0/", endpoint.replace("v2.0/", "")),
        };
        Ok(ServiceClient { endpoint, resource_base })
    }
}

/// Go `utils.response.UnmarshalJSON`: the versions of a version document.
fn parse_versions_document(j: &Json) -> Result<Vec<Json>, String> {
    let fail = || format!("failed to unmarshal versions document: {}", j.stringify());
    let is_version = |v: &Json| matches!(v, Json::Null) || (matches!(v, Json::Obj(_)) && ["id", "status", "version", "max_version", "min_version"].iter().all(|k| matches!(field(v, k), None | Some(Json::Null) | Some(Json::Str(_)))));
    let Json::Obj(_) = j else {
        return Err(format!("json: cannot unmarshal {} into Go value of type utils.intermediateResponse", json_kind(j)));
    };
    if let Some(v @ Json::Obj(_)) = field(j, "version") {
        return Ok(vec![v.clone()]);
    }
    match field(j, "versions") {
        None | Some(Json::Null) => {}
        Some(Json::Arr(a)) if a.iter().all(is_version) => return Ok(a.iter().map(|v| if v.is_null() { Json::obj() } else { v.clone() }).collect()),
        Some(o @ Json::Obj(_)) => match field(o, "values") {
            None | Some(Json::Null) => return Ok(Vec::new()),
            Some(Json::Arr(a)) if a.iter().all(is_version) => return Ok(a.iter().map(|v| if v.is_null() { Json::obj() } else { v.clone() }).collect()),
            _ => {}
        },
        _ => {}
    }
    if let Some(Json::Str(id)) = field(j, "id")
        && !id.is_empty()
    {
        return Ok(vec![Json::obj().with("id", id.as_str())]);
    }
    Err(fail())
}

/// What a page of a linked list holds (Go `<X>Page` with `Extract<X>`).
pub struct Resource {
    /// The body key of the items ("servers").
    pub key: &'static str,
    /// The body key of the links ("servers_links").
    pub links_key: &'static str,
    /// `ExtractIntoSlicePtr` (true): a non-empty body without `key` is an error; `ExtractInto` (false): no items.
    pub strict: bool,
}

/// Go `pagination.Pager.EachPage` over a linked list: `f` gets the items of each non-empty page and returns
/// whether to go on.
pub fn each_page(pc: &ProviderClient, ctx: &CancelToken, url: &str, res: &Resource, mut f: impl FnMut(&[Json]) -> Result<bool, String>) -> Result<(), String> {
    let mut current = url.to_string();
    loop {
        let r = pc.request(ctx, "GET", &current, None, &[200, 204, 300])?;
        if r.status == 204 {
            return Ok(());
        }
        // PageResultFrom: only an application/json body is decoded; IsEmpty then extracts the items.
        if !r.content_type.starts_with("application/json") {
            return Err("json: cannot unmarshal string into Go value of type map[string]interface {}".to_string());
        }
        let body = Json::parse_bytes(&r.body).map_err(|e| e.to_string())?;
        let items: Vec<Json> = match &body {
            Json::Obj(m) => match m.iter().rev().find(|(k, _)| k == res.key || (!res.strict && k.eq_ignore_ascii_case(res.key))) {
                None if res.strict && !m.is_empty() => return Err(format!("expected response key {} not found in response", yaml::go_quote(res.key))),
                None | Some((_, Json::Null)) => Vec::new(),
                Some((_, Json::Arr(a))) => {
                    for it in a {
                        if !matches!(it, Json::Obj(_) | Json::Null) {
                            return Err(format!("json: cannot unmarshal {} into Go value of type {}", json_kind(it), res.key));
                        }
                    }
                    a.iter().map(|v| if v.is_null() { Json::obj() } else { v.clone() }).collect()
                }
                Some((_, v)) => return Err(format!("json: cannot unmarshal {} into Go struct field .{} of type []{}", json_kind(v), res.key, res.key)),
            },
            Json::Null => Vec::new(),
            v => return Err(format!("json: cannot unmarshal {} into Go value of type map[string]interface {{}}", json_kind(v))),
        };
        if items.is_empty() {
            return Ok(());
        }
        if !f(&items)? {
            return Ok(());
        }
        // NextPageURL: the last link with rel "next".
        let mut next = String::new();
        match field(&body, res.links_key) {
            None | Some(Json::Null) => {}
            Some(Json::Arr(links)) => {
                for l in links {
                    if str_field(l, "rel", "Link")? == "next" {
                        next = str_field(l, "href", "Link")?;
                    }
                }
            }
            Some(v) => return Err(format!("json: cannot unmarshal {} into Go struct field .{} of type []gophercloud.Link", json_kind(v), res.links_key)),
        }
        if next.is_empty() {
            return Ok(());
        }
        current = next;
    }
}

/// Go `pagination.Pager.AllPages` followed by the extraction: the items of all pages.
pub fn all_pages(pc: &ProviderClient, ctx: &CancelToken, url: &str, res: &Resource) -> Result<Vec<Json>, String> {
    let mut out = Vec::new();
    each_page(pc, ctx, url, res, |items| {
        out.extend_from_slice(items);
        Ok(true)
    })?;
    Ok(out)
}
