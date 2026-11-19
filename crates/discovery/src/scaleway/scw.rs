//! The subset of scaleway-sdk-go v1.39.0 (`scw` package) the Scaleway SD uses: client settings validation,
//! requests (X-Auth-Token, user agent), `WithAllPages` pagination (`page` + `total_count`), response errors,
//! the API metadata request, zones and regions.
use json::Json;

use crate::gojson::{self, Dec, key_is, sub};

/// Go `defaultUserAgent` of the reference binary (scaleway-sdk-go v1.39.0 built with go1.27.3).
pub(crate) fn default_user_agent() -> String {
    let arch = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        a => a,
    };
    format!(
        "scaleway-sdk-go/v1.39.0 (go1.27.3; {}; {arch})",
        std::env::consts::OS
    )
}

/// Go `AllZones`.
pub(crate) const ALL_ZONES: [&str; 12] = [
    "fr-par-1", "fr-par-2", "fr-par-3", "nl-ams-1", "nl-ams-2", "nl-ams-3", "pl-waw-1", "pl-waw-2",
    "pl-waw-3", "it-mil-1", "it-mil-2", "it-mil-3",
];

fn lower_run(s: &str, min: usize, max: usize) -> bool {
    (min..=max).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_lowercase())
}

/// Go `validation.IsRegion`: `^[a-z]{2}-[a-z]{3,7}$`.
pub(crate) fn is_region(s: &str) -> bool {
    let p: Vec<&str> = s.split('-').collect();
    p.len() == 2 && lower_run(p[0], 2, 2) && lower_run(p[1], 3, 7)
}

/// Go `validation.IsZone`: `^[a-z]{2}-[a-z]{3,7}-[0-9]{1,2}$`.
pub(crate) fn is_zone(s: &str) -> bool {
    let p: Vec<&str> = s.split('-').collect();
    p.len() == 3
        && lower_run(p[0], 2, 2)
        && lower_run(p[1], 3, 7)
        && (1..=2).contains(&p[2].len())
        && p[2].bytes().all(|b| b.is_ascii_digit())
}

/// Go `validation.IsUUID`.
pub(crate) fn is_uuid(s: &str) -> bool {
    let p: Vec<&str> = s.split('-').collect();
    p.len() == 5
        && p.iter()
            .zip([8, 4, 4, 4, 12])
            .all(|(x, n)| x.len() == n && x.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Go `validation.IsAccessKey`: `^SCW[A-Z0-9]{17}$`.
pub(crate) fn is_access_key(s: &str) -> bool {
    s.len() == 20
        && s.starts_with("SCW")
        && s[3..]
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Go `Zone.Region()`.
pub(crate) fn zone_region(zone: &str) -> Result<String, String> {
    if !is_zone(zone) {
        return Err(format!("invalid zone '{zone}'"));
    }
    let p: Vec<&str> = zone.split('-').collect();
    Ok(format!("{}-{}", p[0], p[1]))
}

/// Go `Zone.UnmarshalJSON`: "" leaves the zone; `ParseZone` otherwise.
pub(crate) fn unmarshal_zone(v: &Json, dst: &mut String) -> Result<(), String> {
    let s = gojson::custom_string(v)?;
    if s.is_empty() {
        return Ok(());
    }
    *dst = match s.as_str() {
        "par1" => "fr-par-1".to_string(),
        "ams1" => "nl-ams-1".to_string(),
        _ if is_zone(&s) => s,
        _ => {
            return Err(format!(
                "scaleway-sdk-go: bad zone format, available zones are: {}",
                ALL_ZONES.join(", ")
            ));
        }
    };
    Ok(())
}

/// A generated enum's `UnmarshalJSON`: "" (or null) becomes the default value.
pub(crate) fn unmarshal_enum(v: &Json, dst: &mut String, default: &str) -> Result<(), String> {
    let s = gojson::custom_string(v)?;
    *dst = if s.is_empty() { default.to_string() } else { s };
    Ok(())
}

/// Go `InvalidClientOptionError`.
fn option_error(msg: String) -> String {
    format!("scaleway-sdk-go: {msg}")
}

/// Go `settings.validate` for the settings Prometheus sets (token auth, project, zone, user agent, API URL).
pub(crate) fn validate_settings(
    access_key: &str,
    secret_key: &str,
    project_id: &str,
    zone: &str,
    user_agent: &str,
    api_url: &str,
) -> Result<(), String> {
    if access_key.is_empty() {
        return Err(option_error("access key cannot be empty".into()));
    }
    if !is_access_key(access_key) {
        return Err(option_error(format!(
            "invalid access key format '{access_key}', expected SCWXXXXXXXXXXXXXXXXX format"
        )));
    }
    if secret_key.is_empty() {
        return Err(option_error("secret key cannot be empty".into()));
    }
    if !is_uuid(secret_key) {
        return Err(option_error(format!(
            "invalid secret key format '{secret_key}', expected a UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )));
    }
    if project_id.is_empty() {
        return Err(option_error("default project ID cannot be empty".into()));
    }
    if !is_uuid(project_id) {
        return Err(option_error(format!(
            "invalid project ID format '{project_id}', expected a UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )));
    }
    if zone.is_empty() {
        return Err(option_error("default zone cannot be empty".into()));
    }
    if !is_zone(zone) {
        return Err(option_error(format!(
            "invalid default zone format '{zone}', available zones are: {}",
            ALL_ZONES.join(", ")
        )));
    }
    if !user_agent.is_empty() && user_agent != default_user_agent() {
        if user_agent.len() > 512 {
            return Err(option_error(format!(
                "invalid user agent '{}...[truncated]': length should not be over 512",
                &user_agent[..512]
            )));
        }
        if !user_agent.bytes().all(|b| (0x20..=0x7e).contains(&b)) {
            return Err(option_error(format!(
                "invalid user agent '{user_agent}': contains prohibited characters"
            )));
        }
    }
    if api_url.is_empty() || yaml::url::parse(api_url).is_err() {
        return Err(option_error(format!("invalid API url '{api_url}'")));
    }
    if api_url.ends_with('/') {
        return Err(option_error(format!(
            "invalid API url '{api_url}' it should not have a trailing slash"
        )));
    }
    Ok(())
}

/// Go `scw.Client`, the parts used.
pub(crate) struct Client {
    pub api_url: String,
    pub secret_key: String,
    pub user_agent: String,
    pub default_zone: String,
    pub http: net::Client,
    /// `secret_key_file`: the token read from this file replaces X-Auth-Token on every request (Go
    /// `authTokenFileRoundTripper`).
    pub token_file: Option<String>,
    /// Go `apiMetadata` is cached once non-zero.
    metadata_cached: bool,
}

/// A decoded response body (`None`: nothing decoded, as Go when `Content-Length` is 0) and the
/// `X-Total-Count` header of legacy listers.
pub(crate) struct Body {
    pub json: Option<Json>,
    pub x_total_count: Option<i64>,
}

impl Client {
    pub(crate) fn new(
        api_url: String,
        secret_key: String,
        user_agent: String,
        default_zone: String,
        http: net::Client,
        token_file: Option<String>,
    ) -> Client {
        Client {
            api_url,
            secret_key,
            user_agent,
            default_zone,
            http,
            token_file,
            metadata_cached: false,
        }
    }

    /// Go `Client.do` for a GET of `path?query`.
    pub(crate) fn get(
        &self,
        path: &str,
        query: &[(String, String)],
        legacy_lister: bool,
    ) -> Result<Body, String> {
        let raw = format!("{}{}", self.api_url, path);
        let mut u = yaml::url::parse(&raw)
            .map_err(|e| format!("scaleway-sdk-go: invalid url {raw}: {e}"))?;
        u.raw_query = yaml::url::encode_query(query);
        let url = u.to_url_string();
        let mut req = net::Request::get(&url)
            .map_err(|e| format!("scaleway-sdk-go: could not create request: {e}"))?;
        req.header.set("X-Auth-Token", &self.secret_key);
        req.header.set("User-Agent", &self.user_agent);
        if let Some(f) = &self.token_file {
            match net::types::read_file(f) {
                Ok(b) => req
                    .header
                    .set("X-Auth-Token", String::from_utf8_lossy(&b).trim()),
                Err(e) => {
                    return Err(format!(
                        "scaleway-sdk-go: error executing request: Get {}: unable to read auth token file {f}: {e}",
                        yaml::go_quote(&url)
                    ));
                }
            }
        }
        let resp = self
            .http
            .do_request(req)
            .map_err(|e| format!("scaleway-sdk-go: error executing request: {e}"))?;
        if !(200..=299).contains(&resp.status) {
            return Err(response_error(&resp));
        }
        let content_length: i64 = match resp.header.get("Content-Length") {
            Some(v) => v.trim().parse().unwrap_or(-1),
            None if resp.status == 204 || resp.status == 304 => 0,
            None => -1,
        };
        let mut body = Body {
            json: None,
            x_total_count: None,
        };
        if content_length != 0 {
            let ct = resp.header.get_str("Content-Type");
            if !ct.starts_with("application/json") {
                return Err(format!(
                    "scaleway-sdk-go: could not handle {ct} response body with <nil> result type"
                ));
            }
            body.json = Some(gojson::parse(&resp.body, true).map_err(|e| {
                format!("scaleway-sdk-go: could not parse {ct} response body: {e}")
            })?);
            let x = resp.header.get_str("X-Total-Count");
            if legacy_lister && !x.is_empty() {
                body.x_total_count = Some(gojson::parse_int(x, "ParseInt", 32).map_err(|e| {
                    format!("scaleway-sdk-go: could not parse X-Total-Count header: {e}")
                })?);
            }
        }
        Ok(body)
    }

    /// Go `doListAll`: requests pages 1, 2, ... until a page is empty or the page count from the first
    /// page's total is reached. `page_fn` decodes a page (appending its items) and returns (items, total).
    pub(crate) fn list_all(
        &self,
        path: &str,
        mut query: Vec<(String, String)>,
        legacy_lister: bool,
        mut page_fn: impl FnMut(&Body) -> Result<(u64, u64), String>,
    ) -> Result<(), String> {
        const MAX_PAGE_COUNT: u64 = u32::MAX as u64;
        let mut page_count = MAX_PAGE_COUNT;
        let mut page = 1u64;
        while page <= page_count {
            query.retain(|(k, _)| k != "page");
            query.push(("page".to_string(), page.to_string()));
            let body = self.get(path, &query, legacy_lister)?;
            let (page_size, total) = page_fn(&body)?;
            if page_size == 0 {
                return Ok(());
            }
            if page_count == MAX_PAGE_COUNT {
                page_count = total.div_ceil(page_size);
            }
            page += 1;
        }
        Ok(())
    }

    /// Go `GetAPIMetadata` as the instance and IPAM list calls use it: `GET /metadata` until it gives a
    /// non-zero answer; errors are ignored (the metadata only sets SRN fields Prometheus does not read).
    pub(crate) fn api_metadata(&mut self) {
        if self.metadata_cached {
            return;
        }
        if let Ok(Body { json: Some(v), .. }) = self.get("/metadata", &[], false) {
            let mut d = Dec::new();
            let mut fields = [String::new(), String::new(), String::new()];
            if let Some(o) = d.obj(&v, "", "", "scw.ApiMetadata") {
                for (k, e) in o {
                    for (i, n) in ["platform", "partition", "domain"].iter().enumerate() {
                        if key_is(k, n) {
                            d.str(e, &mut fields[i], "ApiMetadata", k);
                        }
                    }
                }
            }
            if d.finish().is_ok() && fields.iter().any(|f| !f.is_empty()) {
                self.metadata_cached = true;
            }
        }
    }
}

/// Decodes the list root `{"total_count": N, "<field>": [...]}` of Go type `<pkg>.<root>`: calls `item` on each
/// element (null elements are skipped: Go keeps nil pointers, which the SD code would dereference) and
/// returns (number of elements, total count).
pub(crate) fn decode_list(
    body: &Body,
    pkg: &str,
    root: &str,
    field: &str,
    item_type: &str,
    total_type: &str,
    mut item: impl FnMut(&mut Dec, &Json) -> Result<(), String>,
) -> Result<(u64, u64), String> {
    let Some(v) = &body.json else {
        return Ok((0, 0));
    };
    let mut d = Dec::new();
    let (mut n, mut total) = (0u64, 0i64);
    if let Some(o) = d.obj(v, "", "", &format!("{pkg}.{root}")) {
        for (k, e) in o {
            if key_is(k, "total_count") {
                d.int(e, &mut total, root, k, total_type);
            } else if key_is(k, field)
                && let Some(a) = d.arr(e, root, field, &format!("[]*{pkg}.{item_type}"))
            {
                n = a.len() as u64;
                for x in a {
                    if !x.is_null() {
                        item(&mut d, x)?;
                    }
                }
            }
        }
    }
    d.finish()?;
    let mut total = total as u64;
    if let Some(x) = body.x_total_count {
        // UnsafeSetTotalCount: uint32(totalCount)
        total = x as u32 as u64;
    }
    Ok((n, total))
}

/// Go `hasResponseError` for a non-2xx response: the error text.
fn response_error(r: &net::Response) -> String {
    let status = &r.status_text;
    let ct = r.header.get_str("Content-Type");
    let plain = |message: &str, resource: &str, fields: &[(String, Vec<String>)]| {
        let mut s = format!("scaleway-sdk-go: http error {status}");
        if !resource.is_empty() {
            s = format!("{s}: resource {resource}");
        }
        if !message.is_empty() {
            s = format!("{s}: {message}");
        }
        if !fields.is_empty() {
            let mut f = fields.to_vec();
            f.sort();
            let m: Vec<String> = f
                .iter()
                .map(|(k, v)| format!("{k}:[{}]", v.join(" ")))
                .collect();
            s = format!("{s}: map[{}]", m.join(" "));
        }
        s
    };
    if !ct.starts_with("application/json") {
        return plain(status, "", &[]);
    }
    let v = match gojson::parse(&r.body, false) {
        Ok(v) => v,
        Err(e) => return format!("scaleway-sdk-go: could not parse error response body: {e}"),
    };
    let mut d = Dec::new();
    let (mut message, mut ty, mut resource) = (String::new(), String::new(), String::new());
    let mut fields: Vec<(String, Vec<String>)> = Vec::new();
    let st = "tmpResponseError";
    if let Some(o) = d.obj(&v, "", "", "scw.tmpResponseError") {
        for (k, e) in o {
            if key_is(k, "message") {
                d.str(e, &mut message, st, k);
            } else if key_is(k, "type") {
                d.str(e, &mut ty, st, k);
            } else if key_is(k, "resource") {
                d.str(e, &mut resource, st, k);
            } else if key_is(k, "fields") {
                match e {
                    Json::Obj(fo) => {
                        for (fk, fv) in fo {
                            let mut list = Vec::new();
                            d.str_slice(fv, &mut list, st, k);
                            fields.retain(|(n, _)| n != fk);
                            fields.push((fk.clone(), list));
                        }
                    }
                    Json::Null => fields.clear(),
                    _ => d.mismatch(e, st, k, "map[string][]string"),
                }
            }
        }
    }
    if let Err(e) = d.finish() {
        return format!("scaleway-sdk-go: could not parse error response body: {e}");
    }
    if let Some(e) = standard_error(&ty, &v) {
        return e;
    }
    if let Some(e) = non_standard_error(&ty, &v) {
        return e;
    }
    plain(&message, &resource, &fields)
}

/// Reads string fields of an object (Go struct `st`); the type error when one has the wrong kind.
fn str_fields(v: &Json, st: &str, names: &[&str]) -> Result<Vec<String>, String> {
    let mut d = Dec::new();
    let mut out = vec![String::new(); names.len()];
    if let Some(o) = d.obj(v, "", "", &format!("scw.{st}")) {
        for (k, e) in o {
            for (i, n) in names.iter().enumerate() {
                if key_is(k, n) {
                    d.str(e, &mut out[i], st, k);
                }
            }
        }
    }
    d.finish()?;
    Ok(out)
}

/// One detail: its string fields and its integer fields.
type Detail = (Vec<String>, Vec<i64>);

/// The `details` list of an error object, each element's string fields (`int_names` are uint32 fields).
fn details(
    v: &Json,
    st: &str,
    elem: &str,
    names: &[&str],
    int_names: &[&str],
) -> Result<Vec<Detail>, String> {
    let mut d = Dec::new();
    let mut out = Vec::new();
    if let Some(o) = d.obj(v, "", "", &format!("scw.{st}")) {
        for (k, e) in o {
            if !key_is(k, "details") {
                continue;
            }
            if let Some(a) = d.arr(e, st, k, &format!("[]scw.{elem}")) {
                out.clear();
                for x in a {
                    let mut s = vec![String::new(); names.len()];
                    let mut n = vec![0i64; int_names.len()];
                    if let Some(xo) = d.obj(x, st, k, &format!("scw.{elem}")) {
                        for (k2, y) in xo {
                            for (i, f) in names.iter().enumerate() {
                                if key_is(k2, f) {
                                    d.str(y, &mut s[i], elem, &sub(k, k2));
                                }
                            }
                            for (i, f) in int_names.iter().enumerate() {
                                if key_is(k2, f) {
                                    d.int(y, &mut n[i], elem, &sub(k, k2), "uint32");
                                }
                            }
                        }
                    }
                    out.push((s, n));
                }
            }
        }
    }
    d.finish()?;
    Ok(out)
}

/// Go `unmarshalStandardError`.
fn standard_error(ty: &str, v: &Json) -> Option<String> {
    let wrap =
        |e: String| format!("scaleway-sdk-go: could not parse error {ty} response body: {e}");
    let r: Result<String, String> = match ty {
        "invalid_arguments" => details(
            v,
            "InvalidArgumentsError",
            "InvalidArgumentsErrorDetail",
            &["argument_name", "reason", "help_message"],
            &[],
        )
        .map(|ds| {
            let args: Vec<String> = ds
                .iter()
                .map(|(s, _)| {
                    let mut a = s[0].clone();
                    match s[1].as_str() {
                        "unknown" => a += " is invalid for unexpected reason",
                        "required" => a += " is required",
                        "format" => a += " is wrongly formatted",
                        "constraint" => a += " does not respect constraint",
                        _ => {}
                    }
                    if !s[2].is_empty() {
                        a += ", ";
                        a += &s[2];
                    }
                    a
                })
                .collect();
            format!("scaleway-sdk-go: invalid argument(s): {}", args.join("; "))
        }),
        "quotas_exceeded" => details(
            v,
            "QuotasExceededError",
            "QuotasExceededErrorDetail",
            &["resource"],
            &["quota", "current"],
        )
        .map(|ds| {
            let args: Vec<String> = ds
                .iter()
                .map(|(s, n)| format!("{} has reached its quota ({}/{})", s[0], n[1], n[0]))
                .collect();
            format!("scaleway-sdk-go: quota exceeded(s): {}", args.join("; "))
        }),
        "transient_state" => str_fields(
            v,
            "TransientStateError",
            &["resource", "resource_id", "current_state"],
        )
        .map(|f| {
            format!(
                "scaleway-sdk-go: resource {} with ID {} is in a transient state: {}",
                f[0], f[1], f[2]
            )
        }),
        "not_found" => {
            str_fields(v, "ResourceNotFoundError", &["resource", "resource_id"]).map(|f| {
                format!(
                    "scaleway-sdk-go: resource {} with ID {} is not found",
                    f[0], f[1]
                )
            })
        }
        "locked" => str_fields(v, "ResourceLockedError", &["resource", "resource_id"]).map(|f| {
            format!(
                "scaleway-sdk-go: resource {} with ID {} is locked",
                f[0], f[1]
            )
        }),
        "permissions_denied" => {
            let elem = "struct { Resource string \"json:\\\"resource\\\"\"; Action string \"json:\\\"action\\\"\" }";
            details(
                v,
                "PermissionsDeniedError",
                elem,
                &["resource", "action"],
                &[],
            )
            .map(|ds| {
                let args: Vec<String> = ds
                    .iter()
                    .map(|(s, _)| format!("{} {}", s[1], s[0]))
                    .collect();
                format!(
                    "scaleway-sdk-go: insufficient permissions: {}",
                    args.join("; ")
                )
            })
        }
        "out_of_stock" => str_fields(v, "OutOfStockError", &["resource"])
            .map(|f| format!("scaleway-sdk-go: resource {} is out of stock", f[0])),
        "resource_expired" => str_fields(
            v,
            "ResourceExpiredError",
            &["resource", "resource_id", "expired_since"],
        )
        .map(|f| {
            format!(
                "scaleway-sdk-go: resource {} with ID {} expired since {}",
                f[0],
                f[1],
                go_time_string(&f[2])
            )
        }),
        "denied_authentication" => {
            str_fields(v, "DeniedAuthenticationError", &["method", "reason"]).map(|f| {
                let method = match f[0].as_str() {
                    "unknown_method" => "unknown method",
                    "jwt" => "JWT",
                    "api_key" => "API key",
                    _ => "",
                };
                let reason = match f[1].as_str() {
                    "unknown_reason" => "unknown reason".to_string(),
                    "invalid_argument" => format!("invalid {method} format or empty value"),
                    "not_found" => format!("{method} does not exist"),
                    "expired" => format!("{method} is expired"),
                    _ => String::new(),
                };
                format!("scaleway-sdk-go: denied authentication: {reason}")
            })
        }
        "precondition_failed" => str_fields(
            v,
            "PreconditionFailedError",
            &["precondition", "help_message"],
        )
        .map(|f| {
            let mut msg = match f[0].as_str() {
                "unknown_precondition" => "unknown precondition",
                "resource_still_in_use" => "resource is still in use",
                "attribute_must_be_set" => "attribute must be set",
                _ => "",
            }
            .to_string();
            if !f[1].is_empty() {
                msg += ", ";
                msg += &f[1];
            }
            format!("scaleway-sdk-go: precondition failed: {msg}")
        }),
        _ => return None,
    };
    Some(r.unwrap_or_else(wrap))
}

/// Go `time.Time.String()` of an RFC 3339 timestamp (best effort; "" is the zero time).
fn go_time_string(s: &str) -> String {
    if s.is_empty() {
        return "0001-01-01 00:00:00 +0000 UTC".to_string();
    }
    let (dt, zone) = if let Some(d) = s.strip_suffix('Z') {
        (d, "+0000 UTC".to_string())
    } else if s.len() > 6
        && (s.as_bytes()[s.len() - 6] == b'+' || s.as_bytes()[s.len() - 6] == b'-')
    {
        let z = s[s.len() - 6..].replace(':', "");
        (&s[..s.len() - 6], format!("{z} {z}"))
    } else {
        (s, String::new())
    };
    format!("{} {zone}", dt.replacen('T', " ", 1))
}

/// Go `unmarshalNonStandardError`.
fn non_standard_error(ty: &str, v: &Json) -> Option<String> {
    match ty {
        "unknown_resource" => {
            let f = match str_fields(v, "UnknownResource", &["message"]) {
                Ok(f) => f,
                Err(e) => {
                    return Some(format!(
                        "scaleway-sdk-go: could not parse error {ty} response body: {e}"
                    ));
                }
            };
            // ToResourceNotFoundError
            let msg = &f[0];
            let mut parts: Vec<&str> = msg.split('"').collect();
            if parts.len() == 1 {
                parts = msg.split('\'').collect();
            }
            let (resource, id) = match parts.len() {
                2 => (String::new(), parts[0].to_string()),
                3 => (
                    parts[0].trim().to_lowercase().replace(' ', "_"),
                    parts[1].to_string(),
                ),
                _ => return None,
            };
            if !is_uuid(&id) {
                return None;
            }
            Some(format!(
                "scaleway-sdk-go: resource {resource} with ID {id} is not found"
            ))
        }
        "invalid_request_error" => {
            let mut d = Dec::new();
            let (mut message, mut resource) = (String::new(), String::new());
            let mut fields: Vec<(String, Vec<String>)> = Vec::new();
            if let Some(o) = d.obj(v, "", "", "scw.InvalidRequestError") {
                for (k, e) in o {
                    if key_is(k, "message") {
                        d.str(e, &mut message, "InvalidRequestError", k);
                    } else if key_is(k, "resource") {
                        d.str(e, &mut resource, "InvalidRequestError", k);
                    } else if key_is(k, "fields") {
                        match e {
                            Json::Obj(fo) => {
                                for (fk, fv) in fo {
                                    let mut list = Vec::new();
                                    d.str_slice(fv, &mut list, "InvalidRequestError", k);
                                    fields.retain(|(n, _)| n != fk);
                                    fields.push((fk.clone(), list));
                                }
                            }
                            Json::Null => fields.clear(),
                            _ => d.mismatch(e, "InvalidRequestError", k, "map[string][]string"),
                        }
                    }
                }
            }
            if let Err(e) = d.finish() {
                return Some(format!(
                    "scaleway-sdk-go: could not parse error {ty} response body: {e}"
                ));
            }
            if !fields.is_empty() {
                fields.sort();
                let mut args = Vec::new();
                for (name, msgs) in &fields {
                    for m in msgs {
                        let mut a = format!("{name} does not respect constraint");
                        if !m.is_empty() {
                            a += ", ";
                            a += m;
                        }
                        args.push(a);
                    }
                }
                return Some(format!(
                    "scaleway-sdk-go: invalid argument(s): {}",
                    args.join("; ")
                ));
            }
            if message
                .to_lowercase()
                .contains("quota exceeded for this resource")
            {
                return Some(format!(
                    "scaleway-sdk-go: quota exceeded(s): {resource} has reached its quota (0/0)"
                ));
            }
            None
        }
        _ => None,
    }
}
