//! Port of `web/api/testhelpers/openapi.go`: `ValidateOpenAPI` checks a request and its response against the
//! OpenAPI 3.1 and 3.2 golden specs.
//!
//! adapted: Go uses `libopenapi-validator`. This is a test-only validator of the subset of OpenAPI and JSON
//! Schema that the specs use:
//! - request: the path and method must exist; path and query parameters are checked against their schemas
//!   (scalars parsed from the text as the schema type; `explode: false` arrays split on commas; required ones
//!   must be present); a request body must have a declared content type, and a form body is checked against
//!   the body schema (required fields, unknown fields, field types).
//! - response: the status code (or `default`) must be declared, the content type must be one of the declared
//!   ones, and a JSON body must match the schema: `$ref`, `type` (also as a list), `enum`, `properties`,
//!   `required`, `additionalProperties`, `items`, `minItems`/`maxItems`, `oneOf` (exactly one), `anyOf`,
//!   `allOf`, `nullable`, and the `date-time` and `duration` string formats.

use std::sync::OnceLock;

use json::Json;
use yaml::Value;

use super::request::Response;

/// A loaded spec.
pub struct Spec {
    pub version: &'static str,
    root: Value,
}

fn load(version: &'static str, file: &str) -> Spec {
    let p = format!(
        "{}/../../upstream/web/api/v1/testdata/{file}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&p)
        .unwrap_or_else(|e| panic!("failed to read OpenAPI {version} spec from {p}: {e}"));
    let root: Value = yaml::unmarshal(&text)
        .unwrap_or_else(|e| panic!("failed to parse OpenAPI {version} document: {e}"));
    Spec { version, root }
}

/// Go `loadOpenAPIValidators`: both specs, loaded once.
pub fn specs() -> [&'static Spec; 2] {
    static S31: OnceLock<Spec> = OnceLock::new();
    static S32: OnceLock<Spec> = OnceLock::new();
    [
        S31.get_or_init(|| load("3.1", "openapi_3.1_golden.yaml")),
        S32.get_or_init(|| load("3.2", "openapi_3.2_golden.yaml")),
    ]
}

fn map_pairs(v: &Value) -> &[(Value, Value)] {
    match v {
        Value::Map(p) => p,
        _ => &[],
    }
}

fn seq(v: Option<&Value>) -> &[Value] {
    match v {
        Some(Value::Seq(s)) => s,
        _ => &[],
    }
}

fn media_type(ct: &str) -> &str {
    ct.split(';').next().unwrap_or("").trim()
}

/// RFC 3339 date-time (`2006-01-02T15:04:05Z`, optional fraction, `Z` or an offset).
fn is_date_time(s: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")
            .unwrap()
    })
    .is_match(s)
}

/// A Prometheus duration such as `1m30s`.
fn is_duration(s: &str) -> bool {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    !s.is_empty()
        && RE
            .get_or_init(|| {
                regex::Regex::new(r"^(\d+y)?(\d+w)?(\d+d)?(\d+h)?(\d+m)?(\d+s)?(\d+ms)?$").unwrap()
            })
            .is_match(s)
}

impl Spec {
    fn resolve<'a>(&'a self, schema: &'a Value) -> &'a Value {
        let mut s = schema;
        while let Some(Value::Str(r)) = s.get("$ref") {
            let name = r
                .strip_prefix("#/components/schemas/")
                .unwrap_or_else(|| panic!("unsupported $ref {r}"));
            s = self
                .root
                .get("components")
                .and_then(|c| c.get("schemas"))
                .and_then(|c| c.get(name))
                .unwrap_or_else(|| panic!("unknown $ref {r}"));
        }
        s
    }

    /// The path item and operation of a request, and its path parameters.
    fn find_operation(
        &self,
        method: &str,
        path: &str,
    ) -> Result<(&Value, Vec<(String, String)>), String> {
        let server = self
            .root
            .get("servers")
            .and_then(|s| seq(Some(s)).first())
            .and_then(|s| s.get("url"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(rel) = path.strip_prefix(server) else {
            return Err(format!(
                "path {path} not found: it is not under the server URL {server}"
            ));
        };
        let segs: Vec<&str> = rel.split('/').collect();
        for (k, item) in map_pairs(self.root.get("paths").unwrap_or(&Value::Null)) {
            let template = k.as_str().unwrap_or_default();
            let tsegs: Vec<&str> = template.split('/').collect();
            if tsegs.len() != segs.len() {
                continue;
            }
            let mut params = Vec::new();
            let matches = tsegs.iter().zip(&segs).all(|(t, s)| {
                match t.strip_prefix('{').and_then(|t| t.strip_suffix('}')) {
                    Some(name) => {
                        params.push((name.to_string(), s.to_string()));
                        !s.is_empty()
                    }
                    None => t == s,
                }
            });
            if !matches {
                continue;
            }
            return match item.get(&method.to_ascii_lowercase()) {
                Some(op) => Ok((op, params)),
                None => Err(format!(
                    "{method} {path}: method not found in path {template}"
                )),
            };
        }
        Err(format!("{method} {path}: path not found"))
    }

    /// Checks a request; returns the errors.
    pub fn validate_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        content_type: &str,
        body: &[u8],
    ) -> Vec<String> {
        let (op, path_params) = match self.find_operation(method, path) {
            Ok(x) => x,
            Err(e) => return vec![e],
        };
        let mut errs = Vec::new();
        let query = http::parse_query(query);
        for p in seq(op.get("parameters")) {
            let name = p.get("name").and_then(Value::as_str).unwrap_or_default();
            let location = p.get("in").and_then(Value::as_str).unwrap_or_default();
            let required = matches!(p.get("required"), Some(Value::Bool(true)));
            let explode = !matches!(p.get("explode"), Some(Value::Bool(false)));
            let schema = p.get("schema").unwrap_or(&Value::Null);
            let values: Vec<String> = match location {
                "path" => path_params
                    .iter()
                    .filter(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .collect(),
                "query" => query
                    .iter()
                    .filter(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .collect(),
                other => panic!("unsupported parameter location {other}"),
            };
            if values.is_empty() {
                if required {
                    errs.push(format!("{location} parameter {name:?} is missing"));
                }
                continue;
            }
            self.check_text_values(
                schema,
                &values,
                explode,
                &format!("{location} parameter {name:?}"),
                &mut errs,
            );
        }
        match op.get("requestBody") {
            Some(rb) => {
                let required = matches!(rb.get("required"), Some(Value::Bool(true)));
                if body.is_empty() {
                    if required {
                        errs.push("request body is missing".to_string());
                    }
                } else {
                    let ct = media_type(content_type);
                    match rb.get("content").and_then(|c| c.get(ct)) {
                        None => {
                            errs.push(format!("request body content type {ct:?} is not declared"))
                        }
                        Some(mt) => {
                            let schema = mt.get("schema").unwrap_or(&Value::Null);
                            if ct == "application/x-www-form-urlencoded" {
                                let fields =
                                    http::parse_query(std::str::from_utf8(body).unwrap_or(""));
                                self.check_form(schema, &fields, &mut errs);
                            } else if ct == "application/json" {
                                match Json::parse(std::str::from_utf8(body).unwrap_or("")) {
                                    Ok(j) => self.check(schema, &j, "request body", &mut errs),
                                    Err(e) => errs.push(format!("request body is not JSON: {e:?}")),
                                }
                            }
                        }
                    }
                }
            }
            None => {
                if !body.is_empty() {
                    errs.push(format!("{method} {path} takes no request body"));
                }
            }
        }
        errs
    }

    /// Checks a response; returns the errors.
    pub fn validate_response(
        &self,
        method: &str,
        path: &str,
        status: u16,
        content_type: &str,
        body: &str,
    ) -> Vec<String> {
        let (op, _) = match self.find_operation(method, path) {
            Ok(x) => x,
            Err(e) => return vec![e],
        };
        let responses = op.get("responses").unwrap_or(&Value::Null);
        let Some(resp) = responses
            .get(&status.to_string())
            .or_else(|| responses.get("default"))
        else {
            return vec![format!("status {status} is not declared")];
        };
        let Some(content) = resp.get("content") else {
            return Vec::new();
        };
        let ct = media_type(content_type);
        let Some(mt) = content.get(ct) else {
            return vec![format!(
                "response content type {ct:?} is not declared for status {status}"
            )];
        };
        let mut errs = Vec::new();
        if ct == "application/json" {
            let schema = mt.get("schema").unwrap_or(&Value::Null);
            match Json::parse(body) {
                Ok(j) => self.check(schema, &j, "$", &mut errs),
                Err(e) => errs.push(format!("response body is not JSON: {e:?}")),
            }
        }
        errs
    }

    /// A form body: every field must be a property (unless additional properties are allowed), required fields
    /// must be present, and each value must parse as its property's type.
    fn check_form(&self, schema: &Value, fields: &[(String, String)], errs: &mut Vec<String>) {
        let schema = self.resolve(schema);
        let props = schema.get("properties");
        for r in seq(schema.get("required")) {
            let r = r.as_str().unwrap_or_default();
            if !fields.iter().any(|(k, _)| k == r) {
                errs.push(format!("request body field {r:?} is required"));
            }
        }
        let mut seen: Vec<&str> = Vec::new();
        for (k, _) in fields {
            if seen.contains(&k.as_str()) {
                continue;
            }
            seen.push(k);
            let values: Vec<String> = fields
                .iter()
                .filter(|(f, _)| f == k)
                .map(|(_, v)| v.clone())
                .collect();
            match props.and_then(|p| p.get(k)) {
                Some(ps) => self.check_text_values(
                    ps,
                    &values,
                    true,
                    &format!("request body field {k:?}"),
                    errs,
                ),
                None => {
                    if matches!(schema.get("additionalProperties"), Some(Value::Bool(false))) {
                        errs.push(format!("request body field {k:?} is not allowed"));
                    }
                }
            }
        }
    }

    /// Text values of a parameter or form field: an array schema takes all of them; other schemas exactly one.
    fn check_text_values(
        &self,
        schema: &Value,
        values: &[String],
        explode: bool,
        at: &str,
        errs: &mut Vec<String>,
    ) {
        let s = self.resolve(schema);
        if s.get("type").and_then(Value::as_str) == Some("array") {
            let items = s.get("items").unwrap_or(&Value::Null);
            let mut n = 0i64;
            for v in values {
                let parts: Vec<&str> = if explode {
                    vec![v.as_str()]
                } else {
                    v.split(',').collect()
                };
                for p in parts {
                    n += 1;
                    self.check_text(items, p, &format!("{at} item"), errs);
                }
            }
            self.check_len(s, n, at, errs);
            return;
        }
        if values.len() > 1 {
            errs.push(format!("{at} has {} values, want one", values.len()));
        }
        self.check_text(schema, &values[0], at, errs);
    }

    /// A text value read as the schema's type (Go's validator parses parameters by their schema).
    fn check_text(&self, schema: &Value, text: &str, at: &str, errs: &mut Vec<String>) {
        let s = self.resolve(schema);
        if let Some(Value::Str(t)) = s.get("type") {
            let v = match t.as_str() {
                "integer" => match text.parse::<i64>() {
                    Ok(i) => Json::Int(i),
                    Err(_) => return errs.push(format!("{at}: {text:?} is not an integer")),
                },
                "number" => match text.parse::<f64>() {
                    Ok(f) if !text.is_empty() => Json::Float(f),
                    _ => return errs.push(format!("{at}: {text:?} is not a number")),
                },
                "boolean" => match text {
                    "true" => Json::Bool(true),
                    "false" => Json::Bool(false),
                    _ => return errs.push(format!("{at}: {text:?} is not a boolean")),
                },
                _ => Json::Str(text.to_string()),
            };
            return self.check(s, &v, at, errs);
        }
        let alternatives = |key: &str| seq(s.get(key));
        if !alternatives("oneOf").is_empty() || !alternatives("anyOf").is_empty() {
            let one_of = !alternatives("oneOf").is_empty();
            let alts = if one_of {
                alternatives("oneOf")
            } else {
                alternatives("anyOf")
            };
            let ok = alts
                .iter()
                .filter(|a| {
                    let mut e = Vec::new();
                    self.check_text(a, text, at, &mut e);
                    e.is_empty()
                })
                .count();
            if (one_of && ok != 1) || ok == 0 {
                errs.push(format!(
                    "{at}: {text:?} matches {ok} of the {} alternatives",
                    alts.len()
                ));
            }
            return;
        }
        self.check(s, &Json::Str(text.to_string()), at, errs);
    }

    fn check_len(&self, s: &Value, n: i64, at: &str, errs: &mut Vec<String>) {
        if let Some(Value::Int(min)) = s.get("minItems")
            && n < *min
        {
            errs.push(format!("{at}: {n} items, want at least {min}"));
        }
        if let Some(Value::Int(max)) = s.get("maxItems")
            && n > *max
        {
            errs.push(format!("{at}: {n} items, want at most {max}"));
        }
    }

    /// Checks a JSON value against a schema.
    pub fn check(&self, schema: &Value, v: &Json, at: &str, errs: &mut Vec<String>) {
        let s = self.resolve(schema);
        if matches!(v, Json::Null) && matches!(s.get("nullable"), Some(Value::Bool(true))) {
            return;
        }
        if let Some(t) = s.get("type") {
            let types: Vec<&str> = match t {
                Value::Str(t) => vec![t.as_str()],
                Value::Seq(ts) => ts.iter().filter_map(Value::as_str).collect(),
                _ => panic!("unsupported type {t:?}"),
            };
            if !types.iter().any(|t| type_matches(t, v)) {
                errs.push(format!(
                    "{at}: {} is not of type {}",
                    short(v),
                    types.join("|")
                ));
                return;
            }
        }
        let en = seq(s.get("enum"));
        if !en.is_empty() && !en.iter().any(|e| enum_matches(e, v)) {
            errs.push(format!(
                "{at}: {} is not one of the enum values {en:?}",
                short(v)
            ));
        }
        if let Json::Str(text) = v {
            match s.get("format").and_then(Value::as_str) {
                Some("date-time") if !is_date_time(text) => {
                    errs.push(format!("{at}: {text:?} is not a date-time"))
                }
                Some("duration") if !is_duration(text) => {
                    errs.push(format!("{at}: {text:?} is not a duration"))
                }
                _ => {}
            }
        }
        if let Json::Obj(fields) = v {
            for r in seq(s.get("required")) {
                let r = r.as_str().unwrap_or_default();
                if !fields.iter().any(|(k, _)| k == r) {
                    errs.push(format!("{at}: missing required property {r:?}"));
                }
            }
            let props = s.get("properties");
            for (k, fv) in fields {
                let fat = format!("{at}.{k}");
                if let Some(ps) = props.and_then(|p| p.get(k)) {
                    self.check(ps, fv, &fat, errs);
                    continue;
                }
                match s.get("additionalProperties") {
                    Some(Value::Bool(false)) => {
                        errs.push(format!("{at}: property {k:?} is not allowed"))
                    }
                    Some(ap @ Value::Map(_)) => self.check(ap, fv, &fat, errs),
                    _ => {}
                }
            }
        }
        if let Json::Arr(items) = v {
            self.check_len(s, items.len() as i64, at, errs);
            if let Some(is) = s.get("items") {
                for (i, it) in items.iter().enumerate() {
                    self.check(is, it, &format!("{at}[{i}]"), errs);
                }
            }
        }
        for a in seq(s.get("allOf")) {
            self.check(a, v, at, errs);
        }
        let matching = |alts: &[Value]| {
            alts.iter()
                .filter(|a| {
                    let mut e = Vec::new();
                    self.check(a, v, at, &mut e);
                    e.is_empty()
                })
                .count()
        };
        let one_of = seq(s.get("oneOf"));
        if !one_of.is_empty() {
            let n = matching(one_of);
            if n != 1 {
                errs.push(format!(
                    "{at}: {} matches {n} of the oneOf schemas, want exactly 1",
                    short(v)
                ));
            }
        }
        let any_of = seq(s.get("anyOf"));
        if !any_of.is_empty() && matching(any_of) == 0 {
            errs.push(format!(
                "{at}: {} matches none of the anyOf schemas",
                short(v)
            ));
        }
    }
}

fn type_matches(t: &str, v: &Json) -> bool {
    match (t, v) {
        ("object", Json::Obj(_))
        | ("array", Json::Arr(_))
        | ("string", Json::Str(_))
        | ("boolean", Json::Bool(_))
        | ("null", Json::Null) => true,
        ("number", Json::Int(_) | Json::Float(_)) => true,
        ("integer", Json::Int(_)) => true,
        ("integer", Json::Float(f)) => f.fract() == 0.0,
        _ => false,
    }
}

fn enum_matches(e: &Value, v: &Json) -> bool {
    match (e, v) {
        (Value::Str(a), Json::Str(b)) => a == b,
        (Value::Int(a), Json::Int(b)) => a == b,
        (Value::Bool(a), Json::Bool(b)) => a == b,
        _ => false,
    }
}

/// A short text of a value for error messages.
fn short(v: &Json) -> String {
    let mut s = String::new();
    v.write(&mut s);
    if s.len() > 120 {
        let mut end = 120;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push_str("...");
    }
    s
}

impl Response {
    /// Go `ValidateOpenAPI`: the request and the response against both the OpenAPI 3.1 and 3.2 specs.
    pub fn validate_openapi(&self) -> &Self {
        for spec in specs() {
            // Some endpoints (like /notifications/live) only exist in 3.2, not 3.1.
            let skip = spec.version == "3.1" && self.path.contains("/notifications/live");
            if skip {
                continue;
            }
            let errs = spec.validate_request(
                &self.method,
                &self.path,
                &self.query,
                self.request_header("Content-Type"),
                &self.request_body,
            );
            assert!(
                errs.is_empty(),
                "OpenAPI {} request validation failed: request to {} {} failed OpenAPI {} validation:\n{:?}",
                spec.version,
                self.method,
                self.path,
                spec.version,
                errs
            );
            let errs = spec.validate_response(
                &self.method,
                &self.path,
                self.status_code,
                self.response_header("Content-Type"),
                &self.body,
            );
            assert!(
                errs.is_empty(),
                "OpenAPI {} response validation failed: response from {} {} (status {}) failed OpenAPI {} validation:\n{:?}\nbody: {}",
                spec.version,
                self.method,
                self.path,
                self.status_code,
                spec.version,
                errs,
                self.body
            );
        }
        self
    }
}
