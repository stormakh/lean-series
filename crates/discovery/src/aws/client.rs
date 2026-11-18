//! A small AWS API client for the AWS SD roles (what the aws-sdk-go-v2 service clients do for the calls
//! discovery makes): region resolution (Go `loadRegion`), the credential chain (see `credentials`), endpoint
//! resolution (the SD `endpoint` as `BaseEndpoint`, `AWS_ENDPOINT_URL_<SERVICE>`, `AWS_ENDPOINT_URL`, the
//! profile's `endpoint_url`, else `https://<prefix>.<region>.amazonaws.com`), SigV4 signing, the standard retryer
//! (3 attempts, exponential jitter backoff, retry on 5xx, throttling and transport errors), and the wire protocols:
//! EC2 Query and awsQuery (form POST, XML response), awsJson1.1 (ECS, Lightsail) and restJson1 (MSK/Kafka).
//!
//! Errors read as the SDK's: `operation error EC2: DescribeInstances, https response error StatusCode: 400,
//! RequestID: ..., api error Code: message`.
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use json::Json;
use net::sigv4::{EnvFn, sign_request};
use net::{HttpClientConfig, HttpClientOptions};
use yaml::Secret;

use super::credentials::{self, Endpoints, Provider, SharedConfig};
use super::xml;
use crate::cancel::CancelToken;

/// The SDK user agent (aws-sdk-go-v2 sets its own, Prometheus does not add one).
pub const USER_AGENT: &str = "aws-sdk-go-v2/1.47.3 os/linux lang/rust prometheus-rs";

/// Protocol and naming facts of an AWS service.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServiceInfo {
    /// The SDK ServiceID used in errors ("EC2", "ECS", "ElastiCache", "Lightsail", "Kafka", "RDS").
    pub service_id: &'static str,
    /// The SigV4 signing name.
    pub signing_name: &'static str,
    /// The endpoint host prefix.
    pub endpoint_prefix: &'static str,
    /// The service part of `AWS_ENDPOINT_URL_<SERVICE>`.
    pub env_name: &'static str,
    /// Query protocols: the API version sent as `Version`.
    pub api_version: &'static str,
    /// awsJson1.1: the `X-Amz-Target` prefix.
    pub target_prefix: &'static str,
}

pub const EC2: ServiceInfo = ServiceInfo { service_id: "EC2", signing_name: "ec2", endpoint_prefix: "ec2", env_name: "EC2", api_version: "2016-11-15", target_prefix: "" };
pub const ECS: ServiceInfo = ServiceInfo { service_id: "ECS", signing_name: "ecs", endpoint_prefix: "ecs", env_name: "ECS", api_version: "2014-11-13", target_prefix: "AmazonEC2ContainerServiceV20141113" };
pub const ELASTICACHE: ServiceInfo = ServiceInfo { service_id: "ElastiCache", signing_name: "elasticache", endpoint_prefix: "elasticache", env_name: "ELASTICACHE", api_version: "2015-02-02", target_prefix: "" };
pub const LIGHTSAIL: ServiceInfo = ServiceInfo { service_id: "Lightsail", signing_name: "lightsail", endpoint_prefix: "lightsail", env_name: "LIGHTSAIL", api_version: "2016-11-28", target_prefix: "Lightsail_20161128" };
pub const KAFKA: ServiceInfo = ServiceInfo { service_id: "Kafka", signing_name: "kafka", endpoint_prefix: "kafka", env_name: "KAFKA", api_version: "2018-11-14", target_prefix: "" };
pub const RDS: ServiceInfo = ServiceInfo { service_id: "RDS", signing_name: "rds", endpoint_prefix: "rds", env_name: "RDS", api_version: "2014-10-31", target_prefix: "" };

/// The DNS suffix of a region's partition.
pub fn dns_suffix(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else if region.starts_with("us-iso-") {
        "c2s.ic.gov"
    } else if region.starts_with("us-isob-") {
        "sc2s.sgov.gov"
    } else if region.starts_with("eusc-") {
        "amazonaws.eu"
    } else {
        "amazonaws.com"
    }
}

/// The connection settings of an AWS SD config (the fields every role shares).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClientConfig {
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: Secret,
    pub profile: String,
    pub role_arn: String,
    pub external_id: String,
    pub http_client_config: HttpClientConfig,
}

fn default_client() -> net::Client {
    net::Client::new(Arc::new(net::Transport::default_transport()))
}

/// Go `loadRegion`: the configured region, else the AWS config (AWS_REGION, AWS_DEFAULT_REGION, the profile's
/// `region`), else the EC2 instance metadata.
pub fn load_region(specified: &str, env: &EnvFn) -> Result<String, String> {
    if !specified.is_empty() {
        return Ok(specified.to_string());
    }
    let wrap = |e: String| format!("could not determine AWS region: {e}");
    if let Some(r) = env("AWS_REGION").or_else(|| env("AWS_DEFAULT_REGION")) {
        return Ok(r);
    }
    let shared = SharedConfig::load(env);
    let profile = credentials::env_profile(env);
    if profile != "default" && !shared.has_profile(&profile) {
        return Err(wrap(format!("failed to load AWS config: failed to get shared config profile, {profile}")));
    }
    if let Some(r) = shared.config.get(&profile).and_then(|s| s.get("region")).filter(|r| !r.is_empty()) {
        return Ok(r.clone());
    }
    let ep = Endpoints { http: default_client(), env: env.clone() };
    let r = credentials::imds_get(&ep, "GetRegion", "/latest/meta-data/placement/region").map_err(|e| wrap(format!("failed to get region from IMDS: {e}")))?;
    if r.trim().is_empty() {
        return Err(wrap("region not found in AWS config or IMDS".to_string()));
    }
    Ok(r.trim().to_string())
}

/// An API client for one service.
pub struct Client {
    svc: ServiceInfo,
    region: String,
    endpoint: String,
    http: net::Client,
    creds: Provider,
}

/// An error of one attempt: the text after "operation error <svc>: <op>, " and whether it can be retried.
struct AttemptError {
    msg: String,
    retryable: bool,
}

const THROTTLE_CODES: [&str; 14] = [
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "ProvisionedThroughputExceededException",
    "TransactionInProgressException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
    "EC2ThrottledException",
];
const TRANSIENT_CODES: [&str; 2] = ["RequestTimeout", "RequestTimeoutException"];
const MAX_ATTEMPTS: u32 = 3;

/// The error code without a namespace or a trailing description (aws-sdk-go-v2 `SanitizeErrorCode`).
fn sanitize_code(c: &str) -> &str {
    let c = c.split(':').next().unwrap_or(c);
    c.rsplit('#').next().unwrap_or(c)
}

impl Client {
    /// Builds a client like the SD's `xxxClient` methods: the HTTP client from the config, the region
    /// (`loadRegion`), the credential chain ("could not create aws config: ..." on a bad profile) and the
    /// role to assume.
    pub fn new(cfg: &ClientConfig, svc: ServiceInfo, client_name: &str) -> Result<Client, String> {
        Client::with_env(cfg, svc, client_name, credentials::std_env())
    }

    /// `new` with an environment lookup (tests).
    pub fn with_env(cfg: &ClientConfig, svc: ServiceInfo, _client_name: &str, env: EnvFn) -> Result<Client, String> {
        let http = net::new_client_from_config(&cfg.http_client_config, &HttpClientOptions::default()).map_err(|e| e.msg)?;
        let region = load_region(&cfg.region, &env)?;
        let shared = SharedConfig::load(&env);
        let mut source = credentials::resolve_chain(&cfg.access_key, &cfg.secret_key.0, &cfg.profile, &region, &shared, &env).map_err(|e| format!("could not create aws config: {e}"))?;
        if !cfg.role_arn.is_empty() {
            source = credentials::Source::AssumeRole { base: Box::new(source), role_arn: cfg.role_arn.clone(), external_id: cfg.external_id.clone(), session_name: String::new(), region: region.clone() };
        }
        let endpoint = if !cfg.endpoint.is_empty() {
            cfg.endpoint.clone()
        } else {
            let profile = if cfg.profile.is_empty() { credentials::env_profile(&env) } else { cfg.profile.clone() };
            let ignore = env("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS").is_some_and(|v| v.eq_ignore_ascii_case("true")) || shared.get(&profile, "ignore_configured_endpoint_urls").is_some_and(|v| v.eq_ignore_ascii_case("true"));
            let configured = if ignore { None } else { env(&format!("AWS_ENDPOINT_URL_{}", svc.env_name)).or_else(|| env("AWS_ENDPOINT_URL")).or_else(|| shared.get(&profile, "endpoint_url")) };
            configured.unwrap_or_else(|| format!("https://{}.{}.{}", svc.endpoint_prefix, region, dns_suffix(&region)))
        };
        let creds = Provider::new(source, Endpoints { http: http.clone(), env });
        Ok(Client { svc, region, endpoint, http, creds })
    }

    /// The resolved region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// The base endpoint the client sends to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Sends one operation with retries and returns the successful response.
    fn call(&self, ctx: &CancelToken, op: &str, build: &dyn Fn() -> Result<net::Request, String>, parse_err: &dyn Fn(&net::Response) -> (String, String, String)) -> Result<net::Response, String> {
        let prefix = format!("operation error {}: {}, ", self.svc.service_id, op);
        let mut attempt = 0;
        loop {
            attempt += 1;
            if ctx.is_cancelled() {
                return Err(format!("{prefix}https response error StatusCode: 0, RequestID: , canceled, context canceled"));
            }
            let creds = self.creds.retrieve().map_err(|e| format!("{prefix}get identity: get credentials: {e}"))?;
            let mut req = build().map_err(|e| format!("{prefix}{e}"))?;
            req.header.set("User-Agent", &format!("{USER_AGENT} api/{}", self.svc.signing_name));
            req.header.set("Amz-Sdk-Request", &format!("attempt={attempt}; max={MAX_ATTEMPTS}"));
            let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
            sign_request(&mut req, &creds, self.svc.signing_name, &self.region, now);
            let err = match self.http.do_request(req) {
                Ok(resp) if resp.status / 100 == 2 => return Ok(resp),
                Ok(resp) => {
                    let (code, msg, mut rid) = parse_err(&resp);
                    if rid.is_empty() {
                        rid = resp.header.get("X-Amzn-Requestid").or_else(|| resp.header.get("X-Amz-Request-Id")).unwrap_or("").to_string();
                    }
                    let code = sanitize_code(&code).to_string();
                    let retryable = matches!(resp.status, 500 | 502 | 503 | 504) || THROTTLE_CODES.contains(&code.as_str()) || TRANSIENT_CODES.contains(&code.as_str());
                    let api = if code.is_empty() { format!("api error {}: {}", status_code_text(resp.status), msg) } else { format!("api error {code}: {msg}") };
                    AttemptError { msg: format!("https response error StatusCode: {}, RequestID: {rid}, {api}", resp.status), retryable }
                }
                Err(e) => AttemptError { msg: format!("https response error StatusCode: 0, RequestID: , request send failed, {}", e.msg), retryable: true },
            };
            if !err.retryable {
                return Err(format!("{prefix}{}", err.msg));
            }
            if attempt >= MAX_ATTEMPTS {
                return Err(format!("{prefix}exceeded maximum number of attempts, {MAX_ATTEMPTS}, {}", err.msg));
            }
            // ExponentialJitterBackoff: rand[0,1) * 2^attempt seconds, at most 20s
            let mut r = [0u8; 8];
            crypto::random_bytes(&mut r);
            let frac = (u64::from_le_bytes(r) >> 11) as f64 / (1u64 << 53) as f64;
            let backoff = Duration::from_secs_f64((frac * (1u64 << attempt) as f64).min(20.0));
            if ctx.wait_timeout(backoff) {
                return Err(format!("{prefix}https response error StatusCode: 0, RequestID: , canceled, context canceled"));
            }
        }
    }

    fn url(&self, path: &str, query: &[(String, String)]) -> String {
        let base = self.endpoint.trim_end_matches('/');
        let mut u = format!("{base}{}", if path.is_empty() { "/" } else { path });
        if !query.is_empty() {
            u.push('?');
            u.push_str(&yaml::url::encode_query(query).replace('+', "%20"));
        }
        u
    }

    /// An EC2 Query / awsQuery call: POSTs `Action=<op>&Version=<api version>&<params>` and returns the root
    /// element of the response (for example `DescribeInstancesResponse`; awsQuery results are in its
    /// `<op>Result` child).
    pub fn query(&self, ctx: &CancelToken, op: &str, params: &[(String, String)]) -> Result<xml::Element, String> {
        let mut form: Vec<(String, String)> = vec![("Action".to_string(), op.to_string()), ("Version".to_string(), self.svc.api_version.to_string())];
        form.extend(params.iter().cloned());
        let body = yaml::url::encode_query(&form);
        let url = self.url("/", &[]);
        let build = || -> Result<net::Request, String> {
            let mut r = net::Request::new("POST", &url, body.clone().into_bytes()).map_err(|e| e.msg)?;
            r.header.set("Content-Type", "application/x-www-form-urlencoded");
            Ok(r)
        };
        let parse_err = |resp: &net::Response| -> (String, String, String) {
            match xml::parse(&String::from_utf8_lossy(&resp.body)) {
                Ok(root) => {
                    let e = root.path("Errors/Error").or_else(|| root.child("Error"));
                    let rid = root.text_at("RequestID").or_else(|| root.text_at("RequestId")).unwrap_or("").to_string();
                    match e {
                        Some(e) => (e.text_at("Code").unwrap_or("").to_string(), e.text_at("Message").unwrap_or("").to_string(), rid),
                        None => (String::new(), String::new(), rid),
                    }
                }
                Err(_) => (String::new(), String::new(), String::new()),
            }
        };
        let resp = self.call(ctx, op, &build, &parse_err)?;
        let text = String::from_utf8_lossy(&resp.body);
        // smithy FetchRootElement: a body without a root element (io.EOF) is an empty output, not an error
        if !text.contains('<') || text.trim_start().strip_prefix("<?").and_then(|r| r.split_once("?>")).is_some_and(|(_, rest)| !rest.contains('<')) {
            return Ok(xml::Element::default());
        }
        xml::parse(&text).map_err(|e| format!("operation error {}: {}, deserialization failed, failed to decode response body, {e}", self.svc.service_id, op))
    }

    /// An awsJson1.1 call: POSTs `body` with `X-Amz-Target: <prefix>.<op>` and returns the response object.
    pub fn json_rpc(&self, ctx: &CancelToken, op: &str, body: &Json) -> Result<Json, String> {
        let url = self.url("/", &[]);
        let payload = body.stringify();
        let target = format!("{}.{}", self.svc.target_prefix, op);
        let build = || -> Result<net::Request, String> {
            let mut r = net::Request::new("POST", &url, payload.clone().into_bytes()).map_err(|e| e.msg)?;
            r.header.set("Content-Type", "application/x-amz-json-1.1");
            r.header.set("X-Amz-Target", &target);
            Ok(r)
        };
        let resp = self.call(ctx, op, &build, &json_error)?;
        decode_json(&resp.body).map_err(|e| format!("operation error {}: {}, deserialization failed, failed to decode response body, {e}", self.svc.service_id, op))
    }

    /// A restJson1 call: `method` on `path` (already escaped) with query parameters and an optional JSON body.
    pub fn rest_json(&self, ctx: &CancelToken, op: &str, method: &str, path: &str, query: &[(String, String)], body: Option<&Json>) -> Result<Json, String> {
        let url = self.url(path, query);
        let payload = body.map(|b| b.stringify());
        let build = || -> Result<net::Request, String> {
            let mut r = net::Request::new(method, &url, payload.clone().unwrap_or_default().into_bytes()).map_err(|e| e.msg)?;
            if payload.is_some() {
                r.header.set("Content-Type", "application/json");
            }
            Ok(r)
        };
        let resp = self.call(ctx, op, &build, &json_error)?;
        decode_json(&resp.body).map_err(|e| format!("operation error {}: {}, deserialization failed, failed to decode response body, {e}", self.svc.service_id, op))
    }
}

fn decode_json(b: &[u8]) -> Result<Json, String> {
    if b.iter().all(|c| c.is_ascii_whitespace()) {
        return Ok(Json::obj());
    }
    Json::parse_bytes(b).map_err(|e| e.to_string())
}

/// The code, message and request id of an awsJson / restJson error response.
fn json_error(resp: &net::Response) -> (String, String, String) {
    let j = Json::parse_bytes(&resp.body).unwrap_or(Json::Null);
    let mut code = resp.header.get("X-Amzn-Errortype").unwrap_or("").to_string();
    if code.is_empty() {
        code = j.get("__type").or_else(|| j.get("code")).or_else(|| j.get("Code")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    }
    let msg = j.get("message").or_else(|| j.get("Message")).or_else(|| j.get("errorMessage")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    (code, msg, String::new())
}

fn status_code_text(c: u16) -> String {
    match c {
        400 => "BadRequest",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "NotFound",
        500 => "InternalServerError",
        502 => "BadGateway",
        503 => "ServiceUnavailable",
        504 => "GatewayTimeout",
        _ => "UnknownError",
    }
    .to_string()
}
