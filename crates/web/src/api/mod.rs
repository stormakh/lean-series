//! Port of `web/api/v1`: the HTTP API.
//!
//! Build an [`Api`] from [`ApiOptions`] (Go `NewAPI`), then serve it with [`Api::router`] (a [`crate::route::Router`]
//! with the `/api/v1` routes at a prefix) or [`Api::serve`]. Each endpoint is a method `fn(&Api, &ApiRequest) ->
//! ApiFuncResult` (Go `apiFunc`); the wrapper adds CORS, the TSDB-not-ready mapping, the ready check, compression,
//! and writes the JSON response straight into the body buffer (no JSON tree).

pub mod codec;
pub mod jsonutil;
pub mod openapi;
pub mod params;
pub mod retrievers;
pub mod search;
pub mod search_filters;
pub mod translate_ast;
pub mod types;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::{Request as HttpRequest, Response as HttpResponse};
use model::labels::{Builder, Labels, Matcher};
use promql::parser::{Parser, ValueType};
use promql::{PromqlError, QueryOpts, QueryResult};
use storage::{Annotations, Context, Error, SampleAndChunkQueryable, ExemplarQueryable};

use yaml::url::GoUrl;
use crate::gotime::{GoDuration, Time, duration_seconds, duration_string, go_quote};
use crate::httputil::CorsOrigin;
use crate::metrics::Gatherer;
use crate::notifications::{Notification, Subscription};
use crate::route::{HandlerFn, Router};

pub use codec::{Codec, MimeType};
pub use params::{ApiRequest, max_time, min_time, parse_duration, parse_time, parse_time_param};
pub use retrievers::*;
pub use types::*;

/// Non-standard status code (nginx) for a client that closed the connection.
pub const STATUS_CLIENT_CLOSED_CONNECTION: u16 = 499;
/// Tight loops check the context every this many iterations.
pub const CHECK_CONTEXT_EVERY_N_ITERATIONS: usize = 128;

/// Go `errorNum`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorNum {
    None,
    Timeout,
    Canceled,
    Exec,
    BadData,
    Internal,
    Unavailable,
    NotFound,
    NotAcceptable,
    CostLimit,
}

/// Go `errorType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ErrorType {
    pub num: ErrorNum,
    pub str: &'static str,
}

pub const ERROR_NONE: ErrorType = ErrorType { num: ErrorNum::None, str: "" };
pub const ERROR_TIMEOUT: ErrorType = ErrorType { num: ErrorNum::Timeout, str: "timeout" };
pub const ERROR_CANCELED: ErrorType = ErrorType { num: ErrorNum::Canceled, str: "canceled" };
pub const ERROR_EXEC: ErrorType = ErrorType { num: ErrorNum::Exec, str: "execution" };
pub const ERROR_BAD_DATA: ErrorType = ErrorType { num: ErrorNum::BadData, str: "bad_data" };
pub const ERROR_INTERNAL: ErrorType = ErrorType { num: ErrorNum::Internal, str: "internal" };
pub const ERROR_UNAVAILABLE: ErrorType = ErrorType { num: ErrorNum::Unavailable, str: "unavailable" };
pub const ERROR_NOT_FOUND: ErrorType = ErrorType { num: ErrorNum::NotFound, str: "not_found" };
pub const ERROR_NOT_ACCEPTABLE: ErrorType = ErrorType { num: ErrorNum::NotAcceptable, str: "not_acceptable" };
pub const ERROR_COST_LIMIT: ErrorType = ErrorType { num: ErrorNum::CostLimit, str: "cost_limit" };

/// Go `LocalhostRepresentations`.
pub const LOCALHOST_REPRESENTATIONS: [&str; 3] = ["127.0.0.1", "localhost", "::1"];

/// Go `apiError`.
#[derive(Clone, Debug)]
pub struct ApiError {
    pub typ: ErrorType,
    pub err: Error,
}

impl ApiError {
    pub fn new(typ: ErrorType, err: Error) -> ApiError {
        ApiError { typ, err }
    }
    pub fn msg(typ: ErrorType, msg: impl Into<String>) -> ApiError {
        ApiError { typ, err: Error::msg(msg.into()) }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.typ.str, self.err)
    }
}

/// Go `apiFuncResult`. The finalizer runs after the response is written.
#[derive(Default)]
pub struct ApiFuncResult {
    pub data: Option<Data>,
    pub err: Option<ApiError>,
    pub warnings: Annotations,
    pub finalizer: Option<Box<dyn FnOnce() + Send>>,
}

impl fmt::Debug for ApiFuncResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiFuncResult").field("data", &self.data).field("err", &self.err).field("warnings", &self.warnings.len()).finish()
    }
}

impl ApiFuncResult {
    pub fn ok(data: Data) -> ApiFuncResult {
        ApiFuncResult { data: Some(data), ..Default::default() }
    }
    pub fn error(err: ApiError) -> ApiFuncResult {
        ApiFuncResult { err: Some(err), ..Default::default() }
    }
    pub fn none() -> ApiFuncResult {
        ApiFuncResult::default()
    }
    fn with_warnings(mut self, w: Annotations) -> ApiFuncResult {
        self.warnings = w;
        self
    }
}

/// Go `invalidParamError`.
pub fn invalid_param_error(err: impl fmt::Display, parameter: &str) -> ApiFuncResult {
    ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, format!("invalid parameter {}: {}", go_quote(parameter), err)))
}


/// Go `Response`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Response {
    pub status: String,
    pub data: Option<Data>,
    pub error_type: String,
    pub error: String,
    pub warnings: Vec<String>,
    pub infos: Vec<String>,
}

impl Response {
    /// Go `json.Marshal(Response)` (with `omitempty`).
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"status\":");
        json::write_str(&self.status, out);
        if let Some(d) = &self.data {
            out.push_str(",\"data\":");
            d.write_json(out);
        }
        if !self.error_type.is_empty() {
            out.push_str(",\"errorType\":");
            json::write_str(&self.error_type, out);
        }
        if !self.error.is_empty() {
            out.push_str(",\"error\":");
            json::write_str(&self.error, out);
        }
        if !self.warnings.is_empty() {
            out.push_str(",\"warnings\":");
            jsonutil::write_strings(&self.warnings, out);
        }
        if !self.infos.is_empty() {
            out.push_str(",\"infos\":");
            jsonutil::write_strings(&self.infos, out);
        }
        out.push('}');
    }
}

/// Go `JSONCodec`.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn content_type(&self) -> MimeType {
        MimeType::new("application", "json")
    }
    fn can_encode(&self, _resp: &Response) -> bool {
        true
    }
    fn encode(&self, resp: &Response) -> Result<Vec<u8>, String> {
        let mut s = String::with_capacity(256);
        resp.write_json(&mut s);
        Ok(s.into_bytes())
    }
}

/// Go `GlobalURLOptions`.
#[derive(Clone, Debug, Default)]
pub struct GlobalUrlOptions {
    pub listen_address: String,
    pub host: String,
    pub scheme: String,
}

/// Go `StatsRenderer`: renders the statistics for the `stats` parameter; `None` leaves them out.
pub type StatsRenderer = Arc<dyn Fn(&Context, Option<&util::stats::Statistics>, &str) -> Option<QueryStats> + Send + Sync>;

/// Go `DefaultStatsRenderer`.
pub fn default_stats_renderer(_ctx: &Context, s: Option<&util::stats::Statistics>, param: &str) -> Option<QueryStats> {
    if param.is_empty() {
        return None;
    }
    // Go: stats.NewQueryStats(nil) dereferences nil; a query always has statistics.
    let s = s?;
    Some(QueryStats::Builtin(util::stats::new_query_stats(s)))
}

/// Go `OverrideErrorCode`.
pub type OverrideErrorCode = Arc<dyn Fn(ErrorNum, &Error) -> Option<u16> + Send + Sync>;

/// Go's `ready` wrapper: `Some(response)` when the server is not ready (the handler is not called).
pub type ReadyFn = Arc<dyn Fn() -> Option<HttpResponse> + Send + Sync>;

/// A plain HTTP handler (remote read, remote write and OTLP receivers).
pub type HttpHandler = Arc<dyn Fn(HttpRequest) -> HttpResponse + Send + Sync>;

/// Go `notificationsSub`.
pub type NotificationsSub = Arc<dyn Fn() -> Option<Subscription> + Send + Sync>;

/// Everything Go's `NewAPI` takes. Unset retrievers make their endpoints fail as Go's nil interfaces would
/// (with an internal error instead of a panic).
#[derive(Clone)]
pub struct ApiOptions {
    pub query_engine: Option<Arc<dyn QueryEngine>>,
    pub queryable: Option<Arc<dyn SampleAndChunkQueryable>>,
    pub exemplar_queryable: Option<Arc<dyn ExemplarQueryable>>,
    pub scrape_pools_retriever: Option<Arc<dyn ScrapePoolsRetriever>>,
    pub target_retriever: Option<Arc<dyn TargetRetriever>>,
    pub alertmanager_retriever: Option<Arc<dyn AlertmanagerRetriever>>,
    pub rules_retriever: Option<Arc<dyn RulesRetriever>>,
    pub config: Option<Arc<dyn Fn() -> Arc<config::Config> + Send + Sync>>,
    pub flags: BTreeMap<String, String>,
    pub global_url_options: GlobalUrlOptions,
    pub ready: Option<ReadyFn>,
    pub db: Option<Arc<dyn TsdbAdminStats>>,
    pub db_dir: String,
    pub enable_admin: bool,
    pub enable_search: bool,
    pub max_search_limit: i64,
    pub is_agent: bool,
    pub cors_origin: Option<CorsOrigin>,
    pub runtime_info: Option<Arc<dyn Fn() -> Result<RuntimeInfo, Error> + Send + Sync>>,
    pub build_info: PrometheusVersion,
    pub notifications_getter: Option<Arc<dyn Fn() -> Vec<Notification> + Send + Sync>>,
    pub notifications_sub: Option<NotificationsSub>,
    pub gatherer: Option<Arc<dyn Gatherer>>,
    pub stats_renderer: Option<StatsRenderer>,
    pub remote_read_handler: Option<HttpHandler>,
    pub remote_write_handler: Option<HttpHandler>,
    pub otlp_write_handler: Option<HttpHandler>,
    /// The engine's lookback delta (`--query.lookback-delta`).
    pub lookback_delta: GoDuration,
    pub override_error_code: Option<OverrideErrorCode>,
    pub feature_registry: Option<Arc<dyn promql::features::Collector + Send + Sync>>,
    pub openapi_options: openapi::OpenApiOptions,
    pub parser: Option<Arc<Parser>>,
    pub enable_query_cost: bool,
    /// `--query.timeout`.
    pub query_timeout_flag: GoDuration,
    /// The cost sampling view of the queryable (Go's `q.(storage.CostSamplingQueryable)`), if it has one.
    pub cost_sampling: Option<Arc<dyn storage::CostSamplingQueryable + Send + Sync>>,
    /// Overrides the clock (tests).
    pub now: Option<Arc<dyn Fn() -> Time + Send + Sync>>,
}

impl Default for ApiOptions {
    fn default() -> Self {
        ApiOptions {
            query_engine: None,
            queryable: None,
            exemplar_queryable: None,
            scrape_pools_retriever: None,
            target_retriever: None,
            alertmanager_retriever: None,
            rules_retriever: None,
            config: None,
            flags: BTreeMap::new(),
            global_url_options: GlobalUrlOptions::default(),
            ready: None,
            db: None,
            db_dir: String::new(),
            enable_admin: false,
            enable_search: false,
            max_search_limit: 0,
            is_agent: false,
            cors_origin: None,
            runtime_info: None,
            build_info: PrometheusVersion::default(),
            notifications_getter: None,
            notifications_sub: None,
            gatherer: None,
            stats_renderer: None,
            remote_read_handler: None,
            remote_write_handler: None,
            otlp_write_handler: None,
            lookback_delta: 0,
            override_error_code: None,
            feature_registry: None,
            openapi_options: openapi::OpenApiOptions::default(),
            parser: None,
            enable_query_cost: false,
            query_timeout_flag: 0,
            cost_sampling: None,
            now: None,
        }
    }
}

/// The API (Go `API`).
pub struct Api {
    pub o: ApiOptions,
    pub parser: Arc<Parser>,
    codecs: Vec<Arc<dyn Codec>>,
    custom_stats_renderer: bool,
    pub openapi: openapi::OpenApiBuilder,
    pub(crate) meta_cache: search::SearchMetadataCache,
}

/// The result of an endpoint.
pub type ApiFunc = fn(&Api, &ApiRequest) -> ApiFuncResult;

fn agent_unavailable() -> ApiFuncResult {
    ApiFuncResult::error(ApiError::msg(ERROR_EXEC, "unavailable with Prometheus Agent"))
}

/// Go `setUnavailStatusOnTSDBNotReady`.
pub fn set_unavail_status_on_tsdb_not_ready(mut r: ApiFuncResult) -> ApiFuncResult {
    if let Some(e) = &mut r.err {
        if e.err.is(&tsdb::db::ERR_NOT_READY) {
            e.typ = ERROR_UNAVAILABLE;
        }
    }
    r
}

/// Go `returnAPIError`.
pub fn return_api_error(err: Error) -> ApiError {
    if let Some(pe) = PromqlError::find(&err) {
        match pe {
            PromqlError::QueryCanceled(_) => return ApiError::new(ERROR_CANCELED, err),
            PromqlError::QueryTimeout(_) => return ApiError::new(ERROR_TIMEOUT, err),
            PromqlError::TooManySeries(_) | PromqlError::TooManySamplesRead(_) => {
                return ApiError::new(ERROR_COST_LIMIT, err);
            }
            _ => {}
        }
    }
    if err.downcast_ref::<promql::ErrStorage>().is_some() {
        return ApiError::new(ERROR_INTERNAL, err);
    }
    if err.is(&storage::ERR_CANCELED) {
        return ApiError::new(ERROR_CANCELED, err);
    }
    ApiError::new(ERROR_EXEC, err)
}

/// Go `getDefaultErrorCode`.
pub fn default_error_code(t: ErrorType) -> u16 {
    match t.num {
        ErrorNum::BadData => 400,
        ErrorNum::Exec => 422,
        ErrorNum::CostLimit => 422,
        ErrorNum::Canceled => STATUS_CLIENT_CLOSED_CONNECTION,
        ErrorNum::Timeout => 503,
        ErrorNum::Internal => 500,
        ErrorNum::NotFound => 404,
        ErrorNum::NotAcceptable => 406,
        _ => 500,
    }
}

/// Go `getRuleGroupNextToken`.
pub fn get_rule_group_next_token(file: &str, group: &str) -> String {
    let mut s = String::with_capacity(file.len() + group.len() + 1);
    s.push_str(file);
    s.push(';');
    s.push_str(group);
    let h = crypto::sha256(s.as_bytes());
    let mut out = String::with_capacity(64);
    for b in h {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Go `sanitizeSplitHostPort`.
fn sanitize_split_host_port(input: &str) -> Result<(String, String), String> {
    match split_host_port(input) {
        Ok(v) => Ok(v),
        Err(e) if e.ends_with("missing port in address") => match split_host_port(&format!("{}:80", input)) {
            Ok((h, _)) => Ok((h, String::new())),
            Err(_) => Err(e),
        },
        Err(e) => Err(e),
    }
}

/// Go `net.SplitHostPort`.
pub fn split_host_port(hostport: &str) -> Result<(String, String), String> {
    let missing_port = || format!("address {}: missing port in address", hostport);
    let too_many = || format!("address {}: too many colons in address", hostport);
    let Some(i) = hostport.rfind(':') else { return Err(missing_port()) };
    let (host, port);
    if hostport.starts_with('[') {
        let Some(end) = hostport.find(']') else { return Err(format!("address {}: missing ']' in address", hostport)) };
        if end + 1 == hostport.len() {
            return Err(missing_port());
        } else if end + 1 == i {
            // ok
        } else if hostport.as_bytes()[end + 1] == b':' {
            return Err(too_many());
        } else {
            return Err(missing_port());
        }
        host = &hostport[1..end];
        if host.contains('[') || host.contains(']') {
            return Err(format!("address {}: unexpected '[' in address", hostport));
        }
        port = &hostport[i + 1..];
    } else {
        host = &hostport[..i];
        if host.contains(':') {
            return Err(too_many());
        }
        if host.contains('[') || host.contains(']') {
            return Err(format!("address {}: unexpected '[' in address", hostport));
        }
        port = &hostport[i + 1..];
    }
    Ok((host.to_string(), port.to_string()))
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{}]:{}", host, port) } else { format!("{}:{}", host, port) }
}

/// Go `getGlobalURL`.
pub fn get_global_url(u: &GoUrl, opts: &GlobalUrlOptions) -> Result<GoUrl, (GoUrl, String)> {
    let mut u = u.clone();
    let (host, port) = match sanitize_split_host_port(&u.host) {
        Ok(v) => v,
        Err(e) => return Err((u, e)),
    };
    for lhr in LOCALHOST_REPRESENTATIONS {
        if host == lhr {
            let own_port = match split_host_port(&opts.listen_address) {
                Ok((_, p)) => p,
                Err(e) => return Err((u, e)),
            };
            if port == own_port {
                u.scheme = opts.scheme.clone();
                u.host = opts.host.clone();
            } else {
                let h = match sanitize_split_host_port(&opts.host) {
                    Ok((h, _)) => h,
                    Err(e) => return Err((u, e)),
                };
                u.host = h;
                if !port.is_empty() {
                    u.host = join_host_port(&u.host, &port);
                }
            }
            break;
        }
    }
    Ok(u)
}

/// Go `truncateResults`.
fn truncate_results(res: &mut QueryResult, limit: usize) -> bool {
    match &mut res.value {
        Some(promql::Value::Matrix(m)) if m.len() > limit => {
            m.truncate(limit);
            true
        }
        Some(promql::Value::Vector(v)) if v.len() > limit => {
            v.truncate(limit);
            true
        }
        _ => false,
    }
}

fn instant_after(ctx_deadline: Time, now: Time) -> Instant {
    let d = ctx_deadline.sub(&now);
    let base = Instant::now();
    if d <= 0 { base } else { base + Duration::from_nanos(d as u64) }
}

const STATS_TRUE: &str = "true";
const STATS_ALL: &str = "all";

struct PendingCost {
    estimate: promql::estimate::CostEstimate,
    annos: Annotations,
    err: Option<Error>,
}

impl PendingCost {
    fn finish(self, stats: Option<util::stats::Statistics>, warnings: &mut Annotations) -> Option<QueryCostComparison> {
        warnings.merge(self.annos);
        if let Some(e) = self.err {
            warnings.add(Error::wrap("cost estimation failed", e));
            return None;
        }
        let mut c = QueryCostComparison {
            estimated: CostEstimate { series_touched: self.estimate.series_touched, samples_read: self.estimate.samples_read, peak_samples: 0 },
            actual: CostEstimate::default(),
        };
        if let Some(s) = stats {
            if let Some(sm) = &s.samples {
                c.actual = CostEstimate { series_touched: sm.total_series, samples_read: sm.samples_read, peak_samples: sm.peak_samples as i64 };
            }
        }
        Some(c)
    }
}

impl Api {
    /// Go `NewAPI`.
    pub fn new(o: ApiOptions) -> Api {
        let parser = o.parser.clone().unwrap_or_else(|| Arc::new(Parser::new(Default::default())));
        let custom = o.stats_renderer.is_some();
        let openapi = openapi::OpenApiBuilder::new(o.openapi_options.clone());
        Api {
            o,
            parser,
            codecs: vec![Arc::new(JsonCodec)],
            custom_stats_renderer: custom,
            openapi,
            meta_cache: search::SearchMetadataCache::default(),
        }
    }

    /// Go `InstallCodec`.
    pub fn install_codec(&mut self, c: Arc<dyn Codec>) {
        self.codecs.push(c);
    }

    /// Go `ClearCodecs`.
    pub fn clear_codecs(&mut self) {
        self.codecs.clear();
    }

    pub fn now(&self) -> Time {
        match &self.o.now {
            Some(f) => f(),
            None => Time::now(),
        }
    }

    fn queryable(&self) -> Result<Arc<dyn SampleAndChunkQueryable>, ApiError> {
        self.o.queryable.clone().ok_or_else(|| ApiError::msg(ERROR_INTERNAL, "no queryable configured"))
    }

    fn engine(&self) -> Result<Arc<dyn QueryEngine>, ApiError> {
        self.o.query_engine.clone().ok_or_else(|| ApiError::msg(ERROR_INTERNAL, "no query engine configured"))
    }

    fn config(&self) -> Option<Arc<config::Config>> {
        self.o.config.as_ref().map(|f| f())
    }

    // ------------------------------------------------------------------------------------------------------
    // Routing.

    /// The routes of the API under `prefix` (Go `Register` on `router.WithPrefix(prefix)`).
    pub fn register(self: &Arc<Self>, r: &mut Router, prefix: &str) {
        let p = |s: &str| format!("{}{}", prefix, s);
        let wrap = |f: ApiFunc, agent: bool| -> HandlerFn {
            let api = self.clone();
            Arc::new(move |req: HttpRequest| api.serve_api_func(req, f, agent))
        };
        let raw = |f: fn(&Api, HttpRequest) -> HttpResponse, ready: bool| -> HandlerFn {
            let api = self.clone();
            Arc::new(move |req: HttpRequest| {
                if ready {
                    if let Some(rf) = &api.o.ready {
                        if let Some(resp) = rf() {
                            return resp;
                        }
                    }
                }
                f(&api, req)
            })
        };

        r.options(&p("/*path"), wrap(Api::options, false));

        for (path, f) in [
            ("/query", Api::query as ApiFunc),
            ("/query_range", Api::query_range),
            ("/query_exemplars", Api::query_exemplars),
        ] {
            r.get(&p(path), wrap(f, true));
            r.post(&p(path), wrap(f, true));
        }
        if self.o.enable_query_cost {
            for (path, f) in [("/query_cost", Api::query_cost as ApiFunc), ("/query_range_cost", Api::query_range_cost)] {
                r.get(&p(path), wrap(f, true));
                r.post(&p(path), wrap(f, true));
            }
        }
        for (path, f) in [("/format_query", Api::format_query as ApiFunc), ("/parse_query", Api::parse_query), ("/labels", Api::label_names)] {
            r.get(&p(path), wrap(f, true));
            r.post(&p(path), wrap(f, true));
        }
        r.get(&p("/label/:name/values"), wrap(Api::label_values, true));
        r.get(&p("/series"), wrap(Api::series, true));
        r.post(&p("/series"), wrap(Api::series, true));

        r.get(&p("/scrape_pools"), wrap(Api::scrape_pools, false));
        r.get(&p("/scrape_pools/config"), wrap(Api::scrape_pool_config, false));
        r.get(&p("/targets"), wrap(Api::targets, false));
        r.get(&p("/targets/metadata"), wrap(Api::target_metadata, false));
        r.get(&p("/targets/relabel_steps"), wrap(Api::target_relabel_steps, false));
        r.get(&p("/alertmanagers"), wrap(Api::alertmanagers, true));

        r.get(&p("/metadata"), wrap(Api::metric_metadata, false));

        r.get(&p("/status/config"), wrap(Api::serve_config, false));
        r.get(&p("/status/runtimeinfo"), wrap(Api::serve_runtime_info, false));
        r.get(&p("/status/buildinfo"), wrap(Api::serve_build_info, false));
        r.get(&p("/status/flags"), wrap(Api::serve_flags, false));
        r.get(&p("/status/tsdb"), wrap(Api::serve_tsdb_status, true));
        r.get(&p("/status/tsdb/blocks"), wrap(Api::serve_tsdb_blocks, true));
        r.get(&p("/status/self_metrics"), wrap(Api::self_metrics, false));
        r.get(&p("/features"), wrap(Api::features, false));
        r.get(&p("/status/walreplay"), raw(Api::serve_wal_replay_status, false));
        r.get(&p("/notifications"), raw(Api::notifications, false));
        r.get(&p("/notifications/live"), raw(Api::notifications_sse, false));
        r.post(&p("/read"), raw(Api::remote_read, true));
        r.post(&p("/write"), raw(Api::remote_write, true));
        r.post(&p("/otlp/v1/metrics"), raw(Api::otlp_write, true));

        for (path, f) in [
            ("/search/metric_names", search::search_metric_names as fn(&Api, HttpRequest) -> HttpResponse),
            ("/search/label_names", search::search_label_names),
            ("/search/label_values", search::search_label_values),
        ] {
            r.get(&p(path), raw(f, true));
            r.post(&p(path), raw(f, true));
        }

        r.get(&p("/alerts"), wrap(Api::alerts, true));
        r.get(&p("/rules"), wrap(Api::rules, true));

        for (path, f) in [
            ("/admin/tsdb/delete_series", Api::delete_series as ApiFunc),
            ("/admin/tsdb/clean_tombstones", Api::clean_tombstones),
            ("/admin/tsdb/snapshot", Api::snapshot),
        ] {
            r.post(&p(path), wrap(f, true));
            r.put(&p(path), wrap(f, true));
        }

        r.get(&p("/openapi.yaml"), raw(|api, req| api.openapi.serve_openapi(&req), true));
    }

    /// A router with the API routes at `prefix` (for example "/api/v1").
    pub fn router(self: &Arc<Self>, prefix: &str) -> Router {
        let mut r = Router::new();
        self.register(&mut r, prefix);
        r
    }

    /// Runs an endpoint with Go's `wrap` (and `wrapAgent` when `agent`): ready check, CORS, the TSDB-not-ready
    /// mapping, the response, compression.
    pub fn serve_api_func(&self, req: HttpRequest, f: ApiFunc, agent: bool) -> HttpResponse {
        if let Some(rf) = &self.o.ready {
            if let Some(resp) = rf() {
                return resp;
            }
        }
        let ar = ApiRequest::from_http(&req);
        let result = if agent && self.o.is_agent { agent_unavailable() } else { f(self, &ar) };
        let mut result = set_unavail_status_on_tsdb_not_ready(result);
        let finalizer = result.finalizer.take();
        let mut resp = if let Some(e) = result.err {
            self.respond_error(&e, result.data.as_ref())
        } else if let Some(d) = result.data {
            self.respond(&req, d, &result.warnings, ar.form_value("query"))
        } else {
            HttpResponse::new(204)
        };
        if let Some(fin) = finalizer {
            fin();
        }
        crate::httputil::set_cors(&mut resp, self.o.cors_origin.as_ref(), &req);
        crate::httputil::compress(&mut resp, &req);
        resp
    }

    /// Go `respond`.
    pub fn respond(&self, req: &HttpRequest, data: Data, warnings: &Annotations, query: &str) -> HttpResponse {
        let (warn, info) = warnings.as_strings(query, 10, 10);
        let resp = Response { status: "success".to_string(), data: Some(data), warnings: warn, infos: info, ..Default::default() };
        let codec = match self.negotiate_codec(req, &resp) {
            Ok(c) => c,
            Err(e) => return self.respond_error(&ApiError::msg(ERROR_NOT_ACCEPTABLE, e), None),
        };
        match codec.encode(&resp) {
            Ok(b) => {
                let mut r = HttpResponse::new(200);
                r.set_header("Content-Type", &codec.content_type().to_string());
                r.body = http::Body::Bytes(b);
                r
            }
            Err(e) => crate::route::http_error(&e, 500),
        }
    }

    fn negotiate_codec(&self, req: &HttpRequest, resp: &Response) -> Result<Arc<dyn Codec>, String> {
        for clause in codec::parse_accept(req.header("accept").unwrap_or("")) {
            for c in &self.codecs {
                if c.content_type().satisfies(&clause) && c.can_encode(resp) {
                    return Ok(c.clone());
                }
            }
        }
        let Some(def) = self.codecs.first() else { return Err("no codecs installed".to_string()) };
        if !def.can_encode(resp) {
            return Err(format!("cannot encode response as {}", def.content_type()));
        }
        Ok(def.clone())
    }

    /// Go `respondError`.
    pub fn respond_error(&self, e: &ApiError, data: Option<&Data>) -> HttpResponse {
        let resp = Response {
            status: "error".to_string(),
            error_type: e.typ.str.to_string(),
            error: e.err.to_string(),
            data: data.cloned(),
            ..Default::default()
        };
        let mut s = String::with_capacity(128);
        resp.write_json(&mut s);
        let code = match &self.o.override_error_code {
            Some(f) => f(e.typ.num, &e.err).unwrap_or_else(|| default_error_code(e.typ)),
            None => default_error_code(e.typ),
        };
        HttpResponse::json_text(code, s)
    }

    // ------------------------------------------------------------------------------------------------------
    // Endpoints.

    pub fn options(&self, _r: &ApiRequest) -> ApiFuncResult {
        ApiFuncResult::none()
    }

    /// Go `queryTimeout`.
    fn query_timeout(&self, r: &ApiRequest) -> Result<GoDuration, ApiError> {
        let v = r.form_value("timeout");
        if v.is_empty() {
            return Ok(0);
        }
        let t = parse_duration(v).map_err(|e| ApiError::msg(ERROR_BAD_DATA, format!("invalid parameter {}: {}", go_quote("timeout"), e)))?;
        if t <= 0 {
            return Err(ApiError::msg(
                ERROR_BAD_DATA,
                format!("invalid parameter {}: timeout must be positive, got {}", go_quote("timeout"), duration_string(t)),
            ));
        }
        Ok(t)
    }

    /// Go `queryCostCeilings`.
    fn query_cost_ceilings(&self) -> (u64, u64, GoDuration) {
        if !self.o.enable_query_cost {
            return (0, 0, 0);
        }
        let Some(cfg) = self.config() else { return (0, 0, 0) };
        let g = &cfg.global_config;
        let mut d = g.query_max_duration.0 as GoDuration;
        if d <= 0 {
            d = self.o.query_timeout_flag;
        }
        (g.query_max_series, g.query_max_samples_read, d)
    }

    fn scrape_interval(&self) -> GoDuration {
        self.config().map(|c| c.global_config.scrape_interval.0 as GoDuration).unwrap_or(0)
    }

    fn subquery_default_step(&self) -> GoDuration {
        self.config().map(|c| c.global_config.evaluation_interval.0 as GoDuration).unwrap_or(0)
    }

    fn effective_lookback_delta(&self, opts: Option<&QueryOpts>) -> GoDuration {
        if let Some(o) = opts {
            if !o.lookback_delta.is_zero() {
                return o.lookback_delta.as_nanos() as GoDuration;
            }
        }
        self.o.lookback_delta
    }

    /// Go `extractQueryOpts`.
    pub fn extract_query_opts(&self, r: &ApiRequest) -> Result<QueryOpts, String> {
        let mut duration: GoDuration = 0;
        let s = r.form_value("lookback_delta");
        if !s.is_empty() {
            duration = parse_duration(s).map_err(|e| format!("error parsing lookback delta duration: {}", e))?;
        }
        let mut use_st = None;
        let h = r.header("X-Prometheus-Use-Start-Timestamps");
        if !h.is_empty() {
            use_st = Some(params::parse_bool(h).map_err(|e| format!("error parsing X-Prometheus-Use-Start-Timestamps header: {}", e))?);
        }
        let per_step = r.form_value("stats") == STATS_ALL;
        let lookback = go_duration_to_std(duration);
        if !self.o.enable_query_cost {
            return Ok(QueryOpts::new(per_step, lookback, use_st));
        }
        let (series_ceiling, samples_ceiling, duration_ceiling) = self.query_cost_ceilings();
        let mut max_series = 0u64;
        let v = r.form_value("max_series");
        if !v.is_empty() {
            max_series = parse_uint(v).map_err(|e| format!("error parsing max_series: {}", e))?;
        }
        let mut max_samples_read = 0u64;
        let v = r.form_value("max_samples_read");
        if !v.is_empty() {
            max_samples_read = parse_uint(v).map_err(|e| format!("error parsing max_samples_read: {}", e))?;
        }
        let mut max_query_duration: GoDuration = 0;
        let v = r.form_value("max_query_duration");
        if !v.is_empty() {
            let d = parse_duration(v).map_err(|e| format!("error parsing max_query_duration: {}", e))?;
            if d < 0 {
                return Err(format!("error parsing max_query_duration: must not be negative, got {}", duration_string(d)));
            }
            max_query_duration = d;
        }
        above_ceiling("max_series", max_series, series_ceiling)?;
        above_ceiling("max_samples_read", max_samples_read, samples_ceiling)?;
        if max_query_duration > 0 && duration_ceiling > 0 && max_query_duration > duration_ceiling {
            return Err(PromqlError::LimitAboveCeiling {
                limit: "max_query_duration".to_string(),
                requested: duration_string(max_query_duration),
                ceiling: duration_string(duration_ceiling),
            }
            .to_string());
        }
        Ok(QueryOpts::with_limits(per_step, lookback, max_series, max_samples_read, go_duration_to_std(max_query_duration), use_st))
    }

    /// Go `costRequested`.
    fn cost_requested(&self, r: &ApiRequest) -> Result<bool, String> {
        if !self.o.enable_query_cost {
            return Ok(false);
        }
        let v = r.form_value("cost");
        if v.is_empty() {
            return Ok(false);
        }
        params::parse_bool(v).map_err(|_| "must be a boolean".to_string())
    }

    /// Go `statsParamWarning`.
    fn stats_param_warning(&self, s: &str) -> Option<Error> {
        if self.custom_stats_renderer {
            return None;
        }
        match s {
            "" | STATS_TRUE | STATS_ALL => None,
            _ => Some(Error::msg(format!(
                "value {} for parameter \"stats\" is deprecated and will be rejected in the next major release, use {} or {}",
                go_quote(s),
                go_quote(STATS_TRUE),
                go_quote(STATS_ALL)
            ))),
        }
    }

    fn render_stats(&self, ctx: &Context, s: Option<&util::stats::Statistics>, param: &str) -> Option<QueryStats> {
        match &self.o.stats_renderer {
            Some(f) => f(ctx, s, param),
            None => default_stats_renderer(ctx, s, param),
        }
    }

    /// Go `estimateQueryCost`.
    #[allow(clippy::too_many_arguments)]
    fn estimate_query_cost(
        &self,
        ctx: &Context,
        limit_start: Instant,
        qs: &str,
        start: Time,
        end: Time,
        step: GoDuration,
        opts: Option<&QueryOpts>,
    ) -> (promql::estimate::CostEstimate, Annotations, Option<Error>) {
        let (_, _, mut duration) = self.query_cost_ceilings();
        if let Some(o) = opts {
            let m = o.max_query_duration.as_nanos() as GoDuration;
            if m > 0 && (duration <= 0 || m < duration) {
                duration = m;
            }
        }
        let mut ctx = ctx.clone();
        let mut _cancel = None;
        if duration > 0 {
            let (c, t) = ctx.with_deadline(limit_start + Duration::from_nanos(duration as u64));
            ctx = c;
            _cancel = Some(t);
        }
        let _slot = match self.o.query_engine.as_ref().and_then(|e| e.acquire_query_slot(&ctx, qs)) {
            Some(Ok(s)) => Some(s),
            Some(Err(e)) => return (Default::default(), Annotations::new(), Some(e)),
            None => None,
        };
        let q = match self.queryable() {
            Ok(q) => q,
            Err(e) => return (Default::default(), Annotations::new(), Some(e.err)),
        };
        let sampling = self.o.cost_sampling.as_deref().map(|c| c as &dyn storage::CostSamplingQueryable);
        promql::estimate::estimate_cost(
            &ctx,
            q.as_ref(),
            sampling,
            self.parser.as_ref(),
            qs,
            start.unix_milli(),
            end.unix_milli(),
            step / 1_000_000,
            self.effective_lookback_delta(opts) / 1_000_000,
            self.subquery_default_step() / 1_000_000,
            self.scrape_interval() / 1_000_000,
        )
    }

    /// Go `estimateCostResult`.
    fn estimate_cost_result(&self, ctx: &Context, qs: &str, start: Time, end: Time, step: GoDuration, opts: Option<&QueryOpts>) -> ApiFuncResult {
        let expr = match self.parser.parse_expr(qs) {
            Ok(e) => e,
            Err(e) => return invalid_param_error(e, "query"),
        };
        if step > 0 && expr.value_type() != ValueType::Vector && expr.value_type() != ValueType::Scalar {
            return invalid_param_error(
                format!(
                    "invalid expression type {} for range query, must be Scalar or instant Vector",
                    go_quote(promql::parser::documented_type(expr.value_type()))
                ),
                "query",
            );
        }
        if let Err(e) = promql::preprocess_expr(expr, start.unix_milli(), end.unix_milli(), step / 1_000_000) {
            return invalid_param_error(e, "query");
        }
        let (est, annos, err) = self.estimate_query_cost(ctx, Instant::now(), qs, start, end, step, opts);
        if let Some(e) = err {
            if e.is(&storage::ERR_DEADLINE_EXCEEDED) {
                return ApiFuncResult::error(ApiError::new(ERROR_TIMEOUT, Error::wrap("cost estimation exceeded the query duration limit", e)))
                    .with_warnings(annos);
            }
            return ApiFuncResult::error(return_api_error(e)).with_warnings(annos);
        }
        ApiFuncResult::ok(Data::QueryCost(QueryCostData {
            estimate: CostEstimate { series_touched: est.series_touched, samples_read: est.samples_read, peak_samples: 0 },
        }))
        .with_warnings(annos)
    }

    fn deadline_ctx(&self, r: &ApiRequest, timeout: GoDuration) -> (Context, Option<storage::CancelToken>) {
        if timeout > 0 {
            let now = self.now();
            let (c, t) = r.ctx.with_deadline(instant_after(now.add(timeout), Time::now()));
            (c, Some(t))
        } else {
            (r.ctx.clone(), None)
        }
    }

    /// GET/POST /query.
    pub fn query(&self, r: &ApiRequest) -> ApiFuncResult {
        let limit = match params::parse_limit_param(r.form_value("limit")) {
            Ok(l) => l,
            Err(e) => return invalid_param_error(e, "limit"),
        };
        let ts = match parse_time_param(r, "time", self.now()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "time"),
        };
        let timeout = match self.query_timeout(r) {
            Ok(t) => t,
            Err(e) => return ApiFuncResult::error(e),
        };
        let (ctx, _cancel) = self.deadline_ctx(r, timeout);
        let opts = match self.extract_query_opts(r) {
            Ok(o) => o,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let cost_on = match self.cost_requested(r) {
            Ok(c) => c,
            Err(e) => return invalid_param_error(e, "cost"),
        };
        let (engine, q) = match (self.engine(), self.queryable()) {
            (Ok(e), Ok(q)) => (e, q),
            (Err(e), _) | (_, Err(e)) => return ApiFuncResult::error(e),
        };
        let limit_start = Instant::now();
        let qs = r.form_value("query");
        let mut qry = match engine.new_instant_query(&ctx, q as Arc<dyn storage::Queryable>, Some(&opts), qs, ts.unix_milli()) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(query_construction_error(e)),
        };
        let pending = if cost_on { Some(self.estimate_for_comparison(&ctx, limit_start, qs, ts, ts, 0, &opts)) } else { None };
        self.finish_query(r, &ctx, qry.as_mut(), limit, pending).with_close(qry)
    }

    #[allow(clippy::too_many_arguments)]
    fn estimate_for_comparison(&self, ctx: &Context, limit_start: Instant, qs: &str, start: Time, end: Time, step: GoDuration, opts: &QueryOpts) -> PendingCost {
        let (estimate, annos, err) = self.estimate_query_cost(ctx, limit_start, qs, start, end, step, Some(opts));
        PendingCost { estimate, annos, err }
    }

    fn finish_query(&self, r: &ApiRequest, ctx: &Context, qry: &mut dyn ApiQuery, limit: i64, pending: Option<PendingCost>) -> ApiFuncResult {
        let mut res = qry.exec(ctx);
        if let Some(e) = res.err.take() {
            return ApiFuncResult::error(return_api_error(e)).with_warnings(res.warnings);
        }
        let mut warnings = std::mem::take(&mut res.warnings);
        if limit > 0 && truncate_results(&mut res, limit as usize) {
            warnings.add(Error::msg("results truncated due to limit"));
        }
        if let Some(w) = self.stats_param_warning(r.form_value("stats")) {
            warnings.add(w);
        }
        let stats = qry.stats();
        let qs = self.render_stats(ctx, stats.as_ref(), r.form_value("stats"));
        let cost = pending.and_then(|p| p.finish(stats, &mut warnings));
        let value = res.value.unwrap_or(promql::Value::Vector(Vec::new()));
        ApiFuncResult::ok(Data::Query(QueryData { result_type: value.value_type(), result: value, stats: qs, cost })).with_warnings(warnings)
    }

    /// GET/POST /query_range.
    pub fn query_range(&self, r: &ApiRequest) -> ApiFuncResult {
        let limit = match params::parse_limit_param(r.form_value("limit")) {
            Ok(l) => l,
            Err(e) => return invalid_param_error(e, "limit"),
        };
        let (start, end, step) = match parse_range(r) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let timeout = match self.query_timeout(r) {
            Ok(t) => t,
            Err(e) => return ApiFuncResult::error(e),
        };
        let (ctx, _cancel) = if timeout > 0 {
            let (c, t) = r.ctx.with_timeout(Duration::from_nanos(timeout as u64));
            (c, Some(t))
        } else {
            (r.ctx.clone(), None)
        };
        let opts = match self.extract_query_opts(r) {
            Ok(o) => o,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let cost_on = match self.cost_requested(r) {
            Ok(c) => c,
            Err(e) => return invalid_param_error(e, "cost"),
        };
        let (engine, q) = match (self.engine(), self.queryable()) {
            (Ok(e), Ok(q)) => (e, q),
            (Err(e), _) | (_, Err(e)) => return ApiFuncResult::error(e),
        };
        let limit_start = Instant::now();
        let qs = r.form_value("query");
        let mut qry = match engine.new_range_query(
            &ctx,
            q as Arc<dyn storage::Queryable>,
            Some(&opts),
            qs,
            start.unix_milli(),
            end.unix_milli(),
            step / 1_000_000,
        ) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(query_construction_error(e)),
        };
        let pending = if cost_on { Some(self.estimate_for_comparison(&ctx, limit_start, qs, start, end, step, &opts)) } else { None };
        self.finish_query(r, &ctx, qry.as_mut(), limit, pending).with_close(qry)
    }

    /// GET/POST /query_cost.
    pub fn query_cost(&self, r: &ApiRequest) -> ApiFuncResult {
        if let Err(e) = params::parse_limit_param(r.form_value("limit")) {
            return invalid_param_error(e, "limit");
        }
        if let Err(e) = self.cost_requested(r) {
            return invalid_param_error(e, "cost");
        }
        let ts = match parse_time_param(r, "time", self.now()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "time"),
        };
        let timeout = match self.query_timeout(r) {
            Ok(t) => t,
            Err(e) => return ApiFuncResult::error(e),
        };
        let (ctx, _cancel) = self.deadline_ctx(r, timeout);
        let opts = match self.extract_query_opts(r) {
            Ok(o) => o,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let mut res = self.estimate_cost_result(&ctx, r.form_value("query"), ts, ts, 0, Some(&opts));
        if let Some(w) = self.stats_param_warning(r.form_value("stats")) {
            res.warnings.add(w);
        }
        res
    }

    /// GET/POST /query_range_cost.
    pub fn query_range_cost(&self, r: &ApiRequest) -> ApiFuncResult {
        if let Err(e) = params::parse_limit_param(r.form_value("limit")) {
            return invalid_param_error(e, "limit");
        }
        if let Err(e) = self.cost_requested(r) {
            return invalid_param_error(e, "cost");
        }
        let (start, end, step) = match parse_range(r) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let timeout = match self.query_timeout(r) {
            Ok(t) => t,
            Err(e) => return ApiFuncResult::error(e),
        };
        let (ctx, _cancel) = if timeout > 0 {
            let (c, t) = r.ctx.with_timeout(Duration::from_nanos(timeout as u64));
            (c, Some(t))
        } else {
            (r.ctx.clone(), None)
        };
        let opts = match self.extract_query_opts(r) {
            Ok(o) => o,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let mut res = self.estimate_cost_result(&ctx, r.form_value("query"), start, end, step, Some(&opts));
        if let Some(w) = self.stats_param_warning(r.form_value("stats")) {
            res.warnings.add(w);
        }
        res
    }

    /// GET/POST /format_query.
    pub fn format_query(&self, r: &ApiRequest) -> ApiFuncResult {
        match self.parser.parse_expr(r.form_value("query")) {
            Ok(e) => ApiFuncResult::ok(Data::String(e.pretty(0))),
            Err(e) => invalid_param_error(e, "query"),
        }
    }

    /// GET/POST /parse_query.
    pub fn parse_query(&self, r: &ApiRequest) -> ApiFuncResult {
        match self.parser.parse_expr(r.form_value("query")) {
            Ok(e) => ApiFuncResult::ok(Data::Json(translate_ast::translate_ast(&e))),
            Err(e) => invalid_param_error(e, "query"),
        }
    }

    /// GET/POST /query_exemplars.
    pub fn query_exemplars(&self, r: &ApiRequest) -> ApiFuncResult {
        let start = match parse_time_param(r, "start", min_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "start"),
        };
        let end = match parse_time_param(r, "end", max_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "end"),
        };
        if end.before(&start) {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "end timestamp must not be before start timestamp"));
        }
        let expr = match self.parser.parse_expr(r.form_value("query")) {
            Ok(e) => e,
            Err(e) => return ApiFuncResult::error(ApiError::new(ERROR_BAD_DATA, Error::other(e))),
        };
        let selectors = promql::parser::extract_selectors(&expr);
        if selectors.is_empty() {
            return ApiFuncResult::none();
        }
        let Some(eq) = &self.o.exemplar_queryable else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no exemplar queryable configured"));
        };
        let q = match eq.exemplar_querier(&r.ctx) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(return_api_error(e)),
        };
        match q.select(start.unix_milli(), end.unix_milli(), &selectors) {
            Ok(res) => ApiFuncResult::ok(Data::Exemplars(res)),
            Err(e) => ApiFuncResult::error(return_api_error(e)),
        }
    }

    /// GET/POST /labels.
    pub fn label_names(&self, r: &ApiRequest) -> ApiFuncResult {
        let limit = match params::parse_limit_param(r.form_value("limit")) {
            Ok(l) => l,
            Err(e) => return invalid_param_error(e, "limit"),
        };
        let start = match parse_time_param(r, "start", min_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "start"),
        };
        let end = match parse_time_param(r, "end", max_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "end"),
        };
        let sets = match params::parse_matchers_param(&self.parser, &r.form_values("match[]")) {
            Ok(s) => s,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let hints = storage::LabelHints { limit: params::to_hint_limit(limit), ..Default::default() };
        let queryable = match self.queryable() {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(e),
        };
        let mut q = match queryable.querier(start.unix_milli(), end.unix_milli()) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(return_api_error(e)),
        };
        let mut warnings = Annotations::new();
        let mut names: Vec<String>;
        if sets.len() > 1 {
            let mut set = std::collections::HashSet::new();
            for ms in &sets {
                let (vals, w, err) = q.label_names(&r.ctx, Some(&hints), ms);
                if let Some(e) = err {
                    q.close();
                    return ApiFuncResult::error(return_api_error(e)).with_warnings(warnings);
                }
                warnings.merge(w);
                set.extend(vals);
            }
            names = set.into_iter().collect();
            names.sort();
        } else {
            let ms: &[Matcher] = if sets.len() == 1 { &sets[0] } else { &[] };
            let (vals, w, err) = q.label_names(&r.ctx, Some(&hints), ms);
            if let Some(e) = err {
                q.close();
                return ApiFuncResult::error(ApiError::new(ERROR_EXEC, e)).with_warnings(w);
            }
            names = vals;
            warnings = w;
        }
        q.close();
        if limit > 0 && names.len() > limit as usize {
            names.truncate(limit as usize);
            warnings.add(Error::msg("results truncated due to limit"));
        }
        ApiFuncResult::ok(Data::Strings(names)).with_warnings(warnings)
    }

    /// GET /label/:name/values.
    pub fn label_values(&self, r: &ApiRequest) -> ApiFuncResult {
        let mut name = r.param("name").to_string();
        if name.starts_with("U__") {
            name = params::unescape_name(&name);
        }
        if name.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, format!("invalid label name: {}", go_quote(&name))));
        }
        let limit = match params::parse_limit_param(r.form_value("limit")) {
            Ok(l) => l,
            Err(e) => return invalid_param_error(e, "limit"),
        };
        let start = match parse_time_param(r, "start", min_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "start"),
        };
        let end = match parse_time_param(r, "end", max_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "end"),
        };
        let sets = match params::parse_matchers_param(&self.parser, &r.form_values("match[]")) {
            Ok(s) => s,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let hints = storage::LabelHints { limit: params::to_hint_limit(limit), ..Default::default() };
        let queryable = match self.queryable() {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(e),
        };
        let mut q = match queryable.querier(start.unix_milli(), end.unix_milli()) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(ApiError::new(ERROR_EXEC, e)),
        };
        let mut warnings = Annotations::new();
        let mut vals: Vec<String>;
        if sets.len() > 1 {
            let mut set = std::collections::HashSet::new();
            for ms in &sets {
                let (v, w, err) = q.label_values(&r.ctx, &name, Some(&hints), ms);
                if let Some(e) = err {
                    q.close();
                    return ApiFuncResult::error(ApiError::new(ERROR_EXEC, e)).with_warnings(warnings);
                }
                warnings.merge(w);
                set.extend(v);
            }
            vals = set.into_iter().collect();
        } else {
            let ms: &[Matcher] = if sets.len() == 1 { &sets[0] } else { &[] };
            let (v, w, err) = q.label_values(&r.ctx, &name, Some(&hints), ms);
            if let Some(e) = err {
                q.close();
                return ApiFuncResult::error(ApiError::new(ERROR_EXEC, e)).with_warnings(w);
            }
            vals = v;
            warnings = w;
        }
        q.close();
        vals.sort();
        if limit > 0 && vals.len() > limit as usize {
            vals.truncate(limit as usize);
            warnings.add(Error::msg("results truncated due to limit"));
        }
        ApiFuncResult::ok(Data::Strings(vals)).with_warnings(warnings)
    }

    /// GET/POST /series.
    pub fn series(&self, r: &ApiRequest) -> ApiFuncResult {
        let matches = r.form_values("match[]");
        if matches.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "no match[] parameter provided"));
        }
        let limit = match params::parse_limit_param(r.form_value("limit")) {
            Ok(l) => l,
            Err(e) => return invalid_param_error(e, "limit"),
        };
        let start = match parse_time_param(r, "start", min_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "start"),
        };
        let end = match parse_time_param(r, "end", max_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "end"),
        };
        let sets = match params::parse_matchers_param(&self.parser, &matches) {
            Ok(s) => s,
            Err(e) => return invalid_param_error(e, "match[]"),
        };
        let queryable = match self.queryable() {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(e),
        };
        let mut q = match queryable.querier(start.unix_milli(), end.unix_milli()) {
            Ok(q) => q,
            Err(e) => return ApiFuncResult::error(return_api_error(e)),
        };
        let hints = storage::SelectHints {
            start: start.unix_milli(),
            end: end.unix_milli(),
            func: "series".to_string(),
            limit: params::to_hint_limit(limit),
            ..Default::default()
        };
        let ctx = &r.ctx;
        let result = {
            let mut set: storage::BoxSeriesSet = if sets.len() > 1 {
                let mut v = Vec::with_capacity(sets.len());
                for ms in &sets {
                    v.push(q.select(ctx, true, Some(&hints), ms));
                }
                storage::new_merge_series_set(v, 0, Arc::new(storage::chained_series_merge))
            } else {
                q.select(ctx, false, Some(&hints), &sets[0])
            };
            let mut metrics: Vec<Labels> = Vec::new();
            let mut warnings = set.warnings();
            let mut i = 1usize;
            let mut out = None;
            while set.next() {
                if i % CHECK_CONTEXT_EVERY_N_ITERATIONS == 0 {
                    if let Some(e) = ctx.err() {
                        out = Some(ApiFuncResult::error(return_api_error(e)).with_warnings(warnings.clone()));
                        break;
                    }
                }
                i += 1;
                metrics.push(set.at().labels().clone());
                if limit > 0 && metrics.len() > limit as usize {
                    metrics.truncate(limit as usize);
                    warnings.add(Error::msg("results truncated due to limit"));
                    out = Some(ApiFuncResult::ok(Data::Series(std::mem::take(&mut metrics))).with_warnings(warnings.clone()));
                    break;
                }
            }
            match out {
                Some(o) => o,
                None => match set.err() {
                    Some(e) => ApiFuncResult::error(return_api_error(e)).with_warnings(warnings),
                    None => ApiFuncResult::ok(Data::Series(metrics)).with_warnings(warnings),
                },
            }
        };
        q.close();
        result
    }

    fn target_retriever(&self) -> Result<&Arc<dyn TargetRetriever>, ApiFuncResult> {
        self.o.target_retriever.as_ref().ok_or_else(|| ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no target retriever configured")))
    }

    /// GET /scrape_pools.
    pub fn scrape_pools(&self, _r: &ApiRequest) -> ApiFuncResult {
        let Some(sp) = &self.o.scrape_pools_retriever else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no scrape pools retriever configured"));
        };
        let mut names = sp.scrape_pools();
        names.sort();
        ApiFuncResult::ok(Data::ScrapePools(names))
    }

    /// GET /scrape_pools/config.
    pub fn scrape_pool_config(&self, r: &ApiRequest) -> ApiFuncResult {
        let pool = r.form_value("scrapePool");
        if pool.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "no scrapePool parameter provided"));
        }
        let tr = match self.target_retriever() {
            Ok(t) => t,
            Err(e) => return e,
        };
        let sc = match tr.scrape_pool_config(pool) {
            Ok(c) => c,
            Err(e) => return ApiFuncResult::error(ApiError::new(ERROR_BAD_DATA, Error::wrap("error retrieving scrape config", e))),
        };
        match yaml::marshal(sc.as_ref()) {
            Ok(y) => ApiFuncResult::ok(Data::Config(y)),
            Err(e) => ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, format!("error marshaling scrape config: {}", e))),
        }
    }

    /// GET /targets.
    pub fn targets(&self, r: &ApiRequest) -> ApiFuncResult {
        let tr = match self.target_retriever() {
            Ok(t) => t,
            Err(e) => return e,
        };
        let scrape_pool = r.query_value("scrapePool");
        let state = r.query_value("state").to_lowercase();
        let show_active = state.is_empty() || state == "any" || state == "active";
        let show_dropped = state.is_empty() || state == "any" || state == "dropped";
        let mut res = TargetDiscovery::default();
        let mut builder = Builder::new(Labels::empty());
        if show_active {
            let active = tr.targets_active();
            let mut pools: Vec<&String> = active.keys().collect();
            pools.sort();
            let n: usize = active.values().map(|v| v.len()).sum();
            res.active_targets.reserve(n);
            for pool in pools {
                if !scrape_pool.is_empty() && pool != scrape_pool {
                    continue;
                }
                for target in &active[pool] {
                    let last_err_str = target.last_error().map(|e| e.to_string()).unwrap_or_default();
                    let url = target.url();
                    let (global, gerr) = match get_global_url(&url, &self.o.global_url_options) {
                        Ok(u) => (u, None),
                        Err((u, e)) => (u, Some(e)),
                    };
                    let last_error = match (&gerr, last_err_str.is_empty()) {
                        (None, true) => String::new(),
                        (Some(e), _) => format!("{}: {}", last_err_str, e),
                        (None, false) => last_err_str,
                    };
                    res.active_targets.push(Target {
                        discovered_labels: target.discovered_labels(&mut builder),
                        labels: target.labels(),
                        scrape_pool: pool.clone(),
                        scrape_url: url.to_url_string(),
                        global_url: global.to_url_string(),
                        last_error,
                        last_scrape: target.last_scrape().map(Time::from_system).unwrap_or_default(),
                        last_scrape_duration: target.last_scrape_duration().as_secs_f64(),
                        health: target.health().as_str().to_string(),
                        scrape_interval: target.get_value("__scrape_interval__").to_string(),
                        scrape_timeout: target.get_value("__scrape_timeout__").to_string(),
                    });
                }
            }
        }
        if show_dropped {
            let counts = tr.targets_dropped_counts();
            res.dropped_target_counts = Some(counts.into_iter().collect());
            let dropped = tr.targets_dropped();
            let mut pools: Vec<&String> = dropped.keys().collect();
            pools.sort();
            for pool in pools {
                if !scrape_pool.is_empty() && pool != scrape_pool {
                    continue;
                }
                for target in &dropped[pool] {
                    res.dropped_targets.push(DroppedTarget { discovered_labels: target.discovered_labels(&mut builder), scrape_pool: pool.clone() });
                }
            }
        }
        ApiFuncResult::ok(Data::Targets(res))
    }

    /// GET /targets/metadata.
    pub fn target_metadata(&self, r: &ApiRequest) -> ApiFuncResult {
        let mut limit: i64 = -1;
        let s = r.form_value("limit");
        if !s.is_empty() {
            match params::atoi(s) {
                Ok(l) => limit = l,
                Err(_) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "limit must be a number")),
            }
        }
        let match_target = r.form_value("match_target");
        let mut matchers = Vec::new();
        if !match_target.is_empty() {
            match self.parser.parse_metric_selector(match_target) {
                Ok(m) => matchers = m,
                Err(e) => return invalid_param_error(e, "match_target"),
            }
        }
        let tr = match self.target_retriever() {
            Ok(t) => t,
            Err(e) => return e,
        };
        let metric = r.form_value("metric");
        let mut res: Vec<MetricMetadataItem> = Vec::new();
        for tt in tr.targets_active().values() {
            for t in tt {
                if limit >= 0 && res.len() as i64 >= limit {
                    break;
                }
                let tl = t.labels();
                if !match_target.is_empty() && !matchers.iter().all(|m| m.matches(tl.get(&m.name))) {
                    continue;
                }
                if metric.is_empty() {
                    for md in t.list_metadata() {
                        res.push(MetricMetadataItem {
                            target: tl.clone(),
                            metric_family: md.metric_family.clone(),
                            typ: md.typ.as_str().to_string(),
                            help: md.help.clone(),
                            unit: md.unit.clone(),
                        });
                    }
                    continue;
                }
                if let Some(md) = t.get_metadata(metric) {
                    res.push(MetricMetadataItem {
                        target: tl.clone(),
                        metric_family: String::new(),
                        typ: md.typ.as_str().to_string(),
                        help: md.help.clone(),
                        unit: md.unit.clone(),
                    });
                }
            }
        }
        ApiFuncResult::ok(Data::TargetMetadata(res))
    }

    /// GET /targets/relabel_steps.
    pub fn target_relabel_steps(&self, r: &ApiRequest) -> ApiFuncResult {
        let pool = r.form_value("scrapePool");
        if pool.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "no scrapePool parameter provided"));
        }
        let lj = r.form_value("labels");
        if lj.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "no labels parameter provided"));
        }
        let lbls = match Labels::unmarshal_json(lj) {
            Ok(l) => l,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, format!("error parsing labels: {}", e))),
        };
        let tr = match self.target_retriever() {
            Ok(t) => t,
            Err(e) => return e,
        };
        let sc = match tr.scrape_pool_config(pool) {
            Ok(c) => c,
            Err(e) => return ApiFuncResult::error(ApiError::new(ERROR_BAD_DATA, Error::wrap("error retrieving scrape config", e))),
        };
        let mut lb = Builder::new(lbls);
        let mut keep = true;
        let mut steps = Vec::with_capacity(sc.relabel_configs.len());
        for rule in &sc.relabel_configs {
            if keep {
                keep = model::relabel::process_builder(&mut lb, std::iter::once(rule));
            }
            let output = if keep { lb.labels() } else { Labels::empty() };
            steps.push(RelabelStep { rule_json: rule.to_json().stringify(), output, keep });
        }
        ApiFuncResult::ok(Data::RelabelSteps(steps))
    }

    /// GET /alertmanagers.
    pub fn alertmanagers(&self, _r: &ApiRequest) -> ApiFuncResult {
        let Some(ar) = &self.o.alertmanager_retriever else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no alertmanager retriever configured"));
        };
        ApiFuncResult::ok(Data::Alertmanagers { active: ar.alertmanagers(), dropped: ar.dropped_alertmanagers() })
    }

    fn rules_retriever(&self) -> Result<&Arc<dyn RulesRetriever>, ApiFuncResult> {
        self.o.rules_retriever.as_ref().ok_or_else(|| ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no rules retriever configured")))
    }

    /// GET /alerts.
    pub fn alerts(&self, _r: &ApiRequest) -> ApiFuncResult {
        let rr = match self.rules_retriever() {
            Ok(r) => r,
            Err(e) => return e,
        };
        let mut alerts = Vec::new();
        for ar in rr.alerting_rules() {
            alerts.extend(rules_alerts_to_api_alerts(&ar.active_alerts()));
        }
        ApiFuncResult::ok(Data::Alerts(alerts))
    }

    /// GET /metadata.
    pub fn metric_metadata(&self, r: &ApiRequest) -> ApiFuncResult {
        let mut limit: i64 = -1;
        let s = r.form_value("limit");
        if !s.is_empty() {
            match params::atoi(s) {
                Ok(l) => limit = l,
                Err(_) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "limit must be a number")),
            }
        }
        let mut limit_per_metric: i64 = -1;
        let s = r.form_value("limit_per_metric");
        if !s.is_empty() {
            match params::atoi(s) {
                Ok(l) => limit_per_metric = l,
                Err(_) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "limit_per_metric must be a number")),
            }
        }
        let tr = match self.target_retriever() {
            Ok(t) => t,
            Err(e) => return e,
        };
        let metric = r.form_value("metric");
        // Insertion-ordered sets per metric (Go uses a map as a set).
        let mut metrics: HashMap<String, Vec<Metadata>> = HashMap::new();
        let mut add = |family: &str, m: Metadata| {
            let ms = metrics.get(family);
            if limit_per_metric > 0 && ms.map(|v| v.len()).unwrap_or(0) as i64 >= limit_per_metric {
                return;
            }
            let ms = metrics.entry(family.to_string()).or_default();
            if !ms.contains(&m) {
                ms.push(m);
            }
        };
        let active = tr.targets_active();
        let mut pools: Vec<&String> = active.keys().collect();
        pools.sort();
        for pool in pools {
            for t in &active[pool] {
                if metric.is_empty() {
                    for mm in t.list_metadata() {
                        add(&mm.metric_family, Metadata { typ: mm.typ.as_str().to_string(), help: mm.help.clone(), unit: mm.unit.clone() });
                    }
                    continue;
                }
                if let Some(md) = t.get_metadata(metric) {
                    add(&md.metric_family, Metadata { typ: md.typ.as_str().to_string(), help: md.help.clone(), unit: md.unit.clone() });
                }
            }
        }
        let mut names: Vec<String> = metrics.keys().cloned().collect();
        names.sort();
        let mut res = BTreeMap::new();
        for name in names {
            if limit >= 0 && res.len() as i64 >= limit {
                break;
            }
            let v = metrics.remove(&name).unwrap_or_default();
            res.insert(name, v);
        }
        ApiFuncResult::ok(Data::Metadata(res))
    }

    /// GET /rules.
    pub fn rules(&self, r: &ApiRequest) -> ApiFuncResult {
        let rn_set: std::collections::HashSet<String> = r.form_values("rule_name[]").into_iter().collect();
        let rg_set: std::collections::HashSet<String> = r.form_values("rule_group[]").into_iter().collect();
        let f_set: std::collections::HashSet<String> = r.form_values("file[]").into_iter().collect();
        let sets = match params::parse_matchers_param(&self.parser, &r.form_values("match[]")) {
            Ok(s) => s,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, e)),
        };
        let rr = match self.rules_retriever() {
            Ok(r) => r,
            Err(e) => return e,
        };
        let groups = rr.rule_groups();
        let typ = r.query_value("type").to_lowercase();
        if !typ.is_empty() && typ != "alert" && typ != "record" {
            return invalid_param_error(format!("not supported value {}", go_quote(&typ)), "type");
        }
        let return_alerts = typ.is_empty() || typ == "alert";
        let return_recording = typ.is_empty() || typ == "record";
        let exclude_alerts = {
            let p = r.query_value("exclude_alerts").to_lowercase();
            if p.is_empty() {
                false
            } else {
                match params::parse_bool(&p) {
                    Ok(b) => b,
                    Err(e) => return invalid_param_error(format!("error converting exclude_alerts: {}", e), "exclude_alerts"),
                }
            }
        };
        let (max_groups, next_token) = match parse_list_rules_pagination_request(r) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let mut res = RuleDiscovery::default();
        let mut rgs = Vec::with_capacity(groups.len());
        let mut found_token = false;
        for grp in &groups {
            if max_groups > 0 && !next_token.is_empty() && !found_token {
                if next_token != get_rule_group_next_token(&grp.file(), &grp.name()) {
                    continue;
                }
                found_token = true;
            }
            if !rg_set.is_empty() && !rg_set.contains(&grp.name()) {
                continue;
            }
            if !f_set.is_empty() && !f_set.contains(&grp.file()) {
                continue;
            }
            let mut g = RuleGroup {
                name: grp.name(),
                file: grp.file(),
                interval: duration_seconds(grp.interval()),
                limit: grp.limit(),
                rules: Vec::new(),
                evaluation_time: duration_seconds(grp.evaluation_time()),
                last_evaluation: grp.last_evaluation(),
            };
            for rule in grp.rules(&sets) {
                let rv = rule.as_rule();
                if !rn_set.is_empty() && !rn_set.contains(&rv.name()) {
                    continue;
                }
                let last_error = rv.last_error().unwrap_or_default();
                match &rule {
                    RuleRef::Alerting(ar) => {
                        if !return_alerts {
                            continue;
                        }
                        let alerts = if exclude_alerts { None } else { Some(rules_alerts_to_api_alerts(&ar.active_alerts())) };
                        g.rules.push(Rule::Alerting(AlertingRule {
                            state: ar.state(),
                            name: ar.name(),
                            query: ar.query(),
                            duration: duration_seconds(ar.hold_duration()),
                            keep_firing_for: duration_seconds(ar.keep_firing_for()),
                            labels: ar.labels(),
                            annotations: ar.annotations(),
                            alerts,
                            health: ar.health(),
                            last_error,
                            evaluation_time: duration_seconds(ar.evaluation_duration()),
                            last_evaluation: ar.evaluation_timestamp(),
                        }));
                    }
                    RuleRef::Recording(rr) => {
                        if !return_recording {
                            continue;
                        }
                        g.rules.push(Rule::Recording(RecordingRule {
                            name: rr.name(),
                            query: rr.query(),
                            labels: rr.labels(),
                            health: rr.health(),
                            last_error,
                            evaluation_time: duration_seconds(rr.evaluation_duration()),
                            last_evaluation: rr.evaluation_timestamp(),
                        }));
                    }
                }
            }
            if !g.rules.is_empty() {
                if max_groups > 0 && rgs.len() as i64 == max_groups {
                    res.group_next_token = get_rule_group_next_token(&grp.file(), &grp.name());
                    break;
                }
                rgs.push(g);
            }
        }
        if max_groups > 0 && !next_token.is_empty() && !found_token {
            return invalid_param_error(format!("invalid group_next_token '{}'. were rule groups changed?", next_token), "group_next_token");
        }
        res.rule_groups = rgs;
        ApiFuncResult::ok(Data::Rules(res))
    }

    /// GET /status/runtimeinfo.
    pub fn serve_runtime_info(&self, _r: &ApiRequest) -> ApiFuncResult {
        let Some(f) = &self.o.runtime_info else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no runtime info configured"));
        };
        match f() {
            Ok(s) => ApiFuncResult::ok(Data::RuntimeInfo(s)),
            Err(e) => ApiFuncResult { data: Some(Data::RuntimeInfo(RuntimeInfo::default())), err: Some(ApiError::new(ERROR_INTERNAL, e)), ..Default::default() },
        }
    }

    /// GET /status/buildinfo.
    pub fn serve_build_info(&self, _r: &ApiRequest) -> ApiFuncResult {
        ApiFuncResult::ok(Data::BuildInfo(self.o.build_info.clone()))
    }

    /// GET /status/config.
    pub fn serve_config(&self, _r: &ApiRequest) -> ApiFuncResult {
        let y = self.config().map(|c| c.to_string()).unwrap_or_default();
        ApiFuncResult::ok(Data::Config(y))
    }

    /// GET /status/flags.
    pub fn serve_flags(&self, _r: &ApiRequest) -> ApiFuncResult {
        ApiFuncResult::ok(Data::Flags(self.o.flags.clone()))
    }

    /// GET /features.
    pub fn features(&self, _r: &ApiRequest) -> ApiFuncResult {
        let Some(fr) = &self.o.feature_registry else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "feature registry not configured"));
        };
        let mut out = BTreeMap::new();
        for (k, v) in fr.get() {
            out.insert(k, v.into_iter().collect::<BTreeMap<_, _>>());
        }
        ApiFuncResult::ok(Data::Features(out))
    }

    /// GET /status/self_metrics.
    pub fn self_metrics(&self, r: &ApiRequest) -> ApiFuncResult {
        let mut filter = None;
        let pattern = r.form_value("metric_name_pattern");
        if !pattern.is_empty() {
            match model::regexp::Regexp::compile(&format!("^(?:{})$", pattern)) {
                Ok(re) => filter = Some(re),
                Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, format!("invalid metric_name_pattern: {}", e))),
            }
        }
        let Some(g) = &self.o.gatherer else {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "error gathering self metrics: no gatherer configured"));
        };
        let mfs = match g.gather() {
            Ok(m) => m,
            Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, format!("error gathering self metrics: {}", e))),
        };
        let mut out = Vec::with_capacity(mfs.len());
        for mf in &mfs {
            if let Some(f) = &filter {
                if !f.is_match(&mf.name) {
                    continue;
                }
            }
            out.push(crate::metrics::protojson(mf));
        }
        ApiFuncResult::ok(Data::RawList(out))
    }

    fn db(&self) -> Result<&Arc<dyn TsdbAdminStats>, ApiFuncResult> {
        self.o.db.as_ref().ok_or_else(|| ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, "no TSDB configured")))
    }

    /// GET /status/tsdb/blocks.
    pub fn serve_tsdb_blocks(&self, _r: &ApiRequest) -> ApiFuncResult {
        let db = match self.db() {
            Ok(d) => d,
            Err(e) => return e,
        };
        match db.block_metas() {
            Ok(b) => ApiFuncResult::ok(Data::Blocks(b)),
            Err(e) => ApiFuncResult::error(ApiError::new(ERROR_INTERNAL, Error::wrap("error getting block metadata", e))),
        }
    }

    /// GET /status/tsdb.
    pub fn serve_tsdb_status(&self, r: &ApiRequest) -> ApiFuncResult {
        const MAX_TSDB_LIMIT: i64 = 10000;
        let mut limit: i64 = 10;
        let s = r.form_value("limit");
        if !s.is_empty() {
            match params::atoi(s) {
                Ok(l) if l >= 1 => limit = l,
                _ => return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "limit must be a positive number")),
            }
            if limit > MAX_TSDB_LIMIT {
                return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, format!("limit must not exceed {}", MAX_TSDB_LIMIT)));
            }
        }
        let db = match self.db() {
            Ok(d) => d,
            Err(e) => return e,
        };
        let s = match db.stats("__name__", limit as usize) {
            Ok(s) => s,
            Err(e) => return ApiFuncResult::error(ApiError::new(ERROR_INTERNAL, e)),
        };
        let mut chunk_count = i64::MIN; // Go int64(math.NaN()) on amd64.
        if let Some(g) = &self.o.gatherer {
            match g.gather() {
                Ok(mfs) => {
                    for mf in mfs {
                        if mf.name == "prometheus_tsdb_head_chunks" {
                            if let Some(m) = mf.metrics.first() {
                                if let crate::metrics::MetricValue::Gauge(v) = m.value {
                                    chunk_count = params::go_f64_to_i64(v);
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, format!("error gathering runtime status: {}", e))),
            }
        }
        let conv = |v: &[tsdb_format::index::Stat]| v.iter().map(|s| TsdbStat { name: s.name.clone(), value: s.count }).collect::<Vec<_>>();
        let ips = &s.index_posting_stats;
        ApiFuncResult::ok(Data::TsdbStatus(TsdbStatus {
            head_stats: HeadStats {
                num_series: s.num_series,
                num_label_pairs: ips.num_label_pairs as i64,
                chunk_count,
                min_time: s.min_time,
                max_time: s.max_time,
            },
            series_count_by_metric_name: conv(&ips.cardinality_metrics_stats),
            label_value_count_by_label_name: conv(&ips.cardinality_label_stats),
            memory_in_bytes_by_label_name: conv(&ips.label_value_stats),
            series_count_by_label_value_pair: conv(&ips.label_value_pairs_stats),
        }))
    }

    /// GET /status/walreplay (not wrapped: no ready check, no compression).
    pub fn serve_wal_replay_status(&self, req: HttpRequest) -> HttpResponse {
        let mut resp = match self.o.db.as_ref().map(|d| d.wal_replay_status()) {
            Some(Ok(s)) => self.respond(&req, Data::WalReplay { min: s.min, max: s.max, current: s.current }, &Annotations::new(), ""),
            Some(Err(e)) => self.respond_error(&ApiError::new(ERROR_INTERNAL, e), None),
            None => self.respond_error(&ApiError::msg(ERROR_INTERNAL, "no TSDB configured"), None),
        };
        crate::httputil::set_cors(&mut resp, self.o.cors_origin.as_ref(), &req);
        resp
    }

    /// GET /notifications.
    pub fn notifications(&self, req: HttpRequest) -> HttpResponse {
        let n = self.o.notifications_getter.as_ref().map(|f| f()).unwrap_or_default();
        let mut resp = self.respond(&req, Data::Notifications(n), &Annotations::new(), "");
        crate::httputil::set_cors(&mut resp, self.o.cors_origin.as_ref(), &req);
        resp
    }

    /// GET /notifications/live: server-sent events.
    pub fn notifications_sse(&self, req: HttpRequest) -> HttpResponse {
        let sub = self.o.notifications_sub.as_ref().and_then(|f| f());
        let mut resp = match sub {
            None => {
                let mut r = HttpResponse::new(204);
                r.set_header("Content-Type", "text/event-stream");
                r.set_header("Cache-Control", "no-cache");
                r.set_header("Connection", "keep-alive");
                r
            }
            Some(sub) => {
                let mut r = HttpResponse::new(200);
                r.set_header("Content-Type", "text/event-stream");
                r.set_header("Cache-Control", "no-cache");
                r.set_header("Connection", "keep-alive");
                r.body = http::Body::Stream(Box::new(move |w: &mut dyn http::StreamWriter| {
                    w.flush()?;
                    loop {
                        match sub.rx.recv_timeout(Duration::from_secs(1)) {
                            Ok(n) => {
                                let mut s = String::from("data: ");
                                n.write_json(&mut s);
                                s.push_str("\n\n");
                                w.write_all(s.as_bytes())?;
                                w.flush()?;
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                if w.peer_closed() {
                                    return Ok(());
                                }
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        }
                    }
                }));
                r
            }
        };
        crate::httputil::set_cors(&mut resp, self.o.cors_origin.as_ref(), &req);
        resp
    }

    /// POST /read.
    pub fn remote_read(&self, req: HttpRequest) -> HttpResponse {
        match &self.o.remote_read_handler {
            Some(h) => h(req),
            None => crate::route::http_error("not found", 404),
        }
    }

    /// POST /write.
    pub fn remote_write(&self, req: HttpRequest) -> HttpResponse {
        match &self.o.remote_write_handler {
            Some(h) => h(req),
            None => crate::route::http_error("remote write receiver needs to be enabled with --web.enable-remote-write-receiver", 404),
        }
    }

    /// POST /otlp/v1/metrics.
    pub fn otlp_write(&self, req: HttpRequest) -> HttpResponse {
        match &self.o.otlp_write_handler {
            Some(h) => h(req),
            None => crate::route::http_error("otlp write receiver needs to be enabled with --web.enable-otlp-receiver", 404),
        }
    }

    /// POST/PUT /admin/tsdb/delete_series.
    pub fn delete_series(&self, r: &ApiRequest) -> ApiFuncResult {
        if !self.o.enable_admin {
            return ApiFuncResult::error(ApiError::msg(ERROR_UNAVAILABLE, "admin APIs disabled"));
        }
        let matches = r.form_values("match[]");
        if matches.is_empty() {
            return ApiFuncResult::error(ApiError::msg(ERROR_BAD_DATA, "no match[] parameter provided"));
        }
        let start = match parse_time_param(r, "start", min_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "start"),
        };
        let end = match parse_time_param(r, "end", max_time()) {
            Ok(t) => t,
            Err(e) => return invalid_param_error(e, "end"),
        };
        let db = match self.db() {
            Ok(d) => d,
            Err(e) => return e,
        };
        for s in &matches {
            let ms = match self.parser.parse_metric_selector(s) {
                Ok(m) => m,
                Err(e) => return invalid_param_error(e, "match[]"),
            };
            if let Err(e) = db.delete(&r.ctx, start.unix_milli(), end.unix_milli(), &ms) {
                return ApiFuncResult::error(ApiError::new(ERROR_INTERNAL, e));
            }
        }
        ApiFuncResult::none()
    }

    /// POST/PUT /admin/tsdb/snapshot.
    pub fn snapshot(&self, r: &ApiRequest) -> ApiFuncResult {
        if !self.o.enable_admin {
            return ApiFuncResult::error(ApiError::msg(ERROR_UNAVAILABLE, "admin APIs disabled"));
        }
        let mut skip_head = false;
        let v = r.form_value("skip_head");
        if !v.is_empty() {
            match params::parse_bool(v) {
                Ok(b) => skip_head = b,
                Err(e) => return invalid_param_error(format!("unable to parse boolean: {}", e), "skip_head"),
            }
        }
        let db = match self.db() {
            Ok(d) => d,
            Err(e) => return e,
        };
        let now = Time::now();
        let snapdir = std::path::Path::new(&self.o.db_dir).join("snapshots");
        let name = format!("{}-{:016x}", format_snapshot_time(now), random_i63());
        let dir = snapdir.join(&name);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            return ApiFuncResult::error(ApiError::msg(ERROR_INTERNAL, format!("create snapshot directory: {}", go_io_error(&e, &dir))));
        }
        if let Err(e) = db.snapshot(&dir, !skip_head) {
            return ApiFuncResult::error(ApiError::new(ERROR_INTERNAL, Error::wrap("create snapshot", e)));
        }
        ApiFuncResult::ok(Data::Snapshot(name))
    }

    /// POST/PUT /admin/tsdb/clean_tombstones.
    pub fn clean_tombstones(&self, _r: &ApiRequest) -> ApiFuncResult {
        if !self.o.enable_admin {
            return ApiFuncResult::error(ApiError::msg(ERROR_UNAVAILABLE, "admin APIs disabled"));
        }
        let db = match self.db() {
            Ok(d) => d,
            Err(e) => return e,
        };
        match db.clean_tombstones() {
            Ok(()) => ApiFuncResult::none(),
            Err(e) => ApiFuncResult::error(ApiError::new(ERROR_INTERNAL, e)),
        }
    }
}

impl ApiFuncResult {
    /// Closes the query after the response is written (Go's `qry.Close` finalizer).
    fn with_close(mut self, mut q: Box<dyn ApiQuery>) -> ApiFuncResult {
        if self.err.is_some() && self.data.is_none() {
            // Go returns the finalizer with execution errors too.
        }
        self.finalizer = Some(Box::new(move || q.close()));
        self
    }
}

fn go_duration_to_std(d: GoDuration) -> Duration {
    if d <= 0 { Duration::ZERO } else { Duration::from_nanos(d as u64) }
}

/// Go `strconv.ParseUint(v, 10, 64)` with Go's error text.
fn parse_uint(v: &str) -> Result<u64, String> {
    match v.parse::<u64>() {
        Ok(n) if !v.starts_with('+') => Ok(n),
        Err(e) if matches!(e.kind(), std::num::IntErrorKind::PosOverflow) && v.bytes().all(|c| c.is_ascii_digit()) => {
            Err(format!("strconv.ParseUint: parsing {}: value out of range", go_quote(v)))
        }
        _ => Err(format!("strconv.ParseUint: parsing {}: invalid syntax", go_quote(v))),
    }
}

/// Go `aboveCeiling`.
fn above_ceiling(name: &str, requested: u64, ceiling: u64) -> Result<(), String> {
    if requested == 0 || ceiling == 0 || requested <= ceiling {
        return Ok(());
    }
    Err(PromqlError::LimitAboveCeiling { limit: name.to_string(), requested: requested.to_string(), ceiling: ceiling.to_string() }.to_string())
}

/// Go `queryConstructionError`.
pub fn query_construction_error(err: Error) -> ApiError {
    match PromqlError::find(&err) {
        Some(PromqlError::LimitAboveCeiling { .. }) => ApiError::new(ERROR_BAD_DATA, err),
        Some(PromqlError::QueryTimeout(_)) | Some(PromqlError::QueryCanceled(_)) => return_api_error(err),
        _ => ApiError::new(ERROR_BAD_DATA, Error::wrap(format!("invalid parameter {}", go_quote("query")), err)),
    }
}

/// The start, end and step of a range query (shared by query_range and query_range_cost).
fn parse_range(r: &ApiRequest) -> Result<(Time, Time, GoDuration), ApiFuncResult> {
    let start = parse_time(r.form_value("start")).map_err(|e| invalid_param_error(e, "start"))?;
    let end = parse_time(r.form_value("end")).map_err(|e| invalid_param_error(e, "end"))?;
    if end.before(&start) {
        return Err(invalid_param_error("end timestamp must not be before start time", "end"));
    }
    let step = parse_duration(r.form_value("step")).map_err(|e| invalid_param_error(e, "step"))?;
    if step <= 0 {
        return Err(invalid_param_error("zero or negative query resolution step widths are not accepted. Try a positive integer", "step"));
    }
    if end.sub(&start) / step > 11000 {
        return Err(ApiFuncResult::error(ApiError::msg(
            ERROR_BAD_DATA,
            "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)",
        )));
    }
    Ok((start, end, step))
}

/// Go `parseListRulesPaginationRequest`.
fn parse_list_rules_pagination_request(r: &ApiRequest) -> Result<(i64, String), ApiFuncResult> {
    let max_groups = r.query_value("group_limit");
    let next_token = r.query_value("group_next_token");
    if !next_token.is_empty() && max_groups.is_empty() {
        return Err(invalid_param_error("group_limit needs to be present in order to paginate over the groups", "group_next_token"));
    }
    let mut parsed: i64 = -1;
    if !max_groups.is_empty() {
        parsed = match max_groups.parse::<i32>() {
            Ok(v) => v as i64,
            Err(e) => {
                let msg = if matches!(e.kind(), std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow) {
                    format!("strconv.ParseInt: parsing {}: value out of range", go_quote(max_groups))
                } else {
                    format!("strconv.ParseInt: parsing {}: invalid syntax", go_quote(max_groups))
                };
                return Err(invalid_param_error(format!("group_limit needs to be a valid number: {}", msg), "group_limit"));
            }
        };
        if parsed <= 0 {
            return Err(invalid_param_error("group_limit needs to be greater than 0", "group_limit"));
        }
    }
    if parsed > 0 {
        return Ok((parsed, next_token.to_string()));
    }
    Ok((-1, String::new()))
}

/// Go `rulesAlertsToAPIAlerts`.
pub fn rules_alerts_to_api_alerts(alerts: &[RuleAlert]) -> Vec<Alert> {
    alerts
        .iter()
        .map(|a| {
            let mut v = String::new();
            jsonutil::write_float_e(a.value, &mut v);
            Alert {
                labels: a.labels.clone(),
                annotations: a.annotations.clone(),
                state: a.state.clone(),
                active_at: Some(a.active_at),
                keep_firing_since: if a.keep_firing_since.is_zero() { None } else { Some(a.keep_firing_since) },
                value: v,
            }
        })
        .collect()
}

/// `time.Now().UTC().Format("20060102T150405Z0700")`.
fn format_snapshot_time(t: Time) -> String {
    let s = t.format_rfc3339();
    // "2006-01-02T15:04:05Z" -> "20060102T150405Z"
    s.chars().filter(|c| *c != '-' && *c != ':').collect()
}

/// Go `rand.Int63()`.
fn random_i63() -> i64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) >> 1) as i64
}

/// Go's `*PathError` text for `os.MkdirAll`.
fn go_io_error(e: &std::io::Error, p: &std::path::Path) -> String {
    let msg = match e.kind() {
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        _ => {
            let s = e.to_string();
            match s.find(" (os error") {
                Some(i) => s[..i].to_lowercase(),
                None => s,
            }
        }
    };
    format!("mkdir {}: {}", p.display(), msg)
}

