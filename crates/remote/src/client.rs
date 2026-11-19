//! Port of `storage/remote/client.go`: the remote write and remote read HTTP clients.
//!
//! The HTTP layer is the `net` crate (blocking HTTP/1.1 + rustls) built from the `http_client_config`, with the
//! extra layers upstream adds: injected headers, SigV4, Azure AD. A request's `storage::Context` deadline and
//! the client timeout become the request timeout.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use discovery::metrics::{CounterVec, GaugeVec, HistogramVec, MetricVec, Opts};
use net::{HttpClientConfig, HttpClientOptions, RoundTripper, SigV4Config};
use storage::{BoxSeriesSet, Context, Error};

use crate::chunked::{ChunkedReader, ERR_EOF};
use crate::codec::{from_query_result, new_chunked_series_set, snappy_decode, snappy_encode, ConcreteSeriesSet};
use crate::prompb::{Message, Query, QueryResult, ReadRequest, ReadResponse, ResponseType};
pub use crate::stats::{parse_write_response_stats, WriteResponseStats};

pub const MAX_ERR_MSG_LEN: usize = 1024;
pub const REMOTE_WRITE_VERSION_HEADER: &str = "X-Prometheus-Remote-Write-Version";
pub const REMOTE_WRITE_VERSION1_HEADER_VALUE: &str = "0.1.0";
pub const REMOTE_WRITE_VERSION20_HEADER_VALUE: &str = "2.0.0";
pub const APP_PROTO_CONTENT_TYPE: &str = "application/x-protobuf";

/// Go `UserAgent` (`version.PrometheusUserAgent()`).
pub const USER_AGENT: &str = "Prometheus/3.16.0";

/// Go `remoteapi.WriteV1MessageType`.
pub const WRITE_V1_MESSAGE_TYPE: &str = "prometheus.WriteRequest";
/// Go `remoteapi.WriteV2MessageType`.
pub const WRITE_V2_MESSAGE_TYPE: &str = "io.prometheus.write.v2.Request";

/// Go `remoteWriteContentTypeHeaders`.
pub fn remote_write_content_type(msg: &str) -> &'static str {
    if msg == WRITE_V2_MESSAGE_TYPE { "application/x-protobuf;proto=io.prometheus.write.v2.Request" } else { APP_PROTO_CONTENT_TYPE }
}

/// Go `AcceptedResponseTypes`.
pub const ACCEPTED_RESPONSE_TYPES: [ResponseType; 2] = [ResponseType::STREAMED_XOR_CHUNKS, ResponseType::SAMPLES];

/// Go `defaultBackoff`.
pub const DEFAULT_BACKOFF: i64 = 0;

/// Go `RecoverableError`: the send may be retried, after `retry_after` when it is positive.
/// It travels as `storage::Error::Other`; find it with [`as_recoverable`] (Go `errors.As`).
#[derive(Clone, Debug)]
pub struct RecoverableError {
    pub err: Error,
    /// Go `model.Duration` in nanoseconds; may be negative (a Retry-After date in the past).
    pub retry_after: i64,
}

impl fmt::Display for RecoverableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.err)
    }
}

impl std::error::Error for RecoverableError {}

impl RecoverableError {
    pub fn new(err: Error, retry_after: i64) -> Error {
        Error::Other(Arc::new(RecoverableError { err, retry_after }))
    }
}

/// Go `errors.As(err, &RecoverableError{})`.
pub fn as_recoverable(err: &Error) -> Option<&RecoverableError> {
    let mut cur = err;
    loop {
        match cur {
            Error::Other(e) => return e.downcast_ref::<RecoverableError>(),
            Error::Wrapped(w) => cur = &w.1,
            _ => return None,
        }
    }
}

/// Go `WriteClient` (declared in `queue_manager.go`).
pub trait WriteClient: Send + Sync {
    /// Stores the encoded (compressed) request.
    fn store(&self, ctx: &Context, req: &[u8], retry_attempt: i32) -> Result<WriteResponseStats, Error>;
    /// Uniquely identifies the remote storage.
    fn name(&self) -> &str;
    /// The remote read or write endpoint.
    fn endpoint(&self) -> &str;
}

/// Go `ReadClient`.
pub trait ReadClient: Send + Sync {
    fn read(&self, ctx: &Context, query: &Query, sort_series: bool) -> Result<BoxSeriesSet, Error>;
    fn read_multiple(&self, ctx: &Context, queries: &[Query], sort_series: bool) -> Result<BoxSeriesSet, Error>;
}

/// Go `ClientConfig`.
#[derive(Clone, Default)]
pub struct ClientConfig {
    pub url: String,
    pub timeout: Duration,
    pub http_client_config: HttpClientConfig,
    pub sigv4_config: Option<SigV4Config>,
    pub azure_ad_config: Option<config::AzureAdConfig>,
    pub google_iam_config: Option<config::GoogleIamConfig>,
    pub headers: BTreeMap<String, String>,
    pub retry_on_rate_limit: bool,
    /// "" means remote write 1.0.
    pub write_proto_msg: String,
    pub chunked_read_limit: u64,
    pub round_robin_dns: bool,
    pub accepted_response_types: Vec<ResponseType>,
}

/// The read client metrics (Go package-level `remoteReadQueriesTotal`, `remoteReadQueries`,
/// `remoteReadQueryDuration`). Register [`ReadClientMetrics::collectors`] in the server registry.
pub struct ReadClientMetrics {
    pub queries_total: Arc<CounterVec>,
    pub queries: Arc<GaugeVec>,
    pub request_duration_seconds: Arc<HistogramVec>,
}

impl ReadClientMetrics {
    pub fn collectors(&self) -> Vec<Arc<dyn discovery::metrics::Collector>> {
        vec![self.queries_total.clone(), self.queries.clone(), self.request_duration_seconds.clone()]
    }
}

/// The process-wide read client metrics.
pub fn read_client_metrics() -> &'static ReadClientMetrics {
    static M: OnceLock<ReadClientMetrics> = OnceLock::new();
    M.get_or_init(|| ReadClientMetrics {
        queries_total: MetricVec::new_counter_vec(
            Opts::new("prometheus_remote_storage_read_client_queries_total", "The total number of remote read queries."),
            &["remote_name", "endpoint", "response_type", "code"],
        ),
        queries: MetricVec::new_gauge_vec(
            Opts::new("prometheus_remote_storage_remote_read_client_queries", "The number of in-flight remote read queries."),
            &["remote_name", "endpoint"],
        ),
        request_duration_seconds: MetricVec::new_histogram_vec(
            Opts::new(
                "prometheus_remote_storage_remote_read_client_request_duration_seconds",
                "Histogram of the latency for remote read requests. Note that for streamed responses this is only the duration of the initial call and does not include the processing of the stream.",
            ),
            &["remote_name", "endpoint", "response_type"],
            &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0],
        ),
    })
}

/// Go `injectHeadersRoundTripper`: sets the configured headers on every request.
struct InjectHeadersRoundTripper {
    headers: BTreeMap<String, String>,
    next: Arc<dyn RoundTripper>,
}

impl RoundTripper for InjectHeadersRoundTripper {
    fn round_trip(&self, mut req: net::Request) -> Result<net::Response, net::Error> {
        for (k, v) in &self.headers {
            req.header.set(k, v);
        }
        self.next.round_trip(req)
    }
}

/// Go `Client`: reads from and writes to a remote HTTP endpoint.
pub struct Client {
    remote_name: String,
    url_string: String,
    client: net::Client,
    timeout: Duration,
    retry_on_rate_limit: bool,
    chunked_read_limit: u64,
    accepted_response_types: Vec<ResponseType>,
    write_proto_msg: String,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Client({}, {})", self.remote_name, self.url_string)
    }
}

fn net_err(e: net::Error) -> Error {
    Error::Msg(e.to_string().into())
}

impl Client {
    /// Go `NewReadClient`.
    pub fn new_read_client(name: &str, conf: &ClientConfig) -> Result<Client, Error> {
        Self::new_read_client_with_options(name, conf, &HttpClientOptions::default())
    }

    /// Go `NewReadClient` with HTTP client options.
    pub fn new_read_client_with_options(name: &str, conf: &ClientConfig, opts: &HttpClientOptions) -> Result<Client, Error> {
        let mut client = net::new_client_from_config(&conf.http_client_config, opts).map_err(net_err)?;
        if !conf.headers.is_empty() {
            client.transport = Arc::new(InjectHeadersRoundTripper { headers: conf.headers.clone(), next: client.transport });
        }
        let accepted = if conf.accepted_response_types.is_empty() {
            ACCEPTED_RESPONSE_TYPES.to_vec()
        } else {
            conf.accepted_response_types.clone()
        };
        Ok(Client {
            remote_name: name.to_string(),
            url_string: conf.url.clone(),
            client,
            timeout: conf.timeout,
            retry_on_rate_limit: false,
            chunked_read_limit: conf.chunked_read_limit,
            accepted_response_types: accepted,
            write_proto_msg: String::new(),
        })
    }

    /// Go `NewWriteClient`.
    pub fn new_write_client(name: &str, conf: &ClientConfig) -> Result<Client, Error> {
        let mut opts = HttpClientOptions::default();
        if conf.round_robin_dns {
            opts = opts.with_dial(crate::dial_context::new_dial_context_with_round_robin_dns().dial_context_fn());
        }
        let mut client = net::new_client_from_config(&conf.http_client_config, &opts).map_err(net_err)?;
        let mut t = client.transport.clone();
        if !conf.headers.is_empty() {
            t = Arc::new(InjectHeadersRoundTripper { headers: conf.headers.clone(), next: t });
        }
        if let Some(sc) = &conf.sigv4_config {
            t = Arc::new(net::SigV4RoundTripper::new(sc, t, Default::default()).map_err(net_err)?);
        }
        if let Some(ac) = &conf.azure_ad_config {
            t = crate::azuread::new_azure_ad_round_tripper(ac, Some(t)).map_err(|e| Error::Msg(e.into()))?;
        }
        if conf.google_iam_config.is_some() {
            // Google IAM needs Google's OAuth2 libraries (application default credentials); not ported.
            return Err(Error::Msg("google_iam is not supported by this build".into()));
        }
        client.transport = t;
        let write_proto_msg =
            if conf.write_proto_msg.is_empty() { WRITE_V1_MESSAGE_TYPE.to_string() } else { conf.write_proto_msg.clone() };
        Ok(Client {
            remote_name: name.to_string(),
            url_string: conf.url.clone(),
            client,
            timeout: conf.timeout,
            retry_on_rate_limit: conf.retry_on_rate_limit,
            chunked_read_limit: 0,
            accepted_response_types: Vec::new(),
            write_proto_msg,
        })
    }

    /// The request timeout: the client timeout, cut to the context deadline.
    fn request_timeout(&self, ctx: &Context) -> (Duration, bool) {
        match ctx.deadline() {
            Some(d) => {
                let left = d.saturating_duration_since(Instant::now());
                if left < self.timeout { (left, false) } else { (self.timeout, true) }
            }
            None => (self.timeout, true),
        }
    }

    /// Go `ReadMultiple`... `executeReadRequest` and `handleReadResponse`.
    fn read_impl(&self, ctx: &Context, queries: &[Query], sort_series: bool) -> Result<BoxSeriesSet, Error> {
        let m = read_client_metrics();
        let in_flight = m.queries.with_label_values(&[&self.remote_name, &self.url_string]);
        in_flight.inc();
        struct Dec(Arc<discovery::metrics::Value>);
        impl Drop for Dec {
            fn drop(&mut self) {
                self.0.dec();
            }
        }
        let _dec = Dec(in_flight);

        let req = ReadRequest { queries: queries.to_vec(), accepted_response_types: self.accepted_response_types.clone() };
        let data = req.marshal();
        let compressed = snappy_encode(&data);
        let mut http_req = net::Request::new("POST", &self.url_string, compressed)
            .map_err(|e| Error::Wrapped(Arc::new((format!("unable to create request: {e}"), net_err(e)))))?;
        http_req.header.add("Content-Encoding", "snappy");
        http_req.header.add("Accept-Encoding", "snappy");
        http_req.header.set("Content-Type", "application/x-protobuf");
        http_req.header.set("User-Agent", USER_AGENT);
        http_req.header.set("X-Prometheus-Remote-Read-Version", "0.1.0");
        if let Some(e) = ctx.err() {
            return Err(Error::Wrapped(Arc::new((format!("error sending request: {e}"), e))));
        }
        let (timeout, ours) = self.request_timeout(ctx);
        http_req.timeout = Some(timeout);

        let start = Instant::now();
        let resp = match self.client.do_request(http_req) {
            Ok(r) => r,
            Err(e) => {
                let mut msg = format!("error sending request: {e}");
                if e.kind == net::ErrorKind::Timeout && ours {
                    // Go: context.WithTimeoutCause(ctx, timeout, fmt.Errorf("%w: request timed out after %s", ...)).
                    msg.push_str(&format!(": request timed out after {}", util::stats::go_duration_string(self.timeout)));
                }
                return Err(Error::Msg(msg.into()));
            }
        };

        if resp.status / 100 != 2 {
            let body = String::from_utf8_lossy(&resp.body);
            let err_str = body.trim_matches('\n').to_string();
            let inner = Error::Msg(err_str.into());
            return Err(Error::Wrapped(Arc::new((
                format!("remote server {} returned http status {}: {}", self.url_string, resp.status_text, inner),
                inner,
            ))));
        }

        let content_type = resp.header.get_str("Content-Type").to_string();
        let elapsed = start.elapsed().as_secs_f64();
        let code = resp.status.to_string();
        if content_type.starts_with("application/x-protobuf") {
            m.request_duration_seconds.with_label_values(&[&self.remote_name, &self.url_string, "sampled"]).observe(elapsed);
            m.queries_total.with_label_values(&[&self.remote_name, &self.url_string, "sampled", &code]).inc();
            handle_sampled_response(&req, &resp, sort_series)
        } else if content_type.starts_with("application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse") {
            m.request_duration_seconds.with_label_values(&[&self.remote_name, &self.url_string, "chunked"]).observe(elapsed);
            let mut min_start = i64::MAX;
            let mut max_end = i64::MIN;
            for q in queries {
                min_start = min_start.min(q.start_timestamp_ms);
                max_end = max_end.max(q.end_timestamp_ms);
            }
            let reader = ChunkedReader::new(Cursor::new(resp.body), self.chunked_read_limit, Vec::new());
            let total = m.queries_total.clone();
            let (name, url) = (self.remote_name.clone(), self.url_string.clone());
            Ok(new_chunked_series_set(reader, min_start, max_end, move |err: &Error| {
                let c = if err.is(&ERR_EOF) { code.as_str() } else { "aborted_stream" };
                total.with_label_values(&[&name, &url, "chunked", c]).inc();
            }))
        } else {
            m.request_duration_seconds.with_label_values(&[&self.remote_name, &self.url_string, "unsupported"]).observe(elapsed);
            m.queries_total.with_label_values(&[&self.remote_name, &self.url_string, "unsupported", &code]).inc();
            Err(Error::Msg(format!("unsupported content type: {content_type}").into()))
        }
    }
}

fn handle_sampled_response(req: &ReadRequest, resp: &net::Response, sort_series: bool) -> Result<BoxSeriesSet, Error> {
    let uncompressed =
        snappy_decode(&resp.body).map_err(|e| Error::Wrapped(Arc::new((format!("error reading response: {e}"), e))))?;
    let r = ReadResponse::unmarshal(&uncompressed).map_err(|e| {
        let inner = Error::Msg(e.to_string().into());
        Error::Wrapped(Arc::new((format!("unable to unmarshal response body: {e}"), inner)))
    })?;
    if r.results.len() != req.queries.len() {
        return Err(Error::Msg(format!("responses: want {}, got {}", req.queries.len(), r.results.len()).into()));
    }
    combine_query_results(r.results, sort_series)
}

/// Go `combineQueryResults`: one series set from several query results.
pub fn combine_query_results(results: Vec<QueryResult>, sort_series: bool) -> Result<BoxSeriesSet, Error> {
    if results.is_empty() {
        return Ok(Box::new(ConcreteSeriesSet::new(Vec::new())));
    }
    if results.len() == 1 {
        return Ok(from_query_result(sort_series, results.into_iter().next().unwrap()));
    }
    if sort_series {
        let mut sets = Vec::with_capacity(results.len());
        for r in results {
            let ss = from_query_result(true, r);
            if let Some(e) = ss.err() {
                return Err(Error::Wrapped(Arc::new((format!("error reading series from query result: {e}"), e))));
            }
            sets.push(ss);
        }
        return Ok(storage::new_merge_series_set(sets, 0, Arc::new(storage::chained_series_merge)));
    }
    let mut all = Vec::new();
    for r in results {
        let mut ss = from_query_result(false, r);
        while ss.next() {
            all.push(ss.at().clone());
        }
        if let Some(e) = ss.err() {
            return Err(Error::Wrapped(Arc::new((format!("error reading series from query result: {e}"), e))));
        }
    }
    Ok(Box::new(ConcreteSeriesSet::new(all)))
}

/// Go `retryAfterDuration`: the Retry-After header as nanoseconds (an HTTP date or whole seconds),
/// `DEFAULT_BACKOFF` when it cannot be parsed.
pub fn retry_after_duration(t: &str) -> i64 {
    // Go time.Parse(http.TimeFormat, t): "Mon, 02 Jan 2006 15:04:05 GMT" with the literal GMT zone.
    if let Some(unix) = t.ends_with(" GMT").then(|| http::parse_http_date(t)).flatten() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        // Go: model.Duration(time.Until(d).Seconds()) * time.Second: the seconds are truncated to an integer.
        let s = (unix as f64 - now) as i64;
        return s * 1_000_000_000;
    }
    match go_atoi(t) {
        Some(d) => d.saturating_mul(1_000_000_000),
        None => DEFAULT_BACKOFF,
    }
}

/// Go `strconv.Atoi`: optional sign, decimal digits only.
fn go_atoi(s: &str) -> Option<i64> {
    let b = s.strip_prefix(['+', '-']).unwrap_or(s);
    if b.is_empty() || !b.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse::<i64>().ok()
}

impl WriteClient for Client {
    /// Go `Store`: sends a batch of samples to the HTTP endpoint; the request is the encoded and compressed
    /// protobuf.
    fn store(&self, ctx: &Context, req: &[u8], attempt: i32) -> Result<WriteResponseStats, Error> {
        // Errors from NewRequest are from unparsable URLs, so are not recoverable.
        let mut http_req = net::Request::new("POST", &self.url_string, req.to_vec()).map_err(net_err)?;
        http_req.header.add("Content-Encoding", "snappy");
        http_req.header.set("Content-Type", remote_write_content_type(&self.write_proto_msg));
        http_req.header.set("User-Agent", USER_AGENT);
        if self.write_proto_msg == WRITE_V1_MESSAGE_TYPE {
            // Compatibility mode for 1.0.
            http_req.header.set(REMOTE_WRITE_VERSION_HEADER, REMOTE_WRITE_VERSION1_HEADER_VALUE);
        } else {
            http_req.header.set(REMOTE_WRITE_VERSION_HEADER, REMOTE_WRITE_VERSION20_HEADER_VALUE);
        }
        if attempt > 0 {
            http_req.header.set("Retry-Attempt", &attempt.to_string());
        }
        if let Some(e) = ctx.err() {
            return Err(RecoverableError::new(e, DEFAULT_BACKOFF));
        }
        http_req.timeout = Some(self.request_timeout(ctx).0);
        // Like Go's io.LimitReader on the error body, keep only what the error message needs.
        http_req.max_body_bytes = Some(MAX_ERR_MSG_LEN);
        http_req.truncate_body = true;

        let resp = match self.client.do_request(http_req) {
            Ok(r) => r,
            // Errors from Client.Do are from (for example) network errors, so are recoverable.
            Err(e) => return Err(RecoverableError::new(net_err(e), DEFAULT_BACKOFF)),
        };

        // Parsing errors of the stats headers are ignored, as upstream does.
        let (rs, _) = parse_write_response_stats(&resp.header);
        if resp.status / 100 == 2 {
            return Ok(rs);
        }
        let body = &resp.body[..resp.body.len().min(MAX_ERR_MSG_LEN)];
        let err = Error::Msg(format!("server returned HTTP status {}: {}", resp.status_text, String::from_utf8_lossy(body)).into());
        if resp.status / 100 == 5 || (self.retry_on_rate_limit && resp.status == 429) {
            return Err(RecoverableError::new(err, retry_after_duration(resp.header.get_str("Retry-After"))));
        }
        Err(err)
    }

    fn name(&self) -> &str {
        &self.remote_name
    }

    fn endpoint(&self) -> &str {
        &self.url_string
    }
}

impl ReadClient for Client {
    /// Go `Read`. `sort_series` is only respected for a samples response; chunked responses arrive sorted.
    fn read(&self, ctx: &Context, query: &Query, sort_series: bool) -> Result<BoxSeriesSet, Error> {
        self.read_impl(ctx, std::slice::from_ref(query), sort_series)
    }

    /// Go `ReadMultiple`: several queries in one request; one series set with the series of all queries.
    fn read_multiple(&self, ctx: &Context, queries: &[Query], sort_series: bool) -> Result<BoxSeriesSet, Error> {
        self.read_impl(ctx, queries, sort_series)
    }
}

impl Client {
    pub fn name(&self) -> &str {
        &self.remote_name
    }
    pub fn endpoint(&self) -> &str {
        &self.url_string
    }
}
