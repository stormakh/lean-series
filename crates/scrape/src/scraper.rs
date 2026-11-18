//! The HTTP side of a scrape (Go `scraper`, `targetScraper`, `acceptHeader`, `acceptEncodingHeader`).

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use config::{EscapingScheme, ScrapeProtocol};
use storage::{ERR_CANCELED, ERR_DEADLINE_EXCEEDED, Error, Sentinel};

use crate::ctx::Ctx;
use crate::metrics::ScrapeMetrics;
use crate::target::Target;
use crate::util::format_float_f;

static BODY_SIZE_LIMIT: Sentinel = Sentinel::new("body size limit exceeded");
static ZSTD_NOT_ENABLED: Sentinel =
    Sentinel::new("received a zstd-compressed response, but the \"zstd-scrape\" feature flag is not enabled");
/// Go `errBodySizeLimit`.
pub const ERR_BODY_SIZE_LIMIT: Error = Error::Sentinel(&BODY_SIZE_LIMIT);
/// Go `errZstdNotEnabled`.
pub const ERR_ZSTD_NOT_ENABLED: Error = Error::Sentinel(&ZSTD_NOT_ENABLED);

/// Go `zstdMaxWindowSize` (8 MiB).
pub const ZSTD_MAX_WINDOW_SIZE: usize = 8 << 20;

/// Go `UserAgent` (`version.PrometheusUserAgent()`).
pub const USER_AGENT: &str = "Prometheus/3.16.0";

/// Retrieves samples and accepts a status report at the end (Go `scraper`).
pub trait Scraper: Send + Sync {
    /// Sends the request. The response is `None` for scrapers that produce the body in `read_response`.
    fn scrape(&self, ctx: &Ctx) -> Result<Option<net::Response>, Error>;
    /// Writes the (decompressed) body into `w` and returns the content type.
    fn read_response(&self, ctx: &Ctx, resp: Option<net::Response>, w: &mut Vec<u8>) -> Result<String, Error>;
    /// Records the result of a scrape.
    fn report(&self, start: SystemTime, dur: Duration, err: Option<&Error>);
    /// The time until the next scrape cycle, at wall clock `now`.
    fn offset(&self, interval: Duration, offset_seed: u64, now: SystemTime) -> Duration;
    /// The target URL for logs (Go `fmt.Stringer` on the scraper), if any.
    fn target_string(&self) -> Option<String> {
        None
    }
}

/// Go `model.EscapingScheme.String()`.
pub fn escaping_scheme_string(s: EscapingScheme) -> &'static str {
    match s {
        EscapingScheme::NoEscaping => "allow-utf-8",
        EscapingScheme::UnderscoreEscaping => "underscores",
        EscapingScheme::DotsEscaping => "dots",
        EscapingScheme::ValueEncodingEscaping => "values",
    }
}

/// Go `acceptHeader`: the Accept header for the protocols in order of preference.
pub fn accept_header(sps: &[ScrapeProtocol], scheme: EscapingScheme) -> String {
    let mut vals = Vec::with_capacity(sps.len() + 1);
    let mut weight = config::SCRAPE_PROTOCOLS_HEADERS.len() + 1;
    for sp in sps {
        let mut val = sp.header().unwrap_or("").to_string();
        if sp.as_str() == config::PROMETHEUS_TEXT_1_0_0
            || sp.as_str() == config::OPEN_METRICS_TEXT_1_0_0
            || sp.as_str() == config::OPEN_METRICS_TEXT_2_0_0
        {
            val.push_str(";escaping=");
            val.push_str(escaping_scheme_string(scheme));
        }
        val.push_str(&format!(";q=0.{weight}"));
        vals.push(val);
        weight -= 1;
    }
    vals.push(format!("*/*;q=0.{weight}"));
    vals.join(",")
}

/// Go `acceptEncodingHeader`.
pub fn accept_encoding_header(enable_compression: bool, enable_zstd: bool) -> &'static str {
    if enable_compression {
        if enable_zstd {
            return "zstd,gzip";
        }
        return "gzip";
    }
    "identity"
}

/// Converts a client error to a scrape error; timeouts wrap `context deadline exceeded`.
pub fn net_error(e: net::Error) -> Error {
    match e.kind {
        net::ErrorKind::Timeout => Error::wrapped(e.msg, ERR_DEADLINE_EXCEEDED),
        net::ErrorKind::BodyTooLarge => ERR_BODY_SIZE_LIMIT,
        _ => Error::msg(e.msg),
    }
}

/// Scrapes a target over HTTP (Go `targetScraper`).
pub struct TargetScraper {
    pub target: Option<Arc<Target>>,
    pub client: net::Client,
    pub timeout: Duration,
    /// 0 or less: no limit.
    pub body_size_limit: i64,
    pub accept_header: String,
    pub accept_encoding_header: String,
    pub enable_zstd: bool,
    pub logger: discovery::Logger,
    pub metrics: Arc<ScrapeMetrics>,
    req: Mutex<Option<net::Request>>,
}

impl TargetScraper {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target: Option<Arc<Target>>,
        client: net::Client,
        timeout: Duration,
        body_size_limit: i64,
        accept_header: String,
        accept_encoding_header: String,
        enable_zstd: bool,
        logger: discovery::Logger,
        metrics: Arc<ScrapeMetrics>,
    ) -> TargetScraper {
        TargetScraper {
            target,
            client,
            timeout,
            body_size_limit,
            accept_header,
            accept_encoding_header,
            enable_zstd,
            logger,
            metrics,
            req: Mutex::new(None),
        }
    }

    fn url_string(&self) -> String {
        self.target.as_ref().map(|t| t.url_string()).unwrap_or_default()
    }

    fn request(&self) -> Result<net::Request, Error> {
        let mut g = self.req.lock().unwrap();
        if let Some(r) = &*g {
            return Ok(r.clone());
        }
        let mut req = net::Request::get(&self.url_string()).map_err(|e| Error::msg(e.msg))?;
        req.header.add("Accept", &self.accept_header);
        req.header.add("Accept-Encoding", &self.accept_encoding_header);
        req.header.set("User-Agent", USER_AGENT);
        req.header.set("X-Prometheus-Scrape-Timeout-Seconds", &format_float_f(self.timeout.as_secs_f64()));
        req.decompress = false;
        *g = Some(req.clone());
        Ok(req)
    }

    fn limit(&self) -> usize {
        if self.body_size_limit <= 0 { usize::MAX } else { self.body_size_limit as usize }
    }
}

/// Go's message for a request that its context ended: `Get "url": context canceled`.
fn ctx_error(ctx: &Ctx, url: &str) -> Option<Error> {
    let e = ctx.err()?;
    Some(Error::wrapped(format!("Get {}: {}", yaml::go_quote(url), e), e))
}

fn copy_limited(r: &mut dyn Read, w: &mut Vec<u8>, limit: usize) -> std::io::Result<usize> {
    let start = w.len();
    let mut buf = [0u8; 32 * 1024];
    loop {
        let room = limit.saturating_sub(w.len() - start);
        if room == 0 {
            break;
        }
        let cap = buf.len().min(room);
        let n = r.read(&mut buf[..cap])?;
        if n == 0 {
            break;
        }
        w.extend_from_slice(&buf[..n]);
    }
    Ok(w.len() - start)
}

/// Go's gzip error texts.
fn gzip_error(e: &std::io::Error) -> Error {
    let s = e.to_string();
    let msg = if e.kind() == std::io::ErrorKind::UnexpectedEof {
        "unexpected EOF".to_string()
    } else if s.contains("invalid gzip header") {
        "gzip: invalid header".to_string()
    } else if s.contains("corrupt deflate stream") || s.contains("invalid") {
        "flate: corrupt input before offset 0".to_string()
    } else {
        s
    };
    Error::msg(msg)
}

impl Scraper for TargetScraper {
    fn scrape(&self, ctx: &Ctx) -> Result<Option<net::Response>, Error> {
        let mut req = self.request()?;
        if let Some(e) = ctx_error(ctx, &req.url.to_url_string()) {
            return Err(e);
        }
        if let Some(d) = ctx.deadline() {
            let now = std::time::Instant::now();
            req.timeout = Some(d.saturating_duration_since(now));
        }
        // The raw body may not exceed the limit either (it is at most the decoded size for identity bodies).
        if self.body_size_limit > 0 {
            req.max_body_bytes = Some(self.limit());
        }
        let url = req.url.to_url_string();
        match self.client.do_request(req) {
            Ok(r) => Ok(Some(r)),
            Err(e) => {
                if e.kind == net::ErrorKind::BodyTooLarge {
                    self.metrics.target_scrape_exceeded_body_size_limit.get().inc();
                    return Err(ERR_BODY_SIZE_LIMIT);
                }
                if let Some(ce) = ctx_error(ctx, &url)
                    && ctx.err().is_some_and(|x| x.is(&ERR_CANCELED))
                {
                    return Err(ce);
                }
                Err(net_error(e))
            }
        }
    }

    fn read_response(&self, _ctx: &Ctx, resp: Option<net::Response>, w: &mut Vec<u8>) -> Result<String, Error> {
        let Some(resp) = resp else {
            return Err(Error::msg("no response"));
        };
        if resp.status != 200 {
            return Err(Error::msg(format!("server returned HTTP status {}", resp.status_text)));
        }
        let limit = self.limit();
        let content_type = resp.header.get_str("Content-Type").to_string();
        let n = match resp.header.get_str("Content-Encoding") {
            "gzip" => {
                let mut d = flate2::read::GzDecoder::new(&resp.body[..]);
                copy_limited(&mut d, w, limit).map_err(|e| gzip_error(&e))?
            }
            "zstd" => {
                if !self.enable_zstd {
                    return Err(ERR_ZSTD_NOT_ENABLED);
                }
                match &self.target {
                    None => self.logger.debug("Using zstd-compressed scrape response", &[]),
                    Some(t) => self.logger.debug("Using zstd-compressed scrape response", &[("target", &t.url().redacted())]),
                }
                let mut d = zstd::stream::read::Decoder::new(&resp.body[..]).map_err(|e| Error::msg(e.to_string()))?;
                d.window_log_max(23).map_err(|e| Error::msg(e.to_string()))?;
                copy_limited(&mut d, w, limit).map_err(|e| Error::msg(zstd_error_text(&e)))?
            }
            _ => {
                let n = resp.body.len().min(limit);
                if w.is_empty() && n == resp.body.len() {
                    *w = resp.body;
                } else {
                    w.extend_from_slice(&resp.body[..n]);
                }
                n
            }
        };
        if n >= limit {
            self.metrics.target_scrape_exceeded_body_size_limit.get().inc();
            return Err(ERR_BODY_SIZE_LIMIT);
        }
        Ok(content_type)
    }

    fn report(&self, start: SystemTime, dur: Duration, err: Option<&Error>) {
        if let Some(t) = &self.target {
            t.report(start, dur, err);
        }
    }

    fn offset(&self, interval: Duration, offset_seed: u64, now: SystemTime) -> Duration {
        match &self.target {
            Some(t) => t.offset_at(interval, offset_seed, now),
            None => Duration::ZERO,
        }
    }

    fn target_string(&self) -> Option<String> {
        self.target.as_ref().map(|t| t.url_string())
    }
}

fn zstd_error_text(e: &std::io::Error) -> String {
    let s = e.to_string();
    if s.contains("Frame requires too much memory") || s.contains("window") {
        // Go klauspost/compress: "window size exceeded".
        return "zstd: window size exceeded".to_string();
    }
    s
}
