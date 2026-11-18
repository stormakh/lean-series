//! Port of `discovery/http`: target groups from an HTTP endpoint that returns JSON, refreshed at an interval.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled; it ends at its
//! timeout (the refresh interval, as Go's client timeout).
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use model::labelset::{LabelSet, META_LABEL_PREFIX};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry};
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// Go `version.PrometheusUserAgent()`.
pub const USER_AGENT: &str = "Prometheus/3.16.0";

fn match_content_type_re() -> &'static model::regexp::Regexp {
    static R: OnceLock<model::regexp::Regexp> = OnceLock::new();
    R.get_or_init(|| model::regexp::Regexp::must_compile(r#"^(?i:application\/json(;\s*charset=("utf-8"|utf-8))?)$"#))
}

/// Go `matchContentType.MatchString`.
pub fn match_content_type(s: &str) -> bool {
    match_content_type_re().is_match(s)
}

/// Go `http.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub url: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { http_client_config: HttpClientConfig::default_config(), refresh_interval: yaml::Duration::from_secs(60), url: String::new() }
}

yaml::yaml_struct!(SdConfig, "http.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval", omitempty)
    (url: "url")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.url.is_empty() {
                return Err(YError::Msg("URL is missing".into()));
            }
            let u = yaml::url::parse(&self.url).map_err(YError::Msg)?;
            if u.scheme != "http" && u.scheme != "https" {
                return Err(YError::Msg("URL scheme must be 'http' or 'https'".into()));
            }
            if u.host.is_empty() {
                return Err(YError::Msg("host is missing in URL".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "http.plain".to_string()
    }
}
impl yaml::Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "http.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "http"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        HttpMetrics::new(reg, rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `httpMetrics`.
pub struct HttpMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
    pub failures_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl HttpMetrics {
    pub fn new(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<HttpMetrics> {
        let failures_count = CounterVec::new_counter(Opts::new("prometheus_sd_http_failures_total", "Number of HTTP service discovery refresh failures."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![failures_count.clone()]);
        Arc::new(HttpMetrics { refresh_metrics: rmi, failures_count, registerer })
    }
}

impl DiscovererMetrics for HttpMetrics {
    fn register(&self) -> Result<(), String> {
        self.registerer.register_metrics()
    }
    fn unregister(&self) {
        self.registerer.unregister_metrics()
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh function of the HTTP discoverer (Go `Discovery.Refresh` and its state).
pub struct HttpRefresher {
    url: String,
    client: net::Client,
    refresh_interval: Duration,
    tg_last_length: usize,
    metrics: Arc<HttpMetrics>,
}

impl HttpRefresher {
    fn fail(&self, e: String) -> Result<Vec<Arc<Group>>, String> {
        self.metrics.failures_count.get().inc();
        Err(e)
    }
}

impl Refresher for HttpRefresher {
    fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut req = net::Request::get(&self.url).map_err(|e| e.to_string())?;
        req.header.set("User-Agent", USER_AGENT);
        req.header.set("Accept", "application/json");
        let secs = json::Json::Float(self.refresh_interval.as_secs_f64()).stringify();
        req.header.set("X-Prometheus-Refresh-Interval-Seconds", &secs);
        let resp = match self.client.do_request(req) {
            Ok(r) => r,
            Err(e) => return self.fail(e.to_string()),
        };
        if resp.status != 200 {
            return self.fail(format!("server returned HTTP status {}", resp.status_text));
        }
        let ct = resp.header.get_str("Content-Type");
        if !match_content_type(ct.trim()) {
            return self.fail(format!("unsupported content type {}", yaml::go_quote(ct)));
        }
        let groups = match Group::list_from_json(&resp.body) {
            Ok(g) => g,
            Err(e) => return self.fail(e),
        };
        let mut out: Vec<Arc<Group>> = Vec::with_capacity(groups.len());
        let label = format!("{META_LABEL_PREFIX}url");
        for (i, g) in groups.into_iter().enumerate() {
            let Some(mut g) = g else {
                return self.fail("nil target group item found".to_string());
            };
            g.source = url_source(&self.url, i);
            g.labels.insert(label.clone(), self.url.clone());
            out.push(Arc::new(g));
        }
        // Empty updates for sources that disappeared.
        let l = out.len();
        for i in l..self.tg_last_length {
            out.push(Arc::new(Group { targets: Vec::new(), labels: LabelSet::new(), source: url_source(&self.url, i) }));
        }
        self.tg_last_length = l;
        Ok(out)
    }
}

/// Go `http.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<HttpRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<HttpMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut client = net::new_client_from_config(&conf.http_client_config, &opts.http_client_options).map_err(|e| e.to_string())?;
        client.timeout = Some(conf.refresh_interval.to_std());
        let refresher = HttpRefresher { url: conf.url.clone(), client, refresh_interval: conf.refresh_interval.to_std(), tg_last_length: 0, metrics: m.clone() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "http",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `Discovery.Refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// The HTTP failures counter (Go `d.metrics.failuresCount`).
    pub fn failures_count(&mut self) -> f64 {
        self.inner.refresher().metrics.failures_count.get().get()
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// Go `urlSource`.
pub fn url_source(url: &str, i: usize) -> String {
    format!("{url}:{i}")
}
