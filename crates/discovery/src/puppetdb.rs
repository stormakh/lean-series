//! Port of `discovery/puppetdb`: targets from the resources a PQL query returns (POST to `/pdb/query/v4`),
//! refreshed at an interval.
//!
//! Differs from Go: parameters are read in JSON order; when two parameter names sanitize to the same label name,
//! the last one wins (Go: random map order). A request in flight is not aborted on cancel; it ends at its
//! timeout (the refresh interval, as Go's client timeout).
use std::sync::Arc;
use std::time::Duration;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet, META_LABEL_PREFIX};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::linode::gojson::{self, Ctx};
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, join_tags, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

const PDB_LABEL: &str = "__meta_puppetdb_";
const SEPARATOR: &str = ",";

fn pdb_label(name: &str) -> String {
    format!("{PDB_LABEL}{name}")
}

/// Go `puppetdb.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub http_client_config: HttpClientConfig,
    pub refresh_interval: yaml::Duration,
    pub url: String,
    pub query: String,
    pub include_parameters: bool,
    pub port: i64,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { http_client_config: HttpClientConfig::default_config(), refresh_interval: yaml::Duration::from_secs(60), port: 80, ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "puppetdb.plain", [
    (inline http_client_config)
    (refresh_interval: "refresh_interval", omitempty)
    (url: "url")
    (query: "query")
    (include_parameters: "include_parameters")
    (port: "port")
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
            if self.query.is_empty() {
                return Err(YError::Msg("query missing".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "puppetdb.plain".to_string()
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
    const GO_TYPE: &'static str = "puppetdb.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "puppetdb"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(PuppetdbMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `puppetdbMetrics`.
pub struct PuppetdbMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for PuppetdbMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

// ---------------- resources.go ----------------

/// Go `puppetdb.Resource`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resource {
    pub certname: String,
    pub resource: String,
    pub type_: String,
    pub title: String,
    pub exported: bool,
    pub tags: Vec<String>,
    pub file: String,
    pub environment: String,
    /// Go `Parameters` (`map[string]any`): the decoded JSON object, duplicate keys removed (last wins).
    pub parameters: Vec<(String, Json)>,
}

/// A JSON object as a Go `map[string]any`: one entry per key, the last value wins.
fn go_map(o: &[(String, Json)]) -> Vec<(String, Json)> {
    let mut out: Vec<(String, Json)> = Vec::with_capacity(o.len());
    for (k, v) in o {
        match out.iter_mut().find(|(ok, _)| ok == k) {
            Some(e) => e.1 = v.clone(),
            None => out.push((k.clone(), v.clone())),
        }
    }
    out
}

/// Go `json.Unmarshal(b, &[]Resource)`.
pub fn decode_resources(b: &[u8]) -> Result<Vec<Resource>, String> {
    let v = gojson::parse(b)?;
    let top = Ctx::top();
    let mut out = Vec::new();
    for e in gojson::array(&v, &top, "[]puppetdb.Resource")? {
        let mut r = Resource::default();
        if let Some(o) = gojson::object(e, &top, "puppetdb.Resource")? {
            for (k, v) in o {
                let f = |n: &str| top.field("Resource", n);
                let k = k.to_ascii_lowercase();
                match k.as_str() {
                    "certname" => r.certname = gojson::string(v, &f("certname"))?,
                    "resource" => r.resource = gojson::string(v, &f("resource"))?,
                    "type" => r.type_ = gojson::string(v, &f("type"))?,
                    "title" => r.title = gojson::string(v, &f("title"))?,
                    "exported" => r.exported = gojson::bool(v, &f("exported"))?,
                    "tags" => r.tags = gojson::strings(v, &f("tags"))?,
                    "file" => r.file = gojson::string(v, &f("file"))?,
                    "environment" => r.environment = gojson::string(v, &f("environment"))?,
                    "parameters" => r.parameters = gojson::object(v, &f("parameters"), "puppetdb.Parameters")?.map(go_map).unwrap_or_default(),
                    _ => {}
                }
            }
        }
        out.push(r);
    }
    Ok(out)
}

/// A Go `any` scalar as Prometheus formats it: string, bool, float64 ('g', -1). None for other types.
fn scalar_label(v: &Json) -> Option<String> {
    match v {
        Json::Str(s) => Some(s.clone()),
        Json::Bool(b) => Some(b.to_string()),
        // encoding/json decodes every number into `any` as float64
        Json::Int(i) => Some(yaml::format_float_g(*i as f64)),
        Json::Float(f) => Some(yaml::format_float_g(*f)),
        _ => None,
    }
}

/// Go `Parameters.toLabels`.
pub fn parameters_to_labels(p: &[(String, Json)]) -> LabelSet {
    let mut labels = LabelSet::new();
    for (k, v) in p {
        let label_value = match v {
            Json::Arr(a) => {
                if a.is_empty() {
                    continue;
                }
                let values: Vec<String> = a.iter().map(|v| scalar_label(v).unwrap_or_default()).collect();
                values.join(SEPARATOR)
            }
            Json::Obj(o) => {
                let prefix = sanitize_label_name(&format!("{k}_"));
                for (subk, subv) in parameters_to_labels(&go_map(o)).0 {
                    labels.insert(format!("{prefix}{subk}"), subv);
                }
                String::new()
            }
            v => match scalar_label(v) {
                Some(s) => s,
                None => continue,
            },
        };
        if label_value.is_empty() {
            continue;
        }
        labels.insert(sanitize_label_name(k), label_value);
    }
    labels
}

// ---------------- puppetdb.go ----------------

/// The refresh function of the PuppetDB discoverer (Go `Discovery.refresh` and its state).
pub struct PuppetdbRefresher {
    url: String,
    query: String,
    port: i64,
    include_parameters: bool,
    client: net::Client,
}

impl Refresher for PuppetdbRefresher {
    fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let body = Json::obj().with("query", self.query.as_str()).stringify();
        let mut req = net::Request::new("POST", &self.url, body.into_bytes()).map_err(|e| e.to_string())?;
        req.header.set("User-Agent", crate::http::USER_AGENT);
        req.header.set("Accept", "application/json");
        req.header.set("Content-Type", "application/json");
        let resp = self.client.do_request(req).map_err(|e| e.to_string())?;
        if resp.status != 200 {
            return Err(format!("server returned HTTP status {}", resp.status_text));
        }
        let ct = resp.header.get_str("Content-Type");
        if !crate::http::match_content_type(ct) {
            return Err(format!("unsupported content type {ct}"));
        }
        let resources = decode_resources(&resp.body)?;

        let mut tg = Group {
            // Use a pseudo-URL as source.
            source: format!("{}?query={}", self.url, self.query),
            ..Default::default()
        };
        for resource in resources {
            let mut labels: LabelSet = [
                (pdb_label("query"), self.query.clone()),
                (pdb_label("certname"), resource.certname.clone()),
                (pdb_label("resource"), resource.resource.clone()),
                (pdb_label("type"), resource.type_.clone()),
                (pdb_label("title"), resource.title.clone()),
                (pdb_label("exported"), resource.exported.to_string()),
                (pdb_label("file"), resource.file.clone()),
                (pdb_label("environment"), resource.environment.clone()),
            ]
            .into_iter()
            .collect();
            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&resource.certname, self.port as u64));
            if !resource.tags.is_empty() {
                // We surround the separated list with the separator as well. This way regular expressions
                // in relabeling rules don't have to consider tag positions.
                labels.insert(pdb_label("tags"), join_tags(&resource.tags, SEPARATOR));
            }
            // Parameters are not included by default. This should only be enabled on select resources as it
            // might expose secrets on the Prometheus UI for certain resources.
            if self.include_parameters {
                for (k, v) in parameters_to_labels(&resource.parameters).0 {
                    labels.insert(format!("{META_LABEL_PREFIX}puppetdb_parameter_{k}"), v);
                }
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `puppetdb.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<PuppetdbRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<PuppetdbMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut client = net::new_client_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        client.timeout = Some(conf.refresh_interval.to_std());
        let mut u = yaml::url::parse(&conf.url)?;
        u.path = path_join(&u.path, "pdb/query/v4");
        let refresher = PuppetdbRefresher { url: u.to_url_string(), query: conf.query.clone(), port: conf.port, include_parameters: conf.include_parameters, client };
        let interval: Duration = conf.refresh_interval.to_std();
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "puppetdb",
            set_name: &opts.set_name,
            interval,
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `d.url`: the query endpoint.
    pub fn url(&mut self) -> String {
        self.inner.refresher().url.clone()
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// Go `path.Join(a, b)`.
pub(crate) fn path_join(a: &str, b: &str) -> String {
    let joined = match (a.is_empty(), b.is_empty()) {
        (true, true) => return String::new(),
        (true, false) => b.to_string(),
        (false, true) => a.to_string(),
        (false, false) => format!("{a}/{b}"),
    };
    net::config::clean_path(&joined)
}
