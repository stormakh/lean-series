//! Port of `discovery/nomad`: targets from the Nomad service registrations, refreshed at an interval.
//!
//! The part of `github.com/hashicorp/nomad/api` that upstream uses (`NewClient`, `Services().List()` and
//! `Services().Get()`) is ported in [`api`] with the same request paths, query parameters, headers, JSON decoding
//! and error texts.
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::consul::goutil;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry, Value};
use crate::refresh::{self, Refresher};
use crate::sdutil::{join_host_port, join_tags};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// The prefix of the Nomad labels.
pub const NOMAD_LABEL: &str = "__meta_nomad_";
pub const NOMAD_ADDRESS: &str = "__meta_nomad_address";
pub const NOMAD_SERVICE: &str = "__meta_nomad_service";
pub const NOMAD_NAMESPACE: &str = "__meta_nomad_namespace";
pub const NOMAD_NODE_ID: &str = "__meta_nomad_node_id";
pub const NOMAD_DATACENTER: &str = "__meta_nomad_dc";
pub const NOMAD_SERVICE_ADDRESS: &str = "__meta_nomad_service_address";
pub const NOMAD_SERVICE_PORT: &str = "__meta_nomad_service_port";
pub const NOMAD_SERVICE_ID: &str = "__meta_nomad_service_id";
pub const NOMAD_TAGS: &str = "__meta_nomad_tags";

/// Go `nomad.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub allow_stale: bool,
    pub http_client_config: HttpClientConfig,
    pub namespace: String,
    pub refresh_interval: yaml::Duration,
    pub region: String,
    pub server: String,
    pub tag_separator: String,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        allow_stale: true,
        http_client_config: HttpClientConfig::default_config(),
        namespace: "default".to_string(),
        refresh_interval: yaml::Duration::from_secs(60),
        region: "global".to_string(),
        server: "http://localhost:4646".to_string(),
        tag_separator: ",".to_string(),
    }
}

yaml::yaml_struct!(SdConfig, "nomad.plain", [
    (allow_stale: "allow_stale")
    (inline http_client_config)
    (namespace: "namespace")
    (refresh_interval: "refresh_interval")
    (region: "region")
    (server: "server")
    (tag_separator: "tag_separator", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if goutil::trim_space(&self.server).is_empty() {
                return Err(YError::Msg("nomad SD configuration requires a server address".into()));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "nomad.plain".to_string()
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
    const GO_TYPE: &'static str = "nomad.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "nomad"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        NomadMetrics::new(reg, rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `nomadMetrics`.
pub struct NomadMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
    pub failures_count: Arc<Value>,
    registerer: MetricRegisterer,
}

impl NomadMetrics {
    /// Go `newDiscovererMetrics`.
    pub fn new(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<NomadMetrics> {
        let failures = CounterVec::new_counter(Opts::new("prometheus_sd_nomad_failures_total", "Number of nomad service discovery refresh failures."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![failures.clone()]);
        Arc::new(NomadMetrics { refresh_metrics: rmi, failures_count: failures.get(), registerer })
    }
}

impl DiscovererMetrics for NomadMetrics {
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

/// The part of `github.com/hashicorp/nomad/api` that Prometheus uses.
pub mod api {
    use json::Json;
    use yaml::url::GoUrl;

    use crate::consul::goutil::{self, StructTy, Ty, Values};

    /// Go `api.Config` (the fields Prometheus sets; the HTTP client is Prometheus's).
    #[derive(Clone, Debug, Default)]
    pub struct Config {
        pub address: String,
        pub namespace: String,
        pub region: String,
    }

    /// Go `api.Client`.
    pub struct Client {
        pub config: Config,
        url: GoUrl,
        http: net::Client,
    }

    /// Go `api.QueryOptions` (the field Prometheus sets).
    #[derive(Clone, Debug, Default)]
    pub struct QueryOptions {
        pub allow_stale: bool,
    }

    /// Go `api.NewClient` with `config.HttpClient` set. Only an empty address is taken from the defaults
    /// (`NOMAD_ADDR` or "http://127.0.0.1:4646").
    pub fn new_client(mut config: Config, http: net::Client) -> Result<Client, String> {
        if config.address.is_empty() {
            config.address = std::env::var("NOMAD_ADDR").ok().filter(|a| !a.is_empty()).unwrap_or_else(|| "http://127.0.0.1:4646".to_string());
        }
        let url = yaml::url::parse(&config.address).map_err(|e| format!("invalid address '{}': {}", config.address, e))?;
        Ok(Client { config, url, http })
    }

    /// Go `UnexpectedResponseError.Error()` for a response.
    fn unexpected_response_error(status: u16, body: &[u8]) -> String {
        let body = String::from_utf8_lossy(body);
        let body = goutil::trim_space(&body);
        let mut s = String::from("Unexpected response code");
        if !body.is_empty() || status != 0 {
            s.push_str(": ");
        }
        if status != 0 {
            s.push_str(&status.to_string());
            if !body.is_empty() {
                s.push(' ');
            }
        }
        if !body.is_empty() {
            s.push('(');
            s.push_str(body);
            s.push(')');
        }
        s
    }

    impl Client {
        /// Go `Client.query`: GET `endpoint` and decode the JSON body as `ty`.
        fn query(&self, endpoint: &str, ty: &Ty, q: &QueryOptions) -> Result<Json, String> {
            // Go `newRequest`
            let u = yaml::url::parse(endpoint)?;
            let mut url = GoUrl { scheme: self.url.scheme.clone(), user: self.url.user.clone(), host: self.url.host.clone(), path: u.path.clone(), raw_path: u.raw_path.clone(), ..Default::default() };
            if url.scheme == "unix" {
                url.scheme = "http".to_string();
                url.host = "127.0.0.1".to_string();
            }
            let mut params = Values::default();
            if !self.config.region.is_empty() {
                params.set("region", &self.config.region);
            }
            if !self.config.namespace.is_empty() {
                params.set("namespace", &self.config.namespace);
            }
            for (k, v) in u.query_pairs() {
                params.add(&k, &v);
            }
            // Go `setQueryOptions`
            if q.allow_stale {
                params.set("stale", "");
            }
            // Go `toHTTP`
            url.raw_query = params.encode();
            let user = url.user.clone();
            let mut req = net::Request { method: "GET".to_string(), url, header: net::Header::new(), host: String::new(), body: Vec::new(), timeout: None, max_body_bytes: None, decompress: false, redirect_hosts: Vec::new(), abort: None, body_sink: None };
            if let Some(ui) = user {
                req.set_basic_auth(&ui.username, ui.password().unwrap_or(""));
            }
            req.header.add("Accept-Encoding", "gzip");
            // Go `doRequest`
            let mut resp = self.http.do_request(req).map_err(|e| e.to_string())?;
            if resp.header.get_str("Content-Encoding") == "gzip" && !resp.body.is_empty() {
                resp.body = net::transport::decode_body("gzip", &resp.body, usize::MAX).map_err(|e| e.to_string())?;
            }
            // Go `requireOK`
            if resp.status != 200 {
                return Err(unexpected_response_error(resp.status, &resp.body));
            }
            // Go `decodeBody`
            if resp.header.get_str("Content-Length").trim() == "0" {
                return Err("Got 0 byte response with non-nil decode object".to_string());
            }
            goutil::decode_body(&resp.body, ty)
        }

        /// Go `Services().List(q)`.
        pub fn services_list(&self, q: &QueryOptions) -> Result<Vec<ServiceRegistrationListStub>, String> {
            static T: Ty = Ty::Slice(&Ty::Struct(&LIST_STUB_TY), "[]*api.ServiceRegistrationListStub");
            let v = self.query("/v1/services", &T, q)?;
            let stubs = v.as_arr().map(|a| a.as_slice()).unwrap_or(&[]);
            Ok(stubs
                .iter()
                .map(|s| ServiceRegistrationListStub {
                    namespace: goutil::str_field(s, "Namespace"),
                    services: goutil::field(s, "Services")
                        .and_then(Json::as_arr)
                        .map(|a| a.iter().map(|x| ServiceRegistrationStub { service_name: goutil::str_field(x, "ServiceName"), tags: goutil::str_slice(goutil::field(x, "Tags")) }).collect())
                        .unwrap_or_default(),
                })
                .collect())
        }

        /// Go `Services().Get(serviceName, q)`.
        pub fn services_get(&self, service_name: &str, q: &QueryOptions) -> Result<Vec<ServiceRegistration>, String> {
            static T: Ty = Ty::Slice(&Ty::Struct(&REGISTRATION_TY), "[]*api.ServiceRegistration");
            let v = self.query(&format!("/v1/service/{}", yaml::url::path_escape(service_name)), &T, q)?;
            let regs = v.as_arr().map(|a| a.as_slice()).unwrap_or(&[]);
            Ok(regs
                .iter()
                .map(|r| ServiceRegistration {
                    id: goutil::str_field(r, "ID"),
                    service_name: goutil::str_field(r, "ServiceName"),
                    namespace: goutil::str_field(r, "Namespace"),
                    node_id: goutil::str_field(r, "NodeID"),
                    datacenter: goutil::str_field(r, "Datacenter"),
                    job_id: goutil::str_field(r, "JobID"),
                    alloc_id: goutil::str_field(r, "AllocID"),
                    tags: goutil::str_slice(goutil::field(r, "Tags")),
                    address: goutil::str_field(r, "Address"),
                    port: goutil::int_field(r, "Port"),
                })
                .collect())
        }
    }

    /// Go `api.ServiceRegistrationListStub`. A null element (Go: a nil pointer) reads as the zero value.
    #[derive(Clone, Debug, Default)]
    pub struct ServiceRegistrationListStub {
        pub namespace: String,
        pub services: Vec<ServiceRegistrationStub>,
    }

    /// Go `api.ServiceRegistrationStub`.
    #[derive(Clone, Debug, Default)]
    pub struct ServiceRegistrationStub {
        pub service_name: String,
        pub tags: Vec<String>,
    }

    /// Go `api.ServiceRegistration`.
    #[derive(Clone, Debug, Default)]
    pub struct ServiceRegistration {
        pub id: String,
        pub service_name: String,
        pub namespace: String,
        pub node_id: String,
        pub datacenter: String,
        pub job_id: String,
        pub alloc_id: String,
        pub tags: Vec<String>,
        pub address: String,
        pub port: i64,
    }

    static STUB_TY: StructTy = StructTy { name: "ServiceRegistrationStub", go_type: "api.ServiceRegistrationStub", fields: &[("ServiceName", Ty::Str), ("Tags", Ty::Slice(&Ty::Str, "[]string"))] };
    static LIST_STUB_TY: StructTy = StructTy {
        name: "ServiceRegistrationListStub",
        go_type: "api.ServiceRegistrationListStub",
        fields: &[("Namespace", Ty::Str), ("Services", Ty::Slice(&Ty::Struct(&STUB_TY), "[]*api.ServiceRegistrationStub"))],
    };
    static REGISTRATION_TY: StructTy = StructTy {
        name: "ServiceRegistration",
        go_type: "api.ServiceRegistration",
        fields: &[
            ("ID", Ty::Str),
            ("ServiceName", Ty::Str),
            ("Namespace", Ty::Str),
            ("NodeID", Ty::Str),
            ("Datacenter", Ty::Str),
            ("JobID", Ty::Str),
            ("AllocID", Ty::Str),
            ("Tags", Ty::Slice(&Ty::Str, "[]string")),
            ("Address", Ty::Str),
            ("Port", Ty::Int),
            ("CreateIndex", Ty::Uint64),
            ("ModifyIndex", Ty::Uint64),
        ],
    };
}

/// The refresh function of the Nomad discoverer (Go `Discovery.refresh` and its state).
pub struct NomadRefresher {
    allow_stale: bool,
    client: api::Client,
    tag_separator: String,
    metrics: Arc<NomadMetrics>,
}

impl Refresher for NomadRefresher {
    fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let opts = api::QueryOptions { allow_stale: self.allow_stale };
        let stubs = match self.client.services_list(&opts) {
            Ok(s) => s,
            Err(e) => {
                self.metrics.failures_count.inc();
                return Err(e);
            }
        };
        let mut tg = Group { source: "Nomad".to_string(), ..Default::default() };
        for stub in &stubs {
            for service in &stub.services {
                let instances = match self.client.services_get(&service.service_name, &opts) {
                    Ok(i) => i,
                    Err(e) => {
                        self.metrics.failures_count.inc();
                        return Err(format!("failed to fetch services: {e}"));
                    }
                };
                for instance in &instances {
                    let port = instance.port.to_string();
                    let mut labels = LabelSet::from_pairs(&[
                        (NOMAD_ADDRESS, &instance.address),
                        (NOMAD_DATACENTER, &instance.datacenter),
                        (NOMAD_NODE_ID, &instance.node_id),
                        (NOMAD_NAMESPACE, &instance.namespace),
                        (NOMAD_SERVICE_ADDRESS, &instance.address),
                        (NOMAD_SERVICE_ID, &instance.id),
                        (NOMAD_SERVICE_PORT, &port),
                        (NOMAD_SERVICE, &instance.service_name),
                    ]);
                    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&instance.address, &port));
                    if !instance.tags.is_empty() {
                        labels.insert(NOMAD_TAGS.to_string(), join_tags(&instance.tags, &self.tag_separator));
                    }
                    tg.targets.push(labels);
                }
            }
        }
        Ok(vec![Arc::new(tg)])
    }
}

/// Go `nomad.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<NomadRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<NomadMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let http_client = net::new_client_from_config(&conf.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        let config = api::Config { address: conf.server.clone(), namespace: conf.namespace.clone(), region: conf.region.clone() };
        let client = api::new_client(config, http_client)?;
        let refresher = NomadRefresher { allow_stale: conf.allow_stale, client, tag_separator: conf.tag_separator.clone(), metrics: m.clone() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "nomad",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
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

