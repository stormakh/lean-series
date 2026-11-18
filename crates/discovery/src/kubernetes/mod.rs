//! Port of `discovery/kubernetes`: targets from the Kubernetes API for the roles node, pod, service,
//! endpoints, endpointslice and ingress, with namespaces, selectors and attached node/namespace/deployment/job
//! metadata.
//!
//! client-go is replaced by a small in-house client ([`client`]), informers and work queues ([`informer`]) on
//! blocking threads: one reflector thread per watched resource kind and namespace, as client-go informers, and
//! one thread per role discoverer. Objects keep only the fields Prometheus reads ([`api`]).
//!
//! Differences from upstream are listed in `client.rs`, `kubeconfig.rs` and `informer.rs`; the client-go
//! process metrics (`prometheus_sd_kubernetes_http_request_*`, `prometheus_sd_kubernetes_workqueue_*`) are not
//! exported.
pub mod api;
pub mod client;
pub mod endpoints;
pub mod endpointslice;
pub mod fake;
pub mod informer;
pub mod ingress;
pub mod kubeconfig;
pub mod node;
pub mod pod;
pub mod selector;
pub mod service;

use std::any::Any;
use std::sync::{Arc, RwLock};

use model::labelset::LabelSet;
use yaml::{Decoder, Error as YError, Marshal, Node as YNode, Unmarshal, Value};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry};
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

use api::{Kind, Object, ObjectMeta, OwnerReference, Pod as ApiPod};
use client::Client;
use informer::{IndexFunc, Informer, NAMESPACE_INDEX, meta_namespace_index_func};

/// The meta prefix of all labels of this discovery.
pub const META_LABEL_PREFIX: &str = "__meta_kubernetes_";
pub const NAMESPACE_LABEL: &str = "__meta_kubernetes_namespace";
pub const PRESENT_VALUE: &str = "true";

pub const NODE_INDEX: &str = "node";
pub const POD_INDEX: &str = "pod";
pub const REPLICA_SET_INDEX: &str = "replicaset";
pub const JOB_INDEX: &str = "job";
pub const SERVICE_INDEX: &str = "service";

/// The Kubernetes SD role (Go `kubernetes.Role`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Role(pub String);

impl Role {
    pub const NODE: &'static str = "node";
    pub const POD: &'static str = "pod";
    pub const SERVICE: &'static str = "service";
    pub const ENDPOINT: &'static str = "endpoints";
    pub const ENDPOINT_SLICE: &'static str = "endpointslice";
    pub const INGRESS: &'static str = "ingress";

    pub fn new(s: &str) -> Role {
        Role(s.to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Unmarshal for Role {
    fn unmarshal_yaml(&mut self, n: &YNode, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = s;
            match self.0.as_str() {
                Role::NODE | Role::POD | Role::SERVICE | Role::ENDPOINT | Role::ENDPOINT_SLICE | Role::INGRESS => Ok(()),
                _ => Err(YError::Msg(format!("unknown Kubernetes SD role {}", yaml::go_quote(&self.0)))),
            }
        })
    }
    fn go_type() -> String {
        "kubernetes.Role".to_string()
    }
}
impl Marshal for Role {
    fn marshal_yaml(&self) -> Result<Value, String> {
        Ok(Value::Str(self.0.clone()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}

/// Go `MetricLabelRoleAdd` etc.
pub const METRIC_LABEL_ROLE_ADD: &str = "add";
pub const METRIC_LABEL_ROLE_DELETE: &str = "delete";
pub const METRIC_LABEL_ROLE_UPDATE: &str = "update";

/// Go `SelectorConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SelectorConfig {
    pub role: Role,
    pub label: String,
    pub field: String,
}
yaml::yaml_struct!(SelectorConfig, "kubernetes.SelectorConfig", [
    (role: "role", omitempty)
    (label: "label", omitempty)
    (field: "field", omitempty)
]);
yaml::impl_plain_struct!(SelectorConfig);

/// Go `PodMetadataConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PodMetadataConfig {
    pub deployment: bool,
    pub job: bool,
    pub cron_job: bool,
}
yaml::yaml_struct!(PodMetadataConfig, "kubernetes.PodMetadataConfig", [
    (deployment: "deployment")
    (job: "job")
    (cron_job: "cronjob")
]);
yaml::impl_plain_struct!(PodMetadataConfig);

/// Go `AttachMetadataConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AttachMetadataConfig {
    pub node: bool,
    pub namespace: bool,
    pub pod_metadata: PodMetadataConfig,
}
yaml::yaml_struct!(AttachMetadataConfig, "kubernetes.AttachMetadataConfig", [
    (node: "node")
    (namespace: "namespace")
    (inline pod_metadata)
]);
yaml::impl_plain_struct!(AttachMetadataConfig);

/// Go `NamespaceDiscovery`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NamespaceDiscovery {
    pub include_own_namespace: bool,
    pub names: Vec<String>,
}
yaml::yaml_struct!(NamespaceDiscovery, "kubernetes.plain", [
    (include_own_namespace: "own_namespace")
    (names: "names")
]);
impl Unmarshal for NamespaceDiscovery {
    fn unmarshal_yaml(&mut self, n: &YNode, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = NamespaceDiscovery::default();
            d.inner_struct(n, self)
        })
    }
    fn go_type() -> String {
        "kubernetes.plain".to_string()
    }
}
impl Marshal for NamespaceDiscovery {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

/// Go `kubernetes.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub api_server: yaml::Url,
    pub role: Role,
    pub kube_config: String,
    pub http_client_config: net::HttpClientConfig,
    pub namespace_discovery: NamespaceDiscovery,
    pub selectors: Vec<SelectorConfig>,
    pub attach_metadata: AttachMetadataConfig,
}
yaml::yaml_struct!(SdConfig, "kubernetes.plain", [
    (api_server: "api_server", omitempty)
    (role: "role")
    (kube_config: "kubeconfig_file")
    (inline http_client_config)
    (namespace_discovery: "namespaces", omitempty)
    (selectors: "selectors", omitempty)
    (attach_metadata: "attach_metadata", omitempty)
]);

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { http_client_config: net::HttpClientConfig::default_config(), ..Default::default() }
}

fn allowed_selectors(role: &str) -> Option<&'static [&'static str]> {
    Some(match role {
        Role::POD => &[Role::POD, Role::NODE],
        Role::SERVICE => &[Role::SERVICE],
        Role::ENDPOINT_SLICE => &[Role::POD, Role::SERVICE, Role::ENDPOINT_SLICE],
        Role::ENDPOINT => &[Role::POD, Role::SERVICE, Role::ENDPOINT],
        Role::NODE => &[Role::NODE],
        Role::INGRESS => &[Role::INGRESS],
        _ => return None,
    })
}

impl SdConfig {
    /// The validation of Go's `UnmarshalYAML` after decoding.
    pub fn validate(&mut self) -> Result<(), String> {
        if self.role.0.is_empty() {
            return Err("role missing (one of: pod, service, endpoints, endpointslice, node, ingress)".to_string());
        }
        self.http_client_config.validate()?;
        let default_http = net::HttpClientConfig::default_config();
        if self.api_server.0.is_some() && !self.kube_config.is_empty() {
            return Err("cannot use 'kubeconfig_file' and 'api_server' simultaneously".to_string());
        }
        if !self.kube_config.is_empty() && self.http_client_config != default_http {
            return Err("cannot use a custom HTTP client configuration together with 'kubeconfig_file'".to_string());
        }
        if self.api_server.0.is_none() && self.http_client_config != default_http {
            return Err("to use custom HTTP client configuration please provide the 'api_server' URL explicitly".to_string());
        }
        if self.api_server.0.is_some() && self.namespace_discovery.include_own_namespace {
            return Err("cannot use 'api_server' and 'namespaces.own_namespace' simultaneously".to_string());
        }
        if !self.kube_config.is_empty() && self.namespace_discovery.include_own_namespace {
            return Err("cannot use 'kubeconfig_file' and 'namespaces.own_namespace' simultaneously".to_string());
        }
        let mut found: Vec<&str> = Vec::new();
        for s in &self.selectors {
            if found.contains(&s.role.as_str()) {
                return Err(format!("duplicated selector role: {}", s.role));
            }
            found.push(s.role.as_str());
            let Some(allowed) = allowed_selectors(self.role.as_str()) else {
                return Err(format!("invalid role: {}, expecting one of: pod, service, endpoints, endpointslice, node or ingress", yaml::go_quote(self.role.as_str())));
            };
            if !allowed.contains(&s.role.as_str()) {
                return Err(format!("{} role supports only {} selectors", self.role, allowed.join(", ")));
            }
            selector::parse_field_selector(&s.field)?;
            selector::parse_label_selector(&s.label)?;
        }
        Ok(())
    }
}

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &YNode, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            self.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "kubernetes.plain".to_string()
    }
}
impl Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "kubernetes.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "kubernetes"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(SharedDiscovery(Arc::new(Discovery::new(opts.logger, &opts.metrics, self)?))))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, _rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        KubernetesMetrics::new(reg)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
        self.kube_config = net::config::join_dir(dir, &self.kube_config);
    }
}

/// Go `kubernetesMetrics`.
pub struct KubernetesMetrics {
    pub event_count: Arc<CounterVec>,
    pub failures_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl KubernetesMetrics {
    pub fn new(reg: &Arc<Registry>) -> Arc<KubernetesMetrics> {
        let event_count = CounterVec::new_counter_vec(Opts::new("prometheus_sd_kubernetes_events_total", "The number of Kubernetes events handled."), &["role", "event"]);
        let failures_count = CounterVec::new_counter(Opts::new("prometheus_sd_kubernetes_failures_total", "The number of failed WATCH/LIST requests."));
        let registerer = MetricRegisterer::new(reg.clone(), vec![event_count.clone(), failures_count.clone()]);
        for role in [Role::ENDPOINT_SLICE, Role::ENDPOINT, Role::NODE, Role::POD, Role::SERVICE, Role::INGRESS] {
            for evt in [METRIC_LABEL_ROLE_ADD, METRIC_LABEL_ROLE_DELETE, METRIC_LABEL_ROLE_UPDATE] {
                event_count.with_label_values(&[role, evt]);
            }
        }
        failures_count.get().add(0.0);
        Arc::new(KubernetesMetrics { event_count, failures_count, registerer })
    }
    /// The failures counter value.
    pub fn failures(&self) -> f64 {
        self.failures_count.get().get()
    }
}

impl DiscovererMetrics for KubernetesMetrics {
    fn register(&self) -> Result<(), String> {
        self.registerer.register_metrics()
    }
    fn unregister(&self) {
        self.registerer.unregister_metrics()
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// Go `resourceSelector`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceSelector {
    pub label: String,
    pub field: String,
}

/// Go `roleSelector`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoleSelector {
    pub node: ResourceSelector,
    pub pod: ResourceSelector,
    pub service: ResourceSelector,
    pub endpoints: ResourceSelector,
    pub endpointslice: ResourceSelector,
    pub ingress: ResourceSelector,
}

/// Go `mapSelector`.
pub fn map_selector(raw: &[SelectorConfig]) -> RoleSelector {
    let mut rs = RoleSelector::default();
    for r in raw {
        let s = ResourceSelector { label: r.label.clone(), field: r.field.clone() };
        match r.role.as_str() {
            Role::ENDPOINT_SLICE => rs.endpointslice = s,
            Role::ENDPOINT => rs.endpoints = s,
            Role::INGRESS => rs.ingress = s,
            Role::NODE => rs.node = s,
            Role::POD => rs.pod = s,
            Role::SERVICE => rs.service = s,
            _ => {}
        }
    }
    rs
}

/// A role discoverer that `Discovery` runs (Go `discovery.Discoverer` of the role types).
pub trait RoleDiscoverer: Send + Sync {
    fn run(&self, ctx: &CancelToken, up: Sender);
    /// Whether the main informers synced (the Go test helper `hasSynced`).
    fn has_synced(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
}

/// Go `kubernetes.Discovery`.
pub struct Discovery {
    client: Arc<dyn Client>,
    pub role: Role,
    logger: Logger,
    pub namespace_discovery: NamespaceDiscovery,
    discoverers: RwLock<Vec<Arc<dyn RoleDiscoverer>>>,
    pub selectors: RoleSelector,
    pub own_namespace: String,
    pub attach_metadata: AttachMetadataConfig,
    pub metrics: Arc<KubernetesMetrics>,
}

/// The `Discoverer` of a shared `Discovery`.
pub struct SharedDiscovery(pub Arc<Discovery>);

impl Discoverer for SharedDiscovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.0.run(ctx, up)
    }
}

/// Go `version.PrometheusUserAgent()`.
pub const USER_AGENT: &str = "Prometheus/3.16.0";

impl Discovery {
    /// Go `New`.
    pub fn new(logger: Logger, metrics: &Option<Arc<dyn DiscovererMetrics>>, conf: &SdConfig) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<KubernetesMetrics>(metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let mut own_namespace = String::new();
        let rest = if !conf.kube_config.is_empty() {
            kubeconfig::load(&conf.kube_config)?
        } else if conf.api_server.0.is_none() {
            let r = client::in_cluster_config()?;
            if conf.namespace_discovery.include_own_namespace {
                let b = std::fs::read(client::SERVICE_ACCOUNT_NAMESPACE_FILE).map_err(|e| format!("could not determine the pod's namespace: open {}: {}", client::SERVICE_ACCOUNT_NAMESPACE_FILE, net::types::go_io_error(&e)))?;
                if b.is_empty() {
                    return Err("could not read own namespace name (empty file)".to_string());
                }
                own_namespace = String::from_utf8_lossy(&b).to_string();
            }
            logger.info("Using pod service account via in-cluster config", &[]);
            r
        } else {
            let u = conf.api_server.0.as_ref().map(|u| u.to_url_string()).unwrap_or_default();
            client::RestConfig { host: u, http: conf.http_client_config.clone() }
        };
        let c = client::HttpClient::new(&rest.host, &rest.http, USER_AGENT)?;
        Ok(Discovery::with_client(Arc::new(c), conf.role.clone(), logger, conf.namespace_discovery.clone(), map_selector(&conf.selectors), own_namespace, conf.attach_metadata.clone(), m))
    }

    /// A discovery on a given API client (used by `new` and by tests with a fake client).
    #[allow(clippy::too_many_arguments)]
    pub fn with_client(
        client: Arc<dyn Client>,
        role: Role,
        logger: Logger,
        namespace_discovery: NamespaceDiscovery,
        selectors: RoleSelector,
        own_namespace: String,
        attach_metadata: AttachMetadataConfig,
        metrics: Arc<KubernetesMetrics>,
    ) -> Discovery {
        Discovery { client, role, logger, namespace_discovery, discoverers: RwLock::new(Vec::new()), selectors, own_namespace, attach_metadata, metrics }
    }

    /// The role discoverers started by `run` (Go `d.discoverers`).
    pub fn discoverers(&self) -> Vec<Arc<dyn RoleDiscoverer>> {
        self.discoverers.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// The Go test helper `Discovery.hasSynced`.
    pub fn has_synced(&self) -> bool {
        self.discoverers().iter().all(|d| d.has_synced())
    }

    /// Go `getNamespaces`.
    pub fn get_namespaces(&self) -> Vec<String> {
        let mut namespaces = self.namespace_discovery.names.clone();
        let own = self.namespace_discovery.include_own_namespace;
        if namespaces.is_empty() && !own {
            return vec![String::new()];
        }
        if own && !self.own_namespace.is_empty() {
            namespaces.push(self.own_namespace.clone());
        }
        namespaces
    }

    fn informer(&self, kind: Kind, namespace: &str, sel: &ResourceSelector, indexers: Vec<(&'static str, IndexFunc)>) -> Arc<Informer> {
        let m = self.metrics.clone();
        Informer::new(self.client.clone(), kind, namespace, &sel.label, &sel.field, indexers, Box::new(move |_| m.failures_count.get().inc()), self.logger.clone())
    }

    fn new_node_informer(&self) -> Arc<Informer> {
        self.informer(Kind::Node, "", &self.selectors.node, Vec::new())
    }
    fn new_namespace_informer(&self) -> Arc<Informer> {
        // We don't filter on NamespaceDiscovery.
        self.informer(Kind::Namespace, "", &ResourceSelector::default(), Vec::new())
    }
    fn new_replica_set_informer(&self, namespace: &str) -> Arc<Informer> {
        self.informer(Kind::ReplicaSet, namespace, &ResourceSelector::default(), Vec::new())
    }
    fn new_job_informer(&self, namespace: &str) -> Arc<Informer> {
        self.informer(Kind::Job, namespace, &ResourceSelector::default(), Vec::new())
    }

    fn new_indexed_pods_informer(&self, namespace: &str) -> Arc<Informer> {
        let mut ix: Vec<(&'static str, IndexFunc)> = Vec::new();
        if self.attach_metadata.node {
            ix.push((NODE_INDEX, |o| match o {
                Object::Pod(p) => vec![p.spec.node_name.clone()],
                _ => Vec::new(),
            }));
        }
        if self.attach_metadata.namespace {
            ix.push((NAMESPACE_INDEX, meta_namespace_index_func));
        }
        if self.attach_metadata.pod_metadata.deployment {
            ix.push((REPLICA_SET_INDEX, |o| controller_index(o, "ReplicaSet")));
        }
        if self.attach_metadata.pod_metadata.job || self.attach_metadata.pod_metadata.cron_job {
            ix.push((JOB_INDEX, |o| controller_index(o, "Job")));
        }
        self.informer(Kind::Pod, namespace, &self.selectors.pod, ix)
    }

    fn new_indexed_endpoints_informer(&self, namespace: &str) -> Arc<Informer> {
        let mut ix: Vec<(&'static str, IndexFunc)> = vec![(POD_INDEX, |o| {
            let Object::Endpoints(e) = o else { return Vec::new() };
            let mut pods = Vec::new();
            for s in &e.subsets {
                for a in &s.addresses {
                    if let Some(r) = &a.target_ref
                        && r.kind == "Pod"
                    {
                        pods.push(namespaced_name(&r.namespace, &r.name));
                    }
                }
            }
            pods
        })];
        if self.attach_metadata.node {
            ix.push((NODE_INDEX, |o| {
                let Object::Endpoints(e) = o else { return Vec::new() };
                let mut nodes = Vec::new();
                for s in &e.subsets {
                    for a in &s.addresses {
                        if let Some(r) = &a.target_ref {
                            match r.kind.as_str() {
                                "Pod" => {
                                    if let Some(n) = &a.node_name {
                                        nodes.push(n.clone());
                                    }
                                }
                                "Node" => nodes.push(r.name.clone()),
                                _ => {}
                            }
                        }
                    }
                }
                nodes
            }));
        }
        if self.attach_metadata.namespace {
            ix.push((NAMESPACE_INDEX, meta_namespace_index_func));
        }
        self.informer(Kind::Endpoints, namespace, &self.selectors.endpoints, ix)
    }

    fn new_indexed_endpoint_slices_informer(&self, namespace: &str) -> Arc<Informer> {
        let mut ix: Vec<(&'static str, IndexFunc)> = vec![(SERVICE_INDEX, |o| {
            let Object::EndpointSlice(e) = o else { return Vec::new() };
            match api::map_get(&e.meta.labels, endpointslice::LABEL_SERVICE_NAME) {
                Some(svc) => vec![namespaced_name(&e.meta.namespace, svc)],
                None => Vec::new(),
            }
        })];
        if self.attach_metadata.node {
            ix.push((NODE_INDEX, |o| {
                let Object::EndpointSlice(e) = o else { return Vec::new() };
                let mut nodes = Vec::new();
                for t in &e.endpoints {
                    if let Some(r) = &t.target_ref {
                        match r.kind.as_str() {
                            "Pod" => {
                                if let Some(n) = &t.node_name {
                                    nodes.push(n.clone());
                                }
                            }
                            "Node" => nodes.push(r.name.clone()),
                            _ => {}
                        }
                    }
                }
                nodes
            }));
        }
        if self.attach_metadata.namespace {
            ix.push((NAMESPACE_INDEX, meta_namespace_index_func));
        }
        self.informer(Kind::EndpointSlice, namespace, &self.selectors.endpointslice, ix)
    }

    fn new_indexed_services_informer(&self, namespace: &str) -> Arc<Informer> {
        let mut ix: Vec<(&'static str, IndexFunc)> = Vec::new();
        if self.attach_metadata.namespace {
            ix.push((NAMESPACE_INDEX, meta_namespace_index_func));
        }
        self.informer(Kind::Service, namespace, &self.selectors.service, ix)
    }

    fn new_indexed_ingresses_informer(&self, namespace: &str) -> Arc<Informer> {
        let mut ix: Vec<(&'static str, IndexFunc)> = Vec::new();
        if self.attach_metadata.namespace {
            ix.push((NAMESPACE_INDEX, meta_namespace_index_func));
        }
        self.informer(Kind::Ingress, namespace, &self.selectors.ingress, ix)
    }

    /// Go `Run`: starts the informers and role discoverers and blocks until `ctx` is cancelled.
    pub fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut informers: Vec<Arc<Informer>> = Vec::new();
        let mut discoverers: Vec<Arc<dyn RoleDiscoverer>> = Vec::new();
        let am = &self.attach_metadata;
        let pm = &am.pod_metadata;
        let namespaces = self.get_namespaces();
        let mut start = |i: &Arc<Informer>| informers.push(i.clone());
        match self.role.as_str() {
            Role::ENDPOINT_SLICE | Role::ENDPOINT => {
                let slices = self.role.as_str() == Role::ENDPOINT_SLICE;
                for ns in &namespaces {
                    let main = if slices { self.new_indexed_endpoint_slices_informer(ns) } else { self.new_indexed_endpoints_informer(ns) };
                    let svc = self.informer(Kind::Service, ns, &self.selectors.service, Vec::new());
                    let pod = self.informer(Kind::Pod, ns, &self.selectors.pod, Vec::new());
                    let node_inf = am.node.then(|| self.new_node_informer());
                    let ns_inf = am.namespace.then(|| self.new_namespace_informer());
                    let rs_inf = pm.deployment.then(|| self.new_replica_set_informer(ns));
                    let job_inf = (pm.job || pm.cron_job).then(|| self.new_job_informer(ns));
                    for i in [&node_inf, &ns_inf, &rs_inf, &job_inf].into_iter().flatten() {
                        start(i);
                    }
                    let meta = MetadataInformers { node: node_inf, namespace: ns_inf, replica_set: rs_inf, job: job_inf, deployment: pm.deployment, job_metadata: pm.job, cron_job_metadata: pm.cron_job };
                    let d: Arc<dyn RoleDiscoverer> = if slices {
                        endpointslice::EndpointSlice::new(self.logger.with(&[("role", "endpointslice")]), main.clone(), svc.clone(), pod.clone(), meta, &self.metrics.event_count)
                    } else {
                        endpoints::Endpoints::new(self.logger.with(&[("role", "endpoint")]), main.clone(), svc.clone(), pod.clone(), meta, &self.metrics.event_count)
                    };
                    discoverers.push(d);
                    start(&main);
                    start(&svc);
                    start(&pod);
                }
            }
            Role::POD => {
                let node_inf = am.node.then(|| self.new_node_informer());
                let ns_inf = am.namespace.then(|| self.new_namespace_informer());
                for i in [&node_inf, &ns_inf].into_iter().flatten() {
                    start(i);
                }
                for ns in &namespaces {
                    let pods = self.new_indexed_pods_informer(ns);
                    let rs_inf = pm.deployment.then(|| self.new_replica_set_informer(ns));
                    let job_inf = (pm.job || pm.cron_job).then(|| self.new_job_informer(ns));
                    for i in [&rs_inf, &job_inf].into_iter().flatten() {
                        start(i);
                    }
                    let meta = MetadataInformers { node: node_inf.clone(), namespace: ns_inf.clone(), replica_set: rs_inf, job: job_inf, deployment: pm.deployment, job_metadata: pm.job, cron_job_metadata: pm.cron_job };
                    discoverers.push(pod::Pod::new(self.logger.with(&[("role", "pod")]), pods.clone(), meta, &self.metrics.event_count));
                    start(&pods);
                }
            }
            Role::SERVICE => {
                let ns_inf = am.namespace.then(|| self.new_namespace_informer());
                if let Some(i) = &ns_inf {
                    start(i);
                }
                for ns in &namespaces {
                    let inf = self.new_indexed_services_informer(ns);
                    discoverers.push(service::Service::new(self.logger.with(&[("role", "service")]), inf.clone(), ns_inf.clone(), &self.metrics.event_count));
                    start(&inf);
                }
            }
            Role::INGRESS => {
                let ns_inf = am.namespace.then(|| self.new_namespace_informer());
                if let Some(i) = &ns_inf {
                    start(i);
                }
                for ns in &namespaces {
                    let inf = self.new_indexed_ingresses_informer(ns);
                    discoverers.push(ingress::Ingress::new(self.logger.with(&[("role", "ingress")]), inf.clone(), ns_inf.clone(), &self.metrics.event_count));
                    start(&inf);
                }
            }
            Role::NODE => {
                let inf = self.new_node_informer();
                discoverers.push(node::Node::new(self.logger.with(&[("role", "node")]), inf.clone(), &self.metrics.event_count));
                start(&inf);
            }
            _ => self.logger.error("unknown Kubernetes discovery kind", &[("role", self.role.as_str())]),
        }

        let mut inf_threads = Vec::with_capacity(informers.len());
        for i in informers {
            let ctx = ctx.clone();
            let name = format!("k8s-{}", i.kind().resource());
            match std::thread::Builder::new().name(name).spawn(move || i.run(&ctx)) {
                Ok(t) => inf_threads.push(t),
                Err(e) => self.logger.error("cannot start informer thread", &[("err", &e.to_string())]),
            }
        }
        let mut threads = Vec::with_capacity(discoverers.len());
        {
            let mut ds = self.discoverers.write().unwrap_or_else(|e| e.into_inner());
            for d in discoverers {
                ds.push(d.clone());
                let (ctx, up) = (ctx.clone(), up.clone());
                match std::thread::Builder::new().name(format!("k8s-{}", self.role)).spawn(move || d.run(&ctx, up)) {
                    Ok(t) => threads.push(t),
                    Err(e) => self.logger.error("cannot start discoverer thread", &[("err", &e.to_string())]),
                }
            }
        }
        drop(up);
        for t in threads {
            let _ = t.join();
        }
        ctx.wait();
        for t in inf_threads {
            let _ = t.join();
        }
    }
}

/// The informers of attached metadata of a pod-related role.
#[derive(Clone, Default)]
pub struct MetadataInformers {
    pub node: Option<Arc<Informer>>,
    pub namespace: Option<Arc<Informer>>,
    pub replica_set: Option<Arc<Informer>>,
    pub job: Option<Arc<Informer>>,
    pub deployment: bool,
    pub job_metadata: bool,
    pub cron_job_metadata: bool,
}

fn controller_index(o: &Object, kind: &str) -> Vec<String> {
    let Object::Pod(p) = o else { return Vec::new() };
    match get_controller_of(&p.meta) {
        Some(owner) if owner.kind == kind => vec![namespaced_name(&p.meta.namespace, &owner.name)],
        _ => Vec::new(),
    }
}

/// Go `GetControllerOf`.
pub fn get_controller_of(m: &ObjectMeta) -> Option<&OwnerReference> {
    m.owner_references.iter().find(|r| r.controller == Some(true))
}

pub fn namespaced_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

/// Go `addObjectAnnotationsAndLabels`.
pub fn add_object_annotations_and_labels(ls: &mut LabelSet, m: &ObjectMeta, resource: &str) {
    for (k, v) in &m.labels {
        let ln = crate::sanitize_label_name(k);
        ls.0.insert(format!("{META_LABEL_PREFIX}{resource}_label_{ln}"), v.clone());
        ls.0.insert(format!("{META_LABEL_PREFIX}{resource}_labelpresent_{ln}"), PRESENT_VALUE.to_string());
    }
    for (k, v) in &m.annotations {
        let ln = crate::sanitize_label_name(k);
        ls.0.insert(format!("{META_LABEL_PREFIX}{resource}_annotation_{ln}"), v.clone());
        ls.0.insert(format!("{META_LABEL_PREFIX}{resource}_annotationpresent_{ln}"), PRESENT_VALUE.to_string());
    }
}

/// Go `addObjectMetaLabels`.
pub fn add_object_meta_labels(ls: &mut LabelSet, m: &ObjectMeta, role: &str) {
    ls.0.insert(format!("{META_LABEL_PREFIX}{role}_name"), m.name.clone());
    add_object_annotations_and_labels(ls, m, role);
}

/// Go `addNodeLabels`.
pub fn add_node_labels(tg: LabelSet, node_inf: &Informer, node_name: Option<&str>) -> LabelSet {
    let Some(name) = node_name else { return tg };
    let Some(obj) = node_inf.get(name) else { return tg };
    let Object::Node(node) = obj.as_ref() else { return tg };
    let mut ls = LabelSet::new();
    add_object_meta_labels(&mut ls, &node.meta, Role::NODE);
    tg.merge(&ls)
}

/// Go `addNamespaceLabels`.
pub fn add_namespace_labels(tg: LabelSet, ns_inf: &Informer, namespace: &str) -> LabelSet {
    let Some(obj) = ns_inf.get(namespace) else { return tg };
    let mut ls = LabelSet::new();
    add_object_annotations_and_labels(&mut ls, obj.meta(), "namespace");
    tg.merge(&ls)
}

pub const POD_IP_LABEL: &str = "__meta_kubernetes_pod_ip";
pub const POD_CONTAINER_NAME_LABEL: &str = "__meta_kubernetes_pod_container_name";
pub const POD_CONTAINER_ID_LABEL: &str = "__meta_kubernetes_pod_container_id";
pub const POD_CONTAINER_IMAGE_LABEL: &str = "__meta_kubernetes_pod_container_image";
pub const POD_CONTAINER_PORT_NAME_LABEL: &str = "__meta_kubernetes_pod_container_port_name";
pub const POD_CONTAINER_PORT_NUMBER_LABEL: &str = "__meta_kubernetes_pod_container_port_number";
pub const POD_CONTAINER_PORT_PROTOCOL_LABEL: &str = "__meta_kubernetes_pod_container_port_protocol";
pub const POD_CONTAINER_IS_INIT: &str = "__meta_kubernetes_pod_container_init";
pub const POD_CRON_JOB_NAME_LABEL: &str = "__meta_kubernetes_pod_cronjob_name";
pub const POD_DEPLOYMENT_NAME_LABEL: &str = "__meta_kubernetes_pod_deployment_name";
pub const POD_JOB_NAME_LABEL: &str = "__meta_kubernetes_pod_job_name";
pub const POD_READY_LABEL: &str = "__meta_kubernetes_pod_ready";
pub const POD_PHASE_LABEL: &str = "__meta_kubernetes_pod_phase";
pub const POD_NODE_NAME_LABEL: &str = "__meta_kubernetes_pod_node_name";
pub const POD_HOST_IP_LABEL: &str = "__meta_kubernetes_pod_host_ip";
pub const POD_UID: &str = "__meta_kubernetes_pod_uid";
pub const POD_CONTROLLER_KIND: &str = "__meta_kubernetes_pod_controller_kind";
pub const POD_CONTROLLER_NAME: &str = "__meta_kubernetes_pod_controller_name";

/// Go `podReady`.
pub fn pod_ready(pod: &ApiPod) -> String {
    for c in &pod.status.conditions {
        if c.type_ == "Ready" {
            return c.status.to_lowercase();
        }
    }
    "unknown".to_string()
}

/// Go `podLabels`.
pub fn pod_labels(pod: &ApiPod, meta: &MetadataInformers) -> LabelSet {
    let mut ls = LabelSet::new();
    for (k, v) in [
        (POD_IP_LABEL, pod.status.pod_ip.clone()),
        (POD_READY_LABEL, pod_ready(pod)),
        (POD_PHASE_LABEL, pod.status.phase.clone()),
        (POD_NODE_NAME_LABEL, pod.spec.node_name.clone()),
        (POD_HOST_IP_LABEL, pod.status.host_ip.clone()),
        (POD_UID, pod.meta.uid.clone()),
    ] {
        ls.0.insert(k.to_string(), v);
    }
    add_object_meta_labels(&mut ls, &pod.meta, Role::POD);

    if let Some(created_by) = get_controller_of(&pod.meta) {
        if !created_by.kind.is_empty() {
            ls.0.insert(POD_CONTROLLER_KIND.to_string(), created_by.kind.clone());
        }
        if !created_by.name.is_empty() {
            ls.0.insert(POD_CONTROLLER_NAME.to_string(), created_by.name.clone());
        }
        match created_by.kind.as_str() {
            "ReplicaSet" => {
                if let Some(rs_inf) = &meta.replica_set
                    && meta.deployment
                    && let Some(rs) = rs_inf.get(&namespaced_name(&pod.meta.namespace, &created_by.name))
                    && let Object::ReplicaSet(rs) = rs.as_ref()
                    && let Some(owner) = get_controller_of(rs)
                    && owner.kind == "Deployment"
                {
                    ls.0.insert(POD_DEPLOYMENT_NAME_LABEL.to_string(), owner.name.clone());
                }
            }
            "Job" => {
                if meta.job_metadata {
                    ls.0.insert(POD_JOB_NAME_LABEL.to_string(), created_by.name.clone());
                }
                if let Some(job_inf) = &meta.job
                    && meta.cron_job_metadata
                    && let Some(job) = job_inf.get(&namespaced_name(&pod.meta.namespace, &created_by.name))
                    && let Object::Job(job) = job.as_ref()
                    && let Some(owner) = get_controller_of(job)
                    && owner.kind == "CronJob"
                {
                    ls.0.insert(POD_CRON_JOB_NAME_LABEL.to_string(), owner.name.clone());
                }
            }
            _ => {}
        }
    }
    ls
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// Go `strconv.FormatUint(uint64(int32), 10)`: negative values wrap as Go does.
pub fn format_port(p: i32) -> String {
    (p as i64 as u64).to_string()
}

/// What processing one queued key gives.
pub(crate) enum Process {
    Send(model::labelset::LabelSet, Vec<LabelSet>, String),
    /// Nothing to send.
    Skip,
    /// Stop the discoverer (Go `process` returning false).
    Stop,
}

/// The `Run` and `process` loop shared by the roles: waits for the informers to sync, then sends the group of
/// every queued key until `ctx` is cancelled (which shuts the queue down).
pub(crate) fn run_queue(ctx: &CancelToken, up: &Sender, queue: &Arc<informer::WorkQueue>, syncs: &[&Arc<Informer>], logger: &Logger, what: &str, mut process: impl FnMut(&str) -> Process) {
    let q = queue.clone();
    ctx.on_cancel(Box::new(move || q.shut_down()));
    for inf in syncs {
        if !inf.wait_for_sync(ctx) {
            if !ctx.is_cancelled() {
                logger.error(&format!("{what} informer unable to sync cache"), &[]);
            }
            queue.shut_down();
            return;
        }
    }
    while let Some(key) = queue.get() {
        let r = process(&key);
        match r {
            Process::Send(labels, targets, source) => {
                up.send(vec![Arc::new(crate::targetgroup::Group { targets, labels, source })]);
            }
            Process::Skip => {}
            Process::Stop => {
                queue.done(&key);
                return;
            }
        }
        queue.done(&key);
    }
}

/// An event counter of a role (Go `eventCount.WithLabelValues(role, event)`).
pub(crate) fn event_counter(ec: &CounterVec, role: &str, event: &str) -> Arc<crate::metrics::Value> {
    ec.with_label_values(&[role, event])
}
