//! Port of Prometheus `discovery/`: the `Config` and `Discoverer` traits, the registry of SD config types
//! (YAML keys `<name>_sd_configs`), `Configs` with its inline YAML form, the discovery `Manager`, the refresh
//! helper, and the `static`, `file` and `http` providers.
//!
//! Go's `context.Context` becomes a [`CancelToken`]; `chan<- []*targetgroup.Group` becomes [`chan::Sender`].
//! Discoverers run on their own threads and block.
//!
//! See README.md for how to add a provider.
pub mod cancel;
pub mod chan;
pub mod digitalocean;
pub mod file;
pub mod glob;
pub(crate) mod gojson;
pub mod hetzner;
pub mod http;
pub mod log;
pub mod manager;
pub mod metrics;
pub mod refresh;
pub mod registry;
pub mod sdutil;
pub mod targetgroup;

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub use cancel::CancelToken;
pub use chan::{Receiver, Recv, Sender, Update, channel, channel_with_cancel};
pub use log::Logger;
pub use manager::{Manager, ManagerMetrics, ManagerOptions, PoolKey, Provider};
pub use metrics::Registry;
pub use registry::{Configs, SdConfigType, marshal_yaml_with_inline_configs, register_config, registered_config_names, unmarshal_yaml_with_inline_configs, unmarshal_yaml_with_inline_configs_nulls};
pub use targetgroup::Group;

use metrics::{CounterVec, HistogramVec, MetricRegisterer, Opts, SummaryVec};

/// A discoverer sends target groups on `up` until `ctx` is cancelled (Go `Discoverer.Run`). It should first send
/// the full set of groups, then changes. Returning drops `up`, which closes the channel.
pub trait Discoverer: Send {
    fn run(&mut self, ctx: &CancelToken, up: Sender);
}

/// Internal metrics of an SD mechanism (Go `DiscovererMetrics`).
pub trait DiscovererMetrics: Send + Sync + Any {
    fn register(&self) -> Result<(), String>;
    fn unregister(&self);
    /// For downcasting to the provider's own metrics type.
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// Go `NoopDiscovererMetrics`.
#[derive(Debug, Default)]
pub struct NoopDiscovererMetrics;
impl DiscovererMetrics for NoopDiscovererMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// Downcasts discoverer metrics to a provider's type (Go `opts.Metrics.(*fileMetrics)`).
pub fn downcast_metrics<T: Send + Sync + 'static>(m: &Option<Arc<dyn DiscovererMetrics>>) -> Option<Arc<T>> {
    m.clone()?.as_any_arc().downcast::<T>().ok()
}

/// Options for `Config::new_discoverer` (Go `DiscovererOptions`).
#[derive(Clone, Default)]
pub struct DiscovererOptions {
    pub logger: Logger,
    pub metrics: Option<Arc<dyn DiscovererMetrics>>,
    /// Extra HTTP client options; discoverers may ignore them.
    pub http_client_options: net::HttpClientOptions,
    /// The name of the target set (the scrape job).
    pub set_name: String,
}

/// The metrics a refresh-based discoverer updates (Go `RefreshMetrics`).
#[derive(Clone)]
pub struct RefreshMetrics {
    pub failures: Arc<metrics::Value>,
    pub duration: Arc<metrics::Observer>,
    pub duration_histogram: Arc<metrics::Observer>,
}

/// Go `RefreshMetricsInstantiator`.
pub trait RefreshMetricsInstantiator: Send + Sync {
    fn instantiate(&self, mech: &str, set_name: &str) -> RefreshMetrics;
}

/// Go `RefreshMetricsManager`.
pub trait RefreshMetricsManager: DiscovererMetrics + RefreshMetricsInstantiator {
    fn delete_label_values(&self, mech: &str, config: &str);
    fn as_instantiator(self: Arc<Self>) -> Arc<dyn RefreshMetricsInstantiator>;
}

/// Go `RefreshMetricsVecs`.
pub struct RefreshMetricsVecs {
    pub failures_vec: Arc<CounterVec>,
    pub duration_vec: Arc<SummaryVec>,
    pub duration_hist_vec: Arc<HistogramVec>,
    registerer: MetricRegisterer,
}

/// Go `NewRefreshMetrics`.
pub fn new_refresh_metrics(reg: &Arc<Registry>) -> Arc<RefreshMetricsVecs> {
    let failures_vec = CounterVec::new_counter_vec(Opts::new("prometheus_sd_refresh_failures_total", "Number of refresh failures for the given SD mechanism."), &["mechanism", "config"]);
    let duration_vec = SummaryVec::new_summary_vec(Opts::new("prometheus_sd_refresh_duration_seconds", "The duration of a refresh in seconds for the given SD mechanism."), &["mechanism", "config"]);
    let duration_hist_vec = HistogramVec::new_histogram_vec(Opts::new("prometheus_sd_refresh_duration_histogram_seconds", "The duration of a refresh for the given SD mechanism."), &["mechanism"], &[0.01, 0.1, 1.0, 10.0]);
    let registerer = MetricRegisterer::new(reg.clone(), vec![failures_vec.clone(), duration_vec.clone(), duration_hist_vec.clone()]);
    Arc::new(RefreshMetricsVecs { failures_vec, duration_vec, duration_hist_vec, registerer })
}

impl DiscovererMetrics for RefreshMetricsVecs {
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
impl RefreshMetricsInstantiator for RefreshMetricsVecs {
    fn instantiate(&self, mech: &str, set_name: &str) -> RefreshMetrics {
        RefreshMetrics {
            failures: self.failures_vec.with_label_values(&[mech, set_name]),
            duration: self.duration_vec.with_label_values(&[mech, set_name]),
            duration_histogram: self.duration_hist_vec.with_label_values(&[mech]),
        }
    }
}
impl RefreshMetricsManager for RefreshMetricsVecs {
    fn delete_label_values(&self, mech: &str, config: &str) {
        self.failures_vec.delete_label_values(&[mech, config]);
        self.duration_vec.delete_label_values(&[mech, config]);
    }
    fn as_instantiator(self: Arc<Self>) -> Arc<dyn RefreshMetricsInstantiator> {
        self
    }
}

/// All SD metrics (Go `SDMetrics`).
#[derive(Clone)]
pub struct SdMetrics {
    pub mechanism_metrics: HashMap<String, Arc<dyn DiscovererMetrics>>,
    pub refresh_manager: Arc<dyn RefreshMetricsManager>,
}

/// Go `RegisterSDMetrics`: registers the refresh metrics and creates and registers the metrics of every
/// registered mechanism.
pub fn register_sd_metrics(reg: &Arc<Registry>, rmm: Arc<dyn RefreshMetricsManager>) -> Result<HashMap<String, Arc<dyn DiscovererMetrics>>, String> {
    rmm.register().map_err(|e| format!("failed to create service discovery refresh metrics: {e}"))?;
    let mut out = HashMap::new();
    for r in registry::registrations() {
        let m = (r.new_metrics)(reg, rmm.clone().as_instantiator());
        m.register().map_err(|e| format!("failed to create service discovery metrics: {e}"))?;
        out.insert(r.name.to_string(), m);
    }
    // static_configs has no metrics (NoopDiscovererMetrics)
    out.insert("static".to_string(), Arc::new(NoopDiscovererMetrics) as Arc<dyn DiscovererMetrics>);
    Ok(out)
}

/// Go `CreateAndRegisterSDMetrics`.
pub fn create_and_register_sd_metrics(reg: &Arc<Registry>) -> Result<SdMetrics, String> {
    let rm = new_refresh_metrics(reg);
    let mechanism_metrics = register_sd_metrics(reg, rm.clone()).map_err(|e| format!("failed to register service discovery metrics: {e}"))?;
    Ok(SdMetrics { mechanism_metrics, refresh_manager: rm })
}

/// The configuration and constructor of a discoverer (Go `discovery.Config`).
///
/// Implement it for a type that is also `Clone + PartialEq` (Go compares configs with `reflect.DeepEqual` to
/// share providers); `ConfigBase` then comes for free.
pub trait Config: ConfigBase + Send + Sync + fmt::Debug + 'static {
    /// The mechanism name ("file", "kubernetes", ...); the YAML key is `<name>_sd_configs`.
    fn name(&self) -> &'static str;
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String>;
    /// The metrics of this mechanism (created once per process, see `register_sd_metrics`).
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, _rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(NoopDiscovererMetrics)
    }
    /// Go `config.DirectorySetter`: joins relative file paths with `dir`.
    fn set_directory(&mut self, _dir: &str) {}
}

/// Object-safe clone, equality and downcast for `Box<dyn Config>`.
pub trait ConfigBase {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn clone_config(&self) -> Box<dyn Config>;
    fn eq_config(&self, other: &dyn Config) -> bool;
}
impl<T: Config + Clone + PartialEq> ConfigBase for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn clone_config(&self) -> Box<dyn Config> {
        Box::new(self.clone())
    }
    fn eq_config(&self, other: &dyn Config) -> bool {
        other.as_any().downcast_ref::<T>().is_some_and(|o| o == self)
    }
}

impl Clone for Box<dyn Config> {
    fn clone(&self) -> Self {
        self.clone_config()
    }
}
impl PartialEq for Box<dyn Config> {
    fn eq(&self, other: &Self) -> bool {
        self.eq_config(other.as_ref())
    }
}

/// A static list of targets (Go `StaticConfig`), the `static_configs` section.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StaticConfig(pub Vec<Group>);

impl Config for StaticConfig {
    fn name(&self) -> &'static str {
        "static"
    }
    fn new_discoverer(&self, _opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(StaticDiscoverer(self.0.iter().cloned().map(Arc::new).collect())))
    }
}

struct StaticDiscoverer(Vec<Arc<Group>>);

impl Discoverer for StaticDiscoverer {
    fn run(&mut self, _ctx: &CancelToken, up: Sender) {
        // sends once (or stops when cancelled), then closes the channel as upstream does
        up.send(self.0.clone());
    }
}
