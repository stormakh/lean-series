//! Port of `scrape/target.go`: targets, their labels from discovery and relabeling (`populate_labels`), URL,
//! hash, offset and health.

use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use config::ScrapeConfig;
use discovery::targetgroup::Group;
use model::labels::{Builder, Labels};
use model::labelset::LabelSet;
use model::metadata::MetricType;
use model::relabel;
use storage::Error;

use crate::util::{fnv64a, go_quote, parse_bool};

/// Label name constants of prometheus/common `model`.
pub const ADDRESS_LABEL: &str = "__address__";
pub const SCHEME_LABEL: &str = "__scheme__";
pub const METRICS_PATH_LABEL: &str = "__metrics_path__";
pub const SCRAPE_INTERVAL_LABEL: &str = "__scrape_interval__";
pub const SCRAPE_TIMEOUT_LABEL: &str = "__scrape_timeout__";
pub const RESERVED_LABEL_PREFIX: &str = "__";
pub const META_LABEL_PREFIX: &str = "__meta_";
pub const PARAM_LABEL_PREFIX: &str = "__param_";
pub const JOB_LABEL: &str = "job";
pub const INSTANCE_LABEL: &str = "instance";
pub const METRIC_NAME_LABEL: &str = "__name__";
pub const EXPORTED_LABEL_PREFIX: &str = "exported_";

/// Go `convertClassicHistogramsToNHCBLabel`.
pub const CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL: &str = "__convert_classic_histograms_to_nhcb__";
/// Go `alwaysScrapeClassicHistogramsLabel`.
pub const ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL: &str = "__always_scrape_classic_histograms__";
/// Go `scrapeNativeHistogramsLabel`.
pub const SCRAPE_NATIVE_HISTOGRAMS_LABEL: &str = "__scrape_native_histograms__";
/// Go `UnixSocketLabel`: the unix socket to scrape through instead of `__address__`.
pub const UNIX_SOCKET_LABEL: &str = "__unix_socket__";

/// The health of a target after its last scrape (Go `TargetHealth`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetHealth {
    Unknown,
    Good,
    Bad,
}

impl TargetHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            TargetHealth::Unknown => "unknown",
            TargetHealth::Good => "up",
            TargetHealth::Bad => "down",
        }
    }
}

impl fmt::Display for TargetHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Metadata of a metric family (Go `MetricMetadata`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricMetadata {
    pub metric_family: String,
    pub typ: MetricType,
    pub help: String,
    pub unit: String,
}

/// A storage of metadata (Go `MetricMetadataStore`).
pub trait MetricMetadataStore: Send + Sync {
    fn list_metadata(&self) -> Vec<MetricMetadata>;
    fn get_metadata(&self, mf_name: &str) -> Option<MetricMetadata>;
    fn size_metadata(&self) -> usize;
    fn length_metadata(&self) -> usize;
}

struct TargetMut {
    scrape_config: Arc<ScrapeConfig>,
    t_labels: LabelSet,
    tg_labels: LabelSet,
    last_error: Option<Error>,
    last_scrape: Option<SystemTime>,
    last_scrape_duration: Duration,
    health: TargetHealth,
    metadata: Option<Arc<dyn MetricMetadataStore>>,
}

/// A singular HTTP or HTTPS endpoint (Go `Target`).
pub struct Target {
    labels: Labels,
    m: RwLock<TargetMut>,
}

impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Target({})", self.url_string())
    }
}

/// Go `Target.String()`: the URL.
impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.url_string())
    }
}

impl Target {
    /// Go `NewTarget`.
    pub fn new(labels: Labels, scrape_config: Arc<ScrapeConfig>, t_labels: LabelSet, tg_labels: LabelSet) -> Target {
        Target {
            labels,
            m: RwLock::new(TargetMut {
                scrape_config,
                t_labels,
                tg_labels,
                last_error: None,
                last_scrape: None,
                last_scrape_duration: Duration::ZERO,
                health: TargetHealth::Unknown,
                metadata: None,
            }),
        }
    }

    fn r(&self) -> std::sync::RwLockReadGuard<'_, TargetMut> {
        self.m.read().unwrap_or_else(|e| e.into_inner())
    }
    fn w(&self) -> std::sync::RwLockWriteGuard<'_, TargetMut> {
        self.m.write().unwrap_or_else(|e| e.into_inner())
    }

    /// All labels of the target, including the internal ones (Go `t.labels`).
    pub fn raw_labels(&self) -> &Labels {
        &self.labels
    }

    /// Target and target group labels the target was made from.
    pub fn t_labels(&self) -> LabelSet {
        self.r().t_labels.clone()
    }
    pub fn tg_labels(&self) -> LabelSet {
        self.r().tg_labels.clone()
    }
    pub fn scrape_config(&self) -> Arc<ScrapeConfig> {
        self.r().scrape_config.clone()
    }

    pub fn list_metadata(&self) -> Vec<MetricMetadata> {
        match &self.r().metadata {
            Some(m) => m.list_metadata(),
            None => Vec::new(),
        }
    }
    pub fn size_metadata(&self) -> usize {
        self.r().metadata.as_ref().map_or(0, |m| m.size_metadata())
    }
    pub fn length_metadata(&self) -> usize {
        self.r().metadata.as_ref().map_or(0, |m| m.length_metadata())
    }
    /// Type and help metadata of a metric family.
    pub fn get_metadata(&self, mf_name: &str) -> Option<MetricMetadata> {
        self.r().metadata.as_ref().and_then(|m| m.get_metadata(mf_name))
    }
    pub fn set_metadata_store(&self, s: Arc<dyn MetricMetadataStore>) {
        self.w().metadata = Some(s);
    }

    /// Go `hash`: an identifying hash of the labels and the URL.
    pub fn hash(&self) -> u64 {
        let mut b = format!("{:016}", self.labels.hash()).into_bytes();
        b.extend_from_slice(self.url_string().as_bytes());
        fnv64a(&b)
    }

    /// Go `offset`: the time until the next scrape cycle of the target, at wall clock `now`.
    pub fn offset_at(&self, interval: Duration, offset_seed: u64, now: SystemTime) -> Duration {
        let now = now.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0);
        let iv = interval.as_nanos() as i64;
        if iv <= 0 {
            return Duration::ZERO;
        }
        let base = iv - now.rem_euclid(iv);
        let offset = (self.hash() ^ offset_seed) % iv as u64;
        let mut next = base + offset as i64;
        if next > iv {
            next -= iv;
        }
        Duration::from_nanos(next as u64)
    }

    /// Go `offset` with the system clock.
    pub fn offset(&self, interval: Duration, offset_seed: u64) -> Duration {
        self.offset_at(interval, offset_seed, SystemTime::now())
    }

    /// Go `Labels`: the public labels (without the `__` prefix).
    pub fn labels(&self) -> Labels {
        let mut b = Builder::new(Labels::default());
        for (n, v) in self.labels.iter() {
            if !n.starts_with(RESERVED_LABEL_PREFIX) {
                b.set(n, v);
            }
        }
        b.labels()
    }

    /// Go `LabelsRange`: calls `f` on each public label.
    pub fn labels_range(&self, mut f: impl FnMut(&str, &str)) {
        for (n, v) in self.labels.iter() {
            if !n.starts_with(RESERVED_LABEL_PREFIX) {
                f(n, v);
            }
        }
    }

    /// Go `DiscoveredLabels`: the labels before any processing.
    pub fn discovered_labels(&self, lb: &mut Builder) -> Labels {
        let (cfg, t, tg) = {
            let m = self.r();
            (m.scrape_config.clone(), m.t_labels.clone(), m.tg_labels.clone())
        };
        populate_discovered_labels(lb, &cfg, &t, &tg);
        lb.labels()
    }

    /// Go `SetScrapeConfig`.
    pub fn set_scrape_config(&self, cfg: Arc<ScrapeConfig>, t_labels: LabelSet, tg_labels: LabelSet) {
        let mut m = self.w();
        m.scrape_config = cfg;
        m.t_labels = t_labels;
        m.tg_labels = tg_labels;
    }

    /// Go `URL`.
    pub fn url(&self) -> yaml::url::GoUrl {
        let params = self.r().scrape_config.params.clone();
        let mut params: Vec<(String, Vec<String>)> = params.into_iter().collect();
        for (n, v) in self.labels.iter() {
            let Some(ks) = n.strip_prefix(PARAM_LABEL_PREFIX) else { continue };
            match params.iter_mut().find(|(k, _)| k == ks) {
                Some((_, vs)) if !vs.is_empty() => vs[0] = v.to_string(),
                Some((_, vs)) => vs.push(v.to_string()),
                None => params.push((ks.to_string(), vec![v.to_string()])),
            }
        }
        let mut pairs = Vec::new();
        params.sort_by(|a, b| a.0.cmp(&b.0));
        for (k, vs) in params {
            for v in vs {
                pairs.push((k.clone(), v));
            }
        }
        let mut host = self.labels.get(ADDRESS_LABEL).to_string();
        if host.is_empty() && !self.labels.get(UNIX_SOCKET_LABEL).is_empty() {
            host = "localhost".to_string();
        }
        yaml::url::GoUrl {
            scheme: self.labels.get(SCHEME_LABEL).to_string(),
            host,
            path: self.labels.get(METRICS_PATH_LABEL).to_string(),
            raw_query: yaml::url::encode_query(&pairs),
            ..Default::default()
        }
    }

    /// The URL as a string (Go `URL().String()`).
    pub fn url_string(&self) -> String {
        self.url().to_url_string()
    }

    /// Go `Report`: records the result of the last scrape.
    pub fn report(&self, start: SystemTime, dur: Duration, err: Option<&Error>) {
        let mut m = self.w();
        m.health = if err.is_none() { TargetHealth::Good } else { TargetHealth::Bad };
        m.last_error = err.cloned();
        m.last_scrape = Some(start);
        m.last_scrape_duration = dur;
    }

    pub fn last_error(&self) -> Option<Error> {
        self.r().last_error.clone()
    }
    /// The time of the last scrape (None: never scraped, Go's zero time).
    pub fn last_scrape(&self) -> Option<SystemTime> {
        self.r().last_scrape
    }
    pub fn last_scrape_duration(&self) -> Duration {
        self.r().last_scrape_duration
    }
    pub fn health(&self) -> TargetHealth {
        self.r().health
    }

    /// Go `intervalAndTimeout`: interval and timeout from the target labels.
    pub fn interval_and_timeout(
        &self,
        default_interval: Duration,
        default_timeout: Duration,
    ) -> (Duration, Duration, Option<Error>) {
        let il = self.labels.get(SCRAPE_INTERVAL_LABEL);
        let interval = match yaml::types::parse_duration(il) {
            Ok(d) => d,
            Err(e) => {
                return (
                    default_interval,
                    default_timeout,
                    Some(Error::msg(format!("error parsing interval label {}: {}", go_quote(il), e))),
                );
            }
        };
        let tl = self.labels.get(SCRAPE_TIMEOUT_LABEL);
        let timeout = match yaml::types::parse_duration(tl) {
            Ok(d) => d,
            Err(e) => {
                return (
                    default_interval,
                    default_timeout,
                    Some(Error::msg(format!("error parsing timeout label {}: {}", go_quote(tl), e))),
                );
            }
        };
        (interval.to_std(), timeout.to_std(), None)
    }

    /// Go `boolLabel`: the boolean value of a target label, `def` when unset or invalid.
    pub fn bool_label(&self, name: &str, def: bool) -> bool {
        let v = self.labels.get(name);
        if v.is_empty() {
            return def;
        }
        parse_bool(v).unwrap_or(def)
    }

    /// Go `GetValue`: a label value from the entire label set.
    pub fn get_value(&self, name: &str) -> &str {
        self.labels.get(name)
    }
}

/// Go `PopulateDiscoveredLabels`: base labels from target, group and scrape configuration, before relabeling.
pub fn populate_discovered_labels(lb: &mut Builder, cfg: &ScrapeConfig, t_labels: &LabelSet, tg_labels: &LabelSet) {
    lb.reset(Labels::default());
    for (ln, lv) in t_labels.iter() {
        lb.set(ln, lv);
    }
    for (ln, lv) in tg_labels.iter() {
        if !t_labels.contains_key(ln) {
            lb.set(ln, lv);
        }
    }
    let bool_str = |b: bool| if b { "true" } else { "false" };
    let scrape_labels: [(&str, String); 8] = [
        (JOB_LABEL, cfg.job_name.clone()),
        (SCRAPE_INTERVAL_LABEL, cfg.scrape_interval.go_string()),
        (SCRAPE_TIMEOUT_LABEL, cfg.scrape_timeout.go_string()),
        (METRICS_PATH_LABEL, cfg.metrics_path.clone()),
        (SCHEME_LABEL, cfg.scheme.clone()),
        (CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, bool_str(cfg.convert_classic_histograms_to_nhcb_enabled()).to_string()),
        (ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL, bool_str(cfg.always_scrape_classic_histograms_enabled()).to_string()),
        (SCRAPE_NATIVE_HISTOGRAMS_LABEL, bool_str(cfg.scrape_native_histograms_enabled()).to_string()),
    ];
    for (n, v) in scrape_labels.iter() {
        if lb.get(n).is_empty() {
            lb.set(n, v);
        }
    }
    for (k, v) in cfg.params.iter() {
        let name = format!("{PARAM_LABEL_PREFIX}{k}");
        if !v.is_empty() && lb.get(&name).is_empty() {
            lb.set(&name, &v[0]);
        }
    }
}

/// Go `PopulateLabels`: labels from discovery and configuration, relabeled and checked, with `instance`.
/// `Ok(empty)` means that relabeling dropped the target.
pub fn populate_labels(
    lb: &mut Builder,
    cfg: &ScrapeConfig,
    t_labels: &LabelSet,
    tg_labels: &LabelSet,
) -> Result<Labels, Error> {
    populate_discovered_labels(lb, cfg, t_labels, tg_labels);
    let keep = relabel::process_builder(lb, cfg.relabel_configs.iter());
    if !keep {
        return Ok(Labels::default());
    }
    if lb.get(ADDRESS_LABEL).is_empty() {
        return Err(Error::msg("no address"));
    }
    let addr = lb.get(ADDRESS_LABEL).to_string();
    config::check_target_address(&addr).map_err(Error::msg)?;

    let interval = lb.get(SCRAPE_INTERVAL_LABEL).to_string();
    let interval_d = yaml::types::parse_duration(&interval)
        .map_err(|e| Error::msg(format!("error parsing scrape interval: {e}")))?;
    if interval_d.0 == 0 {
        return Err(Error::msg("scrape interval cannot be 0"));
    }
    let timeout = lb.get(SCRAPE_TIMEOUT_LABEL).to_string();
    let timeout_d = yaml::types::parse_duration(&timeout)
        .map_err(|e| Error::msg(format!("error parsing scrape timeout: {e}")))?;
    if timeout_d.0 == 0 {
        return Err(Error::msg("scrape timeout cannot be 0"));
    }
    if timeout_d.0 > interval_d.0 {
        return Err(Error::msg(format!(
            "scrape timeout cannot be greater than scrape interval ({} > {})",
            go_quote(&timeout),
            go_quote(&interval)
        )));
    }
    for (name, desc) in [
        (CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, "convert classic histograms to nhcb"),
        (ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL, "always scrape classic histograms"),
        (SCRAPE_NATIVE_HISTOGRAMS_LABEL, "scrape native histograms"),
    ] {
        let v = lb.get(name);
        if !v.is_empty()
            && let Err(e) = parse_bool(v)
        {
            return Err(Error::msg(format!("error parsing {desc}: {e}")));
        }
    }
    // Meta labels are deleted after relabeling. Other internal labels propagate to the target.
    lb.range_mut(|b, n, _| {
        if n.starts_with(META_LABEL_PREFIX) {
            b.del(n);
        }
    });
    if lb.get(INSTANCE_LABEL).is_empty() {
        lb.set(INSTANCE_LABEL, &addr);
    }
    // Label values are valid UTF-8 by construction (Go checks `model.LabelValue.IsValid`).
    Ok(lb.labels())
}

/// Go `TargetsFromGroup`: targets of a group, and the failures.
pub fn targets_from_group(
    tg: &Group,
    cfg: &Arc<ScrapeConfig>,
    targets: &mut Vec<Arc<Target>>,
    lb: &mut Builder,
) -> Vec<Error> {
    targets.clear();
    let mut failures = Vec::new();
    for (i, t_labels) in tg.targets.iter().enumerate() {
        match populate_labels(lb, cfg, t_labels, &tg.labels) {
            Err(e) => failures.push(Error::wrapped(format!("instance {} in group {}: {}", i, tg, e), e)),
            Ok(lset) => targets.push(Arc::new(Target::new(lset, cfg.clone(), t_labels.clone(), tg.labels.clone()))),
        }
    }
    failures
}

/// Go `Targets` sort order: by URL.
pub fn sort_targets(ts: &mut [Arc<Target>]) {
    ts.sort_by_cached_key(|t| t.url_string());
}
