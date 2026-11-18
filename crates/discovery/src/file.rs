//! Port of `discovery/file`: target groups from JSON or YAML files matched by glob patterns.
//!
//! Watching differs from Go: there is no inotify (fsnotify) here. Instead the discoverer checks every
//! `WATCH_POLL_INTERVAL` whether a watched directory or a file read in the last refresh changed (a stat of each:
//! inode, size, modification time; mode-only changes are ignored, as Go ignores chmod events) and then
//! re-reads everything, as Go does on any event. The full refresh at `refresh_interval` stays.
use std::collections::{BTreeMap, HashMap};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

use model::labelset::META_LABEL_PREFIX;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{Collector, CounterVec, Family, Metric, MetricRegisterer, MetricType, MetricValue, Registry, SummaryVec, Opts};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

/// How often the watched paths are checked for changes (the replacement of fsnotify events).
pub const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(250);

fn pat_file_sd_name() -> &'static model::regexp::Regexp {
    static R: OnceLock<model::regexp::Regexp> = OnceLock::new();
    R.get_or_init(|| model::regexp::Regexp::must_compile(r"^[^*]*(\*[^/]*)?\.(json|yml|yaml|JSON|YML|YAML)$"))
}

/// The label with the path of the file a group came from.
pub fn file_sd_filepath_label() -> String {
    format!("{META_LABEL_PREFIX}filepath")
}

/// Go `file.SDConfig`.
#[derive(Clone, Debug, PartialEq)]
pub struct SdConfig {
    pub files: Vec<String>,
    pub refresh_interval: yaml::Duration,
}

impl Default for SdConfig {
    /// The Go zero value (decoding starts from `default_sd_config`).
    fn default() -> Self {
        SdConfig { files: Vec::new(), refresh_interval: yaml::Duration(0) }
    }
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { files: Vec::new(), refresh_interval: yaml::Duration::from_secs(300) }
}

yaml::yaml_struct!(SdConfig, "file.plain", [
    (files: "files")
    (refresh_interval: "refresh_interval", omitempty)
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.files.is_empty() {
                return Err(YError::Msg("file service discovery config must contain at least one path name".into()));
            }
            for name in &self.files {
                if !pat_file_sd_name().is_match(name) {
                    return Err(YError::Msg(format!("path name {} is not valid for file discovery", yaml::go_quote(name))));
                }
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "file.plain".to_string()
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
    const GO_TYPE: &'static str = "file.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "file"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts.logger, &opts.metrics)?))
    }
    fn new_discoverer_metrics(&self, reg: &Arc<Registry>, _rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        FileMetrics::new(reg)
    }
    fn set_directory(&mut self, dir: &str) {
        for f in self.files.iter_mut() {
            *f = net::config::join_dir(dir, f);
        }
    }
}

// ---------------- metrics ----------------

type Timestamps = Arc<Mutex<BTreeMap<String, f64>>>;

/// Go `TimestampCollector`: `prometheus_sd_file_mtime_seconds{filename}` of the files read by its discoverers.
#[derive(Default)]
pub struct TimestampCollector {
    discoverers: Mutex<HashMap<usize, Timestamps>>,
}

impl Collector for TimestampCollector {
    fn describe(&self) -> Vec<String> {
        vec!["prometheus_sd_file_mtime_seconds".to_string()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let mut unique: BTreeMap<String, f64> = BTreeMap::new();
        for ts in self.discoverers.lock().unwrap_or_else(|e| e.into_inner()).values() {
            for (k, v) in ts.lock().unwrap_or_else(|e| e.into_inner()).iter() {
                unique.insert(k.clone(), *v);
            }
        }
        out.push(Family {
            name: "prometheus_sd_file_mtime_seconds".to_string(),
            help: "Timestamp (mtime) of files read by FileSD. Timestamp is set at read time.".to_string(),
            typ: MetricType::Gauge,
            metrics: unique.into_iter().map(|(f, v)| Metric { labels: vec![("filename".to_string(), f)], value: MetricValue::Gauge(v) }).collect(),
        });
    }
}

/// Go `fileMetrics`.
pub struct FileMetrics {
    pub read_errors: Arc<CounterVec>,
    pub scan_duration: Arc<SummaryVec>,
    pub watcher_errors: Arc<CounterVec>,
    pub timestamps: Arc<TimestampCollector>,
    registerer: MetricRegisterer,
}

impl FileMetrics {
    pub fn new(reg: &Arc<Registry>) -> Arc<FileMetrics> {
        let read_errors = CounterVec::new_counter(Opts::new("prometheus_sd_file_read_errors_total", "The number of File-SD read errors."));
        let scan_duration = SummaryVec::new_summary(Opts::new("prometheus_sd_file_scan_duration_seconds", "The duration of the File-SD scan in seconds."));
        let watcher_errors = CounterVec::new_counter(Opts::new("prometheus_sd_file_watcher_errors_total", "The number of File-SD errors caused by filesystem watch failures."));
        let timestamps = Arc::new(TimestampCollector::default());
        let registerer = MetricRegisterer::new(reg.clone(), vec![read_errors.clone(), scan_duration.clone(), watcher_errors.clone(), timestamps.clone()]);
        Arc::new(FileMetrics { read_errors, scan_duration, watcher_errors, timestamps, registerer })
    }
}

impl DiscovererMetrics for FileMetrics {
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

// ---------------- discovery ----------------

/// The state of a watched path: None if it does not exist.
type Stamp = Option<(u64, u64, i64, i64, u64)>;

fn stamp(path: &str) -> Stamp {
    std::fs::metadata(path).ok().map(|m| (m.dev(), m.ino(), m.mtime(), m.mtime_nsec(), m.size()))
}

/// Go `file.Discovery`.
pub struct Discovery {
    paths: Vec<String>,
    interval: Duration,
    timestamps: Timestamps,
    /// Files found in the last refresh and how many groups they had.
    last_refresh: BTreeMap<String, usize>,
    logger: Logger,
    metrics: Arc<FileMetrics>,
    id: usize,
    watched: Vec<(String, Stamp)>,
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(conf: &SdConfig, logger: Logger, metrics: &Option<Arc<dyn DiscovererMetrics>>) -> Result<Discovery, String> {
        let fm = crate::downcast_metrics::<FileMetrics>(metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let d = Discovery {
            paths: conf.files.clone(),
            interval: conf.refresh_interval.to_std(),
            timestamps: Arc::new(Mutex::new(BTreeMap::new())),
            last_refresh: BTreeMap::new(),
            logger,
            metrics: fm,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            watched: Vec::new(),
        };
        d.metrics.timestamps.discoverers.lock().unwrap_or_else(|e| e.into_inner()).insert(d.id, d.timestamps.clone());
        Ok(d)
    }

    /// Go `listFiles`.
    fn list_files(&self) -> Vec<String> {
        let mut out = Vec::new();
        for p in &self.paths {
            match crate::glob::glob(p) {
                Ok(f) => out.extend(f),
                Err(e) => self.logger.error("Error expanding glob", &[("glob", p), ("err", &e)]),
            }
        }
        out
    }

    /// Go `watchFiles`: the directories of the patterns, plus (instead of directory events for in-place writes)
    /// the files read in the last refresh.
    fn watch_files(&mut self) {
        let mut paths: Vec<String> = Vec::new();
        for p in &self.paths {
            let dir = match p.rfind('/') {
                Some(i) => p[..=i].to_string(),
                None => "./".to_string(),
            };
            if !paths.contains(&dir) {
                paths.push(dir);
            }
        }
        for f in self.last_refresh.keys() {
            if !paths.contains(f) {
                paths.push(f.clone());
            }
        }
        self.watched = paths.into_iter().map(|p| {
            let s = stamp(&p);
            (p, s)
        }).collect();
    }

    fn watch_changed(&self) -> bool {
        self.watched.iter().any(|(p, s)| stamp(p) != *s)
    }

    /// Go `refresh`: reads all files and sends their groups; sends empty groups for sources that disappeared.
    /// Returns false when the channel was cancelled.
    fn refresh(&mut self, ch: &Sender) -> bool {
        let t0 = Instant::now();
        let ok = self.refresh_inner(ch);
        self.metrics.scan_duration.get().observe(t0.elapsed().as_secs_f64());
        ok
    }

    fn refresh_inner(&mut self, ch: &Sender) -> bool {
        let mut r: BTreeMap<String, usize> = BTreeMap::new();
        for p in self.list_files() {
            match self.read_file(&p) {
                Err(e) => {
                    self.metrics.read_errors.get().inc();
                    self.logger.error("Error reading file", &[("path", &p), ("err", &e)]);
                    // Prevent deletion down below.
                    r.insert(p.clone(), self.last_refresh.get(&p).copied().unwrap_or(0));
                    continue;
                }
                Ok(tgs) => {
                    let n = tgs.len();
                    if !ch.send(tgs) {
                        return false;
                    }
                    r.insert(p, n);
                }
            }
        }
        // Send empty updates for sources that disappeared.
        for (f, n) in self.last_refresh.clone() {
            let m = r.get(&f).copied();
            if m.is_none() || n > m.unwrap_or(0) {
                self.logger.debug("file_sd refresh found file that should be removed", &[("file", &f)]);
                self.timestamps.lock().unwrap_or_else(|e| e.into_inner()).remove(&f);
                for i in m.unwrap_or(0)..n {
                    if !ch.send(vec![Arc::new(Group { source: file_source(&f, i), ..Default::default() })]) {
                        return false;
                    }
                }
            }
        }
        self.last_refresh = r;
        self.watch_files();
        true
    }

    /// Go `readFile`: the groups of a JSON or YAML file, with sources and the file path label set.
    pub fn read_file(&self, filename: &str) -> Result<Vec<Arc<Group>>, String> {
        let content = std::fs::read(filename).map_err(|e| format!("open {}: {}", filename, net::types::go_io_error(&e)))?;
        let md = std::fs::metadata(filename).map_err(|e| format!("stat {}: {}", filename, net::types::go_io_error(&e)))?;
        let ext = match filename.rfind(['.', '/']) {
            Some(i) if filename.as_bytes()[i] == b'.' => filename[i..].to_lowercase(),
            _ => String::new(),
        };
        let groups: Vec<Option<Group>> = match ext.as_str() {
            ".json" => Group::list_from_json(&content)?,
            ".yml" | ".yaml" => yaml::unmarshal_strict::<Vec<Option<Group>>>(&String::from_utf8_lossy(&content)).map_err(|e| e.to_string())?,
            _ => panic!("discovery.File.readFile: unhandled file extension {}", yaml::go_quote(&ext)),
        };
        let mut out = Vec::with_capacity(groups.len());
        let label = file_sd_filepath_label();
        for (i, g) in groups.into_iter().enumerate() {
            let Some(mut g) = g else {
                return Err("nil target group item found".to_string());
            };
            g.source = file_source(filename, i);
            g.labels.insert(label.clone(), filename.to_string());
            out.push(Arc::new(g));
        }
        let mtime = md.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| d.as_secs() as f64).unwrap_or(0.0);
        self.timestamps.lock().unwrap_or_else(|e| e.into_inner()).insert(filename.to_string(), mtime);
        Ok(out)
    }

    fn stop(&mut self) {
        self.logger.debug("Stopping file discovery...", &[("paths", &format!("{:?}", self.paths))]);
        self.metrics.timestamps.discoverers.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
        self.logger.debug("File discovery stopped", &[]);
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, ch: Sender) {
        if self.refresh(&ch) {
            let interval = self.interval.max(Duration::from_millis(1));
            let mut next_refresh = Instant::now() + interval;
            loop {
                let now = Instant::now();
                let wait = (next_refresh.saturating_duration_since(now)).min(WATCH_POLL_INTERVAL);
                if ctx.wait_timeout(wait) {
                    break;
                }
                let now = Instant::now();
                let due = now >= next_refresh;
                if due {
                    while next_refresh <= now {
                        next_refresh += interval;
                    }
                }
                if (due || self.watch_changed()) && !self.refresh(&ch) {
                    break;
                }
            }
        }
        self.stop();
    }
}

/// Go `fileSource`: the source id of the i-th group of a file.
pub fn file_source(filename: &str, i: usize) -> String {
    format!("{filename}:{i}")
}
