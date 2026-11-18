//! Port of `scrape/manager.go`: the scrape manager keeps one scrape pool per scrape config, applies config
//! reloads, and syncs the pools with the target groups from service discovery.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::time::Duration;

use config::{Config, ScrapeConfig};
use discovery::metrics::Registry;
use discovery::targetgroup::Group;
use model::labels::Labels;
use storage::{Appendable, AppendableV2, Error};

use crate::failure_log::FailureLogger;
use crate::metrics::{ScrapeMetrics, TargetsGatherer};
use crate::pool::{ScrapePool, stop_loops};
use crate::runtime::{BusyGuard, Runtime};
use crate::scrape_loop::Signal;
use crate::target::Target;
use crate::util::{BufPool, fnv64a};

/// Target groups by scrape job (Go `map[string][]*targetgroup.Group`).
pub type TargetSets = BTreeMap<String, Vec<Arc<Group>>>;

/// A source of target set updates (Go `<-chan map[string][]*targetgroup.Group`).
pub trait TargetSetsSource: Send + 'static {
    /// The next update; None when the source is closed.
    fn recv(&self) -> Option<TargetSets>;
}

impl TargetSetsSource for discovery::manager::SyncReceiver {
    fn recv(&self) -> Option<TargetSets> {
        discovery::manager::SyncReceiver::recv(self)
    }
}

impl TargetSetsSource for std::sync::mpsc::Receiver<TargetSets> {
    fn recv(&self) -> Option<TargetSets> {
        std::sync::mpsc::Receiver::recv(self).ok()
    }
}

/// Makes the failure logger of a scrape config (Go `func(string) (*logging.JSONFileLogger, error)`).
pub type NewFailureLogger = Arc<dyn Fn(&str) -> Result<Option<Arc<dyn FailureLogger>>, String> + Send + Sync>;

/// Configuration of the scrape manager (Go `Options`).
#[derive(Clone)]
pub struct Options {
    /// For downstream users that look metadata up in the appender context; false for Prometheus.
    pub pass_metadata_in_context: bool,
    /// Append scraped metadata to the storage (feature `metadata-wal-records`).
    pub append_metadata: bool,
    /// Throttle for target group updates (default 5s).
    pub discovery_reload_interval: Duration,
    /// Ingest created timestamps as synthetic zero samples (V1 appenders).
    pub enable_start_timestamp_zero_ingestion: bool,
    /// Parse start timestamps from the scrape formats (V2 appenders).
    pub parse_st: bool,
    /// Synthesize start timestamps for cumulative metrics.
    pub synthesize_st: bool,
    pub enable_type_and_unit_labels: bool,
    pub enable_open_metrics2: bool,
    pub enable_zstd_scrape: bool,
    pub http_client_options: net::HttpClientOptions,
    /// A final scrape before the manager closes.
    pub scrape_on_shutdown: bool,
    /// Discover targets at once on start-up instead of after the reload interval.
    pub discovery_reload_on_startup: bool,
    /// A fixed delay added to the per-target offset before the first scrape.
    pub initial_scrape_offset: Duration,
    /// Tests: no jitter offsets (Go private `skipJitterOffsetting`).
    pub skip_jitter_offsetting: bool,
    /// Tests: replaces the FQDN lookup (Go private `fqdn`).
    pub fqdn: String,
    /// The scheduler; None uses the process-wide one.
    pub runtime: Option<Arc<Runtime>>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            pass_metadata_in_context: false,
            append_metadata: false,
            discovery_reload_interval: Duration::ZERO,
            enable_start_timestamp_zero_ingestion: false,
            parse_st: false,
            synthesize_st: false,
            enable_type_and_unit_labels: false,
            enable_open_metrics2: false,
            enable_zstd_scrape: false,
            http_client_options: net::HttpClientOptions::default(),
            scrape_on_shutdown: false,
            discovery_reload_on_startup: false,
            initial_scrape_offset: Duration::ZERO,
            skip_jitter_offsetting: false,
            fqdn: String::new(),
            runtime: None,
        }
    }
}

/// State guarded by the manager mutex (Go `mtxScrape`).
#[derive(Default)]
pub struct ManagerState {
    pub scrape_configs: HashMap<String, Arc<ScrapeConfig>>,
    pub scrape_pools: HashMap<String, Arc<ScrapePool>>,
    pub scrape_failure_loggers: HashMap<String, Option<Arc<dyn FailureLogger>>>,
    pub target_sets: TargetSets,
}

/// The reload trigger and ticker state, guarded by `Manager::trigger`.
#[derive(Default)]
struct ReloadState {
    /// Go `triggerReload` (a channel with a buffer of one).
    trigger: bool,
    /// The reloader blocks waiting for `trigger`.
    waiting_trigger: bool,
    /// The current tick of the reload ticker fired.
    ticked: bool,
    tick_seq: u64,
    /// Busy marks of the runtime handed to the reloader with a tick or trigger (virtual time waits for them).
    guards: Vec<BusyGuard>,
}

/// The scrape manager (Go `Manager`).
pub struct Manager {
    pub opts: Arc<Options>,
    pub logger: discovery::Logger,
    pub appendable: Option<Arc<dyn Appendable>>,
    pub appendable_v2: Option<Arc<dyn AppendableV2>>,
    grace_shut: Signal,
    offset_seed: AtomicU64,
    pub mtx_scrape: Mutex<ManagerState>,
    new_scrape_failure_logger: Option<NewFailureLogger>,
    pub buffers: Arc<BufPool>,
    trigger: Mutex<ReloadState>,
    trigger_cv: Condvar,
    pub metrics: Arc<ScrapeMetrics>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The fully qualified host name (Go `osutil.GetFQDN`; the host name when no DNS name is known).
pub fn get_fqdn() -> Result<String, String> {
    let h = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .map_err(|e| e.to_string())?;
    Ok(h.trim().to_string())
}

impl Manager {
    /// Go `NewManager`: exactly one of `appendable` and `appendable_v2` must be set.
    pub fn new(
        o: Option<Options>,
        logger: Option<discovery::Logger>,
        new_scrape_failure_logger: Option<NewFailureLogger>,
        appendable: Option<Arc<dyn Appendable>>,
        appendable_v2: Option<Arc<dyn AppendableV2>>,
        registerer: Option<Arc<Registry>>,
    ) -> Result<Arc<Manager>, String> {
        let o = o.unwrap_or_default();
        let logger = logger.unwrap_or_default();
        if appendable.is_some() && appendable_v2.is_some() {
            return Err("scrape.NewManager: appendable and appendableV2 cannot be provided at the same time".to_string());
        }
        if appendable.is_none() && appendable_v2.is_none() {
            return Err("scrape.NewManager: provide either appendable or appendableV2".to_string());
        }
        let sm = ScrapeMetrics::new(registerer).map_err(|e| format!("failed to create scrape manager due to error: {e}"))?;
        let m = Arc::new(Manager {
            opts: Arc::new(o),
            logger,
            appendable,
            appendable_v2,
            grace_shut: Signal::default(),
            offset_seed: AtomicU64::new(0),
            mtx_scrape: Mutex::new(ManagerState::default()),
            new_scrape_failure_logger,
            buffers: Arc::new(BufPool::new(1000, 100_000_000, 3)),
            trigger: Mutex::new(ReloadState::default()),
            trigger_cv: Condvar::new(),
            metrics: sm,
        });
        let g: Arc<dyn TargetsGatherer> = m.clone();
        let w: Weak<dyn TargetsGatherer> = Arc::downgrade(&g);
        m.metrics.set_target_metadata_cache_gatherer(w);
        Ok(m)
    }

    /// The offset seed (Go `m.offsetSeed`).
    pub fn offset_seed(&self) -> u64 {
        self.offset_seed.load(Ordering::SeqCst)
    }

    fn runtime(&self) -> Arc<Runtime> {
        self.opts.runtime.clone().unwrap_or_else(Runtime::global)
    }

    /// Go `Run`: receives target set updates and triggers reloads in the background, until the source closes or
    /// the manager stops.
    pub fn run<S: TargetSetsSource>(self: &Arc<Self>, tsets: S) -> Result<(), String> {
        let me = self.clone();
        // The reloader holds the runtime busy until it first blocks (virtual time must not move before it set up
        // its ticker).
        let g = self.runtime().busy_guard();
        std::thread::Builder::new()
            .name("scrape-reloader".into())
            .spawn(move || me.reloader(g))
            .map_err(|e| e.to_string())?;
        let done = Arc::new(Signal::default());
        let me = self.clone();
        let d2 = done.clone();
        std::thread::Builder::new()
            .name("scrape-tsets".into())
            .spawn(move || {
                loop {
                    if me.grace_shut.is_set() {
                        break;
                    }
                    match tsets.recv() {
                        Some(ts) => {
                            let g = me.runtime().busy_guard();
                            me.update_tsets(ts);
                            let mut t = lock(&me.trigger);
                            t.trigger = true;
                            if t.waiting_trigger && !me.grace_shut.is_set() {
                                // Hand the busy mark to the reloader, which reloads now.
                                t.guards.push(g);
                            }
                            me.trigger_cv.notify_all();
                        }
                        None => break,
                    }
                }
                d2.set();
                let _g = lock(&me.trigger);
                me.trigger_cv.notify_all();
            })
            .map_err(|e| e.to_string())?;
        let mut t = lock(&self.trigger);
        while !done.is_set() && !self.grace_shut.is_set() {
            t = self.trigger_cv.wait(t).unwrap_or_else(|e| e.into_inner());
        }
        Ok(())
    }

    /// Takes a pending reload trigger (Go `<-m.triggerReload` in a `select` with `default`); for tests.
    pub fn take_trigger_reload(&self) -> bool {
        std::mem::take(&mut lock(&self.trigger).trigger)
    }

    /// Go `UnregisterMetrics`.
    pub fn unregister_metrics(&self) {
        self.metrics.unregister();
    }

    /// Waits for a reload trigger; false on shutdown. `guards` (busy marks) are dropped when it blocks and the
    /// marks handed over with the trigger are added.
    fn wait_trigger(&self, guards: &mut Vec<BusyGuard>) -> bool {
        let mut t = lock(&self.trigger);
        loop {
            if self.grace_shut.is_set() {
                return false;
            }
            if t.trigger {
                t.trigger = false;
                t.waiting_trigger = false;
                guards.append(&mut t.guards);
                return true;
            }
            t.waiting_trigger = true;
            guards.clear();
            t = self.trigger_cv.wait(t).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Waits for the next tick of the reload ticker (Go `<-ticker.C`, on the runtime clock): `next` is the time
    /// of the next tick; a tick missed while busy is delivered at once, further missed ones are dropped. False on
    /// shutdown.
    fn wait_tick(self: &Arc<Self>, rt: &Arc<Runtime>, next: &mut Duration, interval: Duration, guards: &mut Vec<BusyGuard>) -> bool {
        if *next > rt.now() {
            {
                let mut t = lock(&self.trigger);
                t.ticked = false;
                t.tick_seq += 1;
            }
            let seq = lock(&self.trigger).tick_seq;
            let w = Arc::downgrade(self);
            let rt2 = rt.clone();
            rt.schedule_fn_at(
                *next,
                Box::new(move || {
                    if let Some(m) = w.upgrade() {
                        let g = rt2.busy_guard();
                        let mut t = lock(&m.trigger);
                        if t.tick_seq == seq && !m.grace_shut.is_set() {
                            t.ticked = true;
                            t.guards.push(g);
                            m.trigger_cv.notify_all();
                        }
                    }
                }),
            );
            let mut t = lock(&self.trigger);
            guards.clear();
            loop {
                if self.grace_shut.is_set() {
                    return false;
                }
                if t.ticked {
                    t.ticked = false;
                    guards.append(&mut t.guards);
                    break;
                }
                t = self.trigger_cv.wait(t).unwrap_or_else(|e| e.into_inner());
            }
        } else if self.grace_shut.is_set() {
            return false;
        }
        let now = rt.now();
        *next += interval;
        while *next <= now {
            *next += interval;
        }
        true
    }

    fn reloader(self: Arc<Self>, g: BusyGuard) {
        let rt = self.runtime();
        let mut guards = vec![g];
        let mut interval = self.opts.discovery_reload_interval;
        if interval.is_zero() {
            interval = Duration::from_secs(5);
        }
        let mut next = rt.now() + interval;
        if self.opts.discovery_reload_on_startup {
            if !self.wait_trigger(&mut guards) {
                return;
            }
            self.reload();
            // Go `ticker.Reset`.
            next = rt.now() + interval;
        }
        loop {
            if !self.wait_tick(&rt, &mut next, interval, &mut guards) {
                return;
            }
            if !self.wait_trigger(&mut guards) {
                return;
            }
            self.reload();
        }
    }

    /// Go `reload`: creates missing scrape pools and syncs every pool with its target groups (in parallel).
    pub fn reload(&self) {
        let mut work: Vec<(Arc<ScrapePool>, Vec<Arc<Group>>)> = Vec::new();
        {
            let mut st = lock(&self.mtx_scrape);
            let sets: Vec<(String, Vec<Arc<Group>>)> = st.target_sets.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            for (set_name, groups) in sets {
                if !st.scrape_pools.contains_key(&set_name) {
                    let Some(cfg) = st.scrape_configs.get(&set_name).cloned() else {
                        self.logger.error("error reloading target set", &[("err", &format!("invalid config id:{set_name}"))]);
                        continue;
                    };
                    self.metrics.target_scrape_pools.get().inc();
                    let sp = match ScrapePool::new(
                        cfg.clone(),
                        self.appendable.clone(),
                        self.appendable_v2.clone(),
                        self.offset_seed(),
                        self.logger.with(&[("scrape_pool", &set_name)]),
                        Some(self.buffers.clone()),
                        self.opts.clone(),
                        self.metrics.clone(),
                    ) {
                        Ok(sp) => sp,
                        Err(e) => {
                            self.metrics.target_scrape_pools_failed.get().inc();
                            self.logger.error("error creating new scrape pool", &[("err", &e.to_string()), ("scrape_pool", &set_name)]);
                            continue;
                        }
                    };
                    match st.scrape_failure_loggers.get(&cfg.scrape_failure_log_file) {
                        Some(l) => sp.set_scrape_failure_logger(l.clone()),
                        None => sp.logger.error(
                            "No logger found. This is a bug in Prometheus that should be reported upstream.",
                            &[("scrape_pool", &set_name)],
                        ),
                    }
                    st.scrape_pools.insert(set_name.clone(), sp);
                }
                work.push((st.scrape_pools[&set_name].clone(), groups));
            }
        }
        std::thread::scope(|s| {
            for (sp, groups) in &work {
                s.spawn(move || sp.sync_groups(groups));
            }
        });
    }

    /// Go `setOffsetSeed`: a seed per server from the FQDN and the external labels.
    pub fn set_offset_seed(&self, labels: &Labels) -> Result<(), String> {
        let hostname = if self.opts.fqdn.is_empty() { get_fqdn()? } else { self.opts.fqdn.clone() };
        let s = format!("{}{}", hostname, labels);
        self.offset_seed.store(fnv64a(s.as_bytes()), Ordering::SeqCst);
        Ok(())
    }

    /// Go `Stop`: stops all scrape pools and waits for them.
    pub fn stop(&self) {
        let st = lock(&self.mtx_scrape);
        let pools: Vec<Arc<ScrapePool>> = st.scrape_pools.values().cloned().collect();
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        for chunk in pools.chunks(n.max(1)) {
            std::thread::scope(|s| {
                for sp in chunk {
                    s.spawn(move || sp.stop());
                }
            });
        }
        drop(st);
        self.grace_shut.set();
        let mut t = lock(&self.trigger);
        t.guards.clear();
        self.trigger_cv.notify_all();
    }

    /// Go `updateTsets`.
    pub fn update_tsets(&self, tsets: TargetSets) {
        lock(&self.mtx_scrape).target_sets = tsets;
    }

    /// Go `ApplyConfig`: resets the job configurations and reloads changed pools.
    pub fn apply_config(&self, cfg: &Config) -> Result<(), String> {
        let mut st = lock(&self.mtx_scrape);
        let scfgs = cfg.get_scrape_configs()?;
        let mut c: HashMap<String, Arc<ScrapeConfig>> = HashMap::new();
        let mut loggers: HashMap<String, Option<Arc<dyn FailureLogger>>> = HashMap::new();
        loggers.insert(String::new(), None);
        for scfg in scfgs {
            if !loggers.contains_key(&scfg.scrape_failure_log_file) {
                let l = match &self.new_scrape_failure_logger {
                    Some(f) => f(&scfg.scrape_failure_log_file)?,
                    None => None,
                };
                loggers.insert(scfg.scrape_failure_log_file.clone(), l);
            }
            c.insert(scfg.job_name.clone(), Arc::new(scfg));
        }
        st.scrape_configs = c;
        let old_loggers = std::mem::replace(&mut st.scrape_failure_loggers, loggers);

        if self.opts.skip_jitter_offsetting {
            self.offset_seed.store(0, Ordering::SeqCst);
        } else {
            self.set_offset_seed(&cfg.global_config.external_labels)?;
        }

        let failed = std::sync::atomic::AtomicBool::new(false);
        let to_delete = Mutex::new(Vec::new());
        let pools: Vec<(String, Arc<ScrapePool>)> = st.scrape_pools.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).max(1);
        {
            let st_ref = &*st;
            for chunk in pools.chunks(n) {
                std::thread::scope(|s| {
                    for (name, sp) in chunk {
                        let (failed, to_delete) = (&failed, &to_delete);
                        s.spawn(move || {
                            let Some(cfg) = st_ref.scrape_configs.get(name) else {
                                sp.stop();
                                lock(to_delete).push(name.clone());
                                return;
                            };
                            match st_ref.scrape_failure_loggers.get(&cfg.scrape_failure_log_file) {
                                Some(l) => sp.set_scrape_failure_logger(l.clone()),
                                None => sp.logger.error(
                                    "No logger found. This is a bug in Prometheus that should be reported upstream.",
                                    &[("scrape_pool", name)],
                                ),
                            }
                            if *sp.config() != **cfg
                                && let Err(e) = sp.reload(cfg.clone())
                            {
                                self.logger.error("error reloading scrape pool", &[("err", &e.to_string()), ("scrape_pool", name)]);
                                failed.store(true, Ordering::SeqCst);
                            }
                        });
                    }
                });
            }
        }
        for name in to_delete.into_inner().unwrap_or_default() {
            st.scrape_pools.remove(&name);
        }
        drop(st);
        for l in old_loggers.into_values().flatten() {
            let _ = l.close();
        }
        if failed.load(Ordering::SeqCst) {
            return Err("failed to apply the new configuration".to_string());
        }
        Ok(())
    }

    /// Go `TargetsAll`: active and dropped targets by job.
    pub fn targets_all(&self) -> HashMap<String, Vec<Arc<Target>>> {
        let st = lock(&self.mtx_scrape);
        st.scrape_pools
            .iter()
            .map(|(k, sp)| {
                let mut v = sp.active_targets();
                v.extend(sp.dropped_targets());
                (k.clone(), v)
            })
            .collect()
    }

    /// Go `ScrapePools`: the names of all scrape pools.
    pub fn scrape_pools(&self) -> Vec<String> {
        lock(&self.mtx_scrape).scrape_pools.keys().cloned().collect()
    }

    /// The scrape pool of a job (Go `m.scrapePools[name]`).
    pub fn scrape_pool(&self, name: &str) -> Option<Arc<ScrapePool>> {
        lock(&self.mtx_scrape).scrape_pools.get(name).cloned()
    }

    /// Go `TargetsActive`.
    pub fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        let st = lock(&self.mtx_scrape);
        st.scrape_pools.iter().map(|(k, sp)| (k.clone(), sp.active_targets())).collect()
    }

    /// Go `TargetsDropped`: dropped targets, up to `keep_dropped_targets` per pool.
    pub fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        let st = lock(&self.mtx_scrape);
        st.scrape_pools.iter().map(|(k, sp)| (k.clone(), sp.dropped_targets())).collect()
    }

    /// Go `TargetsDroppedCounts`.
    pub fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        let st = lock(&self.mtx_scrape);
        st.scrape_pools.iter().map(|(k, sp)| (k.clone(), sp.dropped_targets_count())).collect()
    }

    /// Go `ScrapePoolConfig`.
    pub fn scrape_pool_config(&self, scrape_pool: &str) -> Result<Arc<ScrapeConfig>, Error> {
        let st = lock(&self.mtx_scrape);
        match st.scrape_pools.get(scrape_pool) {
            Some(sp) => Ok(sp.config()),
            None => Err(Error::msg(format!("scrape pool {} not found", yaml::go_quote(scrape_pool)))),
        }
    }

    /// Go `DisableEndOfRunStalenessMarkers`.
    pub fn disable_end_of_run_staleness_markers(&self, target_set: &str, targets: &[Arc<Target>]) {
        if targets.is_empty() {
            return;
        }
        let sp = lock(&self.mtx_scrape).scrape_pools.get(target_set).cloned();
        if let Some(sp) = sp {
            sp.disable_end_of_run_staleness_markers(targets);
        }
    }

    /// The scheduler of the manager.
    pub fn scheduler(&self) -> Arc<Runtime> {
        self.runtime()
    }

    /// Stops a list of loops (exposed for tests).
    pub fn stop_loops(loops: Vec<Arc<dyn crate::pool::Loop>>) {
        stop_loops(loops)
    }
}

impl TargetsGatherer for Manager {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        Manager::targets_active(self)
    }
}
