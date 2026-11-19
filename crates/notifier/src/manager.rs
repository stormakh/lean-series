//! Port of `notifier/manager.go`: the notification manager.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use discovery::manager::{SyncReceiver, TargetSets};
use discovery::{Logger, Registry};
use model::labels::{Labels, ValidationScheme};
use model::relabel;
use yaml::url::GoUrl;

use crate::alert::{relabel_alerts, Alert};
use crate::alertmanager_set::AlertmanagerSet;
use crate::metric::AlertMetrics;
use crate::send_loop::DoFn;

/// Go `DefaultMaxBatchSize`: the default maximum number of alerts in one request to an Alertmanager.
pub const DEFAULT_MAX_BATCH_SIZE: usize = 256;
pub const CONTENT_TYPE_JSON: &str = "application/json";
/// Go `version.PrometheusUserAgent()`.
pub const USER_AGENT: &str = "Prometheus/3.16.0";

/// Go `Options`.
#[derive(Clone, Default)]
pub struct Options {
    pub queue_capacity: usize,
    pub drain_on_shutdown: bool,
    pub external_labels: Labels,
    pub relabel_configs: Vec<relabel::Config>,
    /// Sends HTTP requests to the Alertmanager; None uses `send_loop::do_request`.
    pub do_fn: Option<DoFn>,
    pub registerer: Option<Arc<Registry>>,
    /// The maximum number of alerts in a single request; 0 means `DEFAULT_MAX_BATCH_SIZE`.
    pub max_batch_size: usize,
}

/// A source of target set updates (Go `<-chan map[string][]*targetgroup.Group`): `None` means closed.
pub trait TargetSetsRecv: Send {
    fn recv_sets(&mut self) -> Option<TargetSets>;
}

impl TargetSetsRecv for SyncReceiver {
    fn recv_sets(&mut self) -> Option<TargetSets> {
        self.recv()
    }
}

impl TargetSetsRecv for std::sync::mpsc::Receiver<TargetSets> {
    fn recv_sets(&mut self) -> Option<TargetSets> {
        self.recv().ok()
    }
}

/// The state guarded by Go's `Manager.mtx`.
pub struct ManagerState {
    pub external_labels: Labels,
    pub relabel_configs: Vec<relabel::Config>,
    pub alertmanagers: BTreeMap<String, Arc<AlertmanagerSet>>,
}

/// Go `Manager`: dispatches alert notifications to Alertmanagers.
pub struct Manager {
    opts: Arc<Options>,
    metrics: Arc<AlertMetrics>,
    state: Arc<RwLock<ManagerState>>,
    stop_requested: AtomicBool,
    stop_mu: Mutex<()>,
    stop_cv: Condvar,
    logger: Logger,
}

fn set_default_scheme(rcs: &mut [relabel::Config], scheme: ValidationScheme) {
    for rc in rcs {
        match rc.name_validation_scheme {
            ValidationScheme::Legacy | ValidationScheme::Utf8 => {}
            ValidationScheme::Unset => rc.name_validation_scheme = scheme,
        }
    }
}

fn count_ams(st: &ManagerState) -> usize {
    st.alertmanagers.values().map(|s| s.read().ams.len()).sum()
}

impl Manager {
    /// Go `NewManager`.
    pub fn new(mut o: Options, name_validation_scheme: ValidationScheme, logger: Option<Logger>) -> Manager {
        if o.max_batch_size == 0 {
            o.max_batch_size = DEFAULT_MAX_BATCH_SIZE;
        }
        let logger = logger.unwrap_or_else(Logger::nop);
        set_default_scheme(&mut o.relabel_configs, name_validation_scheme);

        let state = Arc::new(RwLock::new(ManagerState {
            external_labels: o.external_labels.clone(),
            relabel_configs: o.relabel_configs.clone(),
            alertmanagers: BTreeMap::new(),
        }));
        let weak = Arc::downgrade(&state);
        let discovered = Box::new(move || match weak.upgrade() {
            Some(s) => count_ams(&s.read().unwrap_or_else(|e| e.into_inner())) as f64,
            None => 0.0,
        });
        let metrics = Arc::new(AlertMetrics::new(o.registerer.as_ref(), discovered));
        metrics.queue_capacity.get().set(o.queue_capacity as f64);

        Manager { opts: Arc::new(o), metrics, state, stop_requested: AtomicBool::new(false), stop_mu: Mutex::new(()), stop_cv: Condvar::new(), logger }
    }

    /// The options shared with the send loops.
    pub fn opts(&self) -> Arc<Options> {
        self.opts.clone()
    }

    pub fn metrics(&self) -> Arc<AlertMetrics> {
        self.metrics.clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, ManagerState> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, ManagerState> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The Alertmanager sets by config key ("config-0", ...).
    pub fn alertmanager_sets(&self) -> BTreeMap<String, Arc<AlertmanagerSet>> {
        self.read().alertmanagers.clone()
    }

    /// Replaces the Alertmanager sets (Go tests assign `n.alertmanagers`).
    pub fn set_alertmanager_sets(&self, sets: BTreeMap<String, Arc<AlertmanagerSet>>) {
        self.write().alertmanagers = sets;
    }

    /// Go `ApplyConfig`: external labels, alert relabeling and the Alertmanager sets. Sets whose config did not
    /// change keep their Alertmanagers and send loops; the loops of removed or changed sets are stopped.
    pub fn apply_config(&self, conf: &config::Config) -> Result<(), String> {
        let mut st = self.write();
        st.external_labels = conf.global_config.external_labels.clone();
        st.relabel_configs = conf.alerting_config.alert_relabel_configs.clone();
        set_default_scheme(&mut st.relabel_configs, conf.global_config.metric_name_validation_scheme);

        let mut am_sets = BTreeMap::new();
        // Maps the config of each old set to the set, to reuse known Alertmanagers without waiting for SD.
        let mut config_to_ams: BTreeMap<String, Arc<AlertmanagerSet>> = BTreeMap::new();
        for old in st.alertmanagers.values() {
            config_to_ams.insert(old.config_hash()?, old.clone());
        }

        for (k, cfg) in conf.alerting_config.alertmanager_configs_map() {
            let ams = AlertmanagerSet::new(cfg.clone(), self.opts.clone(), self.logger.clone(), self.metrics.clone())?;
            let hash = ams.config_hash()?;
            if let Some(old) = config_to_ams.get(&hash) {
                // Only transfer the send loops to the first new config with this hash.
                let mut ost = old.write();
                let loops = ost.send_loops.take();
                ams.adopt(ost.ams.clone(), ost.dropped_ams.clone(), loops);
            }
            am_sets.insert(k, Arc::new(ams));
        }

        // Clean up the send loops that were not transferred (key removed, or hash changed).
        for old in st.alertmanagers.values() {
            let mut ost = old.write();
            if ost.send_loops.is_some() {
                let ams = ost.ams.clone();
                AlertmanagerSet::clean_send_loops_locked(&mut ost, &ams);
            }
        }

        st.alertmanagers = am_sets;
        Ok(())
    }

    /// Go `Run`: processes target updates until Stop is called or `tsets` closes, then waits for Stop and stops
    /// the send loops (draining them when configured). `None` is Go's nil channel (no updates).
    ///
    /// Target updates are read on a separate thread, so Run returns once Stop is called even while that thread
    /// still waits for the next update; the thread ends when `tsets` yields or closes.
    pub fn run(self: &Arc<Self>, tsets: Option<Box<dyn TargetSetsRecv>>) {
        if let Some(mut t) = tsets {
            let m = self.clone();
            let spawned = std::thread::Builder::new().name("notifier-targets".to_string()).spawn(move || m.target_update_loop(&mut *t));
            if let Err(e) = spawned {
                self.logger.error("Starting the target update loop failed", &[("err", &e.to_string())]);
            }
        }
        self.wait_stop();

        let st = self.write();
        for ams in st.alertmanagers.values() {
            ams.clean_send_loops();
        }
    }

    /// Go `targetUpdateLoop`: applies target updates until Stop is called or `tsets` closes.
    pub fn target_update_loop(&self, tsets: &mut dyn TargetSetsRecv) {
        loop {
            if self.is_stopped() {
                return;
            }
            match tsets.recv_sets() {
                // The discovery manager closed the channel; no more updates are coming.
                None => return,
                Some(ts) => {
                    if self.is_stopped() {
                        return;
                    }
                    self.reload(&ts);
                }
            }
        }
    }

    /// Go `reload`: syncs each set with its target groups.
    pub fn reload(&self, tgs: &TargetSets) {
        let st = self.write();
        for (id, groups) in tgs {
            match st.alertmanagers.get(id) {
                Some(am) => am.sync(groups),
                None => self.logger.error("couldn't sync alert manager set", &[("err", &format!("invalid id:{id}"))]),
            }
        }
    }

    /// Go `Send`: applies external labels and alert relabeling, then queues the alerts on every set.
    pub fn send(&self, alerts: Vec<Alert>) {
        let alerts: Vec<Arc<Alert>> = alerts.into_iter().map(Arc::new).collect();
        self.send_arcs(&alerts);
    }

    /// Like `send`, for alerts already shared.
    pub fn send_arcs(&self, alerts: &[Arc<Alert>]) {
        // If we've been asked to stop, that takes priority over accepting new alerts.
        if self.is_stopped() {
            return;
        }
        let st = self.read();
        let alerts = relabel_alerts(&st.relabel_configs, &st.external_labels, alerts);
        if alerts.is_empty() {
            return;
        }
        for ams in st.alertmanagers.values() {
            ams.send(&alerts);
        }
    }

    /// Go `Alertmanagers`: the URLs of the Alertmanagers in use.
    pub fn alertmanagers(&self) -> Vec<GoUrl> {
        let sets = self.alertmanager_sets();
        let mut res = Vec::new();
        for s in sets.values() {
            res.extend(s.read().ams.iter().map(|a| a.url().clone()));
        }
        res
    }

    /// Go `DroppedAlertmanagers`: the URLs of the Alertmanagers dropped by relabeling.
    pub fn dropped_alertmanagers(&self) -> Vec<GoUrl> {
        let sets = self.alertmanager_sets();
        let mut res = Vec::new();
        for s in sets.values() {
            res.extend(s.read().dropped_ams.iter().map(|a| a.url().clone()));
        }
        res
    }

    /// Go `Stop`: signals the manager to shut down and returns at once; Run returns after the shutdown.
    /// Safe to call more than once.
    pub fn stop(&self) {
        self.logger.info("Stopping notification manager...", &[]);
        let _g = self.stop_mu.lock().unwrap_or_else(|e| e.into_inner());
        self.stop_requested.store(true, Ordering::SeqCst);
        self.stop_cv.notify_all();
    }

    pub fn is_stopped(&self) -> bool {
        self.stop_requested.load(Ordering::SeqCst)
    }

    fn wait_stop(&self) {
        let mut g = self.stop_mu.lock().unwrap_or_else(|e| e.into_inner());
        while !self.is_stopped() {
            g = self.stop_cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }
}
