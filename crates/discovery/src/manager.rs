//! Port of `discovery/manager.go`: runs one provider per distinct SD config, merges their target groups per
//! target set (scrape job) and sends the full state to the consumer, throttled.
//!
//! Threads: each provider runs its discoverer on one thread and an updater on another; the sender runs on one
//! thread. The sender sleeps until a provider reports an update (no idle wake-ups); it then waits for the next
//! tick of Go's backoff schedule (100ms growing to `updatert`) and offers the state on the sync channel without
//! blocking, as Go does. After an idle period (more than `updatert` since the last send) Go ticks every
//! ~50-150ms; here the wait after the update is drawn from that range instead of keeping a timer running.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{Receiver as StdReceiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::cancel::CancelToken;
use crate::chan::{self, Recv};
use crate::log::Logger;
use crate::metrics::{CounterVec, GaugeVec, Opts, Registry};
use crate::registry::Configs;
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererOptions, SdMetrics, StaticConfig};

/// The key of a set of target groups: the target set (job) and the provider (Go `poolKey`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolKey {
    pub set_name: String,
    pub provider: String,
}

impl PoolKey {
    pub fn new(set_name: &str, provider: &str) -> PoolKey {
        PoolKey { set_name: set_name.to_string(), provider: provider.to_string() }
    }
}

/// What the manager sends: target set name to all its target groups.
pub type TargetSets = BTreeMap<String, Vec<Arc<Group>>>;

struct ProvState {
    cancel: Option<CancelToken>,
    done: Option<Box<dyn FnOnce() + Send>>,
    subs: BTreeSet<String>,
    new_subs: BTreeSet<String>,
}

/// A discoverer, its config and the target sets that use it (Go `Provider`).
pub struct Provider {
    name: String,
    d: Mutex<Option<Box<dyn Discoverer>>>,
    config: Option<Box<dyn Config>>,
    st: Mutex<ProvState>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Provider {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn config(&self) -> Option<&dyn Config> {
        self.config.as_deref()
    }
    /// Go `IsStarted`.
    pub fn is_started(&self) -> bool {
        lock(&self.st).cancel.is_some()
    }
    /// The target sets subscribed to this provider.
    pub fn subs(&self) -> BTreeSet<String> {
        lock(&self.st).subs.clone()
    }
}

/// The metrics of a manager (Go `discovery.Metrics`), with the const label `name`.
pub struct ManagerMetrics {
    pub failed_configs: Arc<GaugeVec>,
    pub discovered_targets: Arc<GaugeVec>,
    pub received_updates: Arc<CounterVec>,
    pub delayed_updates: Arc<CounterVec>,
    pub sent_updates: Arc<CounterVec>,
    pub last_updated: Arc<GaugeVec>,
}

impl ManagerMetrics {
    /// Go `NewManagerMetrics`: creates and registers the metrics.
    pub fn new(reg: &Arc<Registry>, name: &str) -> Result<ManagerMetrics, String> {
        let o = |n: &str, h: &str| Opts::new(n, h).const_label("name", name);
        let m = ManagerMetrics {
            failed_configs: GaugeVec::new_gauge(o("prometheus_sd_failed_configs", "Current number of service discovery configurations that failed to load.")),
            discovered_targets: GaugeVec::new_gauge_vec(o("prometheus_sd_discovered_targets", "Current number of discovered targets."), &["config"]),
            received_updates: CounterVec::new_counter(o("prometheus_sd_received_updates_total", "Total number of update events received from the SD providers.")),
            delayed_updates: CounterVec::new_counter(o("prometheus_sd_updates_delayed_total", "Total number of update events that couldn't be sent immediately.")),
            sent_updates: CounterVec::new_counter(o("prometheus_sd_updates_total", "Total number of update events sent to the SD consumers.")),
            last_updated: GaugeVec::new_gauge_vec(o("prometheus_sd_last_update_timestamp_seconds", "Timestamp of the last update sent to the SD consumers."), &["config"]),
        };
        for c in m.collectors() {
            reg.register(c)?;
        }
        Ok(m)
    }
    fn collectors(&self) -> Vec<Arc<dyn crate::metrics::Collector>> {
        vec![self.failed_configs.clone(), self.discovered_targets.clone(), self.received_updates.clone(), self.delayed_updates.clone(), self.sent_updates.clone(), self.last_updated.clone()]
    }
    /// Go `Metrics.Unregister`.
    pub fn unregister(&self, reg: &Registry) {
        for c in self.collectors() {
            reg.unregister(&c);
        }
    }
}

/// Options of `Manager::new` (Go's functional options `Name`, `Updatert`, `HTTPClientOptions`).
#[derive(Clone)]
pub struct ManagerOptions {
    pub name: String,
    /// How long to wait at most between updates to the consumer (default 5s).
    pub updatert: Duration,
    pub http_client_options: net::HttpClientOptions,
}

impl Default for ManagerOptions {
    fn default() -> Self {
        ManagerOptions { name: String::new(), updatert: Duration::from_secs(5), http_client_options: net::HttpClientOptions::default() }
    }
}

struct MState {
    providers: Vec<Arc<Provider>>,
    last_provider: usize,
}

struct Trigger {
    pending: bool,
}

struct Inner {
    logger: Logger,
    name: String,
    http_opts: net::HttpClientOptions,
    ctx: CancelToken,
    mtx: RwLock<MState>,
    targets: Mutex<BTreeMap<PoolKey, BTreeMap<String, Arc<Group>>>>,
    sync_tx: Mutex<Option<SyncSender<TargetSets>>>,
    sync_rx: Arc<Mutex<StdReceiver<TargetSets>>>,
    updatert: Mutex<Duration>,
    trigger: Mutex<Trigger>,
    trigger_cv: Condvar,
    registerer: Arc<Registry>,
    metrics: ManagerMetrics,
    sd_metrics: SdMetrics,
}

/// The discovery manager (Go `discovery.Manager`). Cloning shares it.
#[derive(Clone)]
pub struct Manager {
    inner: Arc<Inner>,
}

/// The consumer end of the manager's sync channel (Go `<-chan map[string][]*targetgroup.Group`).
#[derive(Clone)]
pub struct SyncReceiver(Arc<Mutex<StdReceiver<TargetSets>>>);

impl SyncReceiver {
    /// Blocks for the next state; None when the manager stopped (channel closed).
    pub fn recv(&self) -> Option<TargetSets> {
        lock(&self.0).recv().ok()
    }
    /// Like `recv`; `Err(true)` on timeout, `Err(false)` when closed.
    pub fn recv_timeout(&self, d: Duration) -> Result<TargetSets, bool> {
        match lock(&self.0).recv_timeout(d) {
            Ok(v) => Ok(v),
            Err(RecvTimeoutError::Timeout) => Err(true),
            Err(RecvTimeoutError::Disconnected) => Err(false),
        }
    }
}

impl Manager {
    /// Go `NewManager`: the manager stops when `ctx` is cancelled. Fails when its metrics cannot be registered.
    pub fn new(ctx: &CancelToken, logger: Logger, registerer: &Arc<Registry>, sd_metrics: &SdMetrics, opts: ManagerOptions) -> Result<Manager, String> {
        let metrics = ManagerMetrics::new(registerer, &opts.name).inspect_err(|e| {
            logger.error("Failed to create discovery manager metrics", &[("manager", &opts.name), ("err", e)]);
        })?;
        let (tx, rx) = sync_channel(0);
        Ok(Manager {
            inner: Arc::new(Inner {
                logger,
                name: opts.name,
                http_opts: opts.http_client_options,
                ctx: ctx.clone(),
                mtx: RwLock::new(MState { providers: Vec::new(), last_provider: 0 }),
                targets: Mutex::new(BTreeMap::new()),
                sync_tx: Mutex::new(Some(tx)),
                sync_rx: Arc::new(Mutex::new(rx)),
                updatert: Mutex::new(opts.updatert),
                trigger: Mutex::new(Trigger { pending: false }),
                trigger_cv: Condvar::new(),
                registerer: registerer.clone(),
                metrics,
                sd_metrics: sd_metrics.clone(),
            }),
        })
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Sets the throttle interval (Go `Updatert`; tests). Takes effect when `run` starts.
    pub fn set_updatert(&self, d: Duration) {
        *lock(&self.inner.updatert) = d;
    }

    /// The manager's metrics.
    pub fn metrics(&self) -> &ManagerMetrics {
        &self.inner.metrics
    }

    /// Go `UnregisterMetrics`.
    pub fn unregister_metrics(&self) {
        self.inner.metrics.unregister(&self.inner.registerer);
    }

    /// The current providers.
    pub fn providers(&self) -> Vec<Arc<Provider>> {
        self.inner.mtx.read().unwrap_or_else(|e| e.into_inner()).providers.clone()
    }

    /// A copy of the target groups by pool key (Go `m.targets`; tests).
    pub fn targets(&self) -> BTreeMap<PoolKey, BTreeMap<String, Arc<Group>>> {
        lock(&self.inner.targets).clone()
    }

    /// Go `SyncCh`.
    pub fn sync_ch(&self) -> SyncReceiver {
        SyncReceiver(self.inner.sync_rx.clone())
    }

    /// Go `Run`: starts the sender and blocks until the context is cancelled, then stops all discoverers.
    /// Returns Go's `ctx.Err()` text.
    pub fn run(&self) -> Result<(), String> {
        let inner = self.inner.clone();
        let tx = lock(&self.inner.sync_tx).take();
        if let Some(tx) = tx {
            std::thread::Builder::new()
                .name("sd-sender".into())
                .spawn(move || sender(inner, tx))
                .map_err(|e| e.to_string())?;
        }
        // wake the sender when the manager stops
        let weak = Arc::downgrade(&self.inner);
        self.inner.ctx.on_cancel(Box::new(move || {
            if let Some(i) = weak.upgrade() {
                let _g = lock(&i.trigger);
                i.trigger_cv.notify_all();
            }
        }));
        self.inner.ctx.wait();
        self.cancel_discoverers();
        Err("context canceled".to_string())
    }

    fn cancel_discoverers(&self) {
        let st = self.inner.mtx.read().unwrap_or_else(|e| e.into_inner());
        for p in &st.providers {
            if let Some(c) = &lock(&p.st).cancel {
                c.cancel();
            }
        }
    }

    /// Go `ApplyConfig`: keeps providers whose config did not change, stops the others and starts new ones.
    pub fn apply_config(&self, cfg: &BTreeMap<String, Configs>) {
        let inner = &self.inner;
        let mut st = inner.mtx.write().unwrap_or_else(|e| e.into_inner());
        let mut failed = 0;
        for (name, scfg) in cfg {
            failed += self.register_providers(&mut st, scfg, name);
        }
        inner.metrics.failed_configs.get().set(failed as f64);

        let wg = Arc::new((Mutex::new(0usize), Condvar::new()));
        let mut new_providers = Vec::new();
        for prov in st.providers.iter() {
            let mut ps = lock(&prov.st);
            if ps.new_subs.is_empty() && ps.cancel.is_some() {
                *lock(&wg.0) += 1;
                let wg2 = wg.clone();
                ps.done = Some(Box::new(move || {
                    *lock(&wg2.0) -= 1;
                    wg2.1.notify_all();
                }));
                if let Some(c) = &ps.cancel {
                    c.cancel();
                }
                let subs = ps.subs.clone();
                drop(ps);
                let mut targets = lock(&inner.targets);
                for s in &subs {
                    targets.remove(&PoolKey::new(s, &prov.name));
                    inner.metrics.discovered_targets.delete_label_values(&[s]);
                    inner.metrics.last_updated.delete_label_values(&[s]);
                    if let Some(c) = &prov.config {
                        inner.sd_metrics.refresh_manager.delete_label_values(c.name(), s);
                    }
                }
                continue;
            }
            new_providers.push(prov.clone());
            let mut ref_targets: Option<BTreeMap<String, Arc<Group>>> = None;
            {
                let mut targets = lock(&inner.targets);
                for s in ps.subs.iter() {
                    let k = PoolKey::new(s, &prov.name);
                    ref_targets = targets.get(&k).cloned();
                    if !ps.new_subs.contains(s) {
                        targets.remove(&k);
                        inner.metrics.discovered_targets.delete_label_values(&[s]);
                        inner.metrics.last_updated.delete_label_values(&[s]);
                        if let Some(c) = &prov.config {
                            inner.sd_metrics.refresh_manager.delete_label_values(c.name(), s);
                        }
                    }
                }
                for s in ps.new_subs.iter() {
                    if !ps.subs.contains(s) {
                        inner.metrics.discovered_targets.with_label_values(&[s]).set(0.0);
                    }
                    if let Some(rt) = &ref_targets
                        && !rt.is_empty()
                    {
                        targets.insert(PoolKey::new(s, &prov.name), rt.clone());
                    }
                }
            }
            ps.subs = std::mem::take(&mut ps.new_subs);
            let started = ps.cancel.is_some();
            drop(ps);
            if !started {
                self.start_provider(&inner.ctx, prov);
            }
        }
        if !st.providers.is_empty() {
            self.trigger();
        }
        st.providers = new_providers;
        let (m, cv) = &*wg;
        let mut n = lock(m);
        while *n > 0 {
            n = cv.wait(n).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Go `StartCustomProvider` (sdtool and tests).
    pub fn start_custom_provider(&self, ctx: &CancelToken, name: &str, worker: Box<dyn Discoverer>) {
        let p = Arc::new(Provider {
            name: name.to_string(),
            d: Mutex::new(Some(worker)),
            config: None,
            st: Mutex::new(ProvState { cancel: None, done: None, subs: BTreeSet::from([name.to_string()]), new_subs: BTreeSet::new() }),
        });
        self.inner.mtx.write().unwrap_or_else(|e| e.into_inner()).providers.push(p.clone());
        self.start_provider(ctx, &p);
    }

    fn start_provider(&self, ctx: &CancelToken, p: &Arc<Provider>) {
        let inner = &self.inner;
        inner.logger.debug("Starting provider", &[("provider", &p.name), ("subs", &format!("{:?}", lock(&p.st).subs))]);
        let ctx = ctx.child();
        let (tx, rx) = chan::channel_with_cancel(&ctx);
        lock(&p.st).cancel = Some(ctx.clone());
        if let Some(mut d) = lock(&p.d).take() {
            let c2 = ctx.clone();
            let name = format!("sd-{}", p.name);
            let _ = std::thread::Builder::new().name(name).spawn(move || d.run(&c2, tx));
        }
        let m = self.clone();
        let p = p.clone();
        let _ = std::thread::Builder::new().name("sd-updater".into()).spawn(move || m.updater(&ctx, &p, rx));
    }

    fn cleaner(&self, p: &Provider) {
        let mut ps = lock(&p.st);
        {
            let mut targets = lock(&self.inner.targets);
            for s in ps.subs.iter() {
                targets.remove(&PoolKey::new(s, &p.name));
            }
        }
        if let Some(done) = ps.done.take() {
            done();
        }
        ps.cancel = None;
    }

    fn updater(&self, ctx: &CancelToken, p: &Arc<Provider>, rx: chan::Receiver) {
        loop {
            match rx.recv() {
                Recv::Cancelled | Recv::Timeout => break,
                Recv::Closed => {
                    self.inner.metrics.received_updates.get().inc();
                    self.inner.logger.debug("Discoverer channel closed", &[("provider", &p.name)]);
                    ctx.wait();
                    break;
                }
                Recv::Update(tgs) => {
                    self.inner.metrics.received_updates.get().inc();
                    let subs = lock(&p.st).subs.clone();
                    for s in subs {
                        self.update_group(&PoolKey::new(&s, &p.name), &tgs);
                    }
                    self.trigger();
                }
            }
        }
        drop(rx);
        self.cleaner(p);
    }

    fn trigger(&self) {
        let mut t = lock(&self.inner.trigger);
        t.pending = true;
        self.inner.trigger_cv.notify_all();
    }

    /// Go `updateGroup`: stores non-empty groups by source and drops empty ones.
    pub fn update_group(&self, pk: &PoolKey, tgs: &[Arc<Group>]) {
        let mut targets = lock(&self.inner.targets);
        let m = targets.entry(pk.clone()).or_default();
        for tg in tgs {
            if !tg.targets.is_empty() {
                m.insert(tg.source.clone(), tg.clone());
            } else {
                m.remove(&tg.source);
            }
        }
    }

    /// Go `allGroups`: every target set of every provider, with an empty list for sets without targets.
    pub fn all_groups(&self) -> TargetSets {
        let mut tsets: TargetSets = BTreeMap::new();
        let mut n: BTreeMap<String, usize> = BTreeMap::new();
        {
            let st = self.inner.mtx.read().unwrap_or_else(|e| e.into_inner());
            for p in &st.providers {
                let ps = lock(&p.st);
                let targets = lock(&self.inner.targets);
                for s in ps.subs.iter() {
                    let set = tsets.entry(s.clone()).or_default();
                    let count = n.entry(s.clone()).or_insert(0);
                    if let Some(groups) = targets.get(&PoolKey::new(s, &p.name)) {
                        for tg in groups.values() {
                            set.push(tg.clone());
                            *count += tg.targets.len();
                        }
                    }
                }
            }
        }
        for (set, v) in n {
            self.inner.metrics.discovered_targets.with_label_values(&[&set]).set(v as f64);
            self.inner.metrics.last_updated.with_label_values(&[&set]).set_to_current_time();
        }
        tsets
    }

    /// Go `registerProviders`: returns the number of configs that failed.
    fn register_providers(&self, st: &mut MState, cfgs: &Configs, set_name: &str) -> usize {
        let mut failed = 0;
        let mut added = false;
        for c in cfgs.iter() {
            if self.add_provider(st, c.as_ref(), set_name) {
                added = true;
            } else {
                failed += 1;
            }
        }
        if !added {
            // An empty target group makes the consumer drop the targets of this set (when all configs failed or
            // there are none).
            if !self.add_provider(st, &StaticConfig(vec![Group::default()]), set_name) {
                failed += 1;
            }
        }
        failed
    }

    /// Adds `set_name` to the provider with an equal config or starts a new one; false if it failed.
    fn add_provider(&self, st: &mut MState, cfg: &dyn Config, set_name: &str) -> bool {
        let inner = &self.inner;
        for p in &st.providers {
            if let Some(pc) = &p.config
                && pc.eq_config(cfg)
            {
                lock(&p.st).new_subs.insert(set_name.to_string());
                return true;
            }
        }
        let typ = cfg.name();
        let opts = DiscovererOptions {
            logger: inner.logger.with(&[("discovery", typ), ("config", set_name)]),
            metrics: inner.sd_metrics.mechanism_metrics.get(typ).cloned(),
            http_client_options: inner.http_opts.clone(),
            set_name: set_name.to_string(),
        };
        match cfg.new_discoverer(opts) {
            Err(e) => {
                inner.logger.error("Cannot create service discovery", &[("err", &e), ("type", typ), ("config", set_name)]);
                false
            }
            Ok(d) => {
                st.providers.push(Arc::new(Provider {
                    name: format!("{}/{}", typ, st.last_provider),
                    d: Mutex::new(Some(d)),
                    config: Some(cfg.clone_config()),
                    st: Mutex::new(ProvState { cancel: None, done: None, subs: BTreeSet::new(), new_subs: BTreeSet::from([set_name.to_string()]) }),
                }));
                st.last_provider += 1;
                true
            }
        }
    }
}

// ---------------- sender ----------------

/// cenkalti/backoff ExponentialBackOff with Go's defaults (randomization 0.5, multiplier 1.5).
struct Backoff {
    initial: Duration,
    max: Duration,
    current: Duration,
    rng: u64,
}

impl Backoff {
    fn new(initial: Duration, max: Duration) -> Backoff {
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64 | 1;
        Backoff { initial, max, current: Duration::ZERO, rng: seed }
    }
    fn rand(&mut self) -> f64 {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        (self.rng.wrapping_mul(0x2545F4914F6CDD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
    fn reset(&mut self) {
        self.current = self.initial;
    }
    fn next(&mut self) -> Duration {
        if self.current.is_zero() {
            self.current = self.initial;
        }
        let cur = self.current.as_nanos() as f64;
        let delta = 0.5 * cur;
        let (lo, hi) = (cur - delta, cur + delta);
        let next = Duration::from_nanos((lo + self.rand() * (hi - lo + 1.0)) as u64);
        if cur >= self.max.as_nanos() as f64 / 1.5 {
            self.current = self.max;
        } else {
            self.current = Duration::from_nanos((cur * 1.5) as u64);
        }
        next
    }
}

fn sender(inner: Arc<Inner>, tx: SyncSender<TargetSets>) {
    let updatert = *lock(&inner.updatert);
    let mut b = Backoff::new(Duration::from_millis(100), updatert);
    // None: nothing sent yet (Go starts with lastSent = now - updatert, which counts as idle)
    let mut last_sent: Option<Instant> = None;
    let idle_since_send = |last_sent: Option<Instant>, at: Instant| last_sent.is_none_or(|l| at.saturating_duration_since(l) > updatert);
    let mut next_tick = Instant::now() + b.next();
    let m = Manager { inner: inner.clone() };
    loop {
        // Wait for a trigger and the tick after it.
        {
            let mut t = lock(&inner.trigger);
            loop {
                if inner.ctx.is_cancelled() {
                    return; // drops tx: the sync channel closes
                }
                if t.pending {
                    let now = Instant::now();
                    if now >= next_tick {
                        break;
                    }
                    t = inner.trigger_cv.wait_timeout(t, next_tick - now).unwrap_or_else(|e| e.into_inner()).0;
                } else {
                    t = inner.trigger_cv.wait(t).unwrap_or_else(|e| e.into_inner());
                    if t.pending {
                        // Ticks that passed while idle did nothing but advance the schedule.
                        let now = Instant::now();
                        while next_tick < now {
                            if idle_since_send(last_sent, next_tick) {
                                // Go ticks every ~100ms here: the next tick comes within one interval.
                                b.reset();
                                let iv = b.next();
                                next_tick = now + iv.mul_f64(b.rand());
                                break;
                            }
                            next_tick += b.next();
                        }
                    }
                }
            }
            t.pending = false;
        }
        inner.metrics.sent_updates.get().inc();
        let groups = m.all_groups();
        match tx.try_send(groups) {
            Ok(()) => last_sent = Some(Instant::now()),
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                inner.metrics.delayed_updates.get().inc();
                inner.logger.debug("Discovery receiver's channel was full so will retry the next cycle", &[]);
                lock(&inner.trigger).pending = true;
            }
        }
        let now = Instant::now();
        if idle_since_send(last_sent, now) {
            b.reset();
        }
        next_tick = now + b.next();
    }
}
