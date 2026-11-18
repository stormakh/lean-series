//! Port of the scrape pool of `scrape/scrape.go` (Go `scrapePool`): the targets of one scrape config, their
//! loops, reloads and target limits.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use config::ScrapeConfig;
use discovery::targetgroup::Group;
use model::labels::Builder;
use storage::{Appendable, AppendableV2, Error};

use crate::cache::ScrapeCache;
use crate::ctx::Ctx;
use crate::failure_log::FailureLogger;
use crate::manager::Options;
use crate::metrics::ScrapeMetrics;
use crate::runtime::Runtime;
use crate::scrape_loop::{Errc, LabelLimits, ScrapeLoop, mutate_report_sample_labels, mutate_sample_labels, pick_schema};
use crate::scraper::{Scraper, TargetScraper, accept_encoding_header, accept_header};
use crate::target::{
    ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL, CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, SCRAPE_NATIVE_HISTOGRAMS_LABEL, Target,
    UNIX_SOCKET_LABEL, targets_from_group,
};
use crate::util::{BufPool, go_duration_string};

/// A loop that can run and be stopped again; it must not be reused after it was stopped (Go `loop`).
pub trait Loop: Send + Sync + 'static {
    /// Runs the loop until stopped, blocking (Go `run`).
    fn run(self: Arc<Self>, errc: Option<Errc>);
    /// Starts the loop without blocking (Go `go l.run(errc)`).
    fn start(self: Arc<Self>, errc: Option<Errc>) {
        std::thread::spawn(move || self.run(errc));
    }
    fn set_forced_error(&self, err: Option<Error>);
    fn set_scrape_failure_logger(&self, l: Option<Arc<dyn FailureLogger>>);
    /// Asks the loop to stop without waiting.
    fn cancel(&self) {}
    /// Stops the loop and waits until it stopped scraping.
    fn stop(&self);
    fn get_cache(&self) -> Option<Arc<ScrapeCache>>;
    fn disable_end_of_run_staleness_markers(&self);
    /// The real scrape loop behind this loop, if it is one (Go type assertion in tests).
    fn as_scrape_loop(&self) -> Option<&ScrapeLoop> {
        None
    }
}

impl Loop for ScrapeLoop {
    fn run(self: Arc<Self>, errc: Option<Errc>) {
        ScrapeLoop::run(&self, errc)
    }
    fn start(self: Arc<Self>, errc: Option<Errc>) {
        ScrapeLoop::start(&self, errc)
    }
    fn set_forced_error(&self, err: Option<Error>) {
        ScrapeLoop::set_forced_error(self, err)
    }
    fn set_scrape_failure_logger(&self, l: Option<Arc<dyn FailureLogger>>) {
        ScrapeLoop::set_scrape_failure_logger(self, l)
    }
    fn cancel(&self) {
        self.ctx.cancel()
    }
    fn stop(&self) {
        ScrapeLoop::stop(self)
    }
    fn get_cache(&self) -> Option<Arc<ScrapeCache>> {
        Some(self.cache.clone())
    }
    fn disable_end_of_run_staleness_markers(&self) {
        ScrapeLoop::disable_end_of_run_staleness_markers(self)
    }
    fn as_scrape_loop(&self) -> Option<&ScrapeLoop> {
        Some(self)
    }
}

/// Static options of a new loop (Go `scrapeLoopOptions`).
#[derive(Clone)]
pub struct ScrapeLoopOptions {
    pub target: Arc<Target>,
    pub scraper: Arc<dyn Scraper>,
    pub cache: Arc<ScrapeCache>,
    pub interval: std::time::Duration,
    pub timeout: std::time::Duration,
}

/// The mutable state guarded by `mtx` (Go: `config`, `client`, `unixSocketClients`).
pub struct PoolState {
    pub config: Arc<ScrapeConfig>,
    pub client: net::Client,
    pub unix_socket_clients: HashMap<String, net::Client>,
}

/// The state guarded by `target_mtx` (Go: `activeTargets`, `loops`, `droppedTargets`, ...).
#[derive(Default)]
pub struct TargetState {
    pub active_targets: HashMap<u64, Arc<Target>>,
    pub loops: HashMap<u64, Arc<dyn Loop>>,
    pub dropped_targets: Vec<Arc<Target>>,
    pub dropped_targets_count: usize,
    pub scrape_failure_logger: Option<Arc<dyn FailureLogger>>,
}

type NewLoopFn = Box<dyn Fn(ScrapeLoopOptions) -> Arc<dyn Loop> + Send + Sync>;

/// The scrape pool of one scrape config (Go `scrapePool`).
pub struct ScrapePool {
    pub appendable: Option<Arc<dyn Appendable>>,
    pub appendable_v2: Option<Arc<dyn AppendableV2>>,
    pub logger: discovery::Logger,
    pub ctx: Ctx,
    pub options: Arc<Options>,
    /// Must not be taken after `target_mtx`.
    pub mtx: Mutex<PoolState>,
    pub target_mtx: Mutex<TargetState>,
    /// Loop constructor injection for tests (Go `injectTestNewLoop`).
    pub inject_test_new_loop: Option<NewLoopFn>,
    pub metrics: Arc<ScrapeMetrics>,
    pub buffers: Arc<BufPool>,
    pub offset_seed: u64,
    pub runtime: Arc<Runtime>,
}

/// Go `newScrapeClient`.
pub fn new_scrape_client(cfg: &net::HttpClientConfig, opts: &net::HttpClientOptions) -> Result<net::Client, Error> {
    net::new_client_from_config(cfg, opts).map_err(|e| Error::msg(format!("error creating HTTP client: {}", e.msg)))
}

/// Go `newUnixSocketScrapeClient`: a client that dials the unix socket instead of the request URL host.
pub fn new_unix_socket_scrape_client(
    socket_path: &str,
    cfg: &net::HttpClientConfig,
    opts: &net::HttpClientOptions,
) -> Result<net::Client, Error> {
    // TODO(unix): the net transport dials TCP only.
    let _ = (cfg, opts);
    Err(Error::msg(format!("dial unix {socket_path}: unix sockets are not supported")))
}

/// Go `reusableCache`: whether two scrape configs differ only in fields that do not change the scraped series.
pub fn reusable_cache(r: Option<&ScrapeConfig>, l: Option<&ScrapeConfig>) -> bool {
    let (Some(r), Some(l)) = (r, l) else { return false };
    zero_config(r) == zero_config(l)
}

/// Go `zeroConfig`.
pub fn zero_config(c: &ScrapeConfig) -> ScrapeConfig {
    let mut z = c.clone();
    z.scrape_interval = yaml::types::Duration(0);
    z.scrape_timeout = yaml::types::Duration(0);
    z.sample_limit = 0;
    z.http_client_config = net::HttpClientConfig::default();
    z
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Stops loops: cancels all first, then waits for each (in parallel batches, as Go's goroutines do).
pub fn stop_loops(loops: Vec<Arc<dyn Loop>>) {
    for l in &loops {
        l.cancel();
    }
    for chunk in loops.chunks(64) {
        if chunk.len() == 1 {
            chunk[0].stop();
            continue;
        }
        std::thread::scope(|s| {
            for l in chunk {
                s.spawn(move || l.stop());
            }
        });
    }
}

impl ScrapePool {
    /// Go `newScrapePool`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Arc<ScrapeConfig>,
        appendable: Option<Arc<dyn Appendable>>,
        appendable_v2: Option<Arc<dyn AppendableV2>>,
        offset_seed: u64,
        logger: discovery::Logger,
        buffers: Option<Arc<BufPool>>,
        options: Arc<Options>,
        metrics: Arc<ScrapeMetrics>,
    ) -> Result<Arc<ScrapePool>, Error> {
        let buffers = buffers.unwrap_or_else(|| Arc::new(BufPool::new(1000, 1_000_000, 3)));
        let client = new_scrape_client(&cfg.http_client_config, &options.http_client_options)?;
        if let Err(e) = config::to_escaping_scheme(&cfg.metric_name_escaping_scheme, cfg.metric_name_validation_scheme) {
            return Err(Error::msg(format!("invalid metric name escaping scheme, {e}")));
        }
        let runtime = options.runtime.clone().unwrap_or_else(Runtime::global);
        let job = cfg.job_name.clone();
        let target_limit = cfg.target_limit;
        let sp = Arc::new(ScrapePool {
            appendable,
            appendable_v2,
            logger,
            ctx: Ctx::background(),
            options,
            mtx: Mutex::new(PoolState { config: cfg, client, unix_socket_clients: HashMap::new() }),
            target_mtx: Mutex::new(TargetState::default()),
            inject_test_new_loop: None,
            metrics,
            buffers,
            offset_seed,
            runtime,
        });
        sp.metrics.target_scrape_pool_target_limit.with_label_values(&[&job]).set(target_limit as f64);
        Ok(sp)
    }

    /// A pool for tests (Go `newTestScrapePool`): empty config, default client, injected loop constructor.
    pub fn new_for_test(
        appendable: Option<Arc<dyn Appendable>>,
        appendable_v2: Option<Arc<dyn AppendableV2>>,
        inject: Option<NewLoopFn>,
        metrics: Arc<ScrapeMetrics>,
    ) -> ScrapePool {
        ScrapePool {
            appendable,
            appendable_v2,
            logger: discovery::Logger::nop(),
            ctx: Ctx::background(),
            options: Arc::new(Options::default()),
            mtx: Mutex::new(PoolState {
                config: Arc::new(ScrapeConfig::default()),
                client: net::Client::new(Arc::new(
                    net::Transport::new(Default::default(), None, Default::default(), Default::default())
                        .expect("default transport"),
                )),
                unix_socket_clients: HashMap::new(),
            }),
            target_mtx: Mutex::new(TargetState::default()),
            inject_test_new_loop: inject,
            metrics,
            buffers: Arc::new(BufPool::new(1000, 1_000_000, 3)),
            offset_seed: 0,
            runtime: Runtime::global(),
        }
    }

    /// The current scrape config.
    pub fn config(&self) -> Arc<ScrapeConfig> {
        lock(&self.mtx).config.clone()
    }

    /// Replaces the scrape config (tests set Go's `sp.config` directly).
    pub fn set_config(&self, cfg: Arc<ScrapeConfig>) {
        lock(&self.mtx).config = cfg;
    }

    /// The loops by target hash.
    pub fn loops(&self) -> HashMap<u64, Arc<dyn Loop>> {
        lock(&self.target_mtx).loops.clone()
    }

    /// The active targets by hash.
    pub fn active_targets_map(&self) -> HashMap<u64, Arc<Target>> {
        lock(&self.target_mtx).active_targets.clone()
    }

    /// Go `newLoop`.
    pub fn new_loop(&self, cfg: &Arc<ScrapeConfig>, opts: ScrapeLoopOptions) -> Arc<dyn Loop> {
        if let Some(f) = &self.inject_test_new_loop {
            return f(opts);
        }
        new_scrape_loop(self, cfg, opts)
    }

    /// Go `clientForTarget`. Must be called with `mtx` held (passed in).
    pub fn client_for_target(&self, st: &mut PoolState, t: &Target) -> net::Client {
        let socket = t.raw_labels().get(UNIX_SOCKET_LABEL);
        if socket.is_empty() {
            return st.client.clone();
        }
        if let Some(c) = st.unix_socket_clients.get(socket) {
            return c.clone();
        }
        let c = match new_unix_socket_scrape_client(socket, &st.config.http_client_config, &self.options.http_client_options) {
            Ok(c) => c,
            Err(e) => {
                self.logger.error(
                    "Failed to create unix socket scrape client",
                    &[("socket_path", socket), ("err", &e.to_string())],
                );
                let msg = format!("creating unix socket scrape client for {}: {}", yaml::go_quote(socket), e);
                net::Client::new(Arc::new(FailingRoundTripper(msg)))
            }
        };
        st.unix_socket_clients.insert(socket.to_string(), c.clone());
        c
    }

    /// Go `ActiveTargets`.
    pub fn active_targets(&self) -> Vec<Arc<Target>> {
        lock(&self.target_mtx).active_targets.values().cloned().collect()
    }

    /// Go `DroppedTargets`: dropped targets, up to `keep_dropped_targets`.
    pub fn dropped_targets(&self) -> Vec<Arc<Target>> {
        lock(&self.target_mtx).dropped_targets.clone()
    }

    /// Go `DroppedTargetsCount`.
    pub fn dropped_targets_count(&self) -> usize {
        lock(&self.target_mtx).dropped_targets_count
    }

    /// Go `SetScrapeFailureLogger`.
    pub fn set_scrape_failure_logger(&self, l: Option<Arc<dyn FailureLogger>>) {
        let job = self.config().job_name.clone();
        let mut ts = lock(&self.target_mtx);
        let l = l.map(|l| l.with("job_name", &job));
        ts.scrape_failure_logger = l.clone();
        for lp in ts.loops.values() {
            lp.set_scrape_failure_logger(l.clone());
        }
    }

    /// Go `stop`: stops all loops and waits for them.
    pub fn stop(&self) {
        let st = lock(&self.mtx);
        let loops: Vec<Arc<dyn Loop>> = {
            let mut ts = lock(&self.target_mtx);
            ts.active_targets.clear();
            ts.loops.drain().map(|(_, l)| l).collect()
        };
        stop_loops(loops);
        // Cancel after all loops stopped: the shutdown scrape uses this context.
        self.ctx.cancel();
        let job = st.config.job_name.clone();
        let m = &self.metrics;
        m.target_scrape_pool_syncs_counter.delete_label_values(&[&job]);
        m.target_scrape_pool_target_limit.delete_label_values(&[&job]);
        m.target_scrape_pool_targets_added.delete_label_values(&[&job]);
        m.target_scrape_pool_symbol_table_items.delete_label_values(&[&job]);
        m.target_sync_interval_length.delete_label_values(&[&job]);
        m.target_sync_interval_length_histogram.delete_label_values(&[&job]);
        m.target_sync_failed.delete_label_values(&[&job]);
    }

    /// Go `reload`: restarts all loops with the new config; returns after the stopped loops stopped.
    pub fn reload(&self, cfg: Arc<ScrapeConfig>) -> Result<(), Error> {
        let mut st = lock(&self.mtx);
        self.metrics.target_scrape_pool_reloads.get().inc();
        let start = Instant::now();
        let client = match new_scrape_client(&cfg.http_client_config, &self.options.http_client_options) {
            Ok(c) => c,
            Err(e) => {
                self.metrics.target_scrape_pool_reloads_failed.get().inc();
                return Err(e);
            }
        };
        if let Err(e) = config::to_escaping_scheme(&cfg.metric_name_escaping_scheme, cfg.metric_name_validation_scheme) {
            return Err(Error::msg(format!("scrapePool.reload: invalid metric name escaping scheme, {e}")));
        }
        let reuse = reusable_cache(Some(&st.config), Some(&cfg));
        st.config = cfg;
        st.client = client;
        st.unix_socket_clients.clear();
        self.metrics
            .target_scrape_pool_target_limit
            .with_label_values(&[&st.config.job_name])
            .set(st.config.target_limit as f64);
        self.restart_loops(&mut st, reuse);
        let iv = go_duration_string(st.config.scrape_interval.to_std());
        self.metrics.target_reload_interval_length.with_label_values(&[&iv]).observe(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Go `restartLoops`. Must be called with `mtx` held.
    pub fn restart_loops(&self, st: &mut PoolState, reuse_cache: bool) {
        let cfg = st.config.clone();
        let mut pairs: Vec<(Arc<dyn Loop>, Arc<dyn Loop>)> = Vec::new();
        let (forced, logger) = {
            let mut ts = lock(&self.target_mtx);
            let forced = self.refresh_target_limit_err(&ts, &cfg);
            let logger = ts.scrape_failure_logger.clone();
            let fps: Vec<u64> = ts.loops.keys().copied().collect();
            for fp in fps {
                let old = ts.loops[&fp].clone();
                let cache = match old.get_cache() {
                    Some(c) if reuse_cache => {
                        old.disable_end_of_run_staleness_markers();
                        c
                    }
                    _ => ScrapeCache::new(self.metrics.clone()),
                };
                let t = ts.active_targets[&fp].clone();
                let (interval, timeout, err) = t.interval_and_timeout(cfg.scrape_interval.to_std(), cfg.scrape_timeout.to_std());
                let client = self.client_for_target(st, &t);
                let scraper = self.target_scraper(&cfg, t.clone(), client, timeout);
                let nl = self.new_loop(&cfg, ScrapeLoopOptions { target: t, scraper, cache, interval, timeout });
                if let Some(e) = err {
                    nl.set_forced_error(Some(e));
                }
                ts.loops.insert(fp, nl.clone());
                pairs.push((old, nl));
            }
            (forced, logger)
        };
        for (old, _) in &pairs {
            old.cancel();
        }
        std::thread::scope(|s| {
            for chunk in pairs.chunks(64) {
                for (old, nl) in chunk {
                    let (forced, logger) = (forced.clone(), logger.clone());
                    s.spawn(move || {
                        old.stop();
                        nl.set_forced_error(forced);
                        nl.set_scrape_failure_logger(logger);
                        nl.clone().start(None);
                    });
                }
            }
        });
    }

    fn target_scraper(&self, cfg: &ScrapeConfig, t: Arc<Target>, client: net::Client, timeout: std::time::Duration) -> Arc<dyn Scraper> {
        let scheme = config::to_escaping_scheme(&cfg.metric_name_escaping_scheme, cfg.metric_name_validation_scheme)
            .unwrap_or(config::EscapingScheme::NoEscaping);
        let protos = cfg.scrape_protocols.clone().unwrap_or_default();
        Arc::new(TargetScraper::new(
            Some(t),
            client,
            timeout,
            cfg.body_size_limit.0,
            accept_header(&protos, scheme),
            accept_encoding_header(cfg.enable_compression, self.options.enable_zstd_scrape).to_string(),
            self.options.enable_zstd_scrape,
            self.logger.clone(),
            self.metrics.clone(),
        ))
    }

    /// Go `Sync`: turns target groups into targets and syncs the loops with them.
    pub fn sync_groups(&self, tgs: &[Arc<Group>]) {
        let mut st = lock(&self.mtx);
        let start = Instant::now();
        let cfg = st.config.clone();
        let mut all = Vec::new();
        {
            let mut ts = lock(&self.target_mtx);
            let mut lb = Builder::new(Default::default());
            ts.dropped_targets.clear();
            ts.dropped_targets_count = 0;
            let mut targets = Vec::new();
            for tg in tgs {
                let failures = targets_from_group(tg, &cfg, &mut targets, &mut lb);
                for e in &failures {
                    self.logger.error("Creating target failed", &[("err", &e.to_string())]);
                }
                self.metrics.target_sync_failed.with_label_values(&[&cfg.job_name]).add(failures.len() as f64);
                for t in targets.drain(..) {
                    let mut non_empty = false;
                    t.labels_range(|_, _| non_empty = true);
                    if non_empty {
                        all.push(t);
                    } else {
                        if cfg.keep_dropped_targets == 0 || ts.dropped_targets.len() < cfg.keep_dropped_targets {
                            ts.dropped_targets.push(t);
                        }
                        ts.dropped_targets_count += 1;
                    }
                }
            }
            self.metrics.target_scrape_pool_symbol_table_items.with_label_values(&[&cfg.job_name]).set(0.0);
        }
        self.sync(&mut st, all);
        let secs = start.elapsed().as_secs_f64();
        self.metrics.target_sync_interval_length.with_label_values(&[&cfg.job_name]).observe(secs);
        self.metrics.target_sync_interval_length_histogram.with_label_values(&[&cfg.job_name]).observe(secs);
        self.metrics.target_scrape_pool_syncs_counter.with_label_values(&[&cfg.job_name]).inc();
    }

    /// Go `Sync` with plain groups.
    pub fn sync_targets_from_groups(&self, tgs: &[Group]) {
        let v: Vec<Arc<Group>> = tgs.iter().cloned().map(Arc::new).collect();
        self.sync_groups(&v)
    }

    /// Go `sync` for tests: takes `mtx` itself.
    pub fn sync_targets(&self, targets: Vec<Arc<Target>>) {
        let mut st = lock(&self.mtx);
        self.sync(&mut st, targets)
    }

    /// Go `sync`: deduplicates targets, starts loops for new ones and stops loops of vanished ones; returns after
    /// the stopped loops stopped. Must be called with `mtx` held.
    pub fn sync(&self, st: &mut PoolState, targets: Vec<Arc<Target>>) {
        let cfg = st.config.clone();
        let mut unique: HashMap<u64, Option<Arc<dyn Loop>>> = HashMap::new();
        let mut to_stop: Vec<Arc<dyn Loop>> = Vec::new();
        let forced;
        let all_loops: Vec<Arc<dyn Loop>>;
        {
            let mut ts = lock(&self.target_mtx);
            let mut sockets: HashSet<String> = HashSet::new();
            for t in targets {
                let hash = t.hash();
                let socket = t.raw_labels().get(UNIX_SOCKET_LABEL);
                if !socket.is_empty() {
                    sockets.insert(socket.to_string());
                }
                if let Some(existing) = ts.active_targets.get(&hash) {
                    unique.entry(hash).or_insert(None);
                    existing.set_scrape_config(cfg.clone(), t.t_labels(), t.tg_labels());
                    continue;
                }
                let (interval, timeout, err) = t.interval_and_timeout(cfg.scrape_interval.to_std(), cfg.scrape_timeout.to_std());
                let client = self.client_for_target(st, &t);
                let scraper = self.target_scraper(&cfg, t.clone(), client, timeout);
                let l = self.new_loop(
                    &cfg,
                    ScrapeLoopOptions { target: t.clone(), scraper, cache: ScrapeCache::new(self.metrics.clone()), interval, timeout },
                );
                if let Some(e) = err {
                    l.set_forced_error(Some(e));
                }
                l.set_scrape_failure_logger(ts.scrape_failure_logger.clone());
                ts.active_targets.insert(hash, t);
                ts.loops.insert(hash, l.clone());
                unique.insert(hash, Some(l));
            }
            let gone: Vec<u64> = ts.active_targets.keys().filter(|h| !unique.contains_key(h)).copied().collect();
            for h in gone {
                if let Some(l) = ts.loops.remove(&h) {
                    to_stop.push(l);
                }
                ts.active_targets.remove(&h);
            }
            st.unix_socket_clients.retain(|p, _| sockets.contains(p));
            forced = self.refresh_target_limit_err(&ts, &cfg);
            all_loops = ts.loops.values().cloned().collect();
        }
        self.metrics.target_scrape_pool_targets_added.with_label_values(&[&cfg.job_name]).set(unique.len() as f64);
        for l in &all_loops {
            l.set_forced_error(forced.clone());
        }
        for l in unique.into_values().flatten() {
            l.start(None);
        }
        stop_loops(to_stop);
    }

    /// Go `refreshTargetLimitErr`.
    pub fn refresh_target_limit_err(&self, ts: &TargetState, cfg: &ScrapeConfig) -> Option<Error> {
        if cfg.target_limit == 0 {
            return None;
        }
        let l = ts.active_targets.len();
        if l > cfg.target_limit {
            self.metrics.target_scrape_pool_exceeded_target_limit.get().inc();
            return Some(Error::msg(format!(
                "target_limit exceeded (number of targets: {}, limit: {})",
                l, cfg.target_limit
            )));
        }
        None
    }

    /// Go `disableEndOfRunStalenessMarkers`.
    pub fn disable_end_of_run_staleness_markers(&self, targets: &[Arc<Target>]) {
        let _st = lock(&self.mtx);
        let ts = lock(&self.target_mtx);
        for t in targets {
            if let Some(l) = ts.loops.get(&t.hash()) {
                l.disable_end_of_run_staleness_markers();
            }
        }
    }
}

/// Go `newScrapeLoop`: a scrape loop for a target of the pool.
pub fn new_scrape_loop(sp: &ScrapePool, cfg: &Arc<ScrapeConfig>, opts: ScrapeLoopOptions) -> Arc<dyn Loop> {
    Arc::new(build_scrape_loop(sp, cfg, opts))
}

/// Builds the scrape loop of Go `newScrapeLoop` (not started).
pub fn build_scrape_loop(sp: &ScrapePool, cfg: &Arc<ScrapeConfig>, opts: ScrapeLoopOptions) -> ScrapeLoop {
    opts.target.set_metadata_store(opts.cache.clone());
    let o = &sp.options;
    let target = opts.target.clone();
    let mut sl = ScrapeLoop::with_defaults(
        sp.ctx.child(),
        sp.ctx.clone(),
        opts.cache.clone(),
        opts.scraper.clone(),
        sp.metrics.clone(),
        sp.buffers.clone(),
        sp.runtime.clone(),
    );
    sl.l = sp.logger.with(&[("target", &target.url_string())]);
    sl.interval = opts.interval;
    sl.timeout = opts.timeout;
    let (t1, honor, mrc) = (target.clone(), cfg.honor_labels, cfg.metric_relabel_configs.clone());
    sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &t1, honor, &mrc));
    let t2 = target.clone();
    sl.report_sample_mutator = Arc::new(move |l| mutate_report_sample_labels(l, &t2));
    sl.appendable = sp.appendable.clone();
    sl.appendable_v2 = sp.appendable_v2.clone();
    sl.offset_seed = sp.offset_seed;
    sl.sample_limit = cfg.sample_limit;
    sl.bucket_limit = cfg.native_histogram_bucket_limit;
    sl.max_schema = pick_schema(cfg.native_histogram_min_bucket_factor);
    sl.label_limits = Some(LabelLimits {
        label_limit: cfg.label_limit,
        label_name_length_limit: cfg.label_name_length_limit,
        label_value_length_limit: cfg.label_value_length_limit,
    });
    sl.honor_labels = cfg.honor_labels;
    sl.honor_timestamps = cfg.honor_timestamps;
    sl.track_timestamps_staleness = cfg.track_timestamps_staleness;
    sl.enable_native_histogram_scraping = target.bool_label(SCRAPE_NATIVE_HISTOGRAMS_LABEL, cfg.scrape_native_histograms_enabled());
    sl.always_scrape_classic_hist =
        target.bool_label(ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL, cfg.always_scrape_classic_histograms_enabled());
    sl.convert_classic_hist_to_nhcb =
        target.bool_label(CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, cfg.convert_classic_histograms_to_nhcb_enabled());
    sl.fallback_scrape_protocol = cfg.scrape_fallback_protocol.header_media_type();
    sl.enable_compression = cfg.enable_compression;
    sl.mrc = cfg.metric_relabel_configs.clone();
    sl.report_extra_metrics = cfg.extra_scrape_metrics_enabled();
    sl.validation_scheme = cfg.metric_name_validation_scheme;
    sl.enable_st_zero_ingestion = o.enable_start_timestamp_zero_ingestion;
    sl.parse_st = o.parse_st || o.enable_start_timestamp_zero_ingestion;
    sl.synthesize_st = o.synthesize_st;
    sl.enable_type_and_unit_labels = o.enable_type_and_unit_labels;
    sl.enable_open_metrics2 = o.enable_open_metrics2;
    sl.append_metadata_to_wal = o.append_metadata;
    sl.pass_metadata_in_context = o.pass_metadata_in_context;
    sl.skip_jitter_offsetting = o.skip_jitter_offsetting;
    sl.scrape_on_shutdown = o.scrape_on_shutdown;
    sl.initial_scrape_offset = o.initial_scrape_offset;
    sl
}

/// A round tripper that fails every request (Go `failingRoundTripper`).
pub struct FailingRoundTripper(pub String);

impl net::RoundTripper for FailingRoundTripper {
    fn round_trip(&self, _req: net::Request) -> Result<net::Response, net::Error> {
        Err(net::Error::other(self.0.clone()))
    }
}
