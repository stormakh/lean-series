//! Port of `storage/remote/metadata_watcher.go`: periodically reads the metadata of the active scrape targets
//! and sends it to a `MetadataAppender` (remote write 1.0 with `metadata_config.send`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use discovery::log::Logger;
use model::metadata::MetricType;
use scrape::target::{MetricMetadata, Target};

use crate::queue_manager::{Ctx, Signal};

/// Go `MetadataAppender`: sends metadata read from the scrape manager on to somewhere else.
pub trait MetadataAppender: Send + Sync {
    fn append_watcher_metadata(&self, ctx: &Ctx, metadata: &[MetricMetadata]);
}

/// Go `Watchable`: where the active targets for metadata come from.
pub trait Watchable: Send + Sync {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>>;
}

impl Watchable for scrape::manager::Manager {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        scrape::manager::Manager::targets_active(self)
    }
}

/// Go `ReadyScrapeManager`: gives the scrape manager once it is ready.
pub trait ReadyScrapeManager: Send + Sync {
    fn get(&self) -> Result<Arc<dyn Watchable>, String>;
}

/// Go `noopScrapeManager`.
struct NoopScrapeManager;

impl ReadyScrapeManager for NoopScrapeManager {
    fn get(&self) -> Result<Arc<dyn Watchable>, String> {
        Err("scrape manager not ready".to_string())
    }
}

/// Go `MetadataWatcher`: watches the scrape manager for a given `MetadataAppender`.
pub struct MetadataWatcher {
    name: String,
    logger: Logger,

    manager_getter: Arc<dyn ReadyScrapeManager>,
    manager: Mutex<Option<Arc<dyn Watchable>>>,
    writer: Arc<dyn MetadataAppender>,

    interval: Duration,
    deadline: Duration,

    done: Mutex<Arc<Signal>>,
    soft_shutdown: Mutex<Ctx>,
    hard_shutdown: Mutex<Ctx>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl MetadataWatcher {
    /// Go `NewMetadataWatcher`.
    pub fn new(
        logger: Option<Logger>,
        mg: Option<Arc<dyn ReadyScrapeManager>>,
        name: &str,
        w: Arc<dyn MetadataAppender>,
        interval: Duration,
        deadline: Duration,
    ) -> Arc<MetadataWatcher> {
        Arc::new(MetadataWatcher {
            name: name.to_string(),
            logger: logger.unwrap_or_default(),
            manager_getter: mg.unwrap_or_else(|| Arc::new(NoopScrapeManager)),
            manager: Mutex::new(None),
            writer: w,
            interval,
            deadline,
            done: Mutex::new(Arc::new(Signal::new())),
            soft_shutdown: Mutex::new(Ctx::background()),
            hard_shutdown: Mutex::new(Ctx::background()),
            handle: Mutex::new(None),
        })
    }

    /// The name given at construction.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Go `Start`.
    pub fn start(self: &Arc<Self>) {
        self.logger.info("Starting scraped metadata watcher", &[]);
        let hard = Ctx::with_cancel();
        let soft = Ctx::with_cancel();
        let done = Arc::new(Signal::new());
        *lock(&self.hard_shutdown) = hard;
        *lock(&self.soft_shutdown) = soft.clone();
        *lock(&self.done) = done.clone();
        let mw = self.clone();
        let h = std::thread::Builder::new()
            .name("metadata-watcher".to_string())
            .spawn(move || {
                mw.run_loop(&soft);
                done.set();
            })
            .expect("spawn metadata watcher thread");
        *lock(&self.handle) = Some(h);
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        self.logger.info("Stopping metadata watcher...", &[]);
        let soft = lock(&self.soft_shutdown).clone();
        let hard = lock(&self.hard_shutdown).clone();
        let done = lock(&self.done).clone();
        soft.cancel();
        if !done.wait_timeout(self.deadline) {
            self.logger.error("Failed to flush metadata", &[]);
            // The hard shutdown context is the parent of the soft one in Go.
            hard.cancel();
            done.wait();
        }
        if let Some(h) = lock(&self.handle).take() {
            let _ = h.join();
        }
        self.logger.info("Scraped metadata watcher stopped", &[]);
    }

    fn run_loop(&self, soft: &Ctx) {
        // Go `time.NewTicker`: ticks at start + k*interval, dropping ticks for a slow receiver.
        let interval = self.interval.max(Duration::from_nanos(1));
        let mut next = Instant::now() + interval;
        loop {
            let now = Instant::now();
            if now < next && soft.sleep(next - now) {
                return;
            }
            if soft.is_done() {
                return;
            }
            self.collect();
            next += interval;
            let now = Instant::now();
            if next <= now {
                let behind = (now - next).as_nanos() / interval.as_nanos();
                next += interval * (behind as u32 + 1);
            }
        }
    }

    /// Go `collect`.
    pub fn collect(&self) {
        if !self.ready() {
            return;
        }
        let Some(manager) = lock(&self.manager).clone() else { return };

        // We create a set of the metadata to help deduplicating based on the attributes of a
        // scrape.MetricMetadata. In this case, a combination of metric name, help, type, and unit.
        let mut metadata_set: HashSet<(String, MetricType, String, String)> = HashSet::new();
        let mut metadata: Vec<MetricMetadata> = Vec::new();
        for tset in manager.targets_active().values() {
            for target in tset {
                for entry in target.list_metadata() {
                    let key = (entry.metric_family.clone(), entry.typ, entry.help.clone(), entry.unit.clone());
                    if metadata_set.insert(key) {
                        metadata.push(entry);
                    }
                }
            }
        }

        // Blocks until the metadata is sent to the remote write endpoint or hardShutdownContext is expired.
        let hard = lock(&self.hard_shutdown).clone();
        self.writer.append_watcher_metadata(&hard, &metadata);
    }

    /// Go `ready`.
    pub fn ready(&self) -> bool {
        let mut m = lock(&self.manager);
        if m.is_some() {
            return true;
        }
        match self.manager_getter.get() {
            Ok(mgr) => {
                *m = Some(mgr);
                true
            }
            Err(_) => false,
        }
    }

    /// Sets the watched manager (Go tests assign `mw.manager`).
    pub fn set_manager(&self, m: Arc<dyn Watchable>) {
        *lock(&self.manager) = Some(m);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
