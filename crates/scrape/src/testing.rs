//! Test helpers that mirror the unexported helpers of the Go tests (`newTestScrapeLoop`, `testScraper`,
//! `newTestScrapePool`, `testLoop`, `newTestScrapeMetrics`). Tests live in `tests/` and only see public items.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use discovery::metrics::Registry;
use model::labels::{Labels, ValidationScheme};
use storage::{Appendable, AppendableV2, Error};

use crate::cache::ScrapeCache;
use crate::ctx::Ctx;
use crate::failure_log::FailureLogger;
use crate::metrics::ScrapeMetrics;
use crate::pool::{Loop, ScrapeLoopOptions, ScrapePool};
use crate::runtime::Runtime;
use crate::scrape_loop::{Errc, LabelsMutator, ScrapeLoop};
use crate::scraper::Scraper;
use crate::util::BufPool;

/// Go `newTestRegistryAndScrapeMetrics`.
pub fn new_test_registry_and_scrape_metrics() -> (Arc<Registry>, Arc<ScrapeMetrics>) {
    let reg = Registry::new();
    let m = ScrapeMetrics::new(Some(reg.clone())).expect("scrape metrics");
    (reg, m)
}

/// Go `newTestScrapeMetrics`.
pub fn new_test_scrape_metrics() -> Arc<ScrapeMetrics> {
    new_test_registry_and_scrape_metrics().1
}

/// Go `nopMutator`.
pub fn nop_mutator() -> LabelsMutator {
    Arc::new(|l: Labels| l)
}

/// The scrape function of a [`TestScraper`]: writes the body into the buffer.
pub type ScrapeFunc = Arc<dyn Fn(&Ctx, &mut Vec<u8>) -> Result<(), Error> + Send + Sync>;

/// Go `testScraper`: returns configured values and records reports.
#[derive(Default)]
pub struct TestScraper {
    pub offset_dur: Mutex<Duration>,
    pub last_start: Mutex<Option<SystemTime>>,
    pub last_duration: Mutex<Duration>,
    pub last_error: Mutex<Option<Error>>,
    pub scrape_err: Mutex<Option<Error>>,
    pub scrape_func: Mutex<Option<ScrapeFunc>>,
}

impl TestScraper {
    pub fn set_scrape_func(&self, f: impl Fn(&Ctx, &mut Vec<u8>) -> Result<(), Error> + Send + Sync + 'static) {
        *self.scrape_func.lock().unwrap() = Some(Arc::new(f));
    }
    pub fn set_scrape_err(&self, e: Option<Error>) {
        *self.scrape_err.lock().unwrap() = e;
    }
    pub fn set_offset(&self, d: Duration) {
        *self.offset_dur.lock().unwrap() = d;
    }
    pub fn last_error(&self) -> Option<Error> {
        self.last_error.lock().unwrap().clone()
    }
    pub fn last_start(&self) -> Option<SystemTime> {
        *self.last_start.lock().unwrap()
    }
    pub fn last_duration(&self) -> Duration {
        *self.last_duration.lock().unwrap()
    }
}

impl Scraper for TestScraper {
    fn scrape(&self, _ctx: &Ctx) -> Result<Option<net::Response>, Error> {
        match self.scrape_err.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }
    fn read_response(&self, ctx: &Ctx, _resp: Option<net::Response>, w: &mut Vec<u8>) -> Result<String, Error> {
        let f = self.scrape_func.lock().unwrap().clone();
        if let Some(f) = f {
            return f(ctx, w).map(|_| String::new());
        }
        match self.scrape_err.lock().unwrap().clone() {
            Some(e) => Err(e),
            None => Ok(String::new()),
        }
    }
    fn report(&self, start: SystemTime, dur: Duration, err: Option<&Error>) {
        *self.last_start.lock().unwrap() = Some(start);
        *self.last_duration.lock().unwrap() = dur;
        *self.last_error.lock().unwrap() = err.cloned();
    }
    fn offset(&self, _interval: Duration, _seed: u64, _now: SystemTime) -> Duration {
        *self.offset_dur.lock().unwrap()
    }
}

/// Go `newTestScrapeLoop`: a loop with the test defaults and a mock scraper. `opts` may set any field; a `ctx`
/// set by `opts` becomes the root context (Go: `sl.ctx` injection), the loop gets a child of it.
pub fn new_test_scrape_loop(opts: impl FnOnce(&mut ScrapeLoop)) -> (ScrapeLoop, Arc<TestScraper>) {
    new_test_scrape_loop_with_runtime(Runtime::global(), opts)
}

/// [`new_test_scrape_loop`] on a given runtime (for virtual time).
pub fn new_test_scrape_loop_with_runtime(runtime: Arc<Runtime>, opts: impl FnOnce(&mut ScrapeLoop)) -> (ScrapeLoop, Arc<TestScraper>) {
    let metrics = new_test_scrape_metrics();
    let scraper = Arc::new(TestScraper::default());
    let root0 = Ctx::background();
    let mut sl = ScrapeLoop::with_defaults(
        root0.clone(),
        root0.clone(),
        ScrapeCache::new(metrics.clone()),
        scraper.clone(),
        metrics,
        Arc::new(BufPool::new(1000, 1_000_000, 3)),
        runtime,
    );
    sl.interval = Duration::from_millis(10);
    sl.timeout = Duration::from_secs(3600);
    sl.sample_mutator = nop_mutator();
    sl.report_sample_mutator = nop_mutator();
    sl.max_schema = model::histogram::EXPONENTIAL_SCHEMA_MAX;
    sl.honor_timestamps = true;
    sl.enable_compression = true;
    sl.validation_scheme = ValidationScheme::Utf8;
    sl.append_metadata_to_wal = true;
    sl.parse_st = true;
    opts(&mut sl);
    assert!(
        !(sl.appendable.is_some() && sl.appendable_v2.is_some()),
        "select the appendable to use, both were passed, likely a bug"
    );
    let root = sl.ctx.clone();
    sl.ctx = root.child();
    sl.parent_ctx = root;
    (sl, scraper)
}

/// Go `newTestScrapePool`.
pub fn new_test_scrape_pool(
    appendable: Option<Arc<dyn Appendable>>,
    appendable_v2: Option<Arc<dyn AppendableV2>>,
    inject: Option<Box<dyn Fn(ScrapeLoopOptions) -> Arc<dyn Loop> + Send + Sync>>,
) -> ScrapePool {
    ScrapePool::new_for_test(appendable, appendable_v2, inject, new_test_scrape_metrics())
}

/// The start function of a [`TestLoop`]: interval, timeout and the error channel.
pub type StartFunc = Box<dyn Fn(Duration, Duration, Option<Errc>) + Send + Sync>;

/// Go `testLoop`.
#[derive(Default)]
pub struct TestLoop {
    pub start_func: Mutex<Option<StartFunc>>,
    pub stop_func: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    pub forced_err: Mutex<Option<Error>>,
    pub run_once: Mutex<bool>,
    pub interval: Duration,
    pub timeout: Duration,
}

impl TestLoop {
    /// Go `getForcedError`.
    pub fn get_forced_error(&self) -> Option<Error> {
        self.forced_err.lock().unwrap().clone()
    }
}

impl Loop for TestLoop {
    fn run(self: Arc<Self>, errc: Option<Errc>) {
        {
            let mut r = self.run_once.lock().unwrap();
            if *r {
                panic!("loop must be started only once");
            }
            *r = true;
        }
        let f = self.start_func.lock().unwrap().take();
        if let Some(f) = f {
            f(self.interval, self.timeout, errc);
            *self.start_func.lock().unwrap() = Some(f);
        }
    }
    fn set_forced_error(&self, err: Option<Error>) {
        *self.forced_err.lock().unwrap() = err;
    }
    fn set_scrape_failure_logger(&self, _l: Option<Arc<dyn FailureLogger>>) {}
    fn stop(&self) {
        let f = self.stop_func.lock().unwrap();
        if let Some(f) = &*f {
            f();
        }
    }
    fn get_cache(&self) -> Option<Arc<ScrapeCache>> {
        None
    }
    fn disable_end_of_run_staleness_markers(&self) {}
}

/// A failure logger that drops everything (Go `noopFailureLogger`).
pub struct NoopFailureLogger;

impl FailureLogger for NoopFailureLogger {
    fn log_error(&self, _msg: &str) {}
    fn with(&self, _key: &str, _value: &str) -> Arc<dyn FailureLogger> {
        Arc::new(NoopFailureLogger)
    }
    fn close(&self) -> Result<(), String> {
        Ok(())
    }
}

/// A failure logger that records messages (for tests that read the failure log).
#[derive(Default)]
pub struct RecordingFailureLogger {
    pub records: Arc<Mutex<Vec<(String, Vec<(String, String)>)>>>,
    pub attrs: Vec<(String, String)>,
}

impl FailureLogger for RecordingFailureLogger {
    fn log_error(&self, msg: &str) {
        self.records.lock().unwrap().push((msg.to_string(), self.attrs.clone()));
    }
    fn with(&self, key: &str, value: &str) -> Arc<dyn FailureLogger> {
        let mut attrs = self.attrs.clone();
        attrs.push((key.to_string(), value.to_string()));
        Arc::new(RecordingFailureLogger { records: self.records.clone(), attrs })
    }
    fn close(&self) -> Result<(), String> {
        Ok(())
    }
}
