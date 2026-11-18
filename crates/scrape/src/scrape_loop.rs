//! The scrape loop of one target (Go `scrapeLoop`, `scrapeLoopAppender`, `scrapeLoopAppenderV2`): scraping,
//! parsing, appending through the scrape cache, staleness markers, report series, and the timing of the loop.
//!
//! The loop runs as a state machine on the [`Runtime`]: each step runs on a worker and schedules the next one.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime};

use model::exemplar::{self, Exemplar};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Builder, Labels, ValidationScheme};
use model::metadata::{Metadata, MetricType};
use model::relabel;
use model::timestamp;
use model::value::stale_nan;
use storage::{
    AppendOptions, AppendV2Options, Appendable, AppendableV2, Appender, AppenderV2, Context, ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP,
    ERR_NOT_FOUND, ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE, ERR_OUT_OF_ORDER_ST, Error,
    SeriesRef, Sentinel,
};
use textparse::{Entry, ParserOptions};

use crate::cache::{EntryId, ScrapeCache, SeriesCache};
use crate::ctx::Ctx;
use crate::failure_log::FailureLogger;
use crate::limits::{ERR_BUCKET_LIMIT, ERR_SAMPLE_LIMIT, Limits, LimitsV1, LimitsV2};
use crate::metrics::ScrapeMetrics;
use crate::runtime::{Runtime, Task};
use crate::scraper::{ERR_BODY_SIZE_LIMIT, Scraper};
use crate::st_synthesis::StCache;
use crate::target::{EXPORTED_LABEL_PREFIX, METRIC_NAME_LABEL, Target};
use crate::util::{BufPool, go_duration_string};

static NAME_LABEL_MANDATORY: Sentinel = Sentinel::new("missing metric name (__name__ label)");
/// Go `errNameLabelMandatory`.
pub const ERR_NAME_LABEL_MANDATORY: Error = Error::Sentinel(&NAME_LABEL_MANDATORY);

/// Go `ScrapeTimestampTolerance`.
pub const SCRAPE_TIMESTAMP_TOLERANCE: Duration = Duration::from_millis(2);
/// Go `AlignScrapeTimestamps`.
pub static ALIGN_SCRAPE_TIMESTAMPS: AtomicBool = AtomicBool::new(true);

const REJECT_EARLY_OOO: AppendOptions = AppendOptions { discard_out_of_order: true };

/// A function that changes a label set; an empty result means "drop" (Go `labelsMutator`).
pub type LabelsMutator = Arc<dyn Fn(Labels) -> Labels + Send + Sync>;

/// The error channel of a loop (Go `chan<- error`).
pub type Errc = SyncSender<Error>;

/// Go `labelLimits`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LabelLimits {
    pub label_limit: usize,
    pub label_name_length_limit: usize,
    pub label_value_length_limit: usize,
}

fn trunc_runes(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

/// Go `verifyLabelLimits`.
pub fn verify_label_limits(lset: &Labels, limits: Option<&LabelLimits>) -> Result<(), Error> {
    let Some(limits) = limits else { return Ok(()) };
    let met = lset.get(METRIC_NAME_LABEL);
    if limits.label_limit > 0 {
        let n = lset.len();
        if n > limits.label_limit {
            return Err(Error::msg(format!(
                "label_limit exceeded (metric: {}, number of labels: {}, limit: {})",
                trunc_runes(met, 50),
                n,
                limits.label_limit
            )));
        }
    }
    if limits.label_name_length_limit == 0 && limits.label_value_length_limit == 0 {
        return Ok(());
    }
    lset.validate(|name, value| {
        if limits.label_name_length_limit > 0 && name.len() > limits.label_name_length_limit {
            return Err(Error::msg(format!(
                "label_name_length_limit exceeded (metric: {}, label name: {}, length: {}, limit: {})",
                trunc_runes(met, 50),
                trunc_runes(name, 50),
                name.len(),
                limits.label_name_length_limit
            )));
        }
        if limits.label_value_length_limit > 0 && value.len() > limits.label_value_length_limit {
            return Err(Error::msg(format!(
                "label_value_length_limit exceeded (metric: {}, label name: {}, value: {}, length: {}, limit: {})",
                trunc_runes(met, 50),
                trunc_runes(name, 50),
                yaml::go_quote(trunc_runes(value, 50)),
                value.len(),
                limits.label_value_length_limit
            )));
        }
        Ok(())
    })
}

/// Go `mutateSampleLabels`: adds the target labels (honoring or renaming conflicting ones) and applies metric
/// relabeling. An empty result means the sample is dropped.
pub fn mutate_sample_labels(lset: Labels, target: &Target, honor: bool, rc: &[relabel::Config]) -> Labels {
    let mut lb = Builder::new(lset.clone());
    if honor {
        target.labels_range(|n, v| {
            if !lset.has(n) {
                lb.set(n, v);
            }
        });
    } else {
        let mut conflicting: Vec<(String, String)> = Vec::new();
        target.labels_range(|n, v| {
            let existing = lset.get(n);
            if !existing.is_empty() {
                conflicting.push((n.to_string(), existing.to_string()));
            }
            lb.set(n, v);
        });
        if !conflicting.is_empty() {
            resolve_conflicting_exposed_labels(&mut lb, conflicting);
        }
    }
    if !relabel::process_builder(&mut lb, rc.iter()) {
        return Labels::default();
    }
    lb.labels()
}

fn resolve_conflicting_exposed_labels(lb: &mut Builder, mut conflicting: Vec<(String, String)>) {
    conflicting.sort_by_key(|(n, _)| n.len());
    for (name, value) in conflicting {
        let mut new_name = name;
        loop {
            new_name = format!("{EXPORTED_LABEL_PREFIX}{new_name}");
            if lb.get(&new_name).is_empty() {
                lb.set(&new_name, &value);
                break;
            }
        }
    }
}

/// Go `mutateReportSampleLabels`.
pub fn mutate_report_sample_labels(lset: Labels, target: &Target) -> Labels {
    let mut lb = Builder::new(lset.clone());
    target.labels_range(|n, v| {
        lb.set(&format!("{EXPORTED_LABEL_PREFIX}{n}"), lset.get(n));
        lb.set(n, v);
    });
    lb.labels()
}

/// Go `isSeriesPartOfFamily`.
pub fn is_series_part_of_family(m_name: &str, mf_name: &[u8], typ: MetricType) -> bool {
    let Ok(mut mf) = std::str::from_utf8(mf_name) else { return false };
    if !m_name.starts_with(mf) {
        return false;
    }
    fn cut<'a>(s: &'a str, suffix: &str) -> Option<&'a str> {
        s.strip_suffix(suffix)
    }
    let got: &str = match typ {
        MetricType::Counter => {
            mf = cut(mf, "_total").unwrap_or(mf);
            cut(m_name, "_total").unwrap_or(m_name)
        }
        MetricType::Histogram => cut(m_name, "_bucket")
            .or_else(|| cut(m_name, "_sum"))
            .or_else(|| cut(m_name, "_count"))
            .unwrap_or(m_name),
        MetricType::GaugeHistogram => cut(m_name, "_bucket")
            .or_else(|| cut(m_name, "_gsum"))
            .or_else(|| cut(m_name, "_gcount"))
            .unwrap_or(m_name),
        MetricType::Summary => cut(m_name, "_sum").or_else(|| cut(m_name, "_count")).unwrap_or(m_name),
        MetricType::Info => {
            mf = cut(mf, "_info").unwrap_or(mf);
            cut(m_name, "_info").unwrap_or(m_name)
        }
        _ => m_name,
    };
    mf == got
}

/// Go `pickSchema`.
pub fn pick_schema(bucket_factor: f64) -> i32 {
    let bf = if bucket_factor <= 1.0 { 1.00271 } else { bucket_factor };
    let floor = (-(bf.log2().log2())).floor();
    if floor >= model::histogram::EXPONENTIAL_SCHEMA_MAX as f64 {
        model::histogram::EXPONENTIAL_SCHEMA_MAX
    } else if floor <= model::histogram::EXPONENTIAL_SCHEMA_MIN as f64 {
        model::histogram::EXPONENTIAL_SCHEMA_MIN
    } else {
        floor as i32
    }
}

/// An automatically generated series (Go `reportSample`). The name ends with the invalid byte 0xff so that it
/// cannot collide with scraped series in the cache.
#[derive(Clone, Debug)]
pub struct ReportSample {
    pub metadata: Metadata,
    pub name: &'static [u8],
}

impl ReportSample {
    /// The metric name (without the 0xff byte).
    pub fn metric_name(&self) -> &'static str {
        std::str::from_utf8(&self.name[..self.name.len() - 1]).unwrap_or("")
    }
}

fn rs(name: &'static [u8], help: &str, unit: &str) -> ReportSample {
    ReportSample { name, metadata: Metadata { typ: MetricType::Gauge, help: help.to_string(), unit: unit.to_string() } }
}

/// The report series (Go `scrapeHealthMetric` and the others), in report order.
pub fn report_samples() -> &'static [ReportSample; 8] {
    static R: std::sync::OnceLock<[ReportSample; 8]> = std::sync::OnceLock::new();
    R.get_or_init(|| {
        [
            rs(b"up\xff", "Health of the scrape target. 1 means the target is healthy, 0 if the scrape failed.", "targets"),
            rs(b"scrape_duration_seconds\xff", "Duration of the last scrape in seconds.", "seconds"),
            rs(b"scrape_samples_scraped\xff", "Number of samples last scraped.", "samples"),
            rs(b"scrape_samples_post_metric_relabeling\xff", "Number of samples remaining after metric relabeling was applied.", "samples"),
            rs(b"scrape_series_added\xff", "Number of series in the last scrape.", "series"),
            rs(b"scrape_timeout_seconds\xff", "The configured scrape timeout for a target.", "seconds"),
            rs(b"scrape_sample_limit\xff", "The configured sample limit for a target. Returns zero if there is no limit configured.", "samples"),
            rs(
                b"scrape_body_size_bytes\xff",
                "The uncompressed size of the last scrape response, if successful. Scrapes failing because body_size_limit is exceeded report -1, other scrape failures report 0.",
                "bytes",
            ),
        ]
    })
}

/// Go `scrapeHealthMetric` etc. by index into [`report_samples`].
pub fn scrape_health_metric() -> &'static ReportSample {
    &report_samples()[0]
}
pub fn scrape_duration_metric() -> &'static ReportSample {
    &report_samples()[1]
}
pub fn scrape_samples_metric() -> &'static ReportSample {
    &report_samples()[2]
}
pub fn samples_post_relabel_metric() -> &'static ReportSample {
    &report_samples()[3]
}
pub fn scrape_series_added_metric() -> &'static ReportSample {
    &report_samples()[4]
}
pub fn scrape_timeout_metric() -> &'static ReportSample {
    &report_samples()[5]
}
pub fn scrape_sample_limit_metric() -> &'static ReportSample {
    &report_samples()[6]
}
pub fn scrape_body_size_bytes_metric() -> &'static ReportSample {
    &report_samples()[7]
}

/// A one-shot event (Go `close(ch)` / `<-ch`).
#[derive(Default)]
pub struct Signal {
    m: Mutex<bool>,
    cv: Condvar,
}

impl Signal {
    pub fn set(&self) {
        *self.m.lock().unwrap() = true;
        self.cv.notify_all();
    }
    pub fn is_set(&self) -> bool {
        *self.m.lock().unwrap()
    }
    pub fn wait(&self) {
        let mut g = self.m.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
    /// Waits up to `d`; true when set.
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let end = std::time::Instant::now() + d;
        let mut g = self.m.lock().unwrap();
        while !*g {
            let now = std::time::Instant::now();
            if now >= end {
                return false;
            }
            g = self.cv.wait_timeout(g, end - now).unwrap().0;
        }
        true
    }
}

/// Counts of append failures (Go `appendErrors`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppendErrors {
    pub num_out_of_order: usize,
    pub num_duplicates: usize,
    pub num_out_of_bounds: usize,
    pub num_exemplar_out_of_order: usize,
}

/// Reused per-loop scratch space of the append path.
#[derive(Default)]
pub struct Scratch {
    lset: Labels,
    met: Vec<u8>,
    mf: Vec<u8>,
    h: Histogram,
    fh: FloatHistogram,
    meta: Metadata,
    e: Exemplar,
    exemplars: Vec<Exemplar>,
    parser_opts: ParserOptions,
}

/// Statistics of one append (Go returns `total, added, seriesAdded`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppendStats {
    pub total: usize,
    pub added: usize,
    pub series_added: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    NotStarted,
    Offset,
    Scrape,
    Shutdown,
    Stale1,
    Stale2,
    Stale3,
    Done,
}

/// Go's `time.Ticker` with its one-slot channel: ticks at `t0 + k*interval`; ticks missed while the consumer
/// was busy are dropped except one.
#[derive(Clone, Copy, Debug, Default)]
struct Ticker {
    t0: Duration,
    interval: Duration,
    consumed: u64,
}

impl Ticker {
    fn reset(&mut self, now: Duration, interval: Duration) {
        self.t0 = now;
        self.interval = interval;
        self.consumed = 0;
    }
    fn next_due_u64(&self) -> Duration {
        let n = self.consumed + 1;
        self.t0 + Duration::from_nanos((self.interval.as_nanos() as u64).saturating_mul(n))
    }
    /// Takes the pending tick at `now` (the caller checked that one is due).
    fn consume(&mut self, now: Duration) {
        let iv = self.interval.as_nanos().max(1);
        let k = (now.saturating_sub(self.t0).as_nanos() / iv) as u64;
        self.consumed = k.max(self.consumed + 1);
    }
}

struct Sched {
    phase: Phase,
    generation: u64,
    running: bool,
    ticker: Ticker,
    last: Option<SystemTime>,
    aligned: SystemTime,
    stale_time: SystemTime,
    errc: Option<Errc>,
    ctx_waker: Option<u64>,
    parent_waker: Option<u64>,
}

/// The scrape loop of one target (Go `scrapeLoop`). Configure the public fields before wrapping it in an
/// `Arc` and starting it.
pub struct ScrapeLoop {
    /// The local context; `stop` cancels it.
    pub ctx: Ctx,
    /// The pool context (process shutdown).
    pub parent_ctx: Ctx,
    /// The context given to appendables.
    pub appender_ctx: Context,
    pub l: discovery::Logger,
    pub cache: Arc<ScrapeCache>,

    pub interval: Duration,
    pub timeout: Duration,
    pub sample_mutator: LabelsMutator,
    pub report_sample_mutator: LabelsMutator,
    pub scraper: Arc<dyn Scraper>,

    pub appendable: Option<Arc<dyn Appendable>>,
    pub appendable_v2: Option<Arc<dyn AppendableV2>>,
    pub buffers: Arc<BufPool>,

    pub synthesize_st: bool,
    pub offset_seed: u64,
    pub metrics: Arc<ScrapeMetrics>,

    pub sample_limit: usize,
    pub bucket_limit: usize,
    pub max_schema: i32,
    pub label_limits: Option<LabelLimits>,
    pub honor_labels: bool,
    pub honor_timestamps: bool,
    pub track_timestamps_staleness: bool,
    pub enable_native_histogram_scraping: bool,
    pub always_scrape_classic_hist: bool,
    pub convert_classic_hist_to_nhcb: bool,
    pub fallback_scrape_protocol: String,
    pub enable_compression: bool,
    pub mrc: Vec<relabel::Config>,
    pub validation_scheme: ValidationScheme,

    pub enable_st_zero_ingestion: bool,
    pub parse_st: bool,
    pub enable_type_and_unit_labels: bool,
    pub enable_open_metrics2: bool,
    pub report_extra_metrics: bool,
    pub append_metadata_to_wal: bool,
    pub pass_metadata_in_context: bool,
    pub skip_jitter_offsetting: bool,
    pub scrape_on_shutdown: bool,
    pub initial_scrape_offset: Duration,

    pub runtime: Arc<Runtime>,

    forced_err: Mutex<Option<Error>>,
    scrape_failure_logger: RwLock<Option<Arc<dyn FailureLogger>>>,
    last_scrape_size: Mutex<usize>,
    scratch: Mutex<Scratch>,
    disabled_end_of_run_staleness_markers: AtomicBool,
    pub stopped: Signal,
    pub finished: Signal,
    sched: Mutex<Sched>,
}

impl ScrapeLoop {
    /// A loop with Go's zero values and the given parts; callers set the remaining fields.
    pub fn with_defaults(
        ctx: Ctx,
        parent_ctx: Ctx,
        cache: Arc<ScrapeCache>,
        scraper: Arc<dyn Scraper>,
        metrics: Arc<ScrapeMetrics>,
        buffers: Arc<BufPool>,
        runtime: Arc<Runtime>,
    ) -> ScrapeLoop {
        let nop: LabelsMutator = Arc::new(|l| l);
        ScrapeLoop {
            ctx,
            parent_ctx,
            appender_ctx: Context::default(),
            l: discovery::Logger::nop(),
            cache,
            interval: Duration::ZERO,
            timeout: Duration::ZERO,
            sample_mutator: nop.clone(),
            report_sample_mutator: nop,
            scraper,
            appendable: None,
            appendable_v2: None,
            buffers,
            synthesize_st: false,
            offset_seed: 0,
            metrics,
            sample_limit: 0,
            bucket_limit: 0,
            max_schema: model::histogram::EXPONENTIAL_SCHEMA_MAX,
            label_limits: None,
            honor_labels: false,
            honor_timestamps: false,
            track_timestamps_staleness: false,
            enable_native_histogram_scraping: false,
            always_scrape_classic_hist: false,
            convert_classic_hist_to_nhcb: false,
            fallback_scrape_protocol: String::new(),
            enable_compression: false,
            mrc: Vec::new(),
            validation_scheme: ValidationScheme::Utf8,
            enable_st_zero_ingestion: false,
            parse_st: false,
            enable_type_and_unit_labels: false,
            enable_open_metrics2: false,
            report_extra_metrics: false,
            append_metadata_to_wal: false,
            pass_metadata_in_context: false,
            skip_jitter_offsetting: false,
            scrape_on_shutdown: false,
            initial_scrape_offset: Duration::ZERO,
            runtime,
            forced_err: Mutex::new(None),
            scrape_failure_logger: RwLock::new(None),
            last_scrape_size: Mutex::new(0),
            scratch: Mutex::new(Scratch::default()),
            disabled_end_of_run_staleness_markers: AtomicBool::new(false),
            stopped: Signal::default(),
            finished: Signal::default(),
            sched: Mutex::new(Sched {
                phase: Phase::NotStarted,
                generation: 0,
                running: false,
                ticker: Ticker::default(),
                last: None,
                aligned: SystemTime::UNIX_EPOCH,
                stale_time: SystemTime::UNIX_EPOCH,
                errc: None,
                ctx_waker: None,
                parent_waker: None,
            }),
        }
    }

    /// Go `setScrapeFailureLogger`.
    pub fn set_scrape_failure_logger(&self, l: Option<Arc<dyn FailureLogger>>) {
        let l = match (l, self.scraper.target_string()) {
            (Some(l), Some(t)) => Some(l.with("target", &t)),
            (l, _) => l,
        };
        *self.scrape_failure_logger.write().unwrap() = l;
    }

    /// The current scrape failure logger.
    pub fn scrape_failure_logger(&self) -> Option<Arc<dyn FailureLogger>> {
        self.scrape_failure_logger.read().unwrap().clone()
    }

    /// Go `getScrapeOffset`.
    pub fn get_scrape_offset(&self) -> Duration {
        let mut offset = self.scraper.offset(self.interval, self.offset_seed, self.runtime.wall());
        if self.skip_jitter_offsetting {
            offset = Duration::ZERO;
        }
        self.initial_scrape_offset + offset
    }

    /// Go `setForcedError`.
    pub fn set_forced_error(&self, err: Option<Error>) {
        *self.forced_err.lock().unwrap() = err;
    }

    /// Go `getForcedError`.
    pub fn get_forced_error(&self) -> Option<Error> {
        self.forced_err.lock().unwrap().clone()
    }

    /// Go `disableEndOfRunStalenessMarkers`.
    pub fn disable_end_of_run_staleness_markers(&self) {
        self.disabled_end_of_run_staleness_markers.store(true, Ordering::SeqCst);
    }

    /// Whether end-of-run staleness markers are disabled.
    pub fn end_of_run_staleness_markers_disabled(&self) -> bool {
        self.disabled_end_of_run_staleness_markers.load(Ordering::SeqCst)
    }

    /// Go `getCache`.
    pub fn get_cache(&self) -> Arc<ScrapeCache> {
        self.cache.clone()
    }

    /// The size of the last non-empty scrape body.
    pub fn last_scrape_size(&self) -> usize {
        *self.last_scrape_size.lock().unwrap()
    }

    /// Go `appender`: an appender of the configured appendable (V2 preferred).
    pub fn appender(&self) -> LoopAppender<'_> {
        if let Some(a) = &self.appendable_v2 {
            return LoopAppender::V2 { sl: self, app: a.appender_v2(&self.appender_ctx) };
        }
        let a = self.appendable.as_ref().expect("scrape loop without appendable");
        LoopAppender::V1 { sl: self, app: a.appender(&self.appender_ctx) }
    }

    fn parser_options<'s>(&self, sc: &'s mut Scratch, skip_st_series: bool) -> &'s ParserOptions {
        let o = &mut sc.parser_opts;
        o.enable_type_and_unit_labels = self.enable_type_and_unit_labels;
        o.ignore_native_histograms = !self.enable_native_histogram_scraping;
        o.convert_classic_histograms_to_nhcb = self.convert_classic_hist_to_nhcb;
        o.keep_classic_on_classic_and_native_histograms = self.always_scrape_classic_hist;
        o.open_metrics_skip_st_series = skip_st_series;
        o.enable_open_metrics2 = self.enable_open_metrics2;
        if o.fallback_content_type != self.fallback_scrape_protocol {
            o.fallback_content_type.clone_from(&self.fallback_scrape_protocol);
        }
        o
    }

    fn scratch(&self) -> MutexGuard<'_, Scratch> {
        self.scratch.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `checkAddError`: whether the sample was added, and the error that must stop the scrape.
    pub fn check_add_error(
        &self,
        met: &[u8],
        exemplars: &[Exemplar],
        err: Option<Error>,
        sample_limit_err: &mut Option<Error>,
        bucket_limit_err: &mut Option<Error>,
        app_errs: &mut AppendErrors,
    ) -> (bool, Option<Error>) {
        let Some(err) = err else { return (true, None) };
        if err.is(&ERR_SAMPLE_LIMIT) {
            *sample_limit_err = Some(err);
            return (false, None);
        }
        if err.is(&ERR_BUCKET_LIMIT) {
            *bucket_limit_err = Some(err);
            return (false, None);
        }
        if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
            app_errs.num_out_of_order += 1;
            self.l.debug("Out of order sample", &[("series", &String::from_utf8_lossy(met))]);
            self.metrics.target_scrape_sample_out_of_order.get().inc();
            return (false, None);
        }
        if err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP) {
            app_errs.num_duplicates += 1;
            self.l.debug("Duplicate sample for timestamp", &[("series", &String::from_utf8_lossy(met))]);
            self.metrics.target_scrape_sample_duplicate.get().inc();
            return (false, None);
        }
        if err.is(&ERR_OUT_OF_BOUNDS) {
            app_errs.num_out_of_bounds += 1;
            self.l.debug("Out of bounds metric", &[("series", &String::from_utf8_lossy(met))]);
            self.metrics.target_scrape_sample_out_of_bounds.get().inc();
            return (false, None);
        }
        if err.is(&ERR_NOT_FOUND) {
            return (false, Some(ERR_NOT_FOUND));
        }
        if let Some(p) = err.as_append_partial() {
            let ooo = p.exemplar_errors.iter().filter(|e| e.is(&ERR_OUT_OF_ORDER_EXEMPLAR)).count();
            if ooo > 0 && ooo == exemplars.len() {
                app_errs.num_exemplar_out_of_order += ooo;
                self.l.debug("Out of order exemplars", &[("count", &ooo.to_string())]);
                self.metrics.target_scrape_exemplar_out_of_order.get().add(ooo as f64);
            }
            return (true, None);
        }
        (false, Some(err))
    }

    fn warn_append_errors(&self, e: &AppendErrors) {
        if e.num_out_of_order > 0 {
            self.l.warn("Error on ingesting out-of-order samples", &[("num_dropped", &e.num_out_of_order.to_string())]);
        }
        if e.num_duplicates > 0 {
            self.l.warn(
                "Error on ingesting samples with different value but same timestamp",
                &[("num_dropped", &e.num_duplicates.to_string())],
            );
        }
        if e.num_out_of_bounds > 0 {
            self.l.warn(
                "Error on ingesting samples that are too old or are too far into the future",
                &[("num_dropped", &e.num_out_of_bounds.to_string())],
            );
        }
        if e.num_exemplar_out_of_order > 0 {
            self.l.warn(
                "Error on ingesting out-of-order exemplars",
                &[("num_dropped", &e.num_exemplar_out_of_order.to_string())],
            );
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Running.

    /// Go `run`: runs the loop until it is stopped (including the end-of-run staleness markers), blocking.
    pub fn run(self: &Arc<Self>, errc: Option<Errc>) {
        self.start(errc);
        self.finished.wait();
    }

    /// Starts the loop on the runtime without blocking (Go `go l.run(errc)`).
    pub fn start(self: &Arc<Self>, errc: Option<Errc>) {
        let mut s = self.sched.lock().unwrap();
        if s.phase != Phase::NotStarted {
            panic!("scrape loop started twice");
        }
        s.errc = errc;
        s.phase = Phase::Offset;
        let offset = self.get_scrape_offset();
        s.generation += 1;
        let due = self.runtime.now() + offset;
        let generation = s.generation;
        drop(s);
        if offset.is_zero() {
            self.runtime.spawn(self.clone(), generation);
        } else {
            self.runtime.schedule_at(due, self.clone(), generation);
        }
        // Registered without the sched lock: on an already cancelled context (stop before run) the waker runs at
        // once and takes the lock.
        let me = Arc::downgrade(self);
        let id = self.ctx.add_waker(Arc::new(move || {
            if let Some(sl) = me.upgrade() {
                sl.wake();
            }
        }));
        let mut s = self.sched.lock().unwrap();
        if s.phase == Phase::Done {
            drop(s);
            self.ctx.remove_waker(id);
        } else {
            s.ctx_waker = Some(id);
        }
    }

    /// Go `stop`: cancels the loop and waits until it stopped scraping. Stale markers may still be written later.
    pub fn stop(&self) {
        self.ctx.cancel();
        self.stopped.wait();
    }

    fn wake(self: &Arc<Self>) {
        let mut s = self.sched.lock().unwrap();
        if s.running || matches!(s.phase, Phase::NotStarted | Phase::Done) {
            return;
        }
        s.generation += 1;
        let generation = s.generation;
        drop(s);
        self.runtime.spawn(self.clone(), generation);
    }

    fn schedule(self: &Arc<Self>, s: &mut Sched, phase: Phase, due: Option<Duration>) {
        s.phase = phase;
        s.generation += 1;
        let generation = s.generation;
        match due {
            Some(d) if d > self.runtime.now() => self.runtime.schedule_at(d, self.clone(), generation),
            _ => self.runtime.spawn(self.clone(), generation),
        }
    }

    fn send_err(&self, errc: Option<&Errc>, err: &Error) {
        let Some(errc) = errc else { return };
        let mut e = err.clone();
        loop {
            match errc.try_send(e) {
                Ok(()) => return,
                Err(TrySendError::Disconnected(_)) => return,
                Err(TrySendError::Full(back)) => {
                    if self.ctx.wait_timeout(Duration::from_millis(1)) {
                        return;
                    }
                    e = back;
                }
            }
        }
    }

    fn step(self: &Arc<Self>, phase: Phase, errc: Option<Errc>) -> (Phase, Option<Duration>) {
        match phase {
            Phase::NotStarted | Phase::Done => (Phase::Done, None),
            Phase::Offset => {
                if self.ctx.is_done() {
                    return (Phase::Shutdown, None);
                }
                let now = self.runtime.now();
                let mut s = self.sched.lock().unwrap();
                s.ticker.reset(now, self.interval);
                s.aligned = self.runtime.wall();
                drop(s);
                self.step(Phase::Scrape, errc)
            }
            Phase::Scrape => {
                if self.ctx.is_done() {
                    return (Phase::Shutdown, None);
                }
                let mut scrape_time = self.runtime.wall();
                let last = {
                    let mut s = self.sched.lock().unwrap();
                    let now = self.runtime.now();
                    if s.ticker.next_due_u64() <= now {
                        s.ticker.consume(now);
                    }
                    if ALIGN_SCRAPE_TIMESTAMPS.load(Ordering::Relaxed) {
                        let tolerance = (self.interval / 100).min(SCRAPE_TIMESTAMP_TOLERANCE);
                        while scrape_time.duration_since(s.aligned).is_ok_and(|d| d >= self.interval) && !self.interval.is_zero() {
                            s.aligned += self.interval;
                        }
                        // Go: scrapeTime.Sub(aligned) <= tolerance (also true when scrapeTime is before aligned).
                        let within = match scrape_time.duration_since(s.aligned) {
                            Ok(d) => d <= tolerance,
                            Err(_) => true,
                        };
                        if within {
                            scrape_time = s.aligned;
                        }
                    }
                    s.last
                };
                let start = self.scrape_and_report(last, scrape_time, errc.as_ref());
                let mut s = self.sched.lock().unwrap();
                s.last = Some(start);
                if self.ctx.is_done() {
                    return (Phase::Shutdown, None);
                }
                let now = self.runtime.now();
                let due = s.ticker.next_due_u64();
                if due <= now {
                    s.ticker.consume(now);
                    return (Phase::Scrape, None);
                }
                (Phase::Scrape, Some(due))
            }
            Phase::Shutdown => {
                if self.scrape_on_shutdown {
                    let last = self.sched.lock().unwrap().last;
                    let start = self.scrape_and_report(last, self.runtime.wall(), errc.as_ref());
                    self.sched.lock().unwrap().last = Some(start);
                }
                self.stopped.set();
                let last = self.sched.lock().unwrap().last;
                if self.parent_ctx.is_done() || self.end_of_run_staleness_markers_disabled() || last.is_none() {
                    return (Phase::Done, None);
                }
                let me = Arc::downgrade(self);
                let id = self.parent_ctx.add_waker(Arc::new(move || {
                    if let Some(sl) = me.upgrade() {
                        sl.wake();
                    }
                }));
                let mut s = self.sched.lock().unwrap();
                s.parent_waker = Some(id);
                let due = s.ticker.next_due_u64();
                (Phase::Stale1, Some(due))
            }
            Phase::Stale1 => {
                if self.parent_ctx.is_done() {
                    return (Phase::Done, None);
                }
                let now = self.runtime.now();
                let mut s = self.sched.lock().unwrap();
                s.ticker.consume(now);
                s.stale_time = self.runtime.wall();
                (Phase::Stale2, Some(s.ticker.next_due_u64()))
            }
            Phase::Stale2 => {
                if self.parent_ctx.is_done() {
                    return (Phase::Done, None);
                }
                let now = self.runtime.now();
                self.sched.lock().unwrap().ticker.consume(now);
                (Phase::Stale3, Some(now + self.interval / 10))
            }
            Phase::Stale3 => {
                if self.parent_ctx.is_done() || self.end_of_run_staleness_markers_disabled() {
                    return (Phase::Done, None);
                }
                let stale_time = self.sched.lock().unwrap().stale_time;
                self.end_of_run_staleness(stale_time);
                (Phase::Done, None)
            }
        }
    }

    /// The final part of Go `endOfRunStaleness`: stale markers for every series of the last scrape.
    fn end_of_run_staleness(&self, stale_time: SystemTime) {
        let mut app = self.appender();
        let (_, err) = app.append(&[], "", stale_time);
        let mut err = err;
        if let Some(e) = &err {
            app.rollback();
            app = self.appender();
            self.l.warn("Stale append failed", &[("err", &e.to_string())]);
        }
        let rerr = self.report_stale(&mut app, stale_time);
        if let Some(e) = &rerr {
            self.l.warn("Stale report failed", &[("err", &e.to_string())]);
        }
        err = rerr;
        if err.is_some() {
            app.rollback();
            return;
        }
        if let Some(e) = app.commit() {
            self.l.warn("Stale commit failed", &[("err", &e.to_string())]);
        }
    }

    fn finish(&self) {
        let mut s = self.sched.lock().unwrap();
        s.phase = Phase::Done;
        if let Some(id) = s.ctx_waker.take() {
            self.ctx.remove_waker(id);
        }
        if let Some(id) = s.parent_waker.take() {
            self.parent_ctx.remove_waker(id);
        }
        s.errc = None;
        drop(s);
        self.stopped.set();
        self.finished.set();
    }

    /// Go `scrapeAndReport`: one scrape, append and report, with as few appenders as possible. Returns the start
    /// time of the scrape.
    pub fn scrape_and_report(&self, last: Option<SystemTime>, append_time: SystemTime, errc: Option<&Errc>) -> SystemTime {
        let start = self.runtime.wall();
        let start_mono = self.runtime.now();
        if let Some(last) = last {
            let secs = start.duration_since(last).unwrap_or_default().as_secs_f64();
            let iv = go_duration_string(self.interval);
            self.metrics.target_interval_length.with_label_values(&[&iv]).observe(secs);
            self.metrics.target_interval_length_histogram.with_label_values(&[&iv]).observe(secs);
        }
        let mut stats = AppendStats::default();
        let mut bytes_read: i64 = 0;
        let mut app = self.appender();

        let mut scrape_err: Option<Error> = None;
        if let Some(forced) = self.get_forced_error() {
            scrape_err = Some(forced.clone());
            let (_, e) = app.append(&[], "", append_time);
            if let Some(e) = e {
                app.rollback();
                app = self.appender();
                self.l.warn("Append failed", &[("err", &e.to_string())]);
            }
            self.send_err(errc, &forced);
            self.finish_scrape(app, start, start_mono, append_time, stats, bytes_read, scrape_err.as_ref());
            return start;
        }

        let scrape_ctx = self.parent_ctx.with_timeout(self.timeout);
        let mut buf = self.buffers.get(self.last_scrape_size() + 512);
        let mut content_type = String::new();
        match self.scraper.scrape(&scrape_ctx) {
            Ok(resp) => match self.scraper.read_response(&scrape_ctx, resp, &mut buf) {
                Ok(ct) => content_type = ct,
                Err(e) => scrape_err = Some(e),
            },
            Err(e) => scrape_err = Some(e),
        }
        drop(scrape_ctx);
        let empty: &[u8] = &[];
        let body: &[u8] = if scrape_err.is_none() {
            if !buf.is_empty() {
                *self.last_scrape_size.lock().unwrap() = buf.len();
            }
            bytes_read = buf.len() as i64;
            &buf
        } else {
            let e = scrape_err.as_ref().unwrap();
            self.l.debug("Scrape failed", &[("err", &e.to_string())]);
            if let Some(fl) = self.scrape_failure_logger() {
                fl.log_error(&e.to_string());
            }
            self.send_err(errc, e);
            if e.is(&ERR_BODY_SIZE_LIMIT) {
                bytes_read = -1;
            }
            empty
        };

        // A failed scrape is the same as an empty scrape: append still writes the stale markers.
        let (s, app_err) = app.append(body, &content_type, append_time);
        stats = s;
        if let Some(e) = &app_err {
            app.rollback();
            app = self.appender();
            self.l.debug("Append failed", &[("err", &e.to_string())]);
            let (_, e2) = app.append(&[], "", append_time);
            if let Some(e2) = e2 {
                app.rollback();
                app = self.appender();
                self.l.warn("Append failed", &[("err", &e2.to_string())]);
            }
        }
        self.buffers.put(buf);
        if scrape_err.is_none() {
            scrape_err = app_err;
        }
        self.finish_scrape(app, start, start_mono, append_time, stats, bytes_read, scrape_err.as_ref());
        start
    }

    /// The deferred part of Go `scrapeAndReport`: report, then commit (or roll back when the report failed).
    #[allow(clippy::too_many_arguments)]
    fn finish_scrape(
        &self,
        mut app: LoopAppender<'_>,
        _start: SystemTime,
        start_mono: Duration,
        append_time: SystemTime,
        stats: AppendStats,
        bytes_read: i64,
        scrape_err: Option<&Error>,
    ) {
        let dur = self.runtime.now().saturating_sub(start_mono);
        let err = self.report(&mut app, append_time, dur, stats, bytes_read, scrape_err);
        if let Some(e) = &err {
            self.l.warn("Appending scrape report failed", &[("err", &e.to_string())]);
            app.rollback();
            return;
        }
        let cerr = app.commit();
        if self.report_extra_metrics {
            let total = self.runtime.now().saturating_sub(start_mono);
            self.metrics.target_scrape_duration.get().observe(total.as_secs_f64());
        }
        if let Some(e) = cerr {
            self.l.error("Scrape commit failed", &[("err", &e.to_string())]);
        }
    }

    /// Go `report`.
    pub fn report(
        &self,
        app: &mut LoopAppender<'_>,
        start: SystemTime,
        duration: Duration,
        stats: AppendStats,
        bytes: i64,
        scrape_err: Option<&Error>,
    ) -> Option<Error> {
        self.scraper.report(start, duration, scrape_err);
        let ts = timestamp::from_time(start);
        let health = if scrape_err.is_none() { 1.0 } else { 0.0 };
        let r = report_samples();
        let mut vals: [(usize, f64); 8] = [
            (0, health),
            (1, duration.as_secs_f64()),
            (2, stats.total as f64),
            (3, stats.added as f64),
            (4, stats.series_added as f64),
            (5, self.timeout.as_secs_f64()),
            (6, self.sample_limit as f64),
            (7, bytes as f64),
        ];
        let n = if self.report_extra_metrics { 8 } else { 5 };
        for (i, v) in vals.iter_mut().take(n) {
            if let Some(e) = app.add_report_sample(&r[*i], ts, *v, false) {
                return Some(e);
            }
        }
        None
    }

    /// Go `reportStale`.
    pub fn report_stale(&self, app: &mut LoopAppender<'_>, start: SystemTime) -> Option<Error> {
        let ts = timestamp::from_time(start);
        let stale = stale_nan();
        let n = if self.report_extra_metrics { 8 } else { 5 };
        for s in report_samples().iter().take(n) {
            if let Some(e) = app.add_report_sample(s, ts, stale, true) {
                return Some(e);
            }
        }
        None
    }

    // ---------------------------------------------------------------------------------------------------------
    // Appending.

    /// Go `updateStaleMarkers` (V1).
    fn update_stale_markers_v1(&self, app: &mut dyn StaleAppV1, s: &SeriesCache, def_time: i64) -> Option<Error> {
        let mut err = None;
        s.for_each_stale(|r, lset| {
            app.set_opts(Some(&REJECT_EARLY_OOO));
            let res = app.append_float(r, lset, def_time, stale_nan());
            app.set_opts(None);
            match res {
                Ok(_) => true,
                Err(e) if e.is(&ERR_OUT_OF_ORDER_SAMPLE) || e.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP) => true,
                Err(e) => {
                    err = Some(e);
                    false
                }
            }
        });
        err
    }

    /// Go `updateStaleMarkersV2`.
    fn update_stale_markers_v2(&self, app: &mut dyn StaleAppV2, s: &SeriesCache, def_time: i64) -> Option<Error> {
        let mut err = None;
        s.for_each_stale(|r, lset| {
            let opts = AppendV2Options { reject_out_of_order: true, ..Default::default() };
            match app.append_float(r, lset, def_time, stale_nan(), &opts) {
                Ok(_) => true,
                Err(e) if e.is(&ERR_OUT_OF_ORDER_SAMPLE) || e.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP) => true,
                Err(e) => {
                    err = Some(e);
                    false
                }
            }
        });
        err
    }

    fn new_parser<'b>(
        &self,
        sc: &mut Scratch,
        b: &'b [u8],
        content_type: &str,
        skip_st: bool,
    ) -> Result<Box<dyn textparse::Parser + 'b>, Error> {
        let opts = self.parser_options(sc, skip_st);
        let (p, perr) = textparse::new(b, content_type, opts);
        let Some(p) = p else {
            let es = perr.as_ref().map(|e| e.to_string()).unwrap_or_default();
            self.l.error(
                "Failed to determine correct type of scrape target.",
                &[("content_type", content_type), ("fallback_media_type", &self.fallback_scrape_protocol), ("err", &es)],
            );
            return Err(Error::msg(es));
        };
        if let Some(e) = perr {
            self.l.debug(
                "Invalid content type on scrape, using fallback setting.",
                &[("content_type", content_type), ("fallback_media_type", &self.fallback_scrape_protocol), ("err", &e.to_string())],
            );
        }
        Ok(p)
    }

    /// The V1 append (Go `scrapeLoopAppender.append`).
    fn append_v1(&self, base: &mut dyn Appender, b: &[u8], content_type: &str, ts: SystemTime) -> (AppendStats, Option<Error>) {
        let def_time = timestamp::from_time(ts);
        let mut stats = AppendStats::default();
        if b.is_empty() {
            let mut s = self.cache.series();
            let err = self.update_stale_markers_v1(&mut Raw1(base), &s, def_time);
            self.cache.iter_done(&mut s, false);
            return (stats, err);
        }
        let mut scg = self.scratch();
        let sc = &mut *scg;
        let mut p = match self.new_parser(sc, b, content_type, self.enable_st_zero_ingestion) {
            Ok(p) => p,
            Err(e) => return (stats, Some(e)),
        };
        let Scratch { lset, met, mf, h: hbuf, fh: fhbuf, meta, e, exemplars, .. } = sc;
        mf.clear();
        let mut app_errs = AppendErrors::default();
        let mut sample_limit_err: Option<Error> = None;
        let mut bucket_limit_err: Option<Error> = None;
        let mut has_meta = false;
        let mut meta_type = MetricType::Unknown;
        let mut meta_change = 0u64;
        let mut err: Option<Error> = None;
        let mut sg = self.cache.series();
        let s = &mut *sg;
        let mut app = LimitsV1 {
            app: base,
            limits: Limits::new(self.runtime.wall(), self.sample_limit, self.bucket_limit, self.max_schema),
        };

        loop {
            let et = match p.next() {
                Ok(et) => et,
                Err(x) if x.is_eof() => break,
                Err(x) => {
                    err = Some(Error::msg(x.to_string()));
                    break;
                }
            };
            let mut is_hist = false;
            match et {
                Entry::Type => {
                    let (name, t) = p.typ();
                    (meta_type, meta_change) = self.cache.set_type(s.iter, name, t);
                    set_mf(mf, meta, name, &mut has_meta, self.append_metadata_to_wal, &self.cache);
                    continue;
                }
                Entry::Help => {
                    let (name, help) = p.help();
                    (meta_type, meta_change) = self.cache.set_help(s.iter, name, help);
                    set_mf(mf, meta, name, &mut has_meta, self.append_metadata_to_wal, &self.cache);
                    continue;
                }
                Entry::Unit => {
                    let (name, unit) = p.unit();
                    (meta_type, meta_change) = self.cache.set_unit(s.iter, name, unit);
                    set_mf(mf, meta, name, &mut has_meta, self.append_metadata_to_wal, &self.cache);
                    continue;
                }
                Entry::Comment => continue,
                Entry::Histogram => is_hist = true,
                _ => {}
            }
            stats.total += 1;

            let mut t = def_time;
            let mut parsed_ts: Option<i64>;
            let mut val = 0.0;
            let (mut has_h, mut has_fh) = (false, false);
            met.clear();
            if is_hist {
                let (m, pts, h, fh) = p.histogram();
                met.extend_from_slice(m);
                parsed_ts = pts;
                if let Some(h) = h {
                    h.copy_to(hbuf);
                    has_h = true;
                } else if let Some(fh) = fh {
                    fh.copy_to(fhbuf);
                    has_fh = true;
                }
            } else {
                let (m, pts, v) = p.series();
                met.extend_from_slice(m);
                parsed_ts = pts;
                val = v;
            }
            if !self.honor_timestamps {
                parsed_ts = None;
            }
            if let Some(x) = parsed_ts {
                t = x;
            }
            if s.get_dropped(met) {
                continue;
            }
            let (mut ce, already) = s.get(met);
            let series_cached = ce.is_some();
            let mut r: SeriesRef = 0;
            if let Some(id) = ce {
                r = s.entry(id).ref_;
            } else {
                p.labels(lset);
                let l2 = (self.sample_mutator)(std::mem::take(lset));
                *lset = l2;
                if lset.is_empty() {
                    s.add_dropped(met);
                    continue;
                }
                if !lset.has(METRIC_NAME_LABEL) {
                    err = Some(ERR_NAME_LABEL_MANDATORY);
                    break;
                }
                if !lset.is_valid(self.validation_scheme) {
                    err = Some(Error::msg(format!("invalid metric name or label names: {}", lset)));
                    break;
                }
                if let Err(e) = verify_label_limits(lset, self.label_limits.as_ref()) {
                    self.metrics.target_scrape_pool_exceeded_label_limits.get().inc();
                    err = Some(e);
                    break;
                }
            }
            macro_rules! ls {
                () => {
                    match ce {
                        Some(id) => &s.entry(id).lset,
                        None => &*lset,
                    }
                };
            }

            let mut aerr: Option<Error> = None;
            if already && parsed_ts.is_none() {
                aerr = Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP);
            } else {
                if self.enable_st_zero_ingestion {
                    let st = p.start_timestamp();
                    if st != 0 {
                        let res = if is_hist {
                            if has_h {
                                app.append_histogram_st_zero_sample(r, ls!(), t, st, Some(hbuf), None)
                            } else {
                                app.append_histogram_st_zero_sample(r, ls!(), t, st, None, Some(fhbuf))
                            }
                        } else {
                            app.append_st_zero_sample(r, ls!(), t, st)
                        };
                        match res {
                            Ok(x) => r = x,
                            Err(e) => {
                                r = 0;
                                if !e.is(&ERR_OUT_OF_ORDER_ST) {
                                    self.l.debug(
                                        "Error when appending ST in scrape loop",
                                        &[("series", &String::from_utf8_lossy(met)), ("ct", &st.to_string()), ("t", &t.to_string()), ("err", &e.to_string())],
                                    );
                                }
                            }
                        }
                    }
                }
                let res = if is_hist {
                    if has_h {
                        app.append_histogram(r, ls!(), t, Some(hbuf), None)
                    } else {
                        app.append_histogram(r, ls!(), t, None, if has_fh { Some(fhbuf) } else { None })
                    }
                } else {
                    app.append(r, ls!(), t, val)
                };
                match res {
                    Ok(x) => r = x,
                    Err(e) => {
                        r = 0;
                        aerr = Some(e);
                    }
                }
            }
            if aerr.is_none()
                && let Some(id) = ce
            {
                if r != 0 {
                    s.update_ref(id, r);
                }
                let er = s.entry(id).ref_;
                if (parsed_ts.is_none() || self.track_timestamps_staleness) && er != 0 {
                    s.track_staleness(er, id);
                }
            }
            let (sample_added, e2) =
                self.check_add_error(met, &[], aerr, &mut sample_limit_err, &mut bucket_limit_err, &mut app_errs);
            if let Some(e2) = e2 {
                if !e2.is(&ERR_NOT_FOUND) {
                    self.l.debug("Unexpected error", &[("series", &String::from_utf8_lossy(met)), ("err", &e2.to_string())]);
                }
                err = Some(e2);
                break;
            }
            if !series_cached && sample_added {
                let id = s.add_ref(met, r, lset.clone());
                ce = Some(id);
                if r != 0 && (parsed_ts.is_none() || self.track_timestamps_staleness) {
                    s.track_staleness(r, id);
                }
                if sample_limit_err.is_none() && bucket_limit_err.is_none() {
                    stats.series_added += 1;
                }
            }
            stats.added += 1;

            exemplars.clear();
            while p.exemplar(e) {
                if !e.has_ts {
                    if is_hist {
                        *e = Exemplar::default();
                        continue;
                    }
                    e.ts = t;
                }
                exemplars.push(std::mem::take(e));
            }
            if !exemplars.is_empty() {
                exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
                let mut ooo = 0;
                for ex in exemplars.iter() {
                    match app.append_exemplar(r, ls!(), ex) {
                        Ok(_) => {}
                        Err(x) if x.is(&ERR_OUT_OF_ORDER_EXEMPLAR) => ooo += 1,
                        Err(x) => self.l.debug("Error while adding exemplar in AddExemplar", &[("err", &x.to_string())]),
                    }
                }
                if ooo > 0 && ooo == exemplars.len() {
                    app_errs.num_exemplar_out_of_order += ooo;
                    self.l.debug("Out of order exemplars", &[("count", &ooo.to_string())]);
                    self.metrics.target_scrape_exemplar_out_of_order.get().add(ooo as f64);
                }
            }

            if self.append_metadata_to_wal
                && has_meta
                && (!series_cached || meta_change == s.iter)
                && is_series_part_of_family(ls!().get(METRIC_NAME_LABEL), mf, meta_type)
            {
                meta.typ = meta_type;
                if let Err(x) = app.update_metadata(r, ls!(), meta) {
                    self.l.debug("Error when appending metadata in scrape loop", &[("ref", &r.to_string()), ("err", &x.to_string())]);
                }
            }
        }
        if let Some(e) = &sample_limit_err {
            if err.is_none() {
                err = Some(e.clone());
            }
            self.metrics.target_scrape_sample_limit.get().inc();
        }
        if let Some(e) = &bucket_limit_err {
            if err.is_none() {
                err = Some(e.clone());
            }
            self.metrics.target_scrape_native_histogram_bucket_limit.get().inc();
        }
        self.warn_append_errors(&app_errs);
        if err.is_none() {
            err = self.update_stale_markers_v1(&mut app, s, def_time);
        }
        if err.is_none() {
            self.cache.iter_done(s, true);
        }
        (stats, err)
    }

    /// The V2 append (Go `scrapeLoopAppenderV2.append`).
    fn append_v2(&self, base: &mut dyn AppenderV2, b: &[u8], content_type: &str, ts: SystemTime) -> (AppendStats, Option<Error>) {
        let def_time = timestamp::from_time(ts);
        let mut stats = AppendStats::default();
        if b.is_empty() {
            let mut s = self.cache.series();
            let err = self.update_stale_markers_v2(&mut Raw2(base), &s, def_time);
            self.cache.iter_done(&mut s, false);
            return (stats, err);
        }
        let mut scg = self.scratch();
        let sc = &mut *scg;
        let mut p = match self.new_parser(sc, b, content_type, self.parse_st) {
            Ok(p) => p,
            Err(e) => return (stats, Some(e)),
        };
        let Scratch { lset, met, mf, h: hbuf, fh: fhbuf, meta, e, exemplars, .. } = sc;
        mf.clear();
        let mut app_errs = AppendErrors::default();
        let mut sample_limit_err: Option<Error> = None;
        let mut bucket_limit_err: Option<Error> = None;
        let mut has_meta = false;
        let mut meta_type = MetricType::Unknown;
        let mut err: Option<Error> = None;
        let mut sg = self.cache.series();
        let s = &mut *sg;
        let mut app = LimitsV2 {
            app: base,
            limits: Limits::new(self.runtime.wall(), self.sample_limit, self.bucket_limit, self.max_schema),
        };
        // ST synthesis needs the metadata even without metadata appends.
        let want_meta = self.append_metadata_to_wal;

        loop {
            let et = match p.next() {
                Ok(et) => et,
                Err(x) if x.is_eof() => break,
                Err(x) => {
                    err = Some(Error::msg(x.to_string()));
                    break;
                }
            };
            let mut is_hist = false;
            match et {
                Entry::Type => {
                    let (name, t) = p.typ();
                    (meta_type, _) = self.cache.set_type(s.iter, name, t);
                    set_mf(mf, meta, name, &mut has_meta, want_meta, &self.cache);
                    continue;
                }
                Entry::Help => {
                    let (name, help) = p.help();
                    (meta_type, _) = self.cache.set_help(s.iter, name, help);
                    set_mf(mf, meta, name, &mut has_meta, want_meta, &self.cache);
                    continue;
                }
                Entry::Unit => {
                    let (name, unit) = p.unit();
                    (meta_type, _) = self.cache.set_unit(s.iter, name, unit);
                    set_mf(mf, meta, name, &mut has_meta, want_meta, &self.cache);
                    continue;
                }
                Entry::Comment => continue,
                Entry::Histogram => is_hist = true,
                _ => {}
            }
            stats.total += 1;

            let mut t = def_time;
            let mut parsed_ts: Option<i64>;
            let mut val = 0.0;
            let (mut has_h, mut has_fh) = (false, false);
            met.clear();
            if is_hist {
                let (m, pts, h, fh) = p.histogram();
                met.extend_from_slice(m);
                parsed_ts = pts;
                if let Some(h) = h {
                    h.copy_to(hbuf);
                    has_h = true;
                } else if let Some(fh) = fh {
                    fh.copy_to(fhbuf);
                    has_fh = true;
                }
            } else {
                let (m, pts, v) = p.series();
                met.extend_from_slice(m);
                parsed_ts = pts;
                val = v;
            }
            if !self.honor_timestamps {
                parsed_ts = None;
            }
            if let Some(x) = parsed_ts {
                t = x;
            }
            if s.get_dropped(met) {
                continue;
            }
            let (mut ce, already) = s.get(met);
            let series_cached = ce.is_some();
            let mut r: SeriesRef = 0;
            if let Some(id) = ce {
                r = s.entry(id).ref_;
            } else {
                p.labels(lset);
                let l2 = (self.sample_mutator)(std::mem::take(lset));
                *lset = l2;
                if lset.is_empty() {
                    s.add_dropped(met);
                    continue;
                }
                if !lset.has(METRIC_NAME_LABEL) {
                    err = Some(ERR_NAME_LABEL_MANDATORY);
                    break;
                }
                if !lset.is_valid(self.validation_scheme) {
                    err = Some(Error::msg(format!("invalid metric name or label names: {}", lset)));
                    break;
                }
                if let Err(x) = verify_label_limits(lset, self.label_limits.as_ref()) {
                    self.metrics.target_scrape_pool_exceeded_label_limits.get().inc();
                    err = Some(x);
                    break;
                }
            }
            macro_rules! ls {
                () => {
                    match ce {
                        Some(id) => &s.entry(id).lset,
                        None => &*lset,
                    }
                };
            }

            exemplars.clear();
            let mut aerr: Option<Error> = None;
            let mut st_cache: Option<Box<StCache>> = None;
            if already && parsed_ts.is_none() {
                aerr = Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP);
            } else {
                if is_hist && !has_h && !has_fh {
                    err = Some(Error::msg(format!(
                        "parser returned nil histogram/float histogram for a histogram entry type for {} series; parser bug; aborting",
                        ls!()
                    )));
                    break;
                }
                let mut st = 0i64;
                if self.parse_st {
                    st = p.start_timestamp();
                }
                let mut reject_ooo = false;
                let mut skip_append = false;
                if self.synthesize_st && st == 0 {
                    let existing = ce.and_then(|id| s.entry_mut(id).st.take());
                    let c = match existing {
                        Some(c) => Some(c),
                        None => self.st_eligible(ls!(), mf).then(Box::<StCache>::default),
                    };
                    if let Some(mut c) = c {
                        if has_fh {
                            let (adj, nst, skip) = c.synthesize_float_histogram(fhbuf, t);
                            if let Some(a) = adj {
                                *fhbuf = a;
                            }
                            st = nst;
                            skip_append = skip;
                        } else if has_h {
                            let (adj, nst, skip) = c.synthesize_histogram(hbuf, t);
                            if let Some(a) = adj {
                                *hbuf = a;
                            }
                            st = nst;
                            skip_append = skip;
                        } else {
                            let (v, nst, skip) = c.synthesize_float(val, t);
                            val = v;
                            st = nst;
                            skip_append = skip;
                        }
                        st_cache = Some(c);
                    }
                    reject_ooo = true;
                }
                while p.exemplar(e) {
                    if !e.has_ts {
                        if is_hist {
                            *e = Exemplar::default();
                            continue;
                        }
                        e.ts = t;
                    }
                    exemplars.push(std::mem::take(e));
                }
                if !exemplars.is_empty() {
                    exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
                }
                let mut pass_meta = false;
                if self.append_metadata_to_wal && has_meta {
                    if !is_series_part_of_family(ls!().get(METRIC_NAME_LABEL), mf, meta_type) {
                        has_meta = false;
                    } else {
                        pass_meta = true;
                    }
                }
                let mf_str = if pass_meta { std::str::from_utf8(mf).unwrap_or("") } else { "" };
                if pass_meta {
                    meta.typ = meta_type;
                }
                let opts = AppendV2Options {
                    metric_family_name: mf_str,
                    metadata: if pass_meta { Some(&*meta) } else { None },
                    exemplars,
                    reject_out_of_order: reject_ooo,
                };
                if !skip_append {
                    let res = app.append(
                        r,
                        ls!(),
                        st,
                        t,
                        val,
                        if has_h { Some(&mut *hbuf) } else { None },
                        if has_fh { Some(&mut *fhbuf) } else { None },
                        &opts,
                    );
                    match res {
                        Ok(x) => {
                            r = x;
                            if let Some(id) = ce
                                && r != 0
                            {
                                s.update_ref(id, r);
                            }
                        }
                        Err(x) => {
                            r = x.as_append_partial().map_or(0, |p| p.series_ref);
                            aerr = Some(x);
                        }
                    }
                }
            }
            let (sample_added, e2) =
                self.check_add_error(met, exemplars, aerr, &mut sample_limit_err, &mut bucket_limit_err, &mut app_errs);
            if let Some(e2) = e2 {
                if !e2.is(&ERR_NOT_FOUND) {
                    self.l.debug("Unexpected error", &[("series", &String::from_utf8_lossy(met)), ("err", &e2.to_string())]);
                }
                err = Some(e2);
                break;
            }
            if !series_cached && sample_added {
                let id = s.add_ref(met, r, lset.clone());
                ce = Some(id);
                if sample_limit_err.is_none() && bucket_limit_err.is_none() {
                    stats.series_added += 1;
                }
            }
            if let Some(id) = ce
                && self.synthesize_st
            {
                if sample_added {
                    s.entry_mut(id).st = st_cache.take();
                } else if series_cached {
                    s.entry_mut(id).st = None;
                }
            }
            let track = parsed_ts.is_none() || self.track_timestamps_staleness || s_has_st(ce, s, &st_cache);
            if let Some(id) = ce {
                let er = s.entry(id).ref_;
                if er != 0 && track && sample_added {
                    s.track_staleness(er, id);
                }
            }
            stats.added += 1;
        }
        if let Some(x) = &sample_limit_err {
            if err.is_none() {
                err = Some(x.clone());
            }
            self.metrics.target_scrape_sample_limit.get().inc();
        }
        if let Some(x) = &bucket_limit_err {
            if err.is_none() {
                err = Some(x.clone());
            }
            self.metrics.target_scrape_native_histogram_bucket_limit.get().inc();
        }
        self.warn_append_errors(&app_errs);
        if err.is_none() {
            err = self.update_stale_markers_v2(&mut app, s, def_time);
        }
        if err.is_none() {
            self.cache.iter_done(s, true);
        }
        (stats, err)
    }

    /// The eligibility part of Go `checkAndSynthesizeStartTime` for a series without synthesis state.
    fn st_eligible(&self, lset: &Labels, mf: &[u8]) -> bool {
        let Ok(mfs) = std::str::from_utf8(mf) else { return false };
        let Some(m) = crate::target::MetricMetadataStore::get_metadata(&*self.cache, mfs) else { return false };
        let name = lset.get(METRIC_NAME_LABEL);
        if !is_series_part_of_family(name, mf, m.typ) {
            return false;
        }
        match m.typ {
            MetricType::Counter | MetricType::Histogram => true,
            MetricType::Summary => name.ends_with("_count") || name.ends_with("_sum"),
            _ => false,
        }
    }
}

/// Go's `stCache != nil` after the entry took the state: whether synthesis state exists for this sample.
fn s_has_st(ce: Option<EntryId>, s: &SeriesCache, local: &Option<Box<StCache>>) -> bool {
    if local.is_some() {
        return true;
    }
    match ce {
        Some(id) => s.entry(id).st.is_some(),
        None => false,
    }
}

fn set_mf(mf: &mut Vec<u8>, meta: &mut Metadata, name: &[u8], has: &mut bool, want_full: bool, cache: &ScrapeCache) {
    mf.clear();
    mf.extend_from_slice(name);
    *has = true;
    if want_full {
        cache.copy_metadata(name, meta);
    }
}

/// The appender operations that staleness markers need, for raw and limited V1 appenders.
trait StaleAppV1 {
    fn set_opts(&mut self, o: Option<&AppendOptions>);
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error>;
}

struct Raw1<'a>(&'a mut dyn Appender);
impl StaleAppV1 for Raw1<'_> {
    fn set_opts(&mut self, o: Option<&AppendOptions>) {
        self.0.set_options(o)
    }
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.0.append(r, l, t, v)
    }
}
impl StaleAppV1 for LimitsV1<'_> {
    fn set_opts(&mut self, o: Option<&AppendOptions>) {
        self.set_options(o)
    }
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.append(r, l, t, v)
    }
}

trait StaleAppV2 {
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64, o: &AppendV2Options<'_>) -> Result<SeriesRef, Error>;
}
struct Raw2<'a>(&'a mut dyn AppenderV2);
impl StaleAppV2 for Raw2<'_> {
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64, o: &AppendV2Options<'_>) -> Result<SeriesRef, Error> {
        self.0.append(r, l, 0, t, v, None, None, o)
    }
}
impl StaleAppV2 for LimitsV2<'_> {
    fn append_float(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64, o: &AppendV2Options<'_>) -> Result<SeriesRef, Error> {
        self.append(r, l, 0, t, v, None, None, o)
    }
}

/// An appender of a scrape loop (Go `scrapeLoopAppendAdapter`): V1 or V2.
pub enum LoopAppender<'a> {
    V1 { sl: &'a ScrapeLoop, app: Box<dyn Appender> },
    V2 { sl: &'a ScrapeLoop, app: Box<dyn AppenderV2> },
}

impl LoopAppender<'_> {
    pub fn commit(&mut self) -> Option<Error> {
        match self {
            LoopAppender::V1 { app, .. } => app.commit(),
            LoopAppender::V2 { app, .. } => app.commit(),
        }
    }

    pub fn rollback(&mut self) -> Option<Error> {
        match self {
            LoopAppender::V1 { app, .. } => app.rollback(),
            LoopAppender::V2 { app, .. } => app.rollback(),
        }
    }

    /// Go `append`: parses `b` and appends its samples; an empty `b` writes the stale markers.
    pub fn append(&mut self, b: &[u8], content_type: &str, ts: SystemTime) -> (AppendStats, Option<Error>) {
        match self {
            LoopAppender::V1 { sl, app } => sl.append_v1(&mut **app, b, content_type, ts),
            LoopAppender::V2 { sl, app } => sl.append_v2(&mut **app, b, content_type, ts),
        }
    }

    /// Go `addReportSample`.
    pub fn add_report_sample(&mut self, s: &ReportSample, t: i64, v: f64, reject_ooo: bool) -> Option<Error> {
        let sl: &ScrapeLoop = match self {
            LoopAppender::V1 { sl, .. } | LoopAppender::V2 { sl, .. } => sl,
        };
        let mut cache = sl.cache.series();
        let (ce, _) = cache.get(s.name);
        let lset = match ce {
            Some(id) => cache.entry(id).lset.clone(),
            None => {
                let mut b = Builder::new(Labels::default());
                b.set(METRIC_NAME_LABEL, s.metric_name());
                (sl.report_sample_mutator)(b.labels())
            }
        };
        let r = ce.map_or(0, |id| cache.entry(id).ref_);
        let res = match self {
            LoopAppender::V1 { app, .. } => {
                if reject_ooo {
                    app.set_options(Some(&REJECT_EARLY_OOO));
                    let x = app.append(r, &lset, t, v);
                    app.set_options(None);
                    x
                } else {
                    app.append(r, &lset, t, v)
                }
            }
            LoopAppender::V2 { app, .. } => {
                let opts = AppendV2Options {
                    metric_family_name: s.metric_name(),
                    metadata: Some(&s.metadata),
                    exemplars: &[],
                    reject_out_of_order: reject_ooo,
                };
                app.append(r, &lset, 0, t, v, None, None, &opts)
            }
        };
        match res {
            Ok(nr) => {
                if ce.is_none() {
                    cache.add_ref(s.name, nr, lset.clone());
                    if let LoopAppender::V1 { app, .. } = self
                        && sl.append_metadata_to_wal
                        && let Err(e) = app.update_metadata(nr, &lset, &s.metadata)
                    {
                        sl.l.debug("Error when appending metadata in addReportSample", &[("ref", &nr.to_string()), ("err", &e.to_string())]);
                    }
                }
                None
            }
            Err(e) if e.is(&ERR_OUT_OF_ORDER_SAMPLE) || e.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP) => None,
            Err(e) => Some(e),
        }
    }

    /// The scrape loop of the appender.
    pub fn scrape_loop(&self) -> &ScrapeLoop {
        match self {
            LoopAppender::V1 { sl, .. } | LoopAppender::V2 { sl, .. } => sl,
        }
    }
}

impl Task for ScrapeLoop {
    fn run_task(self: Arc<Self>, _rt: &Arc<Runtime>, generation: u64) {
        let (phase, errc) = {
            let mut s = self.sched.lock().unwrap();
            if generation != s.generation || s.running || matches!(s.phase, Phase::Done | Phase::NotStarted) {
                return;
            }
            s.running = true;
            (s.phase, s.errc.clone())
        };
        let (next, due) = self.step(phase, errc);
        let mut s = self.sched.lock().unwrap();
        s.running = false;
        if next == Phase::Done {
            drop(s);
            self.finish();
            return;
        }
        // A cancellation that arrived while the step ran is handled at once.
        let (next, due) =
            if matches!(next, Phase::Scrape | Phase::Offset) && self.ctx.is_done() { (Phase::Shutdown, None) } else { (next, due) };
        self.schedule(&mut s, next, due);
    }
}
