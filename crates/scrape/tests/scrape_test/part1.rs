//! scrape_test.go part 1 (Go lines 1-2299).
//!
//! Not written here (they need the real TSDB, `teststorage.New`): TestStorageHandlesOutOfOrderTimestamps,
//! TestScrapeLoopAppend_WithStorage, TestScrapeLoopAppend_StartTimeSynthesis,
//! TestScrapeLoopAppend_StartTimeSynthesis_WithSTStorage, TestScrapeLoopAppend_StartTimeSynthesis_OutOfOrder.

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, channel, sync_channel};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::ScrapeConfig;
use discovery::metrics::Registry;
use discovery::targetgroup::Group;
use model::labels::{Builder, Labels, ValidationScheme, from_strings};
use model::labelset::LabelSet;
use model::metadata::{Metadata, MetricType};
use model::relabel;
use scrape::ctx::Ctx;
use scrape::limits::{Limits, LimitsV1, LimitsV2};
use scrape::manager::Options;
use scrape::pool::{Loop, ScrapeLoopOptions, ScrapePool};
use scrape::scrape_loop::{
    AppendStats, ScrapeLoop, is_series_part_of_family, mutate_sample_labels, samples_post_relabel_metric,
    scrape_duration_metric, scrape_health_metric, scrape_samples_metric, scrape_series_added_metric,
};
use scrape::target::{ADDRESS_LABEL, MetricMetadataStore, SCRAPE_INTERVAL_LABEL, SCRAPE_TIMEOUT_LABEL, Target};
use scrape::testing::{
    StartFunc, TestLoop, new_test_registry_and_scrape_metrics, new_test_scrape_loop, new_test_scrape_metrics,
    new_test_scrape_pool,
};
use storage::{ERR_DEADLINE_EXCEEDED, Error};
use util::teststorage::{Appendable, Sample, require_equal};

// ---------------- helpers ----------------

/// Go `withAppendable(app, appV2)`.
fn with_appendable(sl: &mut ScrapeLoop, app: &Appendable, v2: bool) {
    if v2 {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
    } else {
        sl.appendable = Some(Arc::new(app.clone()));
    }
}

/// Go `foreachAppendable`.
fn foreach_appendable(f: impl Fn(bool)) {
    for v2 in [false, true] {
        f(v2);
    }
}

/// Go `time.Time{}` (year 1).
fn go_zero_time() -> SystemTime {
    UNIX_EPOCH - Duration::from_secs(62_135_596_800)
}

fn utf8_config() -> ScrapeConfig {
    ScrapeConfig {
        metric_name_validation_scheme: ValidationScheme::Utf8,
        metric_name_escaping_scheme: "allow-utf-8".to_string(),
        ..Default::default()
    }
}

fn dur(d: Duration) -> yaml::types::Duration {
    yaml::types::Duration(d.as_nanos() as i64)
}

fn group(addrs: &[&str]) -> Group {
    Group {
        targets: addrs
            .iter()
            .map(|a| LabelSet([(ADDRESS_LABEL.to_string(), a.to_string())].into_iter().collect()))
            .collect(),
        labels: LabelSet::default(),
        source: String::new(),
    }
}

fn new_pool(cfg: ScrapeConfig, app: &Appendable, v2: bool) -> Arc<ScrapePool> {
    let (a1, a2): (Option<Arc<dyn storage::Appendable>>, Option<Arc<dyn storage::AppendableV2>>) =
        if v2 { (None, Some(Arc::new(app.clone()))) } else { (Some(Arc::new(app.clone())), None) };
    ScrapePool::new(
        Arc::new(cfg),
        a1,
        a2,
        0,
        discovery::Logger::nop(),
        None,
        Arc::new(Options::default()),
        new_test_scrape_metrics(),
    )
    .expect("new scrape pool")
}

/// Go `gatherLabels`: the label pairs of every series of a family.
fn gather_labels(reg: &Registry, family: &str) -> Vec<Vec<(String, String)>> {
    for f in reg.gather() {
        if f.name == family {
            return f.metrics.into_iter().map(|m| m.labels).collect();
        }
    }
    Vec::new()
}

fn test_loop(interval: Duration, timeout: Duration, start: Option<StartFunc>, stop: Option<Box<dyn Fn() + Send + Sync>>) -> Arc<TestLoop> {
    Arc::new(TestLoop {
        start_func: Mutex::new(start),
        stop_func: Mutex::new(stop),
        interval,
        timeout,
        ..Default::default()
    })
}

/// Go `noopLoop`.
fn noop_loop() -> Arc<TestLoop> {
    test_loop(Duration::ZERO, Duration::ZERO, Some(Box::new(|_, _, _| {})), Some(Box::new(|| {})))
}

/// Go `sync.WaitGroup`.
#[derive(Default)]
struct WaitGroup(Mutex<i64>, Condvar);

impl WaitGroup {
    fn add(&self, n: i64) {
        *self.0.lock().unwrap() += n;
    }
    fn done(&self) {
        let mut g = self.0.lock().unwrap();
        *g -= 1;
        self.1.notify_all();
    }
    fn wait(&self) {
        let mut g = self.0.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while *g > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "wait group did not reach zero");
            g = self.1.wait_timeout(g, left).unwrap().0;
        }
    }
}

/// The test loops created by an injected constructor, to find the `*testLoop` behind `sp.loops` entries.
type Registry2 = Arc<Mutex<Vec<Arc<TestLoop>>>>;

fn find_test_loop(reg: &Registry2, l: &Arc<dyn Loop>) -> Arc<TestLoop> {
    let p = Arc::as_ptr(l) as *const ();
    reg.lock().unwrap().iter().find(|t| Arc::as_ptr(t) as *const () == p).cloned().expect("not a test loop")
}

enum Sel {
    Signal,
    Err(Error),
    Timeout,
}

/// Go `select { case <-signal: case err := <-errc: case <-time.After(d): }`.
fn select(sig: &Receiver<()>, errc: Option<&Receiver<Error>>, d: Duration) -> Sel {
    let deadline = Instant::now() + d;
    loop {
        if sig.try_recv().is_ok() {
            return Sel::Signal;
        }
        if let Some(e) = errc.and_then(|r| r.try_recv().ok()) {
            return Sel::Err(e);
        }
        if Instant::now() >= deadline {
            return Sel::Timeout;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn run_in_thread(sl: &Arc<ScrapeLoop>, errc: Option<scrape::scrape_loop::Errc>) -> Receiver<()> {
    let (tx, rx) = channel();
    let sl = sl.clone();
    std::thread::spawn(move || {
        sl.run(errc);
        let _ = tx.send(());
    });
    rx
}

fn md(typ: MetricType, unit: &str, help: &str) -> Metadata {
    Metadata { typ, unit: unit.to_string(), help: help.to_string() }
}

fn meta_sample(l: Labels, m: Metadata) -> Sample {
    Sample { l, m, ..Default::default() }
}

// ---------------- tests ----------------

#[test]
fn test_new_scrape_pool() {
    foreach_appendable(|v2| {
        let app = Appendable::new();
        let cfg = utf8_config();
        let sp = new_pool(cfg.clone(), &app, v2);
        // adapted: Go compares the stored appendable with `app` by value; here the pool's appendable must be a
        // handle to `app`: an append through it shows up in `app`.
        let ctx = storage::Context::default();
        let ts = from_strings(&["__name__", "m"]);
        if v2 {
            let a = sp.appendable_v2.as_ref().expect("Failure to append.");
            let mut ap = a.appender_v2(&ctx);
            ap.append(0, &ts, 0, 1, 1.0, None, None, &Default::default()).unwrap();
            assert!(ap.commit().is_none());
            assert_eq!(app.result_samples().len(), 1, "Wrong sample AppenderV2.");
            assert_eq!(cfg, *sp.config(), "Wrong scrape config.");
            assert!(sp.appendable.is_none());
            return;
        }
        let a = sp.appendable.as_ref().expect("Failure to append.");
        let mut ap = a.appender(&ctx);
        ap.append(0, &ts, 1, 1.0).unwrap();
        assert!(ap.commit().is_none());
        assert_eq!(app.result_samples().len(), 1, "Wrong sample appender.");
        assert_eq!(cfg, *sp.config(), "Wrong scrape config.");
        assert!(sp.appendable_v2.is_none());
    });
}

// Regression test against https://github.com/prometheus/prometheus/issues/15831.
#[test]
fn test_scrape_append_metadata_update() {
    const SCRAPE1: &str = "# TYPE test_metric counter
# HELP test_metric some help text
# UNIT test_metric metric
test_metric_total 1
# TYPE test_metric2 gauge
# HELP test_metric2 other help text
test_metric2{foo=\"bar\"} 2
# TYPE test_metric3 gauge
# HELP test_metric3 this represents tricky case of \"broken\" text that is not trivial to detect
test_metric3_metric4{foo=\"bar\"} 2
# EOF";
    const SCRAPE2: &str = "# TYPE test_metric counter
# HELP test_metric different help text
test_metric_total 11
# TYPE test_metric2 gauge
# HELP test_metric2 other help text
# UNIT test_metric2 metric2
test_metric2{foo=\"bar\"} 22
# EOF";
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(SCRAPE1.as_bytes(), "application/openmetrics-text", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        let first = vec![
            meta_sample(from_strings(&["__name__", "test_metric_total"]), md(MetricType::Counter, "metric", "some help text")),
            meta_sample(from_strings(&["__name__", "test_metric2", "foo", "bar"]), md(MetricType::Gauge, "", "other help text")),
        ];
        require_equal(&first, &app_test.result_metadata());
        app_test.result_reset();

        let mut app = sl.appender();
        let (_, err) = app.append(SCRAPE1.as_bytes(), "application/openmetrics-text", now + Duration::from_secs(15));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        if v2 {
            // Next (the same) scrape should pass new metadata entries as per always-on metadata Appendable V2 contract.
            require_equal(&first, &app_test.result_metadata());
        } else {
            // Next (the same) scrape should not add new metadata entries.
            assert!(app_test.result_metadata().is_empty());
        }
        app_test.result_reset();

        let mut app = sl.appender();
        let (_, err) = app.append(SCRAPE2.as_bytes(), "application/openmetrics-text", now + Duration::from_secs(15));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        require_equal(
            &[
                // Here, technically we should have no unit, but it's a known limitation of the current implementation.
                meta_sample(from_strings(&["__name__", "test_metric_total"]), md(MetricType::Counter, "metric", "different help text")),
                meta_sample(from_strings(&["__name__", "test_metric2", "foo", "bar"]), md(MetricType::Gauge, "metric2", "other help text")),
            ],
            &app_test.result_metadata(),
        );
        app_test.result_reset();
    });
}

#[test]
fn test_scrape_report_metadata() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));
        let mut app = sl.appender();

        let now = SystemTime::now();
        let stats = AppendStats { total: 1, added: 1, series_added: 1 };
        assert!(sl.report(&mut app, now, Duration::from_secs(2), stats, 512, None).is_none());
        assert!(app.commit().is_none());
        require_equal(
            &[
                meta_sample(from_strings(&["__name__", "up"]), scrape_health_metric().metadata.clone()),
                meta_sample(from_strings(&["__name__", "scrape_duration_seconds"]), scrape_duration_metric().metadata.clone()),
                meta_sample(from_strings(&["__name__", "scrape_samples_scraped"]), scrape_samples_metric().metadata.clone()),
                meta_sample(
                    from_strings(&["__name__", "scrape_samples_post_metric_relabeling"]),
                    samples_post_relabel_metric().metadata.clone(),
                ),
                meta_sample(from_strings(&["__name__", "scrape_series_added"]), scrape_series_added_metric().metadata.clone()),
            ],
            &app_test.result_metadata(),
        );
    });
}

#[test]
fn test_scrape_cache_size_metadata() {
    use scrape::ScrapeCache;
    type Steps = fn(&ScrapeCache);
    // adapted: Go sets `c.iter` and calls setType/setHelp/setUnit, which read it; the Rust setters take the
    // iteration as an argument, and iterDone reads the iteration of the series part.
    let cases: Vec<(&str, Steps)> = vec![
        ("insert type on new family", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            assert_eq!(7, c.size_metadata());
        }),
        ("insert help on new family", |c| {
            c.set_help(1, b"metric_b", b"help text");
            assert_eq!(16, c.size_metadata());
        }),
        ("insert unit on new family", |c| {
            c.set_unit(1, b"metric_c", b"bytes");
            assert_eq!(12, c.size_metadata());
        }),
        ("update type on existing family", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_type(1, b"metric_a", MetricType::Gauge);
            assert_eq!(5, c.size_metadata());
        }),
        ("update help on existing family", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_help(1, b"metric_a", b"new help");
            assert_eq!(15, c.size_metadata());
        }),
        ("repeated set with same value does not grow size", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_type(1, b"metric_a", MetricType::Counter);
            assert_eq!(7, c.size_metadata());
        }),
        ("multiple families accumulate", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_type(1, b"metric_b", MetricType::Gauge);
            c.set_help(1, b"metric_b", b"help");
            assert_eq!(16, c.size_metadata());
        }),
        ("stale entry removed on flush", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_help(1, b"metric_a", b"help");
            let mut s = c.series();
            s.iter = 20;
            c.iter_done(&mut s, true);
            drop(s);
            assert_eq!(0, c.size_metadata());
        }),
        ("non-stale entry survives flush", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            let mut s = c.series();
            s.iter = 5;
            c.iter_done(&mut s, true);
            drop(s);
            assert_eq!(7, c.size_metadata());
        }),
        ("partial stale removal on flush", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_type(2, b"metric_b", MetricType::Gauge);
            let mut s = c.series();
            s.iter = 12;
            c.iter_done(&mut s, true);
            drop(s);
            assert_eq!(5, c.size_metadata());
        }),
        ("full flush clears all stale entries", |c| {
            c.set_type(1, b"metric_a", MetricType::Counter);
            c.set_help(1, b"metric_a", b"help");
            c.set_type(1, b"metric_b", MetricType::Gauge);
            c.set_unit(1, b"metric_b", b"bytes");
            let mut s = c.series();
            s.iter = 100;
            c.iter_done(&mut s, true);
            drop(s);
            assert_eq!(0, c.size_metadata());
        }),
    ];
    for (name, steps) in cases {
        let c = ScrapeCache::new(new_test_scrape_metrics());
        c.series().iter = 1;
        eprintln!("case: {name}");
        steps(&c);
    }
}

#[test]
fn test_is_series_part_of_family() {
    use MetricType::*;
    let f = is_series_part_of_family;
    // counter
    assert!(f("http_requests_total", b"http_requests_total", Counter)); // Prometheus text style.
    assert!(f("http_requests_total", b"http_requests", Counter)); // OM text style.
    assert!(f("http_requests_total", b"http_requests_total", Unknown));
    assert!(!f("http_requests_total", b"http_requests", Unknown)); // We don't know.
    assert!(!f("http_requests2_total", b"http_requests_total", Counter));
    assert!(!f("http_requests_requests_total", b"http_requests", Counter));
    assert!(!f("http_requests_total_total", b"http_requests_total", Counter));

    // gauge
    assert!(f("http_requests_count", b"http_requests_count", Gauge));
    assert!(f("http_requests_count", b"http_requests_count", Unknown));
    assert!(!f("http_requests_count2", b"http_requests_count", Counter));

    // histogram
    assert!(f("http_requests_seconds_sum", b"http_requests_seconds", Histogram));
    assert!(f("http_requests_seconds_count", b"http_requests_seconds", Histogram));
    assert!(f("http_requests_seconds_bucket", b"http_requests_seconds", Histogram));
    assert!(f("http_requests_seconds", b"http_requests_seconds", Histogram));
    assert!(!f("http_requests_seconds_sum", b"http_requests_seconds", Unknown)); // We don't know.
    assert!(!f("http_requests_seconds2_sum", b"http_requests_seconds", Histogram));

    // summary
    assert!(f("http_requests_seconds_sum", b"http_requests_seconds", Summary));
    assert!(f("http_requests_seconds_count", b"http_requests_seconds", Summary));
    assert!(f("http_requests_seconds", b"http_requests_seconds", Summary));
    assert!(!f("http_requests_seconds_sum", b"http_requests_seconds", Unknown)); // We don't know.
    assert!(!f("http_requests_seconds2_sum", b"http_requests_seconds", Summary));

    // info
    assert!(f("go_build_info", b"go_build_info", Info)); // Prometheus text style.
    assert!(f("go_build_info", b"go_build", Info)); // OM text style.
    assert!(f("go_build_info", b"go_build_info", Unknown));
    assert!(!f("go_build_info", b"go_build", Unknown)); // We don't know.
    assert!(!f("go_build2_info", b"go_build_info", Info));
    assert!(!f("go_build_build_info", b"go_build_info", Info));
    assert!(!f("go_build_info_info", b"go_build_info", Info));
}

#[test]
fn test_dropped_targets_list() {
    foreach_appendable(|v2| {
        let app = Appendable::new();
        let cfg = ScrapeConfig {
            job_name: "dropMe".to_string(),
            scrape_interval: yaml::types::Duration(1),
            relabel_configs: vec![relabel::Config {
                action: relabel::Action::Drop,
                regex: relabel::Regexp::must_new("dropMe"),
                source_labels: Some(vec!["job".to_string()]),
                name_validation_scheme: ValidationScheme::Utf8,
                ..Default::default()
            }],
            ..utf8_config()
        };
        let tgs = vec![group(&["127.0.0.1:9090", "127.0.0.1:9091"])];
        let sp = new_pool(cfg, &app, v2);
        let expected = "{__address__=\"127.0.0.1:9090\", __always_scrape_classic_histograms__=\"false\", __convert_classic_histograms_to_nhcb__=\"false\", __scrape_interval__=\"0s\", __scrape_native_histograms__=\"false\", __scrape_timeout__=\"0s\", job=\"dropMe\"}";
        let expected_len = 2;
        sp.sync_targets_from_groups(&tgs);
        sp.sync_targets_from_groups(&tgs);
        assert_eq!(sp.dropped_targets().len(), expected_len);
        assert_eq!(expected_len, sp.dropped_targets_count());
        let mut lb = Builder::new(Labels::default());
        assert_eq!(expected, sp.dropped_targets()[0].discovered_labels(&mut lb).to_string());

        // Check that count is still correct when we don't retain all dropped targets.
        let mut c = (*sp.config()).clone();
        c.keep_dropped_targets = 1;
        sp.set_config(Arc::new(c));
        sp.sync_targets_from_groups(&tgs);
        assert_eq!(sp.dropped_targets().len(), 1);
        assert_eq!(expected_len, sp.dropped_targets_count());
    });
}

// TestDiscoveredLabelsUpdate checks that DiscoveredLabels are updated
// even when new labels don't affect the target `hash`.
#[test]
fn test_discovered_labels_update() {
    let sp = new_test_scrape_pool(None, None, None);

    // These are used when syncing so need this to avoid a panic.
    let cfg = Arc::new(ScrapeConfig {
        scrape_interval: yaml::types::Duration(1),
        scrape_timeout: yaml::types::Duration(1),
        ..utf8_config()
    });
    sp.set_config(cfg.clone());
    let ls = |k: &str, v: &str| LabelSet([(k.to_string(), v.to_string())].into_iter().collect());
    let t1 = Arc::new(Target::new(Labels::default(), cfg.clone(), ls("label", "name"), LabelSet::default()));
    sp.target_mtx.lock().unwrap().active_targets.insert(t1.hash(), t1.clone());

    let t2 = Arc::new(Target::new(Labels::default(), cfg.clone(), ls("labelNew", "nameNew"), LabelSet::default()));
    sp.sync_targets(vec![t2.clone()]);

    let mut lb = Builder::new(Labels::default());
    let want = t2.discovered_labels(&mut lb);
    let got = sp.active_targets_map()[&t1.hash()].discovered_labels(&mut lb);
    assert_eq!(want, got);
}

#[test]
fn test_scrape_pool_stop() {
    let sp = new_test_scrape_pool(None, None, None);
    let stopped: Arc<Mutex<HashMap<u64, bool>>> = Arc::default();
    let num_targets = 20u64;

    // Stopping the scrape pool must call stop() on all scrape loops,
    // clean them and the respective targets up. It must wait until each loop's
    // stop function returned before returning itself.
    for i in 0..num_targets {
        let t = Arc::new(Target::new(
            from_strings(&[ADDRESS_LABEL, &format!("example.com:{i}")]),
            Arc::new(ScrapeConfig::default()),
            LabelSet::default(),
            LabelSet::default(),
        ));
        let d = Duration::from_millis((i + 1) * 20);
        let h = t.hash();
        let st = stopped.clone();
        let l = test_loop(
            Duration::ZERO,
            Duration::ZERO,
            None,
            Some(Box::new(move || {
                std::thread::sleep(d);
                st.lock().unwrap().insert(h, true);
            })),
        );
        let mut ts = sp.target_mtx.lock().unwrap();
        ts.active_targets.insert(h, t);
        ts.loops.insert(h, l);
    }

    let sp = Arc::new(sp);
    let (tx, rx) = channel();
    let stop_time = Instant::now();
    let sp2 = sp.clone();
    std::thread::spawn(move || {
        sp2.stop();
        let _ = tx.send(());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Err(_) => panic!("scrapeLoop.stop() did not return as expected"),
        Ok(()) => {
            // This should have taken at least as long as the last target slept.
            assert!(
                stop_time.elapsed() >= Duration::from_millis(num_targets * 20),
                "scrapeLoop.stop() exited before all targets stopped"
            );
        }
    }

    assert_eq!(stopped.lock().unwrap().len(), num_targets as usize, "Unexpected number of stopped loops");
    let ts = sp.target_mtx.lock().unwrap();
    assert!(ts.active_targets.is_empty(), "Targets were not cleared on stopping: {} left", ts.active_targets.len());
    assert!(ts.loops.is_empty(), "Loops were not cleared on stopping: {} left", ts.loops.len());
}

// TestScrapePoolReload tests reloading logic, so:
// * all loops are reloaded, reusing cache if scrape config changed.
// * reloaded loops are stopped before new ones are started.
// * new scrapeLoops are configured with the updated scrape config.
#[test]
fn test_scrape_pool_reload() {
    let num_targets = 20u64;
    let stopped: Arc<Mutex<HashMap<u64, bool>>> = Arc::default();
    // Failures of the start functions (they run on other threads) and the number of started loops.
    let failures: Arc<Mutex<Vec<String>>> = Arc::default();
    let started = Arc::new(WaitGroup::default());

    let cfg0 = Arc::new(ScrapeConfig::default());
    let cfg1 = Arc::new(ScrapeConfig {
        scrape_interval: dur(Duration::from_secs(3)),
        scrape_timeout: dur(Duration::from_secs(2)),
        // Test a few example options.
        sample_limit: 123,
        scrape_fallback_protocol: config::ScrapeProtocol(
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited".to_string(),
        ),
        ..utf8_config()
    });
    let (c1, st1, f1, w1) = (cfg1.clone(), stopped.clone(), failures.clone(), started.clone());
    let new_loop_cfg1 = move |opts: ScrapeLoopOptions| -> Arc<dyn Loop> {
        // adapted: Go checks `opts.sp.config == cfg1` here; Rust loop options have no pool back-reference (and
        // the pool lock is held), so the pool config is checked after the reload below.
        let (c1, st1, f1, w1) = (c1.clone(), st1.clone(), f1.clone(), w1.clone());
        let hash = opts.target.hash();
        w1.add(1);
        // Inject out testLoop that allows mocking start and stop.
        test_loop(
            opts.interval,
            opts.timeout,
            // On start, expect previous loop instances for the same target to be stopped.
            Some(Box::new(move |interval, timeout, _| {
                // Ensure cfg1 interval and timeout are correctly configured.
                if c1.scrape_interval.to_std() != interval {
                    f1.lock().unwrap().push("Unexpected scrape interval".into());
                }
                if c1.scrape_timeout.to_std() != timeout {
                    f1.lock().unwrap().push("Unexpected scrape timeout".into());
                }
                // Go: the hash of the target scraper is the target's hash.
                if !st1.lock().unwrap().get(&hash).copied().unwrap_or(false) {
                    f1.lock().unwrap().push(format!("Scrape loop for {hash} not stopped yet"));
                }
                w1.done();
            })),
            None,
        )
    };

    // Create test pool.
    let (reg, metrics) = new_test_registry_and_scrape_metrics();
    let mut sp = new_test_scrape_pool(None, None, Some(Box::new(new_loop_cfg1)));
    sp.metrics = metrics;

    // Prefill pool with 20 loops, simulating 20 scrape targets.
    for i in 0..num_targets {
        let t = Arc::new(Target::new(
            from_strings(&[ADDRESS_LABEL, &format!("example.com:{i}")]),
            cfg0.clone(),
            LabelSet::default(),
            LabelSet::default(),
        ));
        let d = Duration::from_millis((i + 1) * 20);
        let h = t.hash();
        let st = stopped.clone();
        let l = test_loop(
            Duration::ZERO,
            Duration::ZERO,
            None,
            Some(Box::new(move || {
                std::thread::sleep(d); // Sleep uneven time on stop.
                st.lock().unwrap().insert(h, true);
            })),
        );
        let mut ts = sp.target_mtx.lock().unwrap();
        ts.active_targets.insert(h, t);
        ts.loops.insert(h, l);
    }

    let before_targets = sp.active_targets_map();

    // Reloading a scrape pool with a new scrape configuration must stop all scrape
    // loops and start new ones. A new loop must not be started before the preceding
    // one terminated.
    sp.reload(cfg1.clone()).unwrap();
    assert_eq!(*sp.config(), *cfg1);
    let stopped_count = stopped.lock().unwrap().len();
    assert_eq!(num_targets as usize, stopped_count, "Unexpected number of stopped loops");
    let after = sp.active_targets_map();
    assert_eq!(after.len(), before_targets.len(), "Reloading affected target states unexpectedly");
    for (k, v) in &before_targets {
        assert!(Arc::ptr_eq(v, &after[k]), "Reloading affected target states unexpectedly");
    }
    assert_eq!(sp.loops().len(), num_targets as usize, "Unexpected number of loops after reload");
    // adapted: Go's start functions fail the test from their goroutines; here they record failures, which are
    // checked once every new loop started.
    started.wait();
    let f = failures.lock().unwrap();
    assert!(f.is_empty(), "{f:?}");

    // Check if prometheus_target_reload_length_seconds points to cfg1.ScrapeInterval.
    let got = gather_labels(&reg, "prometheus_target_reload_length_seconds");
    assert_eq!(vec![vec![("interval".to_string(), "3s".to_string())]], got);
    assert_eq!(1.0, sp.metrics.target_scrape_pool_reloads.get().get());
}

#[test]
fn test_scrape_pool_reload_preserve_relabeled_interval_timeout() {
    let reload_cfg = Arc::new(ScrapeConfig {
        scrape_interval: dur(Duration::from_secs(3)),
        scrape_timeout: dur(Duration::from_secs(2)),
        ..utf8_config()
    });
    let failures: Arc<Mutex<Vec<String>>> = Arc::default();
    let started = Arc::new(WaitGroup::default());
    let (f1, w1) = (failures.clone(), started.clone());
    let new_loop = move |opts: ScrapeLoopOptions| -> Arc<dyn Loop> {
        let (f1, w1) = (f1.clone(), w1.clone());
        w1.add(1);
        test_loop(
            opts.interval,
            opts.timeout,
            Some(Box::new(move |interval, timeout, _| {
                if interval != Duration::from_secs(5) {
                    f1.lock().unwrap().push(format!("Unexpected scrape interval {interval:?}"));
                }
                if timeout != Duration::from_secs(3) {
                    f1.lock().unwrap().push(format!("Unexpected scrape timeout {timeout:?}"));
                }
                w1.done();
            })),
            None,
        )
    };
    let (reg, metrics) = new_test_registry_and_scrape_metrics();
    let mut sp = new_test_scrape_pool(None, None, Some(Box::new(new_loop)));
    {
        let mut ts = sp.target_mtx.lock().unwrap();
        ts.active_targets.insert(
            1,
            Arc::new(Target::new(
                from_strings(&[SCRAPE_INTERVAL_LABEL, "5s", SCRAPE_TIMEOUT_LABEL, "3s"]),
                Arc::new(ScrapeConfig::default()),
                LabelSet::default(),
                LabelSet::default(),
            )),
        );
        ts.loops.insert(1, noop_loop());
    }
    sp.metrics = metrics;

    if let Err(e) = sp.reload(reload_cfg) {
        panic!("unable to reload configuration: {e}");
    }
    started.wait();
    let f = failures.lock().unwrap();
    assert!(f.is_empty(), "{f:?}");
    // Check that the reload metric is labeled with the pool interval, not the overridden interval.
    let got = gather_labels(&reg, "prometheus_target_reload_length_seconds");
    assert_eq!(vec![vec![("interval".to_string(), "3s".to_string())]], got);
}

#[test]
fn test_scrape_pool_target_limit() {
    let wg = Arc::new(WaitGroup::default());
    let created: Registry2 = Arc::default();
    // On starting to run, new loops created on reload check whether their preceding
    // equivalents have been stopped.
    let (w, c) = (wg.clone(), created.clone());
    let new_loop = move |_: ScrapeLoopOptions| -> Arc<dyn Loop> {
        w.add(1);
        let w2 = w.clone();
        let l = test_loop(Duration::ZERO, Duration::ZERO, Some(Box::new(move |_, _, _| w2.done())), Some(Box::new(|| {})));
        c.lock().unwrap().push(l.clone());
        l
    };
    let sp = new_test_scrape_pool(None, None, Some(Box::new(new_loop)));

    let mut tgs: Vec<Group> = (0..50).map(|i| group(&[&format!("127.0.0.1:{}", 9090 + i)])).collect();

    let limit = Mutex::new(0usize);
    let reload_with_limit = |l: usize| {
        *limit.lock().unwrap() = l;
        sp.reload(Arc::new(ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(3)),
            scrape_timeout: dur(Duration::from_secs(2)),
            target_limit: l,
            ..utf8_config()
        }))
        .unwrap();
    };
    let targets = Mutex::new(0usize);
    let load_targets = |tgs: &[Group], n: usize| {
        *targets.lock().unwrap() = n;
        sp.sync_targets_from_groups(&tgs[..n]);
    };
    let validate_is_running = || {
        wg.wait();
        for l in sp.loops().values() {
            assert!(*find_test_loop(&created, l).run_once.lock().unwrap(), "loop should be running");
        }
    };
    let validate_error_message = |should_err: bool| {
        for l in sp.loops().values() {
            let lerr = find_test_loop(&created, l).get_forced_error();
            let (targets, limit) = (*targets.lock().unwrap(), *limit.lock().unwrap());
            if should_err {
                let e = lerr.unwrap_or_else(|| panic!("error was expected for {targets} targets with a limit of {limit}"));
                assert_eq!(e.to_string(), format!("target_limit exceeded (number of targets: {targets}, limit: {limit})"));
            } else {
                assert!(lerr.is_none(), "{lerr:?}");
            }
        }
    };

    reload_with_limit(0);
    load_targets(&tgs, 50);
    validate_is_running();

    // Simulate an initial config with a limit.
    let mut c = (*sp.config()).clone();
    c.target_limit = 30;
    sp.set_config(Arc::new(c));
    *limit.lock().unwrap() = 30;
    load_targets(&tgs, 50);
    validate_is_running();
    validate_error_message(true);

    reload_with_limit(50);
    validate_is_running();
    validate_error_message(false);

    reload_with_limit(40);
    validate_is_running();
    validate_error_message(true);

    load_targets(&tgs, 30);
    validate_is_running();
    validate_error_message(false);

    load_targets(&tgs, 40);
    validate_is_running();
    validate_error_message(false);

    load_targets(&tgs, 41);
    validate_is_running();
    validate_error_message(true);

    reload_with_limit(0);
    validate_is_running();
    validate_error_message(false);

    reload_with_limit(51);
    validate_is_running();
    validate_error_message(false);

    tgs.push(group(&["127.0.0.1:1090"]));
    tgs.push(group(&["127.0.0.1:1090"]));

    sp.sync_targets_from_groups(&tgs);
    validate_is_running();
    validate_error_message(false);
}

#[test]
fn test_scrape_pool_appender_with_limits() {
    // adapted: Go stacks one wrapper type per limit and checks the chain with type assertions; the Rust limits
    // live in one struct, and `chain()` names the Go wrappers, outermost first. The base appender check
    // compares the address of the wrapped appender with the appender of the appendable.
    let appendable = Appendable::new();
    let (sl, _) = new_test_scrape_loop(|sl| sl.appendable = Some(Arc::new(appendable.clone())));
    let mut base = sl.appendable.as_ref().unwrap().appender(&sl.appender_ctx);
    let base_ptr = &*base as *const dyn storage::Appender as *const ();
    let max = model::histogram::EXPONENTIAL_SCHEMA_MAX;
    let check = |base: &mut Box<dyn storage::Appender>, sample_limit, bucket_limit, max_schema, want: &[&str]| {
        let wrapped = LimitsV1 { app: &mut **base, limits: Limits::new(SystemTime::now(), sample_limit, bucket_limit, max_schema) };
        assert_eq!(want, wrapped.chain().as_slice());
        assert_eq!(base_ptr, wrapped.app as *const dyn storage::Appender as *const (), "Expected base appender");
    };
    check(&mut base, 0, 0, max, &["timeLimitAppender"]);

    let sample_limit = 100;
    let (sl, _) = new_test_scrape_loop(|sl| {
        sl.appendable = Some(Arc::new(appendable.clone()));
        sl.sample_limit = sample_limit;
    });
    let mut base = sl.appendable.as_ref().unwrap().appender(&sl.appender_ctx);
    let base_ptr = &*base as *const dyn storage::Appender as *const ();
    let check = |base: &mut Box<dyn storage::Appender>, sample_limit, bucket_limit, max_schema, want: &[&str]| {
        let wrapped = LimitsV1 { app: &mut **base, limits: Limits::new(SystemTime::now(), sample_limit, bucket_limit, max_schema) };
        assert_eq!(want, wrapped.chain().as_slice());
        assert_eq!(base_ptr, wrapped.app as *const dyn storage::Appender as *const (), "Expected base appender");
    };
    check(&mut base, sample_limit, 0, max, &["limitAppender", "timeLimitAppender"]);
    check(&mut base, sample_limit, 100, max, &["bucketLimitAppender", "limitAppender", "timeLimitAppender"]);
    check(
        &mut base,
        sample_limit,
        100,
        0,
        &["maxSchemaAppender", "bucketLimitAppender", "limitAppender", "timeLimitAppender"],
    );
}

#[test]
fn test_scrape_pool_appender_with_limits_append_v2() {
    // adapted: see test_scrape_pool_appender_with_limits.
    let appendable = Appendable::new();
    let (sl, _) = new_test_scrape_loop(|sl| sl.appendable_v2 = Some(Arc::new(appendable.clone())));
    let mut base = sl.appendable_v2.as_ref().unwrap().appender_v2(&sl.appender_ctx);
    let base_ptr = &*base as *const dyn storage::AppenderV2 as *const ();
    let max = model::histogram::EXPONENTIAL_SCHEMA_MAX;
    let check = |base: &mut Box<dyn storage::AppenderV2>, sample_limit, bucket_limit, max_schema, want: &[&str]| {
        let wrapped = LimitsV2 { app: &mut **base, limits: Limits::new(SystemTime::now(), sample_limit, bucket_limit, max_schema) };
        assert_eq!(want, wrapped.chain().iter().map(String::as_str).collect::<Vec<_>>().as_slice());
        assert_eq!(base_ptr, wrapped.app as *const dyn storage::AppenderV2 as *const (), "Expected base AppenderV2");
    };
    check(&mut base, 0, 0, max, &["timeLimitAppenderV2"]);

    let sample_limit = 100;
    let (sl, _) = new_test_scrape_loop(|sl| {
        sl.appendable_v2 = Some(Arc::new(appendable.clone()));
        sl.sample_limit = sample_limit;
    });
    let mut base = sl.appendable_v2.as_ref().unwrap().appender_v2(&sl.appender_ctx);
    let base_ptr = &*base as *const dyn storage::AppenderV2 as *const ();
    let check = |base: &mut Box<dyn storage::AppenderV2>, sample_limit, bucket_limit, max_schema, want: &[&str]| {
        let wrapped = LimitsV2 { app: &mut **base, limits: Limits::new(SystemTime::now(), sample_limit, bucket_limit, max_schema) };
        assert_eq!(want, wrapped.chain().iter().map(String::as_str).collect::<Vec<_>>().as_slice());
        assert_eq!(base_ptr, wrapped.app as *const dyn storage::AppenderV2 as *const (), "Expected base AppenderV2");
    };
    check(&mut base, sample_limit, 0, max, &["limitAppenderV2", "timeLimitAppenderV2"]);
    check(&mut base, sample_limit, 100, max, &["bucketLimitAppenderV2", "limitAppenderV2", "timeLimitAppenderV2"]);
    check(
        &mut base,
        sample_limit,
        100,
        0,
        &["maxSchemaAppenderV2", "bucketLimitAppenderV2", "limitAppenderV2", "timeLimitAppenderV2"],
    );
}

#[test]
fn test_scrape_pool_races() {
    foreach_appendable(|v2| {
        let interval = Duration::from_secs(1);
        let timeout = Duration::from_millis(500);
        let new_config = || ScrapeConfig { scrape_interval: dur(interval), scrape_timeout: dur(timeout), ..utf8_config() };
        let sp = new_pool(new_config(), &Appendable::new(), v2);
        let tgts = vec![group(&[
            "127.0.0.1:9090",
            "127.0.0.2:9090",
            "127.0.0.3:9090",
            "127.0.0.4:9090",
            "127.0.0.5:9090",
            "127.0.0.6:9090",
            "127.0.0.7:9090",
            "127.0.0.8:9090",
        ])];

        sp.sync_targets_from_groups(&tgts);
        let active = sp.active_targets();
        let dropped = sp.dropped_targets();
        assert_eq!(active.len(), tgts[0].targets.len(), "Invalid number of active targets");
        assert_eq!(dropped.len(), 0, "Invalid number of dropped targets");

        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(10));
            let _ = sp.reload(Arc::new(new_config()));
        }
        sp.stop();
    });
}

#[test]
fn test_scrape_pool_scrape_loops_started() {
    foreach_appendable(|v2| {
        let wg = Arc::new(WaitGroup::default());
        let created: Registry2 = Arc::default();
        let (w, c) = (wg.clone(), created.clone());
        let new_loop = move |_: ScrapeLoopOptions| -> Arc<dyn Loop> {
            w.add(1);
            let w2 = w.clone();
            let l = test_loop(Duration::ZERO, Duration::ZERO, Some(Box::new(move |_, _, _| w2.done())), Some(Box::new(|| {})));
            c.lock().unwrap().push(l.clone());
            l
        };
        let app = Appendable::new();
        let (a1, a2): (Option<Arc<dyn storage::Appendable>>, Option<Arc<dyn storage::AppendableV2>>) =
            if v2 { (None, Some(Arc::new(app.clone()))) } else { (Some(Arc::new(app.clone())), None) };
        let sp = new_test_scrape_pool(a1, a2, Some(Box::new(new_loop)));

        let tgs = vec![group(&["127.0.0.1:9090"]), group(&["127.0.0.1:9090"])];

        sp.reload(Arc::new(ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(3)),
            scrape_timeout: dur(Duration::from_secs(2)),
            ..utf8_config()
        }))
        .unwrap();
        sp.sync_targets_from_groups(&tgs);

        assert_eq!(sp.loops().len(), 1);

        wg.wait();
        for l in sp.loops().values() {
            assert!(*find_test_loop(&created, l).run_once.lock().unwrap(), "loop should be running");
        }
    });
}

#[test]
fn test_scrape_loop_stop_before_run() {
    let (sl, scraper) = new_test_scrape_loop(|_| {});
    let sl = Arc::new(sl);

    // The scrape pool synchronizes on stopping scrape loops. However, new scrape
    // loops are started asynchronously. Thus, it's possible, that a loop is stopped
    // again before having started properly.
    // Stopping not-yet-started loops must block until the run method was called and exited.
    // The run method must exit immediately.
    let (stop_tx, stop_done) = channel();
    let s2 = sl.clone();
    std::thread::spawn(move || {
        s2.stop();
        let _ = stop_tx.send(());
    });

    if stop_done.recv_timeout(Duration::from_millis(500)).is_ok() {
        panic!("Stopping terminated before run exited successfully.");
    }

    // Running the scrape loop must exit before calling the scraper even once.
    let called = Arc::new(Mutex::new(false));
    let c2 = called.clone();
    scraper.set_scrape_func(move |_, _| {
        *c2.lock().unwrap() = true;
        Ok(())
    });

    let run_done = run_in_thread(&sl, None);
    if run_done.recv_timeout(Duration::from_secs(1)).is_err() {
        panic!("Running terminated scrape loop did not exit.");
    }
    if stop_done.recv_timeout(Duration::from_secs(1)).is_err() {
        panic!("Stopping did not terminate after running exited.");
    }
    assert!(!*called.lock().unwrap(), "Scraper was called for terminated scrape loop.");
}

#[test]
fn test_scrape_loop_stop() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            // Since we're writing samples directly below we need to provide a protocol fallback.
            sl.fallback_scrape_protocol = "text/plain".to_string();
        });
        let sl = Arc::new(sl);
        let me: Arc<OnceLock<Weak<ScrapeLoop>>> = Arc::default();
        me.set(Arc::downgrade(&sl)).unwrap();

        // Terminate loop after 2 scrapes.
        let num_scrapes = Arc::new(Mutex::new(0));
        scraper.set_scrape_func(move |ctx: &Ctx, w: &mut Vec<u8>| {
            let n = {
                let mut n = num_scrapes.lock().unwrap();
                *n += 1;
                *n
            };
            if n == 2 {
                let sl = me.get().unwrap().upgrade().unwrap();
                let s2 = sl.clone();
                std::thread::spawn(move || s2.stop());
                sl.ctx.wait();
            }
            w.extend_from_slice(b"metric_a 42\n");
            match ctx.err() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        });

        let signal = run_in_thread(&sl, None);
        if signal.recv_timeout(Duration::from_secs(5)).is_err() {
            panic!("Scrape wasn't stopped.");
        }

        let got = app_test.result_samples();
        // We expected 1 actual sample for each scrape plus 5 for report samples.
        // At least 2 scrapes were made, plus the final stale markers.
        assert!(got.len() >= 6 * 3, "Expected at least 3 scrapes with 6 samples each.");
        assert_eq!(got.len() % 6, 0, "There is a scrape with missing samples.");
        // All samples in a scrape must have the same timestamp.
        let mut ts = 0;
        for (i, s) in got.iter().enumerate() {
            if i % 6 == 0 {
                ts = s.t;
            } else if s.t != ts {
                panic!("Unexpected multiple timestamps within single scrape");
            }
        }
        // All samples from the last scrape must be stale markers.
        for s in &got[got.len() - 5..] {
            assert!(
                model::value::is_stale_nan(s.v),
                "Appended last sample not as expected. Wanted: stale NaN Got: {:x}",
                s.v.to_bits()
            );
        }
    });
}

#[test]
fn test_scrape_loop_run() {
    foreach_appendable(|v2| {
        let (errc_tx, errc) = sync_channel::<Error>(0);

        let ctx = Ctx::background();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            sl.ctx = ctx.clone();
            with_appendable(sl, &Appendable::new(), v2);
        });
        let sl = Arc::new(sl);
        // The loop must terminate during the initial offset if the context
        // is canceled.
        scraper.set_offset(Duration::from_secs(3600));

        let signal = run_in_thread(&sl, Some(errc_tx.clone()));

        // Wait to make sure we are actually waiting on the offset.
        std::thread::sleep(Duration::from_secs(1));

        ctx.cancel();
        match select(&signal, Some(&errc), Duration::from_secs(5)) {
            Sel::Signal => {}
            Sel::Timeout => panic!("Cancellation during initial offset failed."),
            Sel::Err(e) => panic!("Unexpected error: {e}"),
        }

        let ctx = Ctx::background();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &Appendable::new(), v2);
            sl.ctx = ctx.clone();
            sl.timeout = Duration::from_millis(100);
        });
        let sl = Arc::new(sl);
        // The provided timeout must cause cancellation of the context passed down to the
        // scraper. The scraper has to respect the context.
        scraper.set_offset(Duration::ZERO);

        let block_ctx = Ctx::background();
        let (bc, c2) = (block_ctx.clone(), ctx.clone());
        scraper.set_scrape_func(move |sctx: &Ctx, _| {
            loop {
                if bc.is_done() {
                    c2.cancel();
                    return Ok(());
                }
                if sctx.is_done() {
                    return Err(sctx.err().unwrap());
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        let signal = run_in_thread(&sl, Some(errc_tx.clone()));

        match errc.recv_timeout(Duration::from_secs(3)) {
            Ok(e) => assert!(e.is(&ERR_DEADLINE_EXCEEDED), "{e}"),
            Err(_) => panic!("Expected timeout error but got none."),
        }

        // We already caught the timeout error and are certainly in the loop.
        // Let the scrapes returns immediately to cause no further timeout errors
        // and check whether canceling the parent context terminates the loop.
        block_ctx.cancel();
        match select(&signal, Some(&errc), Duration::from_secs(3)) {
            Sel::Signal => {} // Loop terminated as expected.
            Sel::Err(e) => panic!("Unexpected error: {e}"),
            Sel::Timeout => panic!("Loop did not terminate on context cancellation"),
        }
    });
}

#[test]
fn test_scrape_loop_forced_err() {
    foreach_appendable(|v2| {
        let (errc_tx, errc) = sync_channel::<Error>(0);
        let ctx = Ctx::background();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            sl.ctx = ctx.clone();
            with_appendable(sl, &Appendable::new(), v2);
        });
        let sl = Arc::new(sl);

        let forced_err = Error::msg("forced err");
        sl.set_forced_error(Some(forced_err.clone()));

        let scraped = Arc::new(Mutex::new(false));
        let s2 = scraped.clone();
        scraper.set_scrape_func(move |_, _| {
            *s2.lock().unwrap() = true;
            Ok(())
        });

        let signal = run_in_thread(&sl, Some(errc_tx));

        match errc.recv_timeout(Duration::from_secs(3)) {
            Ok(e) => assert!(e.is(&forced_err), "{e}"),
            Err(_) => panic!("Expected forced error but got none."),
        }
        ctx.cancel();

        if signal.recv_timeout(Duration::from_secs(5)).is_err() {
            panic!("Scrape not stopped.");
        }
        assert!(!*scraped.lock().unwrap(), "Should not be scraped.");
    });
}

#[test]
fn test_scrape_loop_run_context_cancel_terminates_blocked_send() {
    // Regression test for issue #17553
    // adapted: Go also checks for leaked goroutines (goleak); here the loop must finish (run returns).
    foreach_appendable(|v2| {
        let (errc_tx, _errc) = sync_channel::<Error>(0);
        let ctx = Ctx::background();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            sl.ctx = ctx.clone();
            with_appendable(sl, &Appendable::new(), v2);
        });
        let sl = Arc::new(sl);

        sl.set_forced_error(Some(Error::msg("forced err")));
        scraper.set_scrape_func(|_, _| Ok(()));

        let signal = run_in_thread(&sl, Some(errc_tx));
        std::thread::sleep(Duration::from_millis(50));
        ctx.cancel();

        if signal.recv_timeout(Duration::from_secs(3)).is_err() {
            panic!("Scrape loop failed to exit on context cancellation (goroutine leak detected)");
        }
    });
}

#[test]
fn test_scrape_loop_metadata() {
    foreach_appendable(|v2| {
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &Appendable::new(), v2));

        let mut app = sl.appender();
        let (stats, err) = app.append(
            b"# TYPE test_metric counter
# HELP test_metric some help text
# UNIT test_metric metric
test_metric_total 1
# TYPE test_metric_no_help gauge
# HELP test_metric_no_type other help text
# EOF",
            "application/openmetrics-text",
            SystemTime::now(),
        );
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(1, stats.total);

        let md = sl.cache.get_metadata("test_metric").expect("expected metadata to be present");
        assert_eq!(MetricType::Counter, md.typ, "unexpected metric type");
        assert_eq!("some help text", md.help);
        assert_eq!("metric", md.unit);

        let md = sl.cache.get_metadata("test_metric_no_help").expect("expected metadata to be present");
        assert_eq!(MetricType::Gauge, md.typ, "unexpected metric type");
        assert!(md.help.is_empty());
        assert!(md.unit.is_empty());

        let md = sl.cache.get_metadata("test_metric_no_type").expect("expected metadata to be present");
        assert_eq!(MetricType::Unknown, md.typ, "unexpected metric type");
        assert_eq!("other help text", md.help);
        assert!(md.unit.is_empty());
    });
}

#[test]
fn test_scrape_loop_series_added() {
    foreach_appendable(|v2| {
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &Appendable::new(), v2));

        let mut app = sl.appender();
        let (s, err) = app.append(b"test_metric 1\n", "text/plain", go_zero_time());
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!((1, 1, 1), (s.total, s.added, s.series_added));

        let mut app = sl.appender();
        let (s, err) = app.append(b"test_metric 1\n", "text/plain", go_zero_time());
        assert!(app.commit().is_none());
        assert!(err.is_none(), "{err:?}");
        assert_eq!((1, 1, 0), (s.total, s.added, s.series_added));
    });
}

#[test]
fn test_scrape_loop_append_open_metrics2() {
    const CONTENT_TYPE: &str = "application/openmetrics-text; version=2.0.0";
    const BODY: &[u8] = b"# TYPE test_metric gauge\ntest_metric 1\n# EOF\n";
    foreach_appendable(|v2| {
        // enabled
        {
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &Appendable::new(), v2);
                sl.enable_open_metrics2 = true;
            });
            let mut app = sl.appender();
            let (s, err) = app.append(BODY, CONTENT_TYPE, go_zero_time());
            assert!(err.is_none(), "{err:?}");
            assert!(app.commit().is_none());
            assert_eq!((1, 1, 1), (s.total, s.added, s.series_added));
        }
        // disabled
        {
            let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &Appendable::new(), v2));
            let mut app = sl.appender();
            let (_, err) = app.append(BODY, CONTENT_TYPE, go_zero_time());
            let e = err.expect("error expected");
            assert!(e.to_string().contains("the openmetrics2 feature flag is not enabled"), "{e}");
        }
        // disabled falls back
        {
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &Appendable::new(), v2);
                sl.fallback_scrape_protocol = "text/plain".to_string();
            });
            let mut app = sl.appender();
            let (s, err) = app.append(b"test_metric 1\n", CONTENT_TYPE, go_zero_time());
            assert!(err.is_none(), "{err:?}");
            assert!(app.commit().is_none());
            assert_eq!((1, 1), (s.total, s.added));
        }
    });
}

#[test]
fn test_scrape_loop_fail_with_invalid_labels_after_relabel() {
    foreach_appendable(|v2| {
        // adapted: Go uses the target label "pod_label_invalid_012\xff", which labelmap turns into the invalid
        // UTF-8 label name "012\xff"; a Rust string cannot hold invalid UTF-8. The closest invalid name under
        // UTF-8 validation is the empty name: labelmap turns "pod_label_invalid_" into "" (regex (.*) for (.+)).
        let target = Arc::new(Target::new(
            from_strings(&["pod_label_invalid_", "test"]),
            Arc::new(ScrapeConfig::default()),
            LabelSet::default(),
            LabelSet::default(),
        ));
        let relabel_config = vec![relabel::Config {
            action: relabel::Action::LabelMap,
            regex: relabel::Regexp::must_new("pod_label_invalid_(.*)"),
            separator: ";".to_string(),
            replacement: "$1".to_string(),
            name_validation_scheme: ValidationScheme::Utf8,
            ..Default::default()
        }];
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &Appendable::new(), v2);
            sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &target, true, &relabel_config));
        });

        let mut app = sl.appender();
        let (s, err) = app.append(b"test_metric 1\n", "text/plain", go_zero_time());
        let e = err.expect("error expected");
        assert!(e.to_string().contains("invalid metric name or label names"), "{e}");
        assert!(app.rollback().is_none());
        assert_eq!((1, 0, 0), (s.total, s.added, s.series_added));
    });
}

#[test]
fn test_scrape_loop_fail_legacy_under_utf8() {
    foreach_appendable(|v2| {
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &Appendable::new(), v2);
            sl.validation_scheme = ValidationScheme::Legacy;
        });
        let mut app = sl.appender();
        let (s, err) = app.append(b"{\"test.metric\"} 1\n", "text/plain", go_zero_time());
        let e = err.expect("error expected");
        assert!(e.to_string().contains("invalid metric name or label names"), "{e}");
        assert!(app.rollback().is_none());
        assert_eq!((1, 0, 0), (s.total, s.added, s.series_added));

        // When scrapeloop has validation set to UTF-8, the metric is allowed.
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &Appendable::new(), v2);
            sl.validation_scheme = ValidationScheme::Utf8;
        });
        let mut app = sl.appender();
        let (s, err) = app.append(b"{\"test.metric\"} 1\n", "text/plain", go_zero_time());
        assert!(err.is_none(), "{err:?}");
        assert_eq!((1, 1, 1), (s.total, s.added, s.series_added));
    });
}

// ---------------- expfmt helpers (Go promTextToProto, promTextToOM2) ----------------

/// A metric of a family read by [`expfmt_text_to_families`] (Go `dto.Metric`).
struct PMetric {
    labels: Vec<(String, String)>,
    ts: Option<i64>,
    val: PVal,
}

enum PVal {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    Summary { count: Option<u64>, sum: Option<f64>, quantiles: Vec<(f64, f64)> },
    Histogram { count: Option<u64>, count_f: Option<f64>, sum: Option<f64>, buckets: Vec<(f64, Option<u64>, Option<f64>)> },
}

// Go `dto.MetricType` values.
const T_COUNTER: u64 = 0;
const T_GAUGE: u64 = 1;
const T_SUMMARY: u64 = 2;
const T_UNTYPED: u64 = 3;
const T_HISTOGRAM: u64 = 4;
const T_GAUGE_HISTOGRAM: u64 = 5;

/// Go `dto.MetricFamily`.
struct PFamily {
    name: String,
    help: Option<String>,
    typ: Option<u64>,
    metrics: Vec<PMetric>,
}

/// Go `setOrCreateCurrentMF`: the family of a series name and whether the name is a _count or _sum series of it.
fn set_or_create_mf(fams: &mut Vec<PFamily>, by_name: &mut HashMap<String, usize>, name: &str) -> (usize, bool, bool) {
    if let Some(&i) = by_name.get(name) {
        return (i, false, false);
    }
    let is_count = name.len() > 6 && name.ends_with("_count");
    let is_sum = name.len() > 4 && name.ends_with("_sum");
    let is_bucket = name.len() > 7 && name.ends_with("_bucket");
    let summary_name = if is_count {
        &name[..name.len() - 6]
    } else if is_sum {
        &name[..name.len() - 4]
    } else {
        name
    };
    if let Some(&i) = by_name.get(summary_name)
        && fams[i].typ == Some(T_SUMMARY)
    {
        return (i, is_count, is_sum);
    }
    let hist_name = if is_bucket { &name[..name.len() - 7] } else { summary_name };
    if let Some(&i) = by_name.get(hist_name)
        && matches!(fams[i].typ, Some(T_HISTOGRAM | T_GAUGE_HISTOGRAM))
    {
        return (i, is_count, is_sum);
    }
    fams.push(PFamily { name: name.to_string(), help: None, typ: None, metrics: Vec::new() });
    by_name.insert(name.to_string(), fams.len() - 1);
    (fams.len() - 1, false, false)
}

/// Go `strconv.ParseFloat`: NaN is Go's `math.NaN()` bit pattern.
fn go_parse_float(s: &str) -> Option<f64> {
    let v: f64 = s.parse().ok()?;
    Some(if v.is_nan() { f64::from_bits(0x7FF8_0000_0000_0001) } else { v })
}

fn is_blank(c: char) -> bool {
    c == ' ' || c == '\t'
}

/// The parts of Go `expfmt.TextParser.TextToMetricFamilies` (Prometheus text format) that the test inputs use:
/// HELP and TYPE comments, samples with labels and optional timestamps, and the grouping of summary and
/// histogram series into one metric per label set. Families come back sorted by name.
fn expfmt_text_to_families(text: &[u8]) -> Vec<PFamily> {
    let text = std::str::from_utf8(text).expect("utf-8 input");
    let mut fams: Vec<PFamily> = Vec::new();
    let mut by_name: HashMap<String, usize> = HashMap::new();
    // Summary and histogram metrics by family and label set (Go `summaries`/`histograms` by signature).
    let mut grouped: HashMap<(usize, Vec<(String, String)>), usize> = HashMap::new();

    for line in text.split('\n') {
        let line = line.trim_start_matches(is_blank);
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('#') {
            let rest = rest.trim_start_matches(is_blank);
            let (kw, rest) = rest.split_once(is_blank).unwrap_or((rest, ""));
            if kw != "HELP" && kw != "TYPE" {
                continue;
            }
            let rest = rest.trim_start_matches(is_blank);
            let (name, rest) = rest.split_once(is_blank).unwrap_or((rest, ""));
            let (i, _, _) = set_or_create_mf(&mut fams, &mut by_name, name);
            let rest = rest.trim_start_matches(is_blank);
            if kw == "HELP" {
                let mut h = String::new();
                let mut it = rest.chars();
                while let Some(c) = it.next() {
                    if c == '\\' {
                        match it.next() {
                            Some('n') => h.push('\n'),
                            Some('\\') => h.push('\\'),
                            other => panic!("invalid escape sequence in HELP: {other:?}"),
                        }
                    } else {
                        h.push(c);
                    }
                }
                assert!(fams[i].help.is_none(), "second HELP line for metric name {name:?}");
                fams[i].help = Some(h);
            } else {
                assert!(fams[i].metrics.is_empty(), "TYPE line for {name:?} after samples");
                fams[i].typ = Some(match rest.trim_end_matches(is_blank).to_lowercase().as_str() {
                    "counter" => T_COUNTER,
                    "gauge" => T_GAUGE,
                    "summary" => T_SUMMARY,
                    "untyped" => T_UNTYPED,
                    "histogram" => T_HISTOGRAM,
                    "gaugehistogram" => T_GAUGE_HISTOGRAM,
                    t => panic!("unknown metric type {t:?}"),
                });
            }
            continue;
        }
        // A sample.
        let end = line.find(|c: char| c == '{' || is_blank(c)).unwrap_or(line.len());
        let name = &line[..end];
        assert!(!name.is_empty() && !name.starts_with('"'), "unsupported sample line {line:?}");
        let (fi, is_count, is_sum) = set_or_create_mf(&mut fams, &mut by_name, name);
        if fams[fi].typ.is_none() {
            fams[fi].typ = Some(T_UNTYPED);
        }
        let typ = fams[fi].typ.unwrap();
        let special = match typ {
            T_SUMMARY => "quantile",
            T_HISTOGRAM | T_GAUGE_HISTOGRAM => "le",
            _ => "",
        };
        let mut rest = line[end..].trim_start_matches(is_blank);
        let mut labels = Vec::new();
        let mut special_val = f64::NAN;
        if let Some(r) = rest.strip_prefix('{') {
            rest = r;
            loop {
                rest = rest.trim_start_matches(is_blank);
                if let Some(r) = rest.strip_prefix('}') {
                    rest = r;
                    break;
                }
                let eq = rest.find('=').expect("label name");
                let ln = rest[..eq].trim_end_matches(is_blank).to_string();
                rest = rest[eq + 1..].trim_start_matches(is_blank);
                rest = rest.strip_prefix('"').expect("label value quote");
                let mut v = String::new();
                let mut chars = rest.char_indices();
                let close = loop {
                    let (i, c) = chars.next().expect("unterminated label value");
                    match c {
                        '\\' => match chars.next().map(|x| x.1) {
                            Some('n') => v.push('\n'),
                            Some('\\') => v.push('\\'),
                            Some('"') => v.push('"'),
                            other => panic!("invalid escape sequence in label value: {other:?}"),
                        },
                        '"' => break i,
                        c => v.push(c),
                    }
                };
                rest = rest[close + 1..].trim_start_matches(is_blank);
                if ln == special {
                    special_val = go_parse_float(&v).expect("float in le/quantile");
                } else {
                    labels.push((ln, v));
                }
                if let Some(r) = rest.strip_prefix(',') {
                    rest = r;
                }
            }
        }
        let rest = rest.trim_start_matches(is_blank);
        let (vs, ts) = rest.split_once(is_blank).unwrap_or((rest, ""));
        let value: f64 = go_parse_float(vs).unwrap_or_else(|| panic!("expected float as value, got {vs:?}"));
        let ts = ts.trim_matches(is_blank);
        let ts = if ts.is_empty() { None } else { Some(ts.parse::<i64>().expect("integer timestamp")) };

        if special.is_empty() {
            let val = match typ {
                T_COUNTER => PVal::Counter(value),
                T_GAUGE => PVal::Gauge(value),
                _ => PVal::Untyped(value),
            };
            fams[fi].metrics.push(PMetric { labels, ts, val });
            continue;
        }
        let mut sig = labels.clone();
        sig.sort();
        let mi = match grouped.get(&(fi, sig.clone())) {
            Some(&mi) => mi,
            None => {
                let val = if typ == T_SUMMARY {
                    PVal::Summary { count: None, sum: None, quantiles: Vec::new() }
                } else {
                    PVal::Histogram { count: None, count_f: None, sum: None, buckets: Vec::new() }
                };
                fams[fi].metrics.push(PMetric { labels, ts: None, val });
                grouped.insert((fi, sig), fams[fi].metrics.len() - 1);
                fams[fi].metrics.len() - 1
            }
        };
        let m = &mut fams[fi].metrics[mi];
        if ts.is_some() {
            m.ts = ts;
        }
        match &mut m.val {
            PVal::Summary { count, sum, quantiles } => {
                if is_count {
                    *count = Some(value as u64);
                } else if is_sum {
                    *sum = Some(value);
                } else if !special_val.is_nan() {
                    quantiles.push((special_val, value));
                }
            }
            PVal::Histogram { count, count_f, sum, buckets } => {
                let as_uint = (value as u64 as f64 == value).then_some(value as u64);
                if is_count {
                    match as_uint {
                        Some(u) => *count = Some(u),
                        None => *count_f = Some(value),
                    }
                } else if is_sum {
                    *sum = Some(value);
                } else if !special_val.is_nan() {
                    match as_uint {
                        Some(u) => buckets.push((special_val, Some(u), None)),
                        None => buckets.push((special_val, None, Some(value))),
                    }
                }
            }
            _ => unreachable!(),
        }
    }
    // Get rid of empty metric families; order by name for the deterministic tests.
    fams.retain(|f| !f.metrics.is_empty());
    fams.sort_by(|a, b| a.name.cmp(&b.name));
    fams
}

fn pb_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn pb_bytes(out: &mut Vec<u8>, field: u64, b: &[u8]) {
    pb_varint(out, (field << 3) | 2);
    pb_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn pb_double(out: &mut Vec<u8>, field: u64, v: f64) {
    pb_varint(out, (field << 3) | 1);
    out.extend_from_slice(&v.to_le_bytes());
}

fn pb_uint(out: &mut Vec<u8>, field: u64, v: u64) {
    pb_varint(out, field << 3);
    pb_varint(out, v);
}

/// Go `proto.Marshal` of a `dto.MetricFamily`.
fn marshal_family(f: &PFamily) -> Vec<u8> {
    let single = |v: f64| {
        let mut b = Vec::new();
        pb_double(&mut b, 1, v);
        b
    };
    let mut out = Vec::new();
    pb_bytes(&mut out, 1, f.name.as_bytes());
    if let Some(h) = &f.help {
        pb_bytes(&mut out, 2, h.as_bytes());
    }
    if let Some(t) = f.typ {
        pb_uint(&mut out, 3, t);
    }
    for m in &f.metrics {
        let mut mb = Vec::new();
        for (n, v) in &m.labels {
            let mut lb = Vec::new();
            pb_bytes(&mut lb, 1, n.as_bytes());
            pb_bytes(&mut lb, 2, v.as_bytes());
            pb_bytes(&mut mb, 1, &lb);
        }
        match &m.val {
            PVal::Gauge(v) => pb_bytes(&mut mb, 2, &single(*v)),
            PVal::Counter(v) => pb_bytes(&mut mb, 3, &single(*v)),
            PVal::Summary { count, sum, quantiles } => {
                let mut sb = Vec::new();
                if let Some(c) = count {
                    pb_uint(&mut sb, 1, *c);
                }
                if let Some(s) = sum {
                    pb_double(&mut sb, 2, *s);
                }
                for (q, v) in quantiles {
                    let mut qb = Vec::new();
                    pb_double(&mut qb, 1, *q);
                    pb_double(&mut qb, 2, *v);
                    pb_bytes(&mut sb, 3, &qb);
                }
                pb_bytes(&mut mb, 4, &sb);
            }
            PVal::Untyped(v) => pb_bytes(&mut mb, 5, &single(*v)),
            PVal::Histogram { .. } => {}
        }
        if let Some(ts) = m.ts {
            pb_uint(&mut mb, 6, ts as u64);
        }
        if let PVal::Histogram { count, count_f, sum, buckets } = &m.val {
            let mut hb = Vec::new();
            if let Some(c) = count {
                pb_uint(&mut hb, 1, *c);
            }
            if let Some(s) = sum {
                pb_double(&mut hb, 2, *s);
            }
            for (ub, c, cf) in buckets {
                let mut bb = Vec::new();
                if let Some(c) = c {
                    pb_uint(&mut bb, 1, *c);
                }
                pb_double(&mut bb, 2, *ub);
                if let Some(cf) = cf {
                    pb_double(&mut bb, 4, *cf);
                }
                pb_bytes(&mut hb, 3, &bb);
            }
            if let Some(cf) = count_f {
                pb_double(&mut hb, 4, *cf);
            }
            pb_bytes(&mut mb, 7, &hb);
        }
        pb_bytes(&mut out, 4, &mb);
    }
    out
}

/// Go `promTextToProto`: Prometheus text to delimited protobuf families, ordered by name.
/// Given expfmt decoding limitations, it does not support OpenMetrics fully (e.g. exemplars).
fn prom_text_to_proto(text: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    for f in expfmt_text_to_families(text) {
        let o = marshal_family(&f);
        // Write first length, then binary protobuf.
        pb_varint(&mut buf, o.len() as u64);
        buf.extend(o);
    }
    buf
}

/// Go `writeOpenMetricsFloat`.
fn om_float(f: f64) -> String {
    if f == 1.0 {
        "1.0".into()
    } else if f == 0.0 {
        "0.0".into()
    } else if f == -1.0 {
        "-1.0".into()
    } else if f.is_nan() {
        "NaN".into()
    } else if f == f64::INFINITY {
        "+Inf".into()
    } else if f == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        let s = yaml::format_float_g(f);
        if s.contains(['e', '.']) { s } else { s + ".0" }
    }
}

fn om_escape(s: &str, include_quote: bool) -> String {
    let mut o = String::new();
    for c in s.chars() {
        match c {
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '"' if include_quote => o.push_str("\\\""),
            c => o.push(c),
        }
    }
    o
}

/// Go `promTextToOM2` (expfmt `MetricFamilyToOpenMetrics20` and `FinalizeOpenMetrics`) for the families the
/// datasets convert: gauges and untyped families with legacy names and no timestamps.
fn prom_text_to_om2(text: &[u8]) -> Vec<u8> {
    let mut o = String::new();
    for f in expfmt_text_to_families(text) {
        if let Some(h) = &f.help {
            o.push_str(&format!("# HELP {} {}\n", f.name, om_escape(h, false)));
        }
        let t = match f.typ {
            Some(T_GAUGE) => "gauge",
            Some(T_UNTYPED) => "unknown",
            t => panic!("prom_text_to_om2: metric type {t:?} is not supported by this test helper"),
        };
        o.push_str(&format!("# TYPE {} {t}\n", f.name));
        for m in &f.metrics {
            let v = match m.val {
                PVal::Gauge(v) | PVal::Untyped(v) => v,
                _ => unreachable!(),
            };
            o.push_str(&f.name);
            if !m.labels.is_empty() {
                o.push('{');
                for (i, (n, lv)) in m.labels.iter().enumerate() {
                    if i > 0 {
                        o.push(',');
                    }
                    o.push_str(&format!("{n}=\"{}\"", om_escape(lv, true)));
                }
                o.push('}');
            }
            o.push(' ');
            o.push_str(&om_float(v));
            assert!(m.ts.is_none(), "prom_text_to_om2: timestamps are not supported by this test helper");
            o.push('\n');
        }
    }
    o.push_str("# EOF\n");
    o.into_bytes()
}

/// Go `readTextParseTestdata`: a file from model/textparse/testdata.
fn read_text_parse_testdata(file: &str) -> Vec<u8> {
    let p = format!("{}/../../upstream/model/textparse/testdata/{file}", env!("CARGO_MANIFEST_DIR"));
    let b = std::fs::read(&p).unwrap_or_else(|e| panic!("{p}: {e}"));
    // Replace all Carriage Return chars that appear when testing on windows.
    b.into_iter().filter(|&c| c != b'\r').collect()
}

/// Go `readTextParseTestMetrics`.
fn read_text_parse_test_metrics() -> Vec<u8> {
    read_text_parse_testdata("alltypes.237mfs.prom.txt")
}

/// Go `makeTestMetricFamily`.
fn make_test_metric_family(n: usize, name: &str, typ: &str) -> Vec<u8> {
    let mut sb = format!("# TYPE {name} {typ}\n# HELP {name} help text\n");
    for i in 0..n {
        sb.push_str(&format!("{name}{{foo=\"{i}\",bar=\"{}\"}} 1\n", i * 100));
    }
    sb.push_str("# EOF\n");
    sb.into_bytes()
}

/// Go `makeTestGauges`.
fn make_test_gauges(n: usize) -> Vec<u8> {
    make_test_metric_family(n, "metric_a", "gauge")
}

#[test]
fn test_prom_text_to_proto() {
    let metrics_text = read_text_parse_test_metrics();
    let metrics_proto = prom_text_to_proto(&metrics_text);
    // adapted: Go decodes each delimited MetricFamily with expfmt; here each message is split off by its length
    // and its name (field 1, written first) is read.
    fn varint(b: &mut &[u8]) -> u64 {
        let (mut v, mut s) = (0u64, 0);
        loop {
            let c = b[0];
            *b = &b[1..];
            v |= ((c & 0x7f) as u64) << s;
            if c < 0x80 {
                return v;
            }
            s += 7;
        }
    }
    let mut got = Vec::new();
    let mut b = &metrics_proto[..];
    while !b.is_empty() {
        let n = varint(&mut b) as usize;
        let mut m = &b[..n];
        b = &b[n..];
        assert_eq!(varint(&mut m), (1 << 3) | 2, "name must be the first field");
        let l = varint(&mut m) as usize;
        got.push(String::from_utf8(m[..l].to_vec()).unwrap());
    }
    assert_eq!(got.len(), 237);
    // Check a few to see if those are not dups.
    assert_eq!("go_gc_cycles_automatic_gc_cycles_total", got[0]);
    assert_eq!("prometheus_sd_kuma_fetch_duration_seconds", got[128]);
    assert_eq!("promhttp_metric_handler_requests_total", got[236]);
}

/// Go `benchFormatCase`: one scrape payload of a dataset in a given exposition format.
struct BenchFormatCase {
    name: &'static str,
    content_type: &'static str,
    parsable: Vec<u8>,
}

/// Go `allFormatCases`.
fn all_format_cases(prom_text: Vec<u8>) -> Vec<BenchFormatCase> {
    vec![
        BenchFormatCase { name: "PromText", content_type: "text/plain", parsable: prom_text.clone() },
        BenchFormatCase { name: "OMText", content_type: "application/openmetrics-text", parsable: prom_text.clone() },
        BenchFormatCase {
            name: "OMText2",
            content_type: "application/openmetrics-text; version=2.0.0",
            parsable: prom_text_to_om2(&prom_text),
        },
        BenchFormatCase {
            name: "PromProto",
            content_type: "application/vnd.google.protobuf",
            parsable: prom_text_to_proto(&prom_text),
        },
    ]
}

/// Go `prometheusOMFormatCases`.
fn prometheus_om_format_cases() -> Vec<BenchFormatCase> {
    vec![
        BenchFormatCase {
            name: "OMText",
            content_type: "application/openmetrics-text",
            parsable: read_text_parse_testdata("prometheus.om1.txt"),
        },
        BenchFormatCase {
            name: "OMText2",
            content_type: "application/openmetrics-text; version=2.0.0",
            parsable: read_text_parse_testdata("prometheus.om2.txt"),
        },
    ]
}

// TestBenchDatasets checks that every format of a benchmark dataset appends the
// same number of samples, so BenchmarkScrapeLoopAppend compares equal work.
#[test]
fn test_bench_datasets() {
    // Go `benchDatasets`.
    let datasets: Vec<(&str, Vec<BenchFormatCase>, bool)> = vec![
        ("1Fam2000Gauges", all_format_cases(make_test_gauges(2000)), false),
        ("265FamsPrometheus", prometheus_om_format_cases(), false),
        ("265FamsPrometheusNHCB", prometheus_om_format_cases(), true),
    ];
    for (name, cases, convert_to_nhcb) in datasets {
        let mut want = 0;
        for (i, tc) in cases.iter().enumerate() {
            let app_test = Appendable::new();
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &app_test, true);
                sl.enable_native_histogram_scraping = true;
                sl.convert_classic_hist_to_nhcb = convert_to_nhcb;
                sl.always_scrape_classic_hist = false;
                sl.enable_open_metrics2 = true;
            });
            let mut app = sl.appender();
            let (s, err) = app.append(&tc.parsable, tc.content_type, SystemTime::now());
            assert!(err.is_none(), "{name}/{}: {err:?}", tc.name);
            assert!(app.commit().is_none());
            let got = app_test.result_samples().len();
            eprintln!(
                "{name}: fmt={}: bytes={} total={} added={} seriesAdded={} samples={got}",
                tc.name,
                tc.parsable.len(),
                s.total,
                s.added,
                s.series_added
            );
            if i == 0 {
                want = got;
                continue;
            }
            assert_eq!(want, got, "{name}: sample count mismatch between {} and {}", cases[0].name, tc.name);
        }
    }
}

/// Checks the expfmt helpers against the bytes Go wrote (bench/tmp/w2-scrape/st-p1-gogen); not an upstream test.
#[test]
#[ignore]
fn expfmt_helpers_match_go() {
    let dir = format!("{}/../../bench/tmp/w2-scrape/st-p1-gogen/out", env!("CARGO_MANIFEST_DIR"));
    let g = make_test_gauges(2000);
    assert!(prom_text_to_proto(&g) == std::fs::read(format!("{dir}/gauges.proto")).unwrap(), "gauges.proto");
    assert!(prom_text_to_om2(&g) == std::fs::read(format!("{dir}/gauges.om2")).unwrap(), "gauges.om2");
    let all = read_text_parse_test_metrics();
    assert!(prom_text_to_proto(&all) == std::fs::read(format!("{dir}/all.proto")).unwrap(), "all.proto");
}
