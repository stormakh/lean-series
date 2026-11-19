//! scrape_test.go part 4 (Go lines 6500 to the end).
//!
//! Not written here because they query the real TSDB (`teststorage.New` + `Querier`):
//! TestConvertClassicHistogramsToNHCB, TestTypeUnitReLabel, TestNativeHistogramMaxSchemaSet,
//! TestNewScrapeLoopHonorLabelsWiring.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use config::ScrapeConfig;
use discovery::targetgroup::Group;
use model::labels::{Labels, ValidationScheme, from_strings};
use model::labelset::{ADDRESS_LABEL, LabelSet};
use model::relabel;
use model::timestamp::from_time;
use model::value::{STALE_NAN, is_stale_nan};
use scrape::Ctx;
use scrape::limits::ERR_SAMPLE_LIMIT;
use scrape::manager::{Manager, Options};
use scrape::pool::{ScrapeLoopOptions, ScrapePool, build_scrape_loop};
use scrape::runtime::Runtime;
use scrape::scrape_loop::{ScrapeLoop, mutate_sample_labels, pick_schema};
use scrape::target::{METRICS_PATH_LABEL, SCHEME_LABEL, SCRAPE_TIMEOUT_LABEL, Target};
use scrape::testing::{NoopFailureLogger, TestScraper, new_test_scrape_loop, new_test_scrape_metrics, new_test_scrape_pool};
use storage::Error;
use util::teststorage::{Appendable, Sample, require_equal};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

/// Go `foreachAppendable`: runs `f` for the V1 and the V2 appendable.
fn foreach_appendable(f: impl Fn(bool)) {
    for v2 in [false, true] {
        f(v2);
    }
}

/// Go `withAppendable`.
fn set_appendable(sl: &mut ScrapeLoop, app: &Appendable, v2: bool) {
    if v2 {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
    } else {
        sl.appendable = Some(Arc::new(app.clone()));
    }
}

type Apps = (Option<Arc<dyn storage::Appendable>>, Option<Arc<dyn storage::AppendableV2>>);

/// Go `selectAppendable(app, appV2)` with `.V1()` and `.V2()`.
fn select_appendable(app: &Appendable, v2: bool) -> Apps {
    if v2 { (None, Some(Arc::new(app.clone()))) } else { (Some(Arc::new(app.clone())), None) }
}

/// Go `selectAppendable(s, appV2)` for a real TSDB (`teststorage.New`).
fn select_storage(s: &Arc<tsdb::teststorage::TestStorage>, v2: bool) -> Apps {
    if v2 { (None, Some(s.clone())) } else { (Some(s.clone()), None) }
}

fn dur(d: Duration) -> yaml::types::Duration {
    yaml::types::Duration(d.as_nanos() as i64)
}

/// Go `newScrapePool(cfg, app, appV2, 0, nil, nil, opts, newTestScrapeMetrics(t))`.
fn new_pool(cfg: ScrapeConfig, apps: Apps, opts: Options) -> Arc<ScrapePool> {
    ScrapePool::new(Arc::new(cfg), apps.0, apps.1, 0, discovery::Logger::nop(), None, Arc::new(opts), new_test_scrape_metrics())
        .expect("new scrape pool")
}

fn group(targets: Vec<LabelSet>) -> Group {
    Group { targets, labels: LabelSet::new(), source: String::new() }
}

fn ls(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(pairs)
}

fn sample(name: &str, t: i64, v: f64) -> Sample {
    Sample { l: from_strings(&["__name__", name]), t, v, ..Default::default() }
}

/// Go `makeTestMetricFamily`.
fn make_test_metric_family(n: usize, name: &str, typ: &str) -> Vec<u8> {
    let mut s = format!("# TYPE {name} {typ}\n# HELP {name} help text\n");
    for i in 0..n {
        s.push_str(&format!("{name}{{foo=\"{i}\",bar=\"{}\"}} 1\n", i * 100));
    }
    s.push_str("# EOF\n");
    s.into_bytes()
}

/// Go `makeTestGauges`.
fn make_test_gauges(n: usize) -> Vec<u8> {
    make_test_metric_family(n, "metric_a", "gauge")
}

/// A received HTTP request.
#[derive(Clone, Debug, Default)]
struct Req {
    path: String,
    query: String,
    headers: Vec<(String, String)>,
}

impl Req {
    fn header(&self, name: &str) -> String {
        self.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone()).unwrap_or_default()
    }
    fn query_get(&self, name: &str) -> String {
        for kv in self.query.split('&') {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            if k == name {
                return v.to_string();
            }
        }
        String::new()
    }
}

type Handler = Arc<dyn Fn(&Req) -> (Vec<(String, String)>, Vec<u8>) + Send + Sync>;

/// Go `httptest.NewServer`: a blocking HTTP/1.1 server on 127.0.0.1 that closes each connection after one response.
struct TestServer {
    host: String,
    stop: Arc<AtomicBool>,
}

impl TestServer {
    fn new(handler: Handler) -> TestServer {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let host = l.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let st = stop.clone();
        std::thread::spawn(move || {
            for c in l.incoming() {
                if st.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(c) = c else { continue };
                let h = handler.clone();
                std::thread::spawn(move || serve_conn(c, h));
            }
        });
        TestServer { host, stop }
    }
    fn url(&self) -> String {
        format!("http://{}", self.host)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.host);
    }
}

fn serve_conn(c: TcpStream, h: Handler) {
    let _ = c.set_read_timeout(Some(Duration::from_secs(10)));
    let mut r = BufReader::new(c.try_clone().unwrap());
    let mut line = String::new();
    if r.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let target = line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target, String::new()),
    };
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut l = String::new();
        if r.read_line(&mut l).unwrap_or(0) == 0 {
            return;
        }
        let l = l.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            let (k, v) = (k.trim().to_string(), v.trim().to_string());
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }
    let mut body = vec![0u8; content_length];
    let _ = r.read_exact(&mut body);
    let (hdrs, body) = h(&Req { path, query, headers });
    let mut out = String::from("HTTP/1.1 200 OK\r\n");
    for (k, v) in &hdrs {
        out.push_str(&format!("{k}: {v}\r\n"));
    }
    out.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
    let mut c = c;
    let _ = c.write_all(out.as_bytes());
    let _ = c.write_all(&body);
    let _ = c.flush();
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_scrape_loop_run_creates_stale_markers_on_failed_scrape_for_timestamped_metrics() {
    foreach_appendable(|v2| {
        let ctx = Ctx::background().child();
        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            set_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone(); // Since we're writing samples directly below we need to provide a protocol fallback.
            sl.fallback_scrape_protocol = "text/plain".to_string();
            sl.track_timestamps_staleness = true;
        });

        // Succeed once, several failures, then stop.
        let num_scrapes = Arc::new(AtomicUsize::new(0));
        let (n, c) = (num_scrapes.clone(), ctx.clone());
        scraper.set_scrape_func(move |_, w| {
            let k = n.fetch_add(1, Ordering::SeqCst) + 1;
            match k {
                1 => {
                    let ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                    w.extend_from_slice(format!("metric_a 42 {ms}\n").as_bytes());
                    return Ok(());
                }
                5 => c.cancel(),
                _ => {}
            }
            Err(Error::msg("scrape failed"))
        });

        let sl = Arc::new(sl);
        let (tx, rx) = mpsc::channel();
        let s2 = sl.clone();
        std::thread::spawn(move || {
            s2.run(None);
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(5)).expect("Scrape wasn't stopped.");

        let got = app_test.result_samples();
        // 1 successfully scraped sample, 1 stale marker after first fail, 5 report samples for
        // each scrape successful or not.
        assert_eq!(got.len(), 27, "Appended samples not as expected:\n{got:?}");
        assert_eq!(got[0].v, 42.0, "Appended first sample not as expected");
        assert!(
            is_stale_nan(got[6].v),
            "Appended second sample not as expected. Wanted: stale NaN Got: {:x}",
            got[6].v.to_bits()
        );
    });
}

#[test]
fn test_scrape_loop_compression() {
    foreach_appendable(|v2| {
        let s = Arc::new(tsdb::teststorage::new_default());
        let metrics_text = make_test_gauges(10);

        struct Tc {
            enable_compression: bool,
            enable_zstd: bool,
            accept_encoding: &'static str,
        }
        for tc in [
            Tc { enable_compression: true, accept_encoding: "zstd,gzip", enable_zstd: true },
            Tc { enable_compression: true, accept_encoding: "gzip", enable_zstd: false },
            Tc { enable_compression: false, accept_encoding: "identity", enable_zstd: false },
        ] {
            let name = format!("compression={},acceptEncoding={}", tc.enable_compression, tc.accept_encoding);
            let (tx, rx) = mpsc::channel::<String>();
            let tx = Mutex::new(Some(tx));
            let body = metrics_text.clone();
            let ts = TestServer::new(Arc::new(move |r: &Req| {
                // Go closes `scraped` here (the first request only).
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send(r.header("Accept-Encoding"));
                }
                (vec![], body.clone())
            }));

            let cfg = ScrapeConfig {
                job_name: "test".into(),
                sample_limit: 100,
                scheme: "http".into(),
                scrape_interval: dur(Duration::from_millis(100)),
                scrape_timeout: dur(Duration::from_millis(100)),
                enable_compression: tc.enable_compression,
                metric_name_validation_scheme: ValidationScheme::Utf8,
                metric_name_escaping_scheme: config::ALLOW_UTF8.into(),
                ..Default::default()
            };
            let sp = new_pool(
                cfg,
                select_storage(&s, v2),
                Options { enable_zstd_scrape: tc.enable_zstd, ..Default::default() },
            );

            sp.sync_targets_from_groups(&[group(vec![ls(&[(ADDRESS_LABEL, &ts.host)])])]);
            assert_eq!(sp.active_targets().len(), 1, "{name}");

            let got = rx.recv_timeout(Duration::from_secs(5)).unwrap_or_else(|_| panic!("{name}: target was not scraped"));
            assert_eq!(got, tc.accept_encoding, "{name}: invalid value of the Accept-Encoding header");
            sp.stop();
        }
    });
}

#[test]
fn test_pick_schema() {
    let tcs: &[(f64, i32)] = &[
        (65536.0, -4),
        (256.0, -3),
        (16.0, -2),
        (4.0, -1),
        (2.0, 0),
        (1.4, 1),
        (1.1, 2),
        (1.09, 3),
        (1.04, 4),
        (1.02, 5),
        (1.01, 6),
        (1.005, 7),
        (1.002, 8),
        // The default value of native_histogram_min_bucket_factor
        (0.0, 8),
    ];
    for &(factor, schema) in tcs {
        assert_eq!(pick_schema(factor), schema, "factor {factor}");
    }
}

// When a scrape contains multiple instances for the same time series we should increment
// prometheus_target_scrapes_sample_duplicate_timestamp_total metric.
#[test]
fn test_scrape_loop_series_added_duplicates() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| set_appendable(sl, &app_test, v2));
        let dup = || sl.metrics.target_scrape_sample_duplicate.get().get();
        let t0 = SystemTime::UNIX_EPOCH; // adapted: Go time.Time{} (year 1); the zero time is not used for the result.

        let mut app = sl.appender();
        let (st, err) = app.append(b"test_metric 1\ntest_metric 2\ntest_metric 3\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 3);
        assert_eq!(st.added, 3);
        assert_eq!(st.series_added, 1);
        assert_eq!(dup(), 2.0);

        let mut app = sl.appender();
        let (st, err) = app.append(b"test_metric 1\ntest_metric 1\ntest_metric 1\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 3);
        assert_eq!(st.added, 3);
        assert_eq!(st.series_added, 0);
        assert_eq!(dup(), 4.0);

        // When different timestamps are supplied, multiple samples are accepted.
        let mut app = sl.appender();
        let (st, err) = app.append(b"test_metric 1 1001\ntest_metric 1 1002\ntest_metric 1 1003\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 3);
        assert_eq!(st.added, 3);
        assert_eq!(st.series_added, 0);
        // Metric is not higher than last time.
        assert_eq!(dup(), 4.0);
    });
}

#[test]
fn test_target_scrape_config_with_labels() {
    foreach_appendable(|v2| {
        let config_timeout = Duration::from_millis(1500);
        const EXPECTED_TIMEOUT: &str = "1.5";
        const EXPECTED_TIMEOUT_LABEL: &str = "1s500ms";
        let second_timeout = Duration::from_millis(500);
        const SECOND_TIMEOUT_LABEL: &str = "500ms";
        const EXPECTED_PARAM: &str = "value1";
        const SECOND_PARAM: &str = "value2";
        const EXPECTED_PATH: &str = "/metric-ok";
        const SECOND_PATH: &str = "/metric-nok";
        const HTTP_SCHEME: &str = "http";
        const PARAM_LABEL: &str = "__param_param";
        const JOB_NAME: &str = "test";

        let params = |v: &str| {
            let mut m = BTreeMap::new();
            m.insert("param".to_string(), vec![v.to_string()]);
            m
        };
        let base = |timeout: Duration, path: &str, param: &str| ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(2)),
            scrape_timeout: dur(timeout),
            params: params(param),
            job_name: JOB_NAME.into(),
            scheme: HTTP_SCHEME.into(),
            metrics_path: path.into(),
            metric_name_validation_scheme: ValidationScheme::Utf8,
            metric_name_escaping_scheme: config::ALLOW_UTF8.into(),
            ..Default::default()
        };
        let set_rule = |target: &str, replacement: &str| {
            let d = relabel::default_relabel_config();
            relabel::Config {
                action: d.action,
                regex: d.regex.clone(),
                source_labels: d.source_labels.clone(),
                target_label: target.into(),
                replacement: replacement.into(),
                name_validation_scheme: ValidationScheme::Utf8,
                ..Default::default()
            }
        };

        let mut relabeled = base(second_timeout, SECOND_PATH, SECOND_PARAM);
        relabeled.relabel_configs = vec![
            set_rule(SCRAPE_TIMEOUT_LABEL, EXPECTED_TIMEOUT_LABEL),
            set_rule(PARAM_LABEL, EXPECTED_PARAM),
            set_rule(METRICS_PATH_LABEL, EXPECTED_PATH),
        ];

        let cases: Vec<(&str, ScrapeConfig, Vec<LabelSet>)> = vec![
            ("Everything in scrape config", base(config_timeout, EXPECTED_PATH, EXPECTED_PARAM), vec![ls(&[(ADDRESS_LABEL, "")])]),
            (
                "Overridden in target",
                base(second_timeout, SECOND_PATH, SECOND_PARAM),
                vec![ls(&[
                    (ADDRESS_LABEL, ""),
                    (SCRAPE_TIMEOUT_LABEL, EXPECTED_TIMEOUT_LABEL),
                    (METRICS_PATH_LABEL, EXPECTED_PATH),
                    (PARAM_LABEL, EXPECTED_PARAM),
                ])],
            ),
            (
                "Overridden in relabel_config",
                relabeled,
                vec![ls(&[
                    (ADDRESS_LABEL, ""),
                    (SCRAPE_TIMEOUT_LABEL, SECOND_TIMEOUT_LABEL),
                    (METRICS_PATH_LABEL, SECOND_PATH),
                    (PARAM_LABEL, SECOND_PARAM),
                ])],
            ),
        ];

        // Go runs the cases in parallel.
        std::thread::scope(|s| {
            for (name, cfg, targets) in cases {
                s.spawn(move || {
                    let (tx, rx) = mpsc::channel::<Req>();
                    let tx = Mutex::new(Some(tx));
                    let srv = TestServer::new(Arc::new(move |r: &Req| {
                        // Go: `defer close(done)` (first request).
                        if let Some(tx) = tx.lock().unwrap().take() {
                            let _ = tx.send(r.clone());
                        }
                        (vec![("Content-Type".into(), "text/plain; version=0.0.4".into())], b"metric_a 1\nmetric_b 2\n".to_vec())
                    }));
                    // Update target addresses to use the dynamically created server URL.
                    let mut targets = targets;
                    for t in &mut targets {
                        t.0.insert(ADDRESS_LABEL.to_string(), srv.host.clone());
                    }
                    let sp = new_pool(cfg, select_appendable(&Appendable::new(), v2), Options::default());
                    sp.sync_targets_from_groups(&[group(targets)]);

                    let r = rx.recv_timeout(Duration::from_secs(10)).unwrap_or_else(|_| panic!("{name}: timeout after 10 seconds"));
                    assert_eq!(r.header("X-Prometheus-Scrape-Timeout-Seconds"), EXPECTED_TIMEOUT, "{name}");
                    assert_eq!(r.query_get("param"), EXPECTED_PARAM, "{name}");
                    assert_eq!(r.path, EXPECTED_PATH, "{name}");
                    sp.stop();
                });
            }
        });
    });
}

// Regression test for the panic fixed in https://github.com/prometheus/prometheus/pull/15523.
#[test]
fn test_scrape_pool_scrape_after_reload() {
    foreach_appendable(|v2| {
        let h = TestServer::new(Arc::new(|_: &Req| (vec![], vec![0x42, 0x42])));

        let mut static_group = group(vec![ls(&[(ADDRESS_LABEL, &h.url())])]);
        static_group.source = String::new();
        let cfg = ScrapeConfig {
            body_size_limit: yaml::types::Base2Bytes(1),
            job_name: "test".into(),
            scheme: "http".into(),
            scrape_interval: dur(Duration::from_millis(100)),
            scrape_timeout: dur(Duration::from_millis(100)),
            metric_name_validation_scheme: ValidationScheme::Utf8,
            metric_name_escaping_scheme: config::ALLOW_UTF8.into(),
            enable_compression: false,
            service_discovery_configs: discovery::Configs(vec![Box::new(discovery::StaticConfig(vec![static_group]))]),
            ..Default::default()
        };

        let p = new_pool(cfg.clone(), select_appendable(&Appendable::new(), v2), Options::default());
        let mut g = group(vec![ls(&[(ADDRESS_LABEL, h.url().trim_start_matches("http://"))])]);
        g.source = "test".into();
        p.sync_targets_from_groups(&[g]);

        if let Err(e) = p.reload(Arc::new(cfg)) {
            panic!("reload: {e}");
        }

        std::thread::sleep(Duration::from_secs(1));
        p.stop();
    });
}

// Regression test against https://github.com/prometheus/prometheus/issues/16160.
// The first scrape fails with a parsing error, but the second should
// succeed and cause `metric_1=11` to appear in the appender.
#[test]
fn test_scrape_append_with_parse_error() {
    foreach_appendable(|v2| {
        const SCRAPE1: &str = "metric_a 1\n";
        const SCRAPE2: &str = "metric_a 11\n# EOF";

        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| set_appendable(sl, &app_test, v2));
        let now = SystemTime::now();

        let mut app = sl.appender();
        let (_, err) = app.append(SCRAPE1.as_bytes(), "application/openmetrics-text", now);
        assert!(err.is_some());
        assert!(app.rollback().is_none());

        let mut app = sl.appender();
        let (_, err) = app.append(b"", "application/openmetrics-text", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert!(app_test.result_samples().is_empty());

        let mut app = sl.appender();
        let (_, err) = app.append(SCRAPE2.as_bytes(), "application/openmetrics-text", now + Duration::from_secs(15));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let want = vec![sample("metric_a", from_time(now + Duration::from_secs(15)), 11.0)];
        require_equal(&want, &app_test.result_samples());
    });
}

// This test covers a case where there's a target with sample_limit set and some samples
// changes between scrapes.
#[test]
fn test_scrape_loop_append_sample_limit_with_disappearing_series() {
    foreach_appendable(|v2| {
        const SAMPLE_LIMIT: usize = 4;
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            set_appendable(sl, &app_test, v2);
            sl.sample_limit = SAMPLE_LIMIT;
        });
        let stale = f64::from_bits(STALE_NAN);

        let mut now = SystemTime::now();
        let mut app = sl.appender();
        // Start with 3 samples, all accepted.
        let (st, err) = app.append(b"metric_a 1\nmetric_b 1\nmetric_c 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 3); // All on scrape.
        assert_eq!(st.added, 3); // This is series after relabeling.
        assert_eq!(st.series_added, 3); // Newly added to TSDB.
        let t = from_time(now);
        let mut want = vec![sample("metric_a", t, 1.0), sample("metric_b", t, 1.0), sample("metric_c", t, 1.0)];
        require_equal(&want, &app_test.result_samples());

        now += Duration::from_secs(60);
        let mut app = sl.appender();
        // Start exporting 3 more samples, so we're over the limit now.
        let (st, err) =
            app.append(b"metric_a 1\nmetric_b 1\nmetric_c 1\nmetric_d 1\nmetric_e 1\nmetric_f 1\n", "text/plain", now);
        assert!(err.as_ref().is_some_and(|e| e.is(&ERR_SAMPLE_LIMIT)), "{err:?}");
        assert!(app.rollback().is_none());
        assert_eq!(st.total, 6);
        assert_eq!(st.added, 6);
        assert_eq!(st.series_added, 1); // We've added one series before hitting the limit.
        require_equal(&want, &app_test.result_samples());
        {
            let mut s = sl.cache.series();
            sl.cache.iter_done(&mut s, false);
        }

        now += Duration::from_secs(60);
        let mut app = sl.appender();
        // Remove all samples except first 2.
        let (st, err) = app.append(b"metric_a 1\nmetric_b 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 2);
        assert_eq!(st.added, 2);
        assert_eq!(st.series_added, 0);
        // This is where important things happen. We should now see:
        // - Appends for samples from metric_a & metric_b.
        // - Append with stale markers for metric_c - this series was added during first scrape but disappeared during last scrape.
        // - Append with stale marker for metric_d - this series was added during second scrape before we hit the sample_limit.
        // We should NOT see:
        // - Appends with stale markers for metric_e & metric_f - both over the limit during second scrape, and so they never made it into TSDB.
        let t = from_time(now);
        want.extend([
            sample("metric_a", t, 1.0),
            sample("metric_b", t, 1.0),
            sample("metric_c", t, stale),
            sample("metric_d", t, stale),
        ]);
        require_equal(&want, &app_test.result_samples());
    });
}

// This test covers a case where there's a target with sample_limit set and each scrape sees a completely
// different set of samples.
#[test]
fn test_scrape_loop_append_sample_limit_replace_all_samples() {
    foreach_appendable(|v2| {
        const SAMPLE_LIMIT: usize = 4;
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            set_appendable(sl, &app_test, v2);
            sl.sample_limit = SAMPLE_LIMIT;
        });
        let stale = f64::from_bits(STALE_NAN);

        let mut now = SystemTime::now();
        let mut app = sl.appender();
        // Start with 4 samples, all accepted.
        let (st, err) = app.append(b"metric_a 1\nmetric_b 1\nmetric_c 1\nmetric_d 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 4); // All on scrape.
        assert_eq!(st.added, 4); // This is series after relabeling.
        assert_eq!(st.series_added, 4); // Newly added to TSDB.
        let t = from_time(now);
        let mut want = vec![
            sample("metric_a", t, 1.0),
            sample("metric_b", t, 1.0),
            sample("metric_c", t, 1.0),
            sample("metric_d", t, 1.0),
        ];
        require_equal(&want, &app_test.result_samples());

        now += Duration::from_secs(60);
        let mut app = sl.appender();
        // Replace all samples with new time series.
        let (st, err) = app.append(b"metric_e 1\nmetric_f 1\nmetric_g 1\nmetric_h 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(st.total, 4);
        assert_eq!(st.added, 4);
        assert_eq!(st.series_added, 4);
        // We replaced all samples from first scrape with new set of samples.
        // We expected to see:
        // - 4 appends for new samples.
        // - 4 appends with staleness markers for old samples.
        let t = from_time(now);
        want.extend([
            sample("metric_e", t, 1.0),
            sample("metric_f", t, 1.0),
            sample("metric_g", t, 1.0),
            sample("metric_h", t, 1.0),
            sample("metric_a", t, stale),
            sample("metric_b", t, stale),
            sample("metric_c", t, stale),
            sample("metric_d", t, stale),
        ]);
        require_equal(&want, &app_test.result_samples());
    });
}

/// Go `require.Eventually`.
fn eventually(f: impl Fn() -> bool, wait: Duration, tick: Duration, msg: &str) {
    let end = Instant::now() + wait;
    loop {
        if f() {
            return;
        }
        if Instant::now() >= end {
            panic!("{msg}");
        }
        std::thread::sleep(tick);
    }
}

#[test]
fn test_scrape_loop_disable_staleness_marker_injection() {
    foreach_appendable(|v2| {
        let loop_done = Arc::new(AtomicBool::new(false));
        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| set_appendable(sl, &app_test, v2));
        scraper.set_scrape_func(|ctx, w| {
            w.extend_from_slice(b"metric_a 42\n");
            match ctx.err() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        });

        // Start the scrape loop.
        let sl = Arc::new(sl);
        let (s2, d2) = (sl.clone(), loop_done.clone());
        std::thread::spawn(move || {
            s2.run(None);
            d2.store(true, Ordering::SeqCst);
        });

        // Wait for some samples to be appended.
        eventually(
            || app_test.result_samples().len() > 2,
            Duration::from_secs(5),
            Duration::from_millis(100),
            "Scrape loop didn't append any samples.",
        );

        // Disable end of run staleness markers and stop the loop.
        sl.disable_end_of_run_staleness_markers();
        sl.stop();
        eventually(|| loop_done.load(Ordering::SeqCst), Duration::from_secs(5), Duration::from_millis(100), "Scrape loop didn't stop.");

        // No stale markers should be appended, since they were disabled.
        for s in app_test.result_samples() {
            assert!(!is_stale_nan(s.v), "Got stale NaN samples while end of run staleness is disabled: {:x}", s.v.to_bits());
        }
    });
}

/// Go `newTestTarget`.
fn new_test_target(target_url: &str, lbls: Labels) -> Arc<Target> {
    let mut lb = model::labels::Builder::new(lbls);
    lb.set(SCHEME_LABEL, "http");
    lb.set(ADDRESS_LABEL, target_url.trim_start_matches("http://"));
    lb.set(METRICS_PATH_LABEL, "/metrics");
    Arc::new(Target::new(lb.labels(), Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new()))
}

#[test]
fn test_new_scrape_loop_json_logger_target() {
    const TARGET_ADDRESS: &str = "test.invalid:80";
    const WANT_TARGET: &str = "http://test.invalid:80/metrics";

    // adapted: Go formats the record with the promslog JSON handler and decodes the "target" field; the Rust
    // logger hands records to a sink, which here writes the same JSON object with the json crate.
    let output: Arc<Mutex<Vec<String>>> = Arc::default();
    let out = output.clone();
    let mut sp = new_test_scrape_pool(None, None, None);
    sp.logger = discovery::Logger::new(Arc::new(move |level, msg, attrs| {
        let mut o = json::Json::obj();
        o.set("level", json::Json::from(level.as_str()));
        o.set("msg", json::Json::from(msg));
        for (k, v) in attrs {
            o.set(k, json::Json::from(v.as_str()));
        }
        out.lock().unwrap().push(o.to_string());
    }));
    let target = new_test_target(TARGET_ADDRESS, Labels::empty());

    let cfg = sp.config();
    let sl = build_scrape_loop(
        &sp,
        &cfg,
        ScrapeLoopOptions {
            target,
            scraper: Arc::new(TestScraper::default()),
            cache: scrape::ScrapeCache::new(sp.metrics.clone()),
            interval: Duration::ZERO,
            timeout: Duration::ZERO,
        },
    );

    sl.l.warn("test message", &[]);
    sl.ctx.cancel(); // Go t.Cleanup(sl.cancel).

    let lines = output.lock().unwrap().clone();
    assert_eq!(lines.len(), 1);
    let entry = json::Json::parse(&lines[0]).expect("json");
    assert_eq!(entry.get("target").and_then(|v| v.as_str()), Some(WANT_TARGET));
}

#[test]
fn test_drops_series_from_metric_relabeling() {
    foreach_appendable(|v2| {
        let target = Arc::new(Target::new(Labels::empty(), Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new()));
        let relabel_config = vec![
            relabel::Config {
                source_labels: Some(vec!["__name__".into()]),
                regex: relabel::Regexp::must_new("test_metric.*$"),
                action: relabel::Action::Keep,
                name_validation_scheme: ValidationScheme::Utf8,
                ..Default::default()
            },
            relabel::Config {
                source_labels: Some(vec!["__name__".into()]),
                regex: relabel::Regexp::must_new("test_metric_2$"),
                action: relabel::Action::Drop,
                name_validation_scheme: ValidationScheme::Utf8,
                ..Default::default()
            },
        ];
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            set_appendable(sl, &app_test, v2);
            sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &target, true, &relabel_config));
        });
        let t0 = SystemTime::UNIX_EPOCH; // adapted: Go time.Time{}; only the counts are checked.

        let mut app = sl.appender();
        let (st, err) = app.append(b"test_metric_1 1\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert_eq!((st.total, st.added, st.series_added), (1, 1, 1));

        let (st, err) = app.append(b"test_metric_2 1\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert_eq!((st.total, st.added, st.series_added), (1, 0, 0));

        let (st, err) = app.append(b"unwanted_metric 1\n", "text/plain", t0);
        assert!(err.is_none(), "{err:?}");
        assert_eq!((st.total, st.added, st.series_added), (1, 0, 0));

        assert!(app.commit().is_none());
    });
}

// Regression test for concurrent access to scrapeFailureLogger. Both must use targetMtx for synchronization.
#[test]
fn test_scrape_pool_set_scrape_failure_logger_race() {
    let app = Appendable::new();
    let cfg = ScrapeConfig {
        job_name: "test".into(),
        scrape_interval: dur(Duration::from_millis(100)),
        scrape_timeout: dur(Duration::from_millis(50)),
        metric_name_validation_scheme: ValidationScheme::Utf8,
        metric_name_escaping_scheme: config::ALLOW_UTF8.into(),
        ..Default::default()
    };
    let sp = new_pool(cfg, (Some(Arc::new(app)), None), Options::default());

    // Create a target group with a target.
    let tg = group(vec![ls(&[(ADDRESS_LABEL, "127.0.0.1:9090")])]);

    std::thread::scope(|s| {
        s.spawn(|| {
            for _ in 0..100 {
                sp.set_scrape_failure_logger(Some(Arc::new(NoopFailureLogger)));
                sp.set_scrape_failure_logger(None);
            }
        });
        s.spawn(|| {
            for _ in 0..100 {
                sp.sync_targets_from_groups(std::slice::from_ref(&tg));
            }
        });
    });
    sp.stop();
}

/// Go `synctestFQDN`.
const SYNCTEST_FQDN: &str = "synctest.example.com";

#[test]
fn test_scrape_offset_distribution() {
    let interval = Duration::from_secs(5);

    // adapted: Go synctest; a virtual-clock runtime runs the scrape loops, and time only moves in rt.sleep.
    let rt = Runtime::new_virtual(SystemTime::UNIX_EPOCH + Duration::from_secs(946_684_800));
    let start_time = rt.now();

    let scrape_times: Arc<Mutex<HashMap<String, Vec<Duration>>>> = Arc::default();
    let (st, rt2) = (scrape_times.clone(), rt.clone());
    let srv = TestServer::new(Arc::new(move |r: &Req| {
        st.lock().unwrap().entry(r.path.clone()).or_default().push(rt2.now() - start_time);
        (vec![("Content-Type".into(), "text/plain; version=0.0.4".into())], b"expected_metric 1\n".to_vec())
    }));

    // adapted: Go dials a net.Pipe into the fake server for every connection; here every dial goes to the local
    // test server whatever the target host is.
    let host = srv.host.clone();
    let dial: net::transport::DialFunc = Arc::new(move |_, _, _| TcpStream::connect(&host));
    let app = Appendable::new();
    let opts = Options {
        http_client_options: net::HttpClientOptions::default().with_dial(dial),
        // setupSynctestManager is unusable here: it also sets skipJitterOffsetting,
        // which zeroes the offsets asserted below.
        fqdn: SYNCTEST_FQDN.into(),
        runtime: Some(rt.clone()),
        ..Default::default()
    };
    // Go sets scrapeManager.offsetSeed = 1 here; ApplyConfig below replaces it with the FQDN seed.
    let scrape_manager = Manager::new(Some(opts), Some(discovery::Logger::nop()), None, Some(Arc::new(app)), None, None)
        .expect("new manager");

    let mut targets = Vec::new();
    for i in 0..5 {
        targets.push(ls(&[
            (SCHEME_LABEL, "http"),
            (ADDRESS_LABEL, &format!("target-{i}.local")),
            (METRICS_PATH_LABEL, &format!("/metrics/{i}")),
        ]));
    }
    let mut tsets = BTreeMap::new();
    tsets.insert("test".to_string(), vec![Arc::new(group(targets))]);
    scrape_manager.update_tsets(tsets);

    // adapted: Go marshals a config.Config struct to YAML and loads it back; this is the same YAML.
    let cfg_text = "global:\n  scrape_interval: 5s\n  scrape_timeout: 5s\n  scrape_protocols:\n  - PrometheusProto\nscrape_configs:\n- job_name: test\n";
    let cfg = config::load(cfg_text, &discovery::Logger::nop()).expect("load config");
    scrape_manager.apply_config(&cfg).expect("apply config");

    scrape_manager.reload();

    let num_scrapes = 4;
    rt.sleep(interval * num_scrapes + Duration::from_secs(1));
    rt.wait_idle();

    scrape_manager.stop();

    let scrape_times = scrape_times.lock().unwrap();
    for i in 0..num_scrapes as usize {
        let unique: HashSet<Duration> = scrape_times.values().filter_map(|times| times.get(i).copied()).collect();
        assert!(
            unique.len() > 2,
            "Expected targets to be scraped at staggered offsets rather than simultaneously at scrape index {i}: {scrape_times:?}"
        );
    }
}
