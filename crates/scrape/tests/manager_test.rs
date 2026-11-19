//! Port of upstream/scrape/manager_test.go (and the manager helpers of helpers_test.go).
//!
//! Go `synctest` tests run on a virtual-clock runtime (`Runtime::new_virtual` + `Options.runtime`):
//! `time.Sleep` is `rt.sleep`, `synctest.Wait` is `rt.wait_idle`.
//! adapted: Go wires the scraper to an in-memory pipe listener through a custom dialer. Here the custom dialer
//! (`HttpClientOptions.dial`, Go `WithDialContextFunc`) connects every dial to a real HTTP server on
//! 127.0.0.1:0, so the target address (`test.local`) is never resolved.

use std::collections::{BTreeMap, HashMap};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::{Config, ScrapeConfig};
use discovery::targetgroup::Group;
use discovery::{CancelToken, Configs, Logger, ManagerOptions, Registry, SdMetrics, StaticConfig};
use model::histogram::Histogram;
use model::labels::{Builder, Labels, ValidationScheme, from_map};
use model::labelset::LabelSet;
use model::relabel::{self, Action, Regexp};
use scrape::manager::TargetSets;
use scrape::pool::{Loop, ScrapeLoopOptions};
use scrape::runtime::Runtime;
use scrape::target::{
    ADDRESS_LABEL, ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL, CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, INSTANCE_LABEL, JOB_LABEL,
    METRICS_PATH_LABEL, SCHEME_LABEL, SCRAPE_INTERVAL_LABEL, SCRAPE_NATIVE_HISTOGRAMS_LABEL, SCRAPE_TIMEOUT_LABEL,
    populate_discovered_labels,
};
use scrape::testing::dto::proto_marshal_delimited;
use scrape::testing::{TestLoop, new_test_scrape_pool};
use scrape::{Manager, Options, Target, populate_labels};
use util::teststorage::{Appendable, Sample};

// ---------------- helpers ----------------

fn secs(s: u64) -> yaml::Duration {
    yaml::Duration((s * 1_000_000_000) as i64)
}

/// Go `loadConfiguration`.
fn load_configuration(c: &str) -> Config {
    config::load(c, &Logger::nop()).unwrap_or_else(|e| panic!("load config: {e}"))
}

/// Go `noopLoop`.
fn noop_loop() -> Arc<dyn Loop> {
    Arc::new(TestLoop {
        start_func: Mutex::new(Some(Box::new(|_, _, _| {}))),
        stop_func: Mutex::new(Some(Box::new(|| {}))),
        ..Default::default()
    })
}

/// Go `NewManager(opts, nil, nil, nil, teststorage.NewAppendable(), reg)`.
fn new_manager_v2(opts: Options, reg: Arc<Registry>) -> Result<Arc<Manager>, String> {
    Manager::new(Some(opts), None, None, None, Some(Arc::new(Appendable::new())), Some(reg))
}

fn empty_target() -> Arc<Target> {
    Arc::new(Target::new(Labels::default(), Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new()))
}

/// Go `findSamplesForMetric`.
fn find_samples_for_metric(floats: &[Sample], metric_name: &str) -> Vec<Sample> {
    floats.iter().filter(|f| f.l.get("__name__") == metric_name).cloned().collect()
}

/// Go `require.Eventually`.
fn eventually(mut f: impl FnMut() -> bool, wait: Duration, tick: Duration, msg: &str) {
    let end = Instant::now() + wait;
    loop {
        thread::sleep(tick);
        if f() {
            return;
        }
        if Instant::now() >= end {
            panic!("condition never satisfied: {msg}");
        }
    }
}

struct FnService<F: Fn(&http::Request) -> http::Response + Send + Sync>(F);

impl<F: Fn(&http::Request) -> http::Response + Send + Sync> http::server::Service for FnService<F> {
    fn handle(&self, req: http::Request) -> http::Response {
        (self.0)(&req)
    }
}

/// An HTTP server on 127.0.0.1:0 (Go `httptest.NewServer`); returns "127.0.0.1:port".
/// adapted: the server thread is not closed at the end of the test (the crates/http server runs forever).
fn start_server(f: impl Fn(&http::Request) -> http::Response + Send + Sync + 'static) -> String {
    let srv = http::server::Server::bind("127.0.0.1:0", http::server::ServerConfig::default()).expect("bind");
    let port = srv.local_port();
    thread::spawn(move || srv.run(Arc::new(FnService(f))));
    format!("127.0.0.1:{port}")
}

/// Go `setupTestServer`: serves `to_write` once, then answers 500.
fn setup_test_server(typ: &'static str, to_write: Vec<u8>) -> String {
    let once = AtomicBool::new(false);
    start_server(move |_| {
        if !once.swap(true, Ordering::SeqCst) {
            return http::Response::text(200, typ, to_write.clone());
        }
        http::Response::new(500)
    })
}

/// The scrape and discovery managers of a test; stops both when dropped (Go `defer scrapeManager.Stop()` and
/// the cancelled test context).
struct Managers {
    dm: discovery::Manager,
    sm: Arc<Manager>,
    ctx: CancelToken,
}

impl Drop for Managers {
    fn drop(&mut self) {
        self.sm.stop();
        self.ctx.cancel();
    }
}

/// Go `runManagers`.
fn run_managers(
    opts: Option<Options>,
    app: Option<Arc<dyn storage::Appendable>>,
    app_v2: Option<Arc<dyn storage::AppendableV2>>,
) -> Managers {
    let mut opts = opts.unwrap_or_default();
    opts.discovery_reload_interval = Duration::from_millis(100);
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let mechanism_metrics = discovery::register_sd_metrics(&reg, refresh_metrics.clone()).expect("register SD metrics");
    let sd_metrics = SdMetrics { mechanism_metrics, refresh_manager: refresh_metrics };
    let ctx = CancelToken::new();
    let dm = discovery::Manager::new(
        &ctx,
        Logger::nop(),
        &reg,
        &sd_metrics,
        ManagerOptions { updatert: Duration::from_millis(100), ..Default::default() },
    )
    .expect("discovery manager");
    let sm = Manager::new(Some(opts), None, None, app, app_v2, Some(Registry::new())).expect("scrape manager");
    let d2 = dm.clone();
    thread::spawn(move || {
        let _ = d2.run();
    });
    let s2 = sm.clone();
    let ch = dm.sync_ch();
    thread::spawn(move || {
        let _ = s2.run(ch);
    });
    Managers { dm, sm, ctx }
}

/// Go `applyConfig`.
fn apply_config(cfg_text: &str, m: &Managers) {
    let cfg = load_configuration(cfg_text);
    m.sm.apply_config(&cfg).expect("apply scrape config");
    let mut c: BTreeMap<String, Configs> = BTreeMap::new();
    for v in cfg.get_scrape_configs().expect("scrape configs") {
        c.insert(v.job_name.clone(), v.service_discovery_configs.clone());
    }
    m.dm.apply_config(&c);
}

/// Go `writeIntoFile` (os.CreateTemp with a `*targets.json` pattern).
fn write_into_file(content: &str) -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let p = format!(
        "{}/manager_test-{}-{}-targets.json",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    );
    std::fs::write(&p, content).expect("write file");
    p
}

/// Go `requireTargets`.
fn require_targets(m: &Arc<Manager>, job_name: &str, wait_to_appear: bool, expected_targets: Option<Vec<String>>) {
    eventually(
        || {
            let active = m.targets_active();
            let Some(targets) = active.get(job_name) else {
                if wait_to_appear {
                    return false;
                }
                panic!("job {job_name} shouldn't be dropped");
            };
            let Some(expected) = &expected_targets else {
                // Go: `targets == nil` (an empty slice of active targets is nil).
                return targets.is_empty();
            };
            if targets.len() != expected.len() {
                return false;
            }
            let mut s: Vec<String> = targets.iter().map(|t| t.to_string()).collect();
            let mut e = expected.clone();
            s.sort();
            e.sort();
            s == e
        },
        Duration::from_secs(1),
        Duration::from_millis(100),
        "targets",
    );
}

/// Go `writeOpenMetricsFloat`.
fn om_float(f: f64) -> String {
    if f == 1.0 {
        return "1.0".into();
    }
    if f == 0.0 {
        return "0.0".into();
    }
    if f == -1.0 {
        return "-1.0".into();
    }
    let s = yaml::format_float_g(f);
    if f.is_finite() && !s.contains(['e', '.']) { format!("{s}.0") } else { s }
}

fn unix_nanos(t: SystemTime) -> i128 {
    t.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i128
}

/// Go `timestamp.FromTime`.
fn from_time(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// A prototext `google.protobuf.Timestamp` (Go `timestamppb.New`).
fn proto_ts(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap();
    format!("< seconds: {} nanos: {} >", d.as_secs(), d.subsec_nanos())
}

/// Go `prepareTestEncodedCounter`. `st` None is Go's zero time.
fn prepare_test_encoded_counter(format: &str, m_name: &str, v: f64, ts: SystemTime, st: Option<SystemTime>) -> Vec<u8> {
    match format {
        config::PROMETHEUS_PROTO => {
            let created = st.map(|st| format!(" created_timestamp: {}", proto_ts(st))).unwrap_or_default();
            proto_marshal_delimited(&format!(
                "name: \"{m_name}\" type: COUNTER metric: < timestamp_ms: {} counter: < value: {v:?}{created} > >",
                from_time(ts)
            ))
        }
        config::OPEN_METRICS_TEXT_1_0_0 => {
            // adapted: Go encodes the family with expfmt (OpenMetrics, WithCreatedLines); this writes the same
            // bytes as MetricFamilyToOpenMetrics for a counter with a `_total` name and no labels.
            let compliant = m_name.strip_suffix("_total").expect("_total suffix");
            let mut s = format!("# TYPE {compliant} counter\n");
            s.push_str(&format!("{m_name} {} {}\n", om_float(v), om_float(from_time(ts) as f64 / 1000.0)));
            if let Some(st) = st {
                s.push_str(&format!("{compliant}_created {}\n", om_float(unix_nanos(st) as f64 / 1e9)));
            }
            s.push_str("# EOF");
            s.into_bytes()
        }
        f => panic!("not implemented format: {f}"),
    }
}

/// Go `generateTestHistogram`: tsdbutil.GenerateTestHistogram as a prototext `dto.Histogram` body (without the
/// created timestamp); also returns the sample sum.
fn generate_test_histogram(i: i64) -> (String, f64) {
    let h = storage::tsdbutil::generate_test_histogram(i);
    let mut s = format!(
        "sample_count: {} sample_sum: {:?} schema: {} zero_threshold: {:?} zero_count: {}",
        h.count, h.sum, h.schema, h.zero_threshold, h.zero_count
    );
    for sp in &h.positive_spans {
        s.push_str(&format!(" positive_span: < offset: {} length: {} >", sp.offset, sp.length));
    }
    for d in &h.positive_buckets {
        s.push_str(&format!(" positive_delta: {d}"));
    }
    for sp in &h.negative_spans {
        s.push_str(&format!(" negative_span: < offset: {} length: {} >", sp.offset, sp.length));
    }
    for d in &h.negative_buckets {
        s.push_str(&format!(" negative_delta: {d}"));
    }
    (s, h.sum)
}

// ---------------- synctest helpers (helpers_test.go) ----------------

/// Go `synctestFQDN`.
const SYNCTEST_FQDN: &str = "synctest.example.com";

/// Go synctest bubbles start at 2000-01-01 00:00:00 UTC.
fn synctest_wall0() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(946_684_800)
}

/// Go `startFakeHTTPServer`.
fn start_fake_http_server() -> String {
    start_server(|_| http::Response::text(200, "text/plain; version=0.0.4", "expected_metric 1\n"))
}

/// Go `setupSynctestManager`: the manager runs on a virtual-clock runtime and dials the fake server.
fn setup_synctest_manager(opts: Option<Options>) -> (Arc<Manager>, Appendable, Arc<Runtime>) {
    let app = Appendable::new();
    let addr = start_fake_http_server();
    let rt = Runtime::new_virtual(synctest_wall0());
    let mut opts = opts.unwrap_or_default();
    opts.skip_jitter_offsetting = true;
    opts.fqdn = SYNCTEST_FQDN.to_string();
    opts.runtime = Some(rt.clone());
    opts.http_client_options.dial = Some(Arc::new(move |_net: &str, _addr: &str, _deadline: Option<Instant>| TcpStream::connect(&addr)));
    let m = Manager::new(Some(opts), None, None, None, Some(Arc::new(app.clone())), Some(Registry::new())).expect("NewManager");
    (m, app, rt)
}

/// The config of Go `applyDefaultSynctestConfig` and `TestManagerReloader`: built as a struct, marshalled to
/// YAML and loaded again (as Go does).
fn synctest_config(interval: Duration) -> Config {
    let iv = yaml::Duration(interval.as_nanos() as i64);
    let cfg = Config {
        global_config: config::GlobalConfig {
            scrape_interval: iv,
            scrape_timeout: iv,
            scrape_protocols: Some(vec![
                config::ScrapeProtocol::new(config::PROMETHEUS_PROTO),
                config::ScrapeProtocol::new(config::OPEN_METRICS_TEXT_1_0_0),
            ]),
            ..Default::default()
        },
        scrape_configs: vec![ScrapeConfig { job_name: "test".to_string(), ..Default::default() }],
        ..Default::default()
    };
    let text = yaml::marshal(&cfg).expect("marshal config");
    load_configuration(&text)
}

fn test_target(addr: &str) -> LabelSet {
    LabelSet::from_pairs(&[(SCHEME_LABEL, "http"), (ADDRESS_LABEL, addr)])
}

/// Go `applyDefaultSynctestConfig`.
fn apply_default_synctest_config(m: &Arc<Manager>, interval: Duration) {
    m.apply_config(&synctest_config(interval)).expect("apply config");
    let mut ts = TargetSets::new();
    ts.insert("test".to_string(), vec![Arc::new(Group { targets: vec![test_target("test.local")], ..Default::default() })]);
    m.update_tsets(ts);
    m.reload();
}

// ---------------- tests ----------------

#[test]
fn test_populate_labels() {
    struct Case {
        r#in: LabelSet,
        cfg: ScrapeConfig,
        res: Labels,
        res_orig: Labels,
        err: &'static str,
    }
    let base_cfg = |scheme: &str| ScrapeConfig {
        scheme: scheme.to_string(),
        metrics_path: "/metrics".to_string(),
        job_name: "job".to_string(),
        scrape_interval: secs(1),
        scrape_timeout: secs(1),
        ..Default::default()
    };
    let ls = |p: &[(&str, &str)]| LabelSet::from_pairs(p);
    let lm = |p: &[(&str, &str)]| from_map(p.iter().copied());
    let replace_rule = |regex: &str, source: &str, replacement: &str, target: &str| relabel::Config {
        action: Action::Replace,
        regex: Regexp::must_new(regex),
        source_labels: Some(vec![source.to_string()]),
        replacement: replacement.to_string(),
        target_label: target.to_string(),
        name_validation_scheme: ValidationScheme::Utf8,
        separator: String::new(),
        modulus: 0,
    };
    const NHCB: &str = CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL;
    const ALWAYS: &str = ALWAYS_SCRAPE_CLASSIC_HISTOGRAMS_LABEL;
    const NATIVE: &str = SCRAPE_NATIVE_HISTOGRAMS_LABEL;
    let empty = Labels::default;

    let mut cases = vec![
        // Regular population of scrape config options.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), ("custom", "value")]),
            cfg: base_cfg("https"),
            res: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (INSTANCE_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
                ("custom", "value"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                ("custom", "value"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        },
        // Pre-define/overwrite scrape config labels.
        // Leave out port and expect it to be defaulted to scheme.
        Case {
            r#in: ls(&[
                (ADDRESS_LABEL, "1.2.3.4"),
                (SCHEME_LABEL, "http"),
                (METRICS_PATH_LABEL, "/custom"),
                (JOB_LABEL, "custom-job"),
                (SCRAPE_INTERVAL_LABEL, "2s"),
                (SCRAPE_TIMEOUT_LABEL, "2s"),
            ]),
            cfg: base_cfg("https"),
            res: lm(&[
                (ADDRESS_LABEL, "1.2.3.4"),
                (INSTANCE_LABEL, "1.2.3.4"),
                (SCHEME_LABEL, "http"),
                (METRICS_PATH_LABEL, "/custom"),
                (JOB_LABEL, "custom-job"),
                (SCRAPE_INTERVAL_LABEL, "2s"),
                (SCRAPE_TIMEOUT_LABEL, "2s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, "1.2.3.4"),
                (SCHEME_LABEL, "http"),
                (METRICS_PATH_LABEL, "/custom"),
                (JOB_LABEL, "custom-job"),
                (SCRAPE_INTERVAL_LABEL, "2s"),
                (SCRAPE_TIMEOUT_LABEL, "2s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        },
        // Provide instance label. HTTPS port default for IPv6.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "[::1]"), (INSTANCE_LABEL, "custom-instance")]),
            cfg: base_cfg("https"),
            res: lm(&[
                (ADDRESS_LABEL, "[::1]"),
                (INSTANCE_LABEL, "custom-instance"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, "[::1]"),
                (INSTANCE_LABEL, "custom-instance"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        },
        // Address label missing.
        Case { r#in: ls(&[("custom", "value")]), cfg: base_cfg("https"), res: empty(), res_orig: empty(), err: "no address" },
    ];
    // Address label missing, but added in relabelling (the Go table has this case twice).
    for _ in 0..2 {
        cases.push(Case {
            r#in: ls(&[("custom", "host:1234")]),
            cfg: ScrapeConfig { relabel_configs: vec![replace_rule("(.*)", "custom", "${1}", ADDRESS_LABEL)], ..base_cfg("https") },
            res: lm(&[
                (ADDRESS_LABEL, "host:1234"),
                (INSTANCE_LABEL, "host:1234"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
                ("custom", "host:1234"),
            ]),
            res_orig: lm(&[
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
                ("custom", "host:1234"),
            ]),
            err: "",
        });
    }
    // Invalid UTF-8 in label: Go `"custom": "\xbd"` fails with
    // `invalid label value for "custom": "\xbd"`.
    // adapted: a model::labelset::LabelSet value is a Rust String, which cannot hold the byte 0xbd; the closest
    // check is that such a value cannot be made at all.
    assert!(String::from_utf8(b"\xbd".to_vec()).is_err());
    cases.extend([
        // Invalid duration in interval label.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (SCRAPE_INTERVAL_LABEL, "2notseconds")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "error parsing scrape interval: unknown unit \"notseconds\" in duration \"2notseconds\"",
        },
        // Invalid duration in timeout label.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (SCRAPE_TIMEOUT_LABEL, "2notseconds")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "error parsing scrape timeout: unknown unit \"notseconds\" in duration \"2notseconds\"",
        },
        // 0 interval in timeout label.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (SCRAPE_INTERVAL_LABEL, "0s")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "scrape interval cannot be 0",
        },
        // 0 duration in timeout label.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (SCRAPE_TIMEOUT_LABEL, "0s")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "scrape timeout cannot be 0",
        },
        // Timeout less than interval.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (SCRAPE_INTERVAL_LABEL, "1s"), (SCRAPE_TIMEOUT_LABEL, "2s")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "scrape timeout cannot be greater than scrape interval (\"2s\" > \"1s\")",
        },
    ]);
    // Don't attach default port; verify that the default port is not removed (http, https).
    for (addr, scheme) in [("1.2.3.4", "https"), ("1.2.3.4:80", "http"), ("1.2.3.4:443", "https")] {
        cases.push(Case {
            r#in: ls(&[(ADDRESS_LABEL, addr)]),
            cfg: base_cfg(scheme),
            res: lm(&[
                (ADDRESS_LABEL, addr),
                (INSTANCE_LABEL, addr),
                (SCHEME_LABEL, scheme),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, addr),
                (SCHEME_LABEL, scheme),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        });
    }
    cases.extend([
        // convert_classic_histograms_to_nhcb seeded from the config default.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000")]),
            cfg: ScrapeConfig { convert_classic_histograms_to_nhcb: Some(true), ..base_cfg("https") },
            res: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (INSTANCE_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "true"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "true"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        },
        // convert_classic_histograms_to_nhcb overridden via relabeling.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000")]),
            cfg: ScrapeConfig { relabel_configs: vec![replace_rule("false", NHCB, "true", NHCB)], ..base_cfg("https") },
            res: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (INSTANCE_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "true"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            res_orig: lm(&[
                (ADDRESS_LABEL, "1.2.3.4:1000"),
                (SCHEME_LABEL, "https"),
                (METRICS_PATH_LABEL, "/metrics"),
                (JOB_LABEL, "job"),
                (SCRAPE_INTERVAL_LABEL, "1s"),
                (SCRAPE_TIMEOUT_LABEL, "1s"),
                (NHCB, "false"),
                (ALWAYS, "false"),
                (NATIVE, "false"),
            ]),
            err: "",
        },
        // Invalid boolean in convert_classic_histograms_to_nhcb label.
        Case {
            r#in: ls(&[(ADDRESS_LABEL, "1.2.3.4:1000"), (NHCB, "maybe")]),
            cfg: base_cfg("https"),
            res: empty(),
            res_orig: empty(),
            err: "error parsing convert classic histograms to nhcb: strconv.ParseBool: parsing \"maybe\": invalid syntax",
        },
    ]);

    for mut c in cases {
        let r#in = c.r#in.clone();
        let mut lb = Builder::new(Labels::default());
        c.cfg.metric_name_validation_scheme = ValidationScheme::Utf8;
        for rc in c.cfg.relabel_configs.iter_mut() {
            rc.name_validation_scheme = ValidationScheme::Utf8;
        }
        let res = populate_labels(&mut lb, &c.cfg, &c.r#in, &LabelSet::new());
        if !c.err.is_empty() {
            assert_eq!(res.expect_err("expected an error").to_string(), c.err);
        } else {
            let res = res.unwrap_or_else(|e| panic!("unexpected error: {e}"));
            assert_eq!(c.res, res);
            populate_discovered_labels(&mut lb, &c.cfg, &c.r#in, &LabelSet::new());
            assert_eq!(c.res_orig, lb.labels());
        }
        // Check this wasn't altered by PopulateLabels().
        assert_eq!(c.r#in, r#in);
    }
}

#[test]
fn test_manager_apply_config() {
    // Valid initial configuration.
    let cfg_text1 = r#"
scrape_configs:
 - job_name: job1
   static_configs:
   - targets: ["foo:9090"]
"#;
    // Invalid configuration.
    let cfg_text2 = r#"
scrape_configs:
 - job_name: job1
   scheme: https
   static_configs:
   - targets: ["foo:9090"]
   tls_config:
     ca_file: /not/existing/ca/file
"#;
    // Valid configuration.
    let cfg_text3 = r#"
scrape_configs:
 - job_name: job1
   scheme: https
   static_configs:
   - targets: ["foo:9090"]
"#;
    let cfg1 = load_configuration(cfg_text1);
    let cfg2 = load_configuration(cfg_text2);
    let cfg3 = load_configuration(cfg_text3);
    let (tx, rx) = sync_channel::<()>(1);
    let test_registry = Registry::new();

    let scrape_manager = new_manager_v2(Options::default(), test_registry).expect("NewManager");
    let new_loop = move |_: ScrapeLoopOptions| -> Arc<dyn Loop> {
        tx.send(()).unwrap();
        noop_loop()
    };
    let mut sp = new_test_scrape_pool(None, None, Some(Box::new(new_loop)));
    {
        let mut ts = sp.target_mtx.lock().unwrap();
        ts.active_targets.insert(1, empty_target());
        ts.loops.insert(1, noop_loop());
    }
    sp.set_config(Arc::new(cfg1.scrape_configs[0].clone()));
    sp.metrics = scrape_manager.metrics.clone();
    scrape_manager.mtx_scrape.lock().unwrap().scrape_pools = HashMap::from([("job1".to_string(), Arc::new(sp))]);

    // Apply the initial configuration.
    scrape_manager.apply_config(&cfg1).expect("Unable to apply configuration.");
    assert!(rx.try_recv().is_err(), "Reload happened.");

    // Apply a configuration for which the reload fails.
    assert!(scrape_manager.apply_config(&cfg2).is_err(), "Expecting error but got none.");
    assert!(rx.try_recv().is_err(), "Reload happened.");

    // Apply a configuration for which the reload succeeds.
    scrape_manager.apply_config(&cfg3).expect("Unable to apply configuration.");
    assert!(rx.try_recv().is_ok(), "Reload didn't happen.");

    // Re-applying the same configuration shouldn't trigger a reload.
    scrape_manager.apply_config(&cfg3).expect("Unable to apply configuration.");
    assert!(rx.try_recv().is_err(), "Reload happened.");
}

#[test]
fn test_manager_targets_updates() {
    let m = new_manager_v2(Options::default(), Registry::new()).expect("NewManager");

    let (tx, rx) = sync_channel::<TargetSets>(0);
    let m2 = m.clone();
    thread::spawn(move || m2.run(rx));
    struct StopOnDrop(Arc<Manager>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.stop();
        }
    }
    let _stop = StopOnDrop(m.clone());

    let mut tg_sent = TargetSets::new();
    for x in 0..10 {
        tg_sent.insert(x.to_string(), vec![Arc::new(Group { source: x.to_string(), ..Default::default() })]);
        // Go: an unbuffered send that must not block for more than 10ms.
        let deadline = Instant::now() + Duration::from_millis(10);
        let mut v = tg_sent.clone();
        loop {
            match tx.try_send(v) {
                Ok(()) => break,
                Err(TrySendError::Full(b)) => {
                    assert!(Instant::now() < deadline, "Scrape manager's channel remained blocked after the set threshold.");
                    v = b;
                    thread::yield_now();
                }
                Err(TrySendError::Disconnected(_)) => panic!("Scrape manager's channel is closed."),
            }
        }
    }

    // adapted: Go reads m.targetSets right after the last send (the receiver updated it before taking the
    // next value); here the last value is taken by the receiver before it is applied, so wait for it shortly.
    eventually(|| m.mtx_scrape.lock().unwrap().target_sets == tg_sent, Duration::from_secs(1), Duration::from_millis(1), "updates");
    let tset_actual = m.mtx_scrape.lock().unwrap().target_sets.clone();

    // Make sure all updates have been received.
    assert_eq!(tg_sent, tset_actual);

    assert!(m.take_trigger_reload(), "No scrape loops reload was triggered after targets update.");
}

#[test]
fn test_manager_run_returns_when_target_sets_closed() {
    let m = new_manager_v2(Options::default(), Registry::new()).expect("NewManager");
    let (tx, rx) = sync_channel::<TargetSets>(0);
    let (run_tx, run_rx) = sync_channel(1);
    let m2 = m.clone();
    thread::spawn(move || {
        let _ = run_tx.send(m2.run(rx));
    });
    drop(tx);
    match run_rx.recv_timeout(Duration::from_secs(1)) {
        Ok(r) => assert!(r.is_ok(), "{r:?}"),
        Err(_) => panic!("scrape manager did not stop after target sets channel was closed"),
    }
    m.stop();
}

#[test]
fn test_set_offset_seed() {
    let get_config = |prometheus: &str| -> Config {
        let cfg_text = format!(
            "
global:
 external_labels:
   prometheus: '{prometheus}'
"
        );
        yaml::unmarshal_strict::<Config>(&cfg_text).expect("Unable to load YAML config cfgYaml.")
    };

    let scrape_manager = new_manager_v2(Options::default(), Registry::new()).expect("NewManager");

    // Load the first config.
    let cfg1 = get_config("ha1");
    scrape_manager.set_offset_seed(&cfg1.global_config.external_labels).unwrap();
    let offset_seed1 = scrape_manager.offset_seed();

    assert_ne!(offset_seed1, 0, "Offset seed has to be a hash of uint64.");

    // Load the first config.
    let cfg2 = get_config("ha2");
    scrape_manager.set_offset_seed(&cfg2.global_config.external_labels).unwrap();
    let offset_seed2 = scrape_manager.offset_seed();

    assert_ne!(offset_seed1, offset_seed2, "Offset seed should not be the same on different set of external labels.");

    // The fqdn option replaces the name lookup, so the seed follows it.
    let seed_for = |fqdn: &str| -> u64 {
        let m = new_manager_v2(Options { fqdn: fqdn.to_string(), ..Default::default() }, Registry::new()).expect("NewManager");
        m.set_offset_seed(&cfg1.global_config.external_labels).unwrap();
        m.offset_seed()
    };
    let (seed_a, seed_b) = (seed_for("a.example.com"), seed_for("b.example.com"));
    assert_ne!(seed_a, seed_b, "Offset seed should follow the fqdn option.");
    assert_eq!(seed_a, seed_for("a.example.com"), "Offset seed should be stable for a fixed fqdn.");
}

#[test]
fn test_setup_synctest_manager_replaces_fqdn_lookup() {
    let (scrape_manager, _, _) = setup_synctest_manager(None);
    assert_eq!(SYNCTEST_FQDN, scrape_manager.opts.fqdn, "synctest tests must not reach the resolver from inside a bubble");
}

#[test]
fn test_manager_scrape_pools() {
    let cfg_text1 = r#"
scrape_configs:
- job_name: job1
  static_configs:
  - targets: ["foo:9090"]
- job_name: job2
  static_configs:
  - targets: ["foo:9091", "foo:9092"]
"#;
    let cfg_text2 = r#"
scrape_configs:
- job_name: job1
  static_configs:
  - targets: ["foo:9090", "foo:9094"]
- job_name: job3
  static_configs:
  - targets: ["foo:9093"]
"#;
    let cfg1 = load_configuration(cfg_text1);
    let cfg2 = load_configuration(cfg_text2);

    let reload = |scrape_manager: &Arc<Manager>, cfg: &Config| {
        let mut pools = HashMap::new();
        for sc in &cfg.scrape_configs {
            let mut sp = new_test_scrape_pool(None, None, Some(Box::new(|_| noop_loop())));
            sp.target_mtx.lock().unwrap().loops.insert(1, noop_loop());
            sp.set_config(Arc::new(cfg1.scrape_configs[0].clone()));
            sp.metrics = scrape_manager.metrics.clone();
            for c in &sc.service_discovery_configs.0 {
                let static_config = c.as_any().downcast_ref::<StaticConfig>().expect("static config");
                for group in &static_config.0 {
                    for i in 0..group.targets.len() {
                        sp.target_mtx.lock().unwrap().active_targets.insert(i as u64, empty_target());
                    }
                }
            }
            pools.insert(sc.job_name.clone(), Arc::new(sp));
        }
        scrape_manager.mtx_scrape.lock().unwrap().scrape_pools = pools;
    };

    let scrape_manager = new_manager_v2(Options::default(), Registry::new()).expect("NewManager");

    let sorted = |mut v: Vec<String>| {
        v.sort();
        v
    };
    reload(&scrape_manager, &cfg1);
    assert_eq!(vec!["job1", "job2"], sorted(scrape_manager.scrape_pools()));

    reload(&scrape_manager, &cfg2);
    assert_eq!(vec!["job1", "job3"], sorted(scrape_manager.scrape_pools()));
}

/// Waits up to a minute for some samples, polling every 100ms (Go `runutil.Retry` with a 1 minute context).
fn wait_for_samples(app: &Appendable, what: &str) {
    let end = Instant::now() + Duration::from_secs(60);
    while app.result_samples().is_empty() {
        assert!(Instant::now() < end, "{what}: after 1 minute");
        thread::sleep(Duration::from_millis(100));
    }
}

/// TestManagerSTZeroIngestion tests scrape manager for various ST cases.
/// NOTE(bwplotka): There is no AppenderV2 test for this STZeroIngestion feature as in V2 flow it's
/// moved to AppenderV2 implementation (e.g. storage) and it's tested there.
#[test]
fn test_manager_st_zero_ingestion() {
    // _total suffix is required, otherwise expfmt with OMText will mark metric as "unknown"
    const EXPECTED_METRIC_NAME: &str = "expected_metric_total";
    const EXPECTED_CREATED_METRIC_NAME: &str = "expected_metric_created";
    const EXPECTED_SAMPLE_VALUE: f64 = 17.0;

    for test_format in [config::PROMETHEUS_PROTO, config::OPEN_METRICS_TEXT_1_0_0] {
        for test_with_st in [false, true] {
            for test_st_zero_ingest in [false, true] {
                let name = format!("format={test_format}/withST={test_with_st}/stZeroIngest={test_st_zero_ingest}");
                let sample_ts = SystemTime::now();
                let st_ts = if test_with_st { Some(sample_ts - Duration::from_secs(120)) } else { None };

                // TODO(bwplotka): Add more types than just counter?
                let encoded = prepare_test_encoded_counter(test_format, EXPECTED_METRIC_NAME, EXPECTED_SAMPLE_VALUE, sample_ts, st_ts);

                let app = Appendable::new();
                let m = run_managers(
                    Some(Options {
                        enable_start_timestamp_zero_ingestion: test_st_zero_ingest,
                        parse_st: test_st_zero_ingest,
                        skip_jitter_offsetting: true,
                        ..Default::default()
                    }),
                    Some(Arc::new(app.clone())),
                    None,
                );

                let header = config::ScrapeProtocol::new(test_format).header().unwrap();
                let server = setup_test_server(header, encoded);

                let test_config = format!(
                    "
global:
  # Disable regular scrapes.
  scrape_interval: 9999m
  scrape_timeout: 5s

scrape_configs:
- job_name: test
  honor_timestamps: true
  static_configs:
  - targets: ['{server}']
"
                );
                apply_config(&test_config, &m);

                // Wait for one scrape.
                wait_for_samples(&app, &name);

                // Verify results.
                // Verify what we got vs expectations around ST injection.
                let got = find_samples_for_metric(&app.result_samples(), EXPECTED_METRIC_NAME);
                if test_with_st && test_st_zero_ingest {
                    assert_eq!(got.len(), 2, "{name}");
                    assert_eq!(0.0, got[0].v, "{name}");
                    assert_eq!(from_time(st_ts.unwrap()), got[0].t, "{name}");
                    assert_eq!(EXPECTED_SAMPLE_VALUE, got[1].v, "{name}");
                    assert_eq!(from_time(sample_ts), got[1].t, "{name}");
                } else {
                    assert_eq!(got.len(), 1, "{name}");
                    assert_eq!(EXPECTED_SAMPLE_VALUE, got[0].v, "{name}");
                    assert_eq!(from_time(sample_ts), got[0].t, "{name}");
                }

                // Verify what we got vs expectations around additional _created series for OM text.
                // enableSTZeroInjection also kills that _created line.
                let got_st_series = find_samples_for_metric(&app.result_samples(), EXPECTED_CREATED_METRIC_NAME);
                if test_format == config::OPEN_METRICS_TEXT_1_0_0 && test_with_st && !test_st_zero_ingest {
                    // For OM Text, when counter has ST, and feature flag disabled we should see _created lines.
                    assert_eq!(got_st_series.len(), 1, "{name}");
                    // Conversion taken from common/expfmt.writeOpenMetricsFloat.
                    assert_eq!(unix_nanos(st_ts.unwrap()) as f64 / 1e9, got_st_series[0].v, "{name}");
                } else {
                    assert!(got_st_series.is_empty(), "{name}: {got_st_series:?}");
                }
                drop(m);
            }
        }
    }
}

/// NOTE(bwplotka): There is no AppenderV2 test for this STZeroIngestion feature as in V2 flow it's
/// moved to AppenderV2 implementation (e.g. storage) and it's tested there.
#[test]
fn test_manager_st_zero_ingestion_histogram() {
    const M_NAME: &str = "expected_histogram";

    struct Tc {
        name: &'static str,
        /// The prototext of the histogram, its sample sum and whether it has a created timestamp.
        input_hist_sample: (String, f64, bool),
        enable_st_zero_ingestion: bool,
    }
    let with_st = || {
        let (h, sum) = generate_test_histogram(0);
        (format!("{h} created_timestamp: {}", proto_ts(SystemTime::now())), sum, true)
    };
    let without_st = || {
        let (h, sum) = generate_test_histogram(0);
        (h, sum, false)
    };
    let cases = vec![
        Tc { name: "disabled with ST on histogram", input_hist_sample: with_st(), enable_st_zero_ingestion: false },
        Tc { name: "enabled with ST on histogram", input_hist_sample: with_st(), enable_st_zero_ingestion: true },
        Tc { name: "enabled without ST on histogram", input_hist_sample: without_st(), enable_st_zero_ingestion: true },
    ];
    for tc in cases {
        let app = Appendable::new();
        let m = run_managers(
            Some(Options {
                enable_start_timestamp_zero_ingestion: tc.enable_st_zero_ingestion,
                parse_st: tc.enable_st_zero_ingestion,
                skip_jitter_offsetting: true,
                ..Default::default()
            }),
            Some(Arc::new(app.clone())),
            None,
        );

        // Start fake HTTP target to that allow one scrape only.
        let body = proto_marshal_delimited(&format!(
            "name: \"{M_NAME}\" type: HISTOGRAM metric: < histogram: < {} > >",
            tc.input_hist_sample.0
        ));
        let server = setup_test_server(
            "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited",
            body,
        );

        let test_config = format!(
            "
global:
  # Disable regular scrapes.
  scrape_interval: 9999m
  scrape_timeout: 5s

scrape_configs:
- job_name: test
  scrape_native_histograms: true
  static_configs:
  - targets: ['{server}']
"
        );
        apply_config(&test_config, &m);

        // Wait for one scrape.
        wait_for_samples(&app, tc.name);

        let got = find_samples_for_metric(&app.result_samples(), M_NAME);

        // Check for zero samples, assuming we only injected always one histogram sample.
        // Did it contain ST to inject? If yes, was ST zero enabled?
        if tc.input_hist_sample.2 && tc.enable_st_zero_ingestion {
            assert_eq!(got.len(), 2, "{}", tc.name);
            // Zero sample.
            assert_eq!(Some(Histogram::default()), got[0].h, "{}", tc.name);
            // Quick soft check to make sure it's the same sample or at least not zero.
            assert_eq!(tc.input_hist_sample.1, got[1].h.as_ref().unwrap().sum, "{}", tc.name);
            continue;
        }

        // Expect only one, valid sample.
        assert_eq!(got.len(), 1, "{}", tc.name);
        // Quick soft check to make sure it's the same sample or at least not zero.
        assert_eq!(tc.input_hist_sample.1, got[0].h.as_ref().unwrap().sum, "{}", tc.name);
    }
}

#[test]
fn test_unregister_metrics() {
    let reg = Registry::new();
    // Check that all metrics can be unregistered, allowing a second manager to be created.
    for _ in 0..2 {
        let manager = new_manager_v2(Options::default(), reg.clone());
        let manager = manager.expect("NewManager");
        // Unregister all metrics.
        manager.unregister_metrics();
    }
}

/// TestNHCBAndSTZeroIngestion verifies that both ConvertClassicHistogramsToNHCBEnabled
/// and EnableStartTimestampZeroIngestion can be used simultaneously without errors.
#[test]
fn test_nhcb_and_st_zero_ingestion() {
    const M_NAME: &str = "test_histogram";
    // The expected sum of the histogram, as defined by the test's OpenMetrics exposition data.
    const EXPECTED_HISTOGRAM_SUM: f64 = 45.5;

    let app = Appendable::new();
    let m = run_managers(
        Some(Options {
            enable_start_timestamp_zero_ingestion: true,
            parse_st: true,
            skip_jitter_offsetting: true,
            ..Default::default()
        }),
        Some(Arc::new(app.clone())),
        None,
    );

    // Expose a histogram with created timestamp and exemplars to verify no parsing errors occur.
    let server = setup_test_server(
        "application/openmetrics-text",
        br#"# HELP test_histogram A histogram with created timestamp and exemplars
# TYPE test_histogram histogram
test_histogram_bucket{le="0.0"} 1
test_histogram_bucket{le="1.0"} 10 # {trace_id="trace-1"} 0.5 123456789
test_histogram_bucket{le="2.0"} 20 # {trace_id="trace-2"} 1.5 123456780
test_histogram_bucket{le="+Inf"} 30 # {trace_id="trace-3"} 2.5
test_histogram_count 30
test_histogram_sum 45.5
test_histogram_created 1520430001
# EOF
"#
        .to_vec(),
    );

    // Configuration with both convert_classic_histograms_to_nhcb enabled and ST zero ingestion enabled.
    let test_config = format!(
        "
global:
  # Use a very long scrape_interval to prevent automatic scraping during the test.
  scrape_interval: 9999m
  scrape_timeout: 5s

scrape_configs:
- job_name: test
  convert_classic_histograms_to_nhcb: true
  static_configs:
  - targets: ['{server}']
"
    );
    apply_config(&test_config, &m);

    // Verify that the scrape pool was created (proves the blocking check was removed).
    eventually(
        || m.sm.mtx_scrape.lock().unwrap().scrape_pools.contains_key("test"),
        Duration::from_secs(5),
        Duration::from_millis(100),
        "scrape pool should be created for job 'test'",
    );
    eventually(|| !app.result_samples().is_empty(), Duration::from_secs(60), Duration::from_millis(100), "expected histogram samples, got none");

    // Verify that samples were ingested (proving both features work together).
    let got = find_samples_for_metric(&app.result_samples(), M_NAME);

    // With ST zero ingestion enabled and a created timestamp present, we expect 2 samples:
    // one zero sample and one actual sample.
    assert_eq!(got.len(), 2, "expected 2 histogram samples (zero sample + actual sample)");
    assert_eq!(Some(Histogram::default()), got[0].h, "first sample should be zero sample");
    assert!((EXPECTED_HISTOGRAM_SUM - got[1].h.as_ref().unwrap().sum).abs() <= 1e-9, "second sample should retain the expected sum");
    assert_eq!(got[1].es.len(), 2, "expected 2 exemplars on second histogram");
}

/// TestTargetDisappearsAfterProviderRemoved makes sure that when a provider is dropped, (only) its targets are
/// dropped.
#[test]
fn test_target_disappears_after_provider_removed() {
    let my_job = "my-job";
    let my_job_sd_target_url = "my:9876";
    let my_job_static_target_url = "my:5432";

    let sd_file = write_into_file(&format!(r#"[{{"targets": ["{my_job_sd_target_url}"]}}]"#));

    let m = run_managers(None, None, Some(Arc::new(Appendable::new())));

    apply_config(
        &format!(
            "
scrape_configs:
- job_name: {my_job}
  static_configs:
  - targets: ['{my_job_static_target_url}']
  file_sd_configs:
  - files: ['{sd_file}']
"
        ),
        &m,
    );
    // Make sure the jobs targets are taken into account
    require_targets(
        &m.sm,
        my_job,
        true,
        Some(vec![format!("http://{my_job_sd_target_url}/metrics"), format!("http://{my_job_static_target_url}/metrics")]),
    );

    // Apply a new config where a provider is removed
    apply_config(
        &format!(
            "
scrape_configs:
- job_name: {my_job}
  static_configs:
  - targets: ['{my_job_static_target_url}']
"
        ),
        &m,
    );
    // Make sure the corresponding target was dropped
    require_targets(&m.sm, my_job, false, Some(vec![format!("http://{my_job_static_target_url}/metrics")]));

    // Apply a new config with no providers
    apply_config(
        &format!(
            "
scrape_configs:
- job_name: {my_job}
"
        ),
        &m,
    );
    // Make sure the corresponding target was dropped
    require_targets(&m.sm, my_job, false, None);
}

/// TestOnlyProviderStaleTargetsAreDropped makes sure that when a job has only one provider with multiple targets
/// and when the provider can no longer discover some of those targets, only those stale targets are dropped.
#[test]
fn test_only_provider_stale_targets_are_dropped() {
    let job_name = "my-job";
    let job_target1_url = "foo:9876";
    let job_target2_url = "foo:5432";

    let sd_file1 = write_into_file(&format!(r#"[{{"targets": ["{job_target1_url}"]}}]"#));
    let sd_file2 = write_into_file(&format!(r#"[{{"targets": ["{job_target2_url}"]}}]"#));

    let base_config = |f1: &str, f2: &str| {
        format!(
            "
scrape_configs:
- job_name: {job_name}
  file_sd_configs:
  - files: ['{f1}', '{f2}']
"
        )
    };
    let m = run_managers(None, None, Some(Arc::new(Appendable::new())));

    apply_config(&base_config(&sd_file1, &sd_file2), &m);

    // Make sure the job's targets are taken into account
    require_targets(
        &m.sm,
        job_name,
        true,
        Some(vec![format!("http://{job_target1_url}/metrics"), format!("http://{job_target2_url}/metrics")]),
    );

    // Apply the same config for the same job but with a non existing file to make the provider
    // unable to discover some targets
    apply_config(&base_config(&sd_file1, "/idontexistdoi.json"), &m);

    // The old target should get dropped
    require_targets(&m.sm, job_name, false, Some(vec![format!("http://{job_target1_url}/metrics")]));
}

/// TestProviderStaleTargetsAreDropped makes sure that when a job has only one provider and when that provider
/// should no longer discover targets, the targets of that provider are dropped.
/// See: https://github.com/prometheus/prometheus/issues/12858
#[test]
fn test_provider_stale_targets_are_dropped() {
    let job_name = "my-job";
    let job_target_url = "foo:9876";

    let sd_file = write_into_file(&format!(r#"[{{"targets": ["{job_target_url}"]}}]"#));

    let base_config = |f: &str| {
        format!(
            "
scrape_configs:
- job_name: {job_name}
  file_sd_configs:
  - files: ['{f}']
"
        )
    };
    let m = run_managers(None, None, Some(Arc::new(Appendable::new())));

    apply_config(&base_config(&sd_file), &m);

    // Make sure the job's targets are taken into account
    require_targets(&m.sm, job_name, true, Some(vec![format!("http://{job_target_url}/metrics")]));

    // Apply the same config for the same job but with a non existing file to make the provider
    // unable to discover some targets
    apply_config(&base_config("/idontexistdoi.json"), &m);

    // The old target should get dropped
    require_targets(&m.sm, job_name, false, None);
}

/// TestOnlyStaleTargetsAreDropped makes sure that when a job has multiple providers, when one of them should no
/// longer discover targets, only the stale targets of that provider are dropped.
#[test]
fn test_only_stale_targets_are_dropped() {
    let my_job = "my-job";
    let my_job_sd_target_url = "my:9876";
    let my_job_static_target_url = "my:5432";
    let other_job = "other-job";
    let other_job_target_url = "other:1234";

    let sd_file = write_into_file(&format!(r#"[{{"targets": ["{my_job_sd_target_url}"]}}]"#));

    let base_config = |f: &str| {
        format!(
            "
scrape_configs:
- job_name: {my_job}
  static_configs:
  - targets: ['{my_job_static_target_url}']
  file_sd_configs:
  - files: ['{f}']
- job_name: {other_job}
  static_configs:
  - targets: ['{other_job_target_url}']
"
        )
    };

    let m = run_managers(None, None, Some(Arc::new(Appendable::new())));

    // Apply the initial config with an existing file
    apply_config(&base_config(&sd_file), &m);
    // Make sure the jobs targets are taken into account
    require_targets(
        &m.sm,
        my_job,
        true,
        Some(vec![format!("http://{my_job_sd_target_url}/metrics"), format!("http://{my_job_static_target_url}/metrics")]),
    );
    require_targets(&m.sm, other_job, true, Some(vec![format!("http://{other_job_target_url}/metrics")]));

    // Apply the same config with a non existing file for myJob
    apply_config(&base_config("/idontexistdoi.json"), &m);

    // Only the SD target should get dropped for myJob
    require_targets(&m.sm, my_job, false, Some(vec![format!("http://{my_job_static_target_url}/metrics")]));
    // The otherJob should keep its target
    require_targets(&m.sm, other_job, false, Some(vec![format!("http://{other_job_target_url}/metrics")]));
}

#[test]
fn test_manager_disable_end_of_run_staleness_markers() {
    let cfg_text = "
scrape_configs:
 - job_name: one
   scrape_interval: 1m
   scrape_timeout: 1m
 - job_name: two
   scrape_interval: 1m
   scrape_timeout: 1m
";

    let cfg = load_configuration(cfg_text);

    let m = new_manager_v2(Options::default(), Registry::new()).expect("NewManager");
    struct StopOnDrop(Arc<Manager>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.stop();
        }
    }
    let _stop = StopOnDrop(m.clone());
    m.apply_config(&cfg).unwrap();

    // Pass targets to the manager.
    let addr = |a: &str| LabelSet::from_pairs(&[("__address__", a)]);
    let mut tgs = TargetSets::new();
    tgs.insert("one".into(), vec![Arc::new(Group { targets: vec![addr("h1"), addr("h2"), addr("h3")], ..Default::default() })]);
    tgs.insert("two".into(), vec![Arc::new(Group { targets: vec![addr("h4")], ..Default::default() })]);
    m.update_tsets(tgs);
    m.reload();

    let active_targets = m.targets_active();
    let targets_to_disable = vec![active_targets["one"][0].clone(), active_targets["one"][2].clone()];

    // Disable end of run staleness markers for some targets.
    m.disable_end_of_run_staleness_markers("one", &targets_to_disable);
    // This should be a no-op.
    m.disable_end_of_run_staleness_markers("non-existent-job", &targets_to_disable);

    // Check that the end of run staleness markers are disabled for the correct targets.
    for group in ["one", "two"] {
        let loops = m.scrape_pool(group).unwrap().loops();
        for tg in &active_targets[group] {
            let l = &loops[&tg.hash()];
            let sl = l.as_scrape_loop().expect("a scrape loop");
            let expected_disabled = targets_to_disable.iter().any(|t| Arc::ptr_eq(t, tg));
            assert_eq!(expected_disabled, sl.end_of_run_staleness_markers_disabled());
        }
    }
}

#[test]
fn test_manager_initial_scrape_offset() {
    let interval = Duration::from_secs(10);

    struct Tc {
        name: &'static str,
        initial_scrape_offset: Duration,
        run_duration: Duration,
        expected_samples: usize,
    }
    for tcase in [
        Tc { name: "zero offset scrapes immediately", initial_scrape_offset: Duration::ZERO, run_duration: Duration::ZERO, expected_samples: 1 },
        Tc {
            name: "zero offset scrapes twice after one interval",
            initial_scrape_offset: Duration::ZERO,
            run_duration: interval,
            expected_samples: 2,
        },
        Tc {
            name: "large offset prevents immediate scrape",
            initial_scrape_offset: Duration::from_secs(3600),
            run_duration: Duration::from_secs(59 * 60),
            expected_samples: 0,
        },
    ] {
        let opts = Options { initial_scrape_offset: tcase.initial_scrape_offset, ..Default::default() };
        let (scrape_manager, app, rt) = setup_synctest_manager(Some(opts));

        apply_default_synctest_config(&scrape_manager, interval);

        // Wait for the scrape manager to block on its timers.
        rt.wait_idle();

        // Fast-forward the fake clock by the test case's run duration.
        rt.sleep(tcase.run_duration);
        rt.wait_idle();

        // Stop the manager to clean up background goroutines.
        scrape_manager.stop();

        assert_eq!(find_samples_for_metric(&app.result_samples(), "expected_metric").len(), tcase.expected_samples, "{}", tcase.name);
    }
}

#[test]
fn test_manager_scrape_on_shutdown() {
    let interval = Duration::from_secs(10);

    struct Tc {
        name: &'static str,
        scrape_on_shutdown: bool,
        initial_scrape_offset: Duration,
        run_duration: Duration,
        expected_samples_total: usize,
    }
    let z = Duration::ZERO;
    for tcase in [
        Tc { name: "no scrape on shutdown", scrape_on_shutdown: false, initial_scrape_offset: z, run_duration: z, expected_samples_total: 1 },
        Tc { name: "scrape on shutdown", scrape_on_shutdown: true, initial_scrape_offset: z, run_duration: z, expected_samples_total: 2 },
        Tc {
            name: "scrape on shutdown after some scrapes",
            scrape_on_shutdown: true,
            initial_scrape_offset: z,
            run_duration: interval,
            expected_samples_total: 3,
        },
        Tc {
            name: "scrape on shutdown with initial offset",
            scrape_on_shutdown: true,
            initial_scrape_offset: Duration::from_secs(10),
            run_duration: Duration::from_secs(5),
            expected_samples_total: 1,
        },
        Tc {
            name: "scrape on shutdown with short running instance (offset 5s)",
            scrape_on_shutdown: true,
            initial_scrape_offset: Duration::from_secs(5),
            run_duration: Duration::from_secs(8),
            expected_samples_total: 2,
        },
    ] {
        let opts = Options {
            scrape_on_shutdown: tcase.scrape_on_shutdown,
            initial_scrape_offset: tcase.initial_scrape_offset,
            ..Default::default()
        };
        let (scrape_manager, app, rt) = setup_synctest_manager(Some(opts));

        apply_default_synctest_config(&scrape_manager, interval);

        // Wait for the initial scrape to happen exactly at t=0.
        rt.wait_idle();

        // Fast-forward fake time to simulate scheduled scrapes before shutdown.
        if tcase.run_duration > Duration::ZERO {
            rt.sleep(tcase.run_duration);
            rt.wait_idle();
        }

        // Stop the manager. This triggers the ScrapeOnShutdown logic synchronously.
        scrape_manager.stop();

        assert_eq!(
            find_samples_for_metric(&app.result_samples(), "expected_metric").len(),
            tcase.expected_samples_total,
            "{}",
            tcase.name
        );
    }
}

#[test]
fn test_manager_reloader() {
    #[derive(Clone, Copy)]
    struct Tc {
        name: &'static str,
        discovery_reload_on_startup: bool,
        scrape_on_shutdown: bool,
        discovery_reload_interval: Duration,
        update_target: bool,
        run_duration: Duration,
        expected_samples_total: usize,
    }
    let base = Tc {
        name: "",
        discovery_reload_on_startup: false,
        scrape_on_shutdown: false,
        discovery_reload_interval: Duration::ZERO,
        update_target: false,
        run_duration: Duration::ZERO,
        expected_samples_total: 0,
    };
    let s = Duration::from_secs;
    let cases = vec![
        Tc { name: "no startup reload default interval", run_duration: s(6), expected_samples_total: 1, ..base },
        Tc {
            name: "no startup reload short interval",
            discovery_reload_interval: s(1),
            run_duration: s(1),
            expected_samples_total: 1,
            ..base
        },
        Tc {
            name: "no startup reload default interval with target update",
            update_target: true,
            run_duration: s(12),
            expected_samples_total: 1,
            ..base
        },
        Tc {
            name: "no startup reload short interval after ticker",
            discovery_reload_interval: s(1),
            update_target: true,
            run_duration: s(2),
            expected_samples_total: 1,
            ..base
        },
        Tc {
            name: "no startup reload default interval after ticker with update",
            update_target: true,
            run_duration: s(6),
            expected_samples_total: 1,
            ..base
        },
        Tc { name: "startup reload", discovery_reload_on_startup: true, run_duration: s(2), expected_samples_total: 1, ..base },
        Tc {
            name: "startup reload with target update",
            discovery_reload_on_startup: true,
            update_target: true,
            run_duration: s(2),
            expected_samples_total: 1,
            ..base
        },
        Tc {
            name: "startup reload with update after ticker",
            discovery_reload_on_startup: true,
            run_duration: s(6),
            expected_samples_total: 1,
            ..base
        },
        Tc {
            name: "no startup reload with scrape on shutdown after reload",
            scrape_on_shutdown: true,
            run_duration: s(6),
            expected_samples_total: 2,
            ..base
        },
        Tc { name: "stop before no startup reload", scrape_on_shutdown: true, run_duration: s(2), expected_samples_total: 0, ..base },
        Tc {
            name: "startup reload and scrape on shutdown",
            discovery_reload_on_startup: true,
            scrape_on_shutdown: true,
            run_duration: s(2),
            expected_samples_total: 2,
            ..base
        },
    ];
    for tcase in cases {
        let opts = Options {
            discovery_reload_on_startup: tcase.discovery_reload_on_startup,
            scrape_on_shutdown: tcase.scrape_on_shutdown,
            discovery_reload_interval: tcase.discovery_reload_interval,
            ..Default::default()
        };
        let (scrape_manager, app, rt) = setup_synctest_manager(Some(opts));

        scrape_manager.apply_config(&synctest_config(Duration::from_secs(10))).unwrap();

        let (tsets_tx, tsets_rx) = sync_channel::<TargetSets>(0);
        let m2 = scrape_manager.clone();
        thread::spawn(move || m2.run(tsets_rx));

        // Send initial target to trigger the first reload via the normal flow.
        let mut ts = TargetSets::new();
        ts.insert("test".into(), vec![Arc::new(Group { targets: vec![test_target("test.local")], ..Default::default() })]);
        tsets_tx.send(ts).unwrap();
        rt.wait_idle(); // Wait for Run to process tsetsCh and for reloader to trigger reload.

        if tcase.update_target {
            let mut ts = TargetSets::new();
            ts.insert(
                "test".into(),
                vec![Arc::new(Group {
                    source: "test".into(),
                    targets: vec![test_target("test-updated.local")],
                    ..Default::default()
                })],
            );
            tsets_tx.send(ts).unwrap();
            rt.wait_idle(); // Wait for Run to process tsetsCh.
        }

        if tcase.run_duration > Duration::ZERO {
            rt.sleep(tcase.run_duration);
            rt.wait_idle();
        }

        scrape_manager.stop();
        rt.wait_idle();

        assert_eq!(
            find_samples_for_metric(&app.result_samples(), "expected_metric").len(),
            tcase.expected_samples_total,
            "{}",
            tcase.name
        );
        drop(tsets_tx);
    }
}
