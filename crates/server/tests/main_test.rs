//! Port of `cmd/prometheus/main_test.go`.
//!
//! The tests run the real `prometheus` binary (Go re-runs its test binary with `-test.main`).
//! adapted: Go's parallel subtests that start a server run one after the other here (the port range of this
//! agent is small), and `--web.listen-address=0.0.0.0:0` becomes a port of that range.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use common::*;

/// Serializes the tests that start the binary on a port of the agent's range.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

/// A port of [`random_port`] that is released when dropped (declare it before the process that uses it, so the
/// process is killed first).
struct Port(u16);

impl Port {
    fn new() -> Port {
        Port(random_port())
    }
    fn listen(&self) -> String {
        format!("--web.listen-address=0.0.0.0:{}", self.0)
    }
}

impl Drop for Port {
    fn drop(&mut self) {
        release_port(self.0);
    }
}

/// The outcome of running the binary for a while.
struct Run {
    /// `None` when the process was still running at the deadline (it is then killed).
    status: Option<ExitStatus>,
    stderr: String,
}

/// Go `exec.Command(promPath, "-test.main", args...)` + `Start`, then `Wait` up to `timeout`: kills the process
/// if it still runs then.
fn run_for(args: &[String], timeout: Duration) -> Run {
    let cwd = TempDir::new();
    let mut child = Command::new(prom_path())
        .args(args)
        .current_dir(cwd.path())
        .env("no_proxy", "localhost,127.0.0.1,0.0.0.0,:")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start prometheus");
    let mut err = child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = err.read_to_string(&mut s);
        s
    });
    let end = std::time::Instant::now() + timeout;
    let status = loop {
        if let Ok(Some(s)) = child.try_wait() {
            break Some(s);
        }
        if std::time::Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stderr = reader.join().unwrap_or_default();
    Run { status, stderr }
}

/// Go's pattern of the startup tests: exit code 0 means "still running after `startupTime`", anything else the
/// exit status of the process.
fn check_exit(args: &[String], exit_code: i32, case: &str) {
    if exit_code == 0 {
        let r = run_for(args, STARTUP_TIME);
        if let Some(s) = r.status {
            panic!("{case}: prometheus should be still running: {s}\n{}", r.stderr);
        }
        return;
    }
    let r = run_for(args, Duration::from_secs(120));
    let s = r.status.unwrap_or_else(|| panic!("{case}: prometheus did not exit\n{}", r.stderr));
    assert!(!s.success(), "{case}: expected an error\n{}", r.stderr);
    assert_eq!(Some(exit_code), s.code(), "{case}\n{}", r.stderr);
}

fn s(v: &str) -> String {
    v.to_string()
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_ready_storage_cost_sampling_queryable() {
    use promql::estimate::estimate_cost;
    use promql::parser::{self, Parser};
    use server::ready::{LocalDb, ReadyStorage};
    use storage::{
        Appendable, AppendableV2, Appender, AppenderV2, BoxChunkQuerier, BoxQuerier, ChunkQueryable, Context, Error,
        Queryable, Storage,
    };

    /// Go `struct{ storage.Storage }{db}`: the storage without its optional interfaces.
    struct Hidden(Arc<dyn Storage>);
    impl Queryable for Hidden {
        fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
            self.0.querier(mint, maxt)
        }
    }
    impl ChunkQueryable for Hidden {
        fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
            self.0.chunk_querier(mint, maxt)
        }
    }
    impl Appendable for Hidden {
        fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
            self.0.appender(ctx)
        }
    }
    impl AppendableV2 for Hidden {
        fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
            self.0.appender_v2(ctx)
        }
    }
    impl Storage for Hidden {
        fn start_time(&self) -> Result<i64, Error> {
            self.0.start_time()
        }
        fn close(&self) -> Option<Error> {
            self.0.close()
        }
    }

    let db = Arc::new(
        promql::promqltest::loaded_storage(
            "\nload 10s\n  metric 0+1x100\n  nh {{schema:0 sum:10 count:5 buckets:[1 2 2]}}x100\n",
        )
        .unwrap(),
    );
    let dir = TempDir::new();
    let remote_storage: Arc<dyn Storage> = Arc::new(remote::write::new_storage(
        None,
        Some(discovery::metrics::Registry::new()),
        None,
        dir.path(),
        Duration::from_secs(1),
        None,
        false,
        None,
    ));

    struct Case {
        name: &'static str,
        db: Option<Arc<dyn Storage>>,
        supported: bool,
    }
    let cases = vec![
        Case { name: "not ready", db: None, supported: false },
        Case { name: "unsupported", db: Some(Arc::new(Hidden(db.clone()))), supported: false },
        Case { name: "native", db: Some(db.clone()), supported: true },
    ];
    for tc in cases {
        let local = Arc::new(ReadyStorage::new());
        local.set(tc.db.clone().map(LocalDb::Other), 0);
        let fanout = storage::new_fanout(None, local.clone() as Arc<dyn Storage>, vec![remote_storage.clone()]);
        let provider = fanout.as_cost_sampling_queryable().expect("fanout is a CostSamplingQueryable");
        let (source, supported) = provider.cost_sampling_queryable();
        assert_eq!(tc.supported, supported, "{}", tc.name);
        if !tc.supported {
            assert!(source.is_none(), "{}", tc.name);
            continue;
        }
        assert!(source.is_some(), "{}", tc.name);
        for expr in ["rate(metric[5m])", "rate(nh[5m])"] {
            let ctx = Context::background();
            let ts = 1000 * 1000;
            let p = Parser::new(parser::Options::default());
            // Use a different configured interval so missing density sampling changes the estimate.
            let (direct, warnings, err) =
                estimate_cost(&ctx, &*db, Some(&*db), &p, expr, ts, ts, 0, 300_000, 60_000, 60_000);
            assert!(err.is_none(), "{}/{expr}: {err:?}", tc.name);
            assert!(warnings.is_empty(), "{}/{expr}", tc.name);
            assert!(direct.samples_read >= 30, "{}/{expr}: {}", tc.name, direct.samples_read);
            let (wrapped, warnings, err) =
                estimate_cost(&ctx, &fanout, Some(&fanout), &p, expr, ts, ts, 0, 300_000, 60_000, 60_000);
            assert!(err.is_none(), "{}/{expr}: {err:?}", tc.name);
            assert!(warnings.is_empty(), "{}/{expr}", tc.name);
            assert_eq!(direct, wrapped, "{}/{expr}", tc.name);
        }
    }
    assert!(remote_storage.close().is_none());
    assert!(db.close().is_none());
}

#[test]
fn test_compute_external_url() {
    let tests: &[(&str, bool)] = &[
        ("", true),
        ("http://proxy.com/prometheus", true),
        ("'https://url/prometheus'", false),
        ("'relative/path/with/quotes'", false),
        ("http://alertmanager.company.com", true),
        ("https://double--dash.de", true),
        ("'http://starts/with/quote", false),
        ("ends/with/quote\"", false),
    ];
    for (input, valid) in tests {
        let r = server::helpers::compute_external_url(input, "0.0.0.0:9090", "");
        if *valid {
            assert!(r.is_ok(), "input={input:?}: {:?}", r.err());
        } else {
            assert!(r.is_err(), "input={input:?}");
        }
    }
}

#[test]
fn test_compute_external_url_with_web_config() {
    let dir = TempDir::new();
    let tls_config = dir.join("web-config-tls.yml");
    std::fs::write(&tls_config, "\ntls_server_config:\n  cert_file: server.crt\n  key_file: server.key\n").unwrap();
    let basic_auth_only_config = dir.join("web-config-basic-auth.yml");
    std::fs::write(
        &basic_auth_only_config,
        "\nbasic_auth_users:\n  alice: $2y$10$mDwo.lAisC94iLAyP81MCesa29IzH37oigHC/42V2pdJlUprsJPze\n",
    )
    .unwrap();
    let empty_tls_config = dir.join("web-config-empty-tls.yml");
    std::fs::write(&empty_tls_config, "\ntls_server_config:\n").unwrap();
    let non_enabling_tls_config = dir.join("web-config-non-enabling-tls.yml");
    std::fs::write(&non_enabling_tls_config, "\ntls_server_config:\n  min_version: TLS12\n").unwrap();

    let tests = [
        ("no web config file", String::new(), "http"),
        ("web config file with tls_server_config", tls_config, "https"),
        ("web config file without tls_server_config", basic_auth_only_config, "http"),
        ("web config file with empty tls_server_config", empty_tls_config, "http"),
        ("web config file with tls_server_config setting only non-enabling fields", non_enabling_tls_config, "http"),
        ("nonexistent web config file", dir.join("does-not-exist.yml"), "http"),
    ];
    for (name, web_config_file, expected_scheme) in tests {
        let u = server::helpers::compute_external_url("", "0.0.0.0:9090", &web_config_file)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(expected_scheme, u.scheme, "{name}");
    }
}

/// TestComputeExternalURLExplicitSchemeUnaffectedByWebConfig verifies that an explicitly provided
/// --web.external-url is never overridden by TLS detection: only the inferred default URL (empty input) should
/// have its scheme derived from the web config file.
#[test]
fn test_compute_external_url_explicit_scheme_unaffected_by_web_config() {
    let dir = TempDir::new();
    let tls_config = dir.join("web-config-tls.yml");
    std::fs::write(&tls_config, "\ntls_server_config:\n  cert_file: server.crt\n  key_file: server.key\n").unwrap();
    let u = server::helpers::compute_external_url("http://alertmanager.company.com", "0.0.0.0:9090", &tls_config).unwrap();
    assert_eq!("http", u.scheme);
}

/// Let's provide an invalid configuration file and verify the exit status indicates the error.
#[test]
fn test_failed_startup_exit_code() {
    let _g = serial();
    let port = Port::new();
    let args = vec![port.listen(), s("--config.file=fake-input-file")];
    let r = run_for(&args, Duration::from_secs(120));
    let st = r.status.expect("prometheus did not exit");
    assert!(!st.success());
    assert_eq!(Some(2), st.code(), "{}", r.stderr);
}

#[test]
fn test_retention_percentage_starts_with_missing_storage_path() {
    let tmp_dir = TempDir::new();
    if server::helpers::storage_path_fs_size(&tmp_dir.path().to_string_lossy()) == 0 {
        eprintln!("skipping test because filesystem size detection is unavailable.");
        return;
    }
    let config_file = tmp_dir.join("prometheus.yml");
    let storage_path = tmp_dir.path().join("missing").join("data");
    std::fs::write(&config_file, "\nstorage:\n  tsdb:\n    retention:\n      percentage: 1.5\n").unwrap();

    let _g = serial();
    let port = Port::new();
    let _prom = prometheus_command_with_logging(
        &config_file,
        port.0,
        &[format!("--storage.tsdb.path={}", storage_path.to_string_lossy())],
    );
    wait_for_prometheus_ready(port.0);
    assert!(storage_path.is_dir(), "{} does not exist", storage_path.display());
}

#[test]
fn test_send_alerts() {
    use model::labels::from_strings;
    use rules::rule::Time;
    use std::time::UNIX_EPOCH;

    let unix = |s: u64| Some(UNIX_EPOCH + Duration::from_secs(s));
    struct Case {
        input: Vec<rules::Alert>,
        exp: Vec<notifier::Alert>,
    }
    let test_cases = vec![
        Case {
            input: vec![rules::Alert {
                labels: from_strings(&["l1", "v1"]),
                annotations: from_strings(&["a2", "v2"]),
                active_at: Time::unix(1, 0),
                fired_at: Time::unix(2, 0),
                valid_until: Time::unix(3, 0),
                ..Default::default()
            }],
            exp: vec![notifier::Alert {
                labels: from_strings(&["l1", "v1"]),
                annotations: from_strings(&["a2", "v2"]),
                starts_at: unix(2),
                ends_at: unix(3),
                generator_url: s("http://localhost:9090/graph?g0.expr=up&g0.tab=1"),
            }],
        },
        Case {
            input: vec![rules::Alert {
                labels: from_strings(&["l1", "v1"]),
                annotations: from_strings(&["a2", "v2"]),
                active_at: Time::unix(1, 0),
                fired_at: Time::unix(2, 0),
                resolved_at: Time::unix(4, 0),
                ..Default::default()
            }],
            exp: vec![notifier::Alert {
                labels: from_strings(&["l1", "v1"]),
                annotations: from_strings(&["a2", "v2"]),
                starts_at: unix(2),
                ends_at: unix(4),
                generator_url: s("http://localhost:9090/graph?g0.expr=up&g0.tab=1"),
            }],
        },
        Case { input: vec![], exp: vec![] },
    ];

    struct SenderFunc {
        in_empty: bool,
        exp: Vec<notifier::Alert>,
        calls: AtomicUsize,
        failed: Mutex<Option<String>>,
    }
    impl rules::manager::Sender for SenderFunc {
        fn send(&self, alerts: Vec<notifier::Alert>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut f = self.failed.lock().unwrap();
            if self.in_empty {
                *f = Some("sender called with 0 alert".to_string());
            } else if self.exp != alerts {
                *f = Some(format!("expected {:?}, got {:?}", self.exp, alerts));
            }
        }
    }

    for (i, tc) in test_cases.into_iter().enumerate() {
        let sender =
            Arc::new(SenderFunc { in_empty: tc.input.is_empty(), exp: tc.exp, calls: AtomicUsize::new(0), failed: Mutex::new(None) });
        let f = rules::manager::send_alerts(sender.clone(), "http://localhost:9090");
        f(&storage::Context::background(), "up", tc.input);
        if let Some(e) = sender.failed.lock().unwrap().take() {
            panic!("{i}: {e}");
        }
    }
}

#[test]
fn test_wal_segment_size_bounds() {
    let _g = serial();
    for (size, exit_code) in [("9MB", 1), ("257MB", 1), ("10", 2), ("1GB", 1), ("12MB", 0)] {
        let dir = TempDir::new();
        let port = Port::new();
        let args = vec![
            format!("--storage.tsdb.wal-segment-size={size}"),
            port.listen(),
            format!("--config.file={}", prom_config()),
            format!("--storage.tsdb.path={}", dir.join("data")),
        ];
        check_exit(&args, exit_code, size);
    }
}

#[test]
fn test_max_block_chunk_segment_size_bounds() {
    let _g = serial();
    for (size, exit_code) in [("512KB", 1), ("1MB", 0)] {
        let dir = TempDir::new();
        let port = Port::new();
        let args = vec![
            format!("--storage.tsdb.max-block-chunk-segment-size={size}"),
            port.listen(),
            format!("--config.file={}", prom_config()),
            format!("--storage.tsdb.path={}", dir.join("data")),
        ];
        check_exit(&args, exit_code, size);
    }
}

#[test]
fn test_chunk_encoding_startup_validation() {
    let xor2 = "\nstorage:\n  tsdb:\n    chunk_encoding:\n      floats: xor2";
    let xor = "\nstorage:\n  tsdb:\n    chunk_encoding:\n      floats: xor";
    let cases: &[(&str, &str, &str, i32)] = &[
        ("xor2 without any feature flag", xor2, "", 0),
        ("xor2 with st-storage feature", xor2, "st-storage", 0),
        ("xor with st-storage feature", xor, "st-storage", 1),
        ("xor2-encoding flag with explicit xor in config", xor, "xor2-encoding", 0),
        ("st-storage with the xor2-encoding flag and no config field", "", "st-storage,xor2-encoding", 0),
        ("st-storage and xor2-encoding flags with explicit xor in config", xor, "st-storage,xor2-encoding", 1),
        ("st-storage implies ST-capable encodings", "", "st-storage", 0),
        ("xor without st-storage feature", xor, "", 0),
    ];
    let _g = serial();
    for (name, config, features, exit_code) in cases {
        let tmp_dir = TempDir::new();
        let config_file = tmp_dir.join("prometheus.yml");
        std::fs::write(&config_file, config).unwrap();
        let port = Port::new();
        let mut args =
            vec![port.listen(), format!("--config.file={config_file}"), format!("--storage.tsdb.path={}", tmp_dir.join("data"))];
        if !features.is_empty() {
            args.push(format!("--enable-feature={features}"));
        }
        check_exit(&args, *exit_code, name);
    }
}

/// Go `getCurrentGaugeValuesFor`.
fn get_current_gauge_values_for(reg: &discovery::metrics::Registry, names: &[&str]) -> Vec<(String, f64)> {
    let mut res: Vec<(String, f64)> = Vec::new();
    for g in reg.gather() {
        for m in names {
            if g.name != *m {
                continue;
            }
            assert_eq!(1, g.metrics.len());
            assert!(!res.iter().any(|(n, _)| n == m), "expected only one metric family for {m}");
            let v = match g.metrics[0].value {
                discovery::metrics::MetricValue::Gauge(v) => v,
                ref o => panic!("{m} is not a gauge: {o:?}"),
            };
            res.push((m.to_string(), v));
        }
    }
    res.sort_by(|a, b| a.0.cmp(&b.0));
    res
}

#[test]
fn test_time_metrics() {
    let tmp_dir = TempDir::new();
    let reg = discovery::metrics::Registry::new();
    let db = server::ready::open_db_with_metrics(&tmp_dir.path().to_string_lossy(), None, &reg, None, None).unwrap();

    let names = [
        "prometheus_tsdb_lowest_timestamp_seconds",
        "prometheus_tsdb_head_min_time_seconds",
        "prometheus_tsdb_head_max_time_seconds",
    ];
    let sorted = |mut v: Vec<(String, f64)>| {
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    };
    // Check initial values.
    assert_eq!(
        sorted(vec![
            (s("prometheus_tsdb_lowest_timestamp_seconds"), i64::MAX as f64 / 1000.0),
            (s("prometheus_tsdb_head_min_time_seconds"), i64::MAX as f64 / 1000.0),
            (s("prometheus_tsdb_head_max_time_seconds"), i64::MIN as f64 / 1000.0),
        ]),
        get_current_gauge_values_for(&reg, &names)
    );

    let ctx = storage::Context::background();
    let mut app = db.appender_v2(&ctx);
    let l = model::labels::from_strings(&["__name__", "a"]);
    for t in [1000, 2000, 3000] {
        app.append(0, &l, 0, t, 1.0, None, None, &Default::default()).unwrap();
    }
    assert!(app.commit().is_none());

    assert_eq!(
        sorted(vec![
            (s("prometheus_tsdb_lowest_timestamp_seconds"), 1.0),
            (s("prometheus_tsdb_head_min_time_seconds"), 1.0),
            (s("prometheus_tsdb_head_max_time_seconds"), 3.0),
        ]),
        get_current_gauge_values_for(&reg, &names)
    );
    db.close().unwrap();
}

#[test]
fn test_agent_successful_startup() {
    let _g = serial();
    let port = Port::new();
    let args = vec![s("--agent"), port.listen(), format!("--config.file={}", agent_config())];
    let r = run_for(&args, STARTUP_TIME);
    let mut actual_exit_status = 0;
    if let Some(st) = r.status {
        eprintln!("prometheus agent exited early: {st}\n{}", r.stderr);
        actual_exit_status = st.code().unwrap_or(-1);
    }
    assert_eq!(0, actual_exit_status);
}

#[test]
fn test_agent_failed_startup_with_server_flag() {
    let _g = serial();
    let port = Port::new();
    let args = vec![s("--agent"), s("--storage.tsdb.path=."), port.listen(), format!("--config.file={}", prom_config())];
    let r = run_for(&args, STARTUP_TIME);
    let mut actual_exit_status = 0;
    if let Some(st) = r.status {
        eprintln!("prometheus agent should not be running: {st}");
        actual_exit_status = st.code().unwrap_or(-1);
    }
    assert_eq!(3, actual_exit_status, "{}", r.stderr);

    // Assert on last line.
    let last = r.stderr.split('\n').next_back().unwrap_or("");
    assert_eq!("The following flag(s) can not be used in agent mode: [\"--storage.tsdb.path\"]", last);
}

#[test]
fn test_agent_failed_startup_with_invalid_config() {
    let _g = serial();
    let port = Port::new();
    let args = vec![s("--agent"), port.listen(), format!("--config.file={}", prom_config())];
    let r = run_for(&args, STARTUP_TIME);
    let mut actual_exit_status = 0;
    if let Some(st) = r.status {
        eprintln!("prometheus agent should not be running: {st}");
        actual_exit_status = st.code().unwrap_or(-1);
    }
    assert_eq!(2, actual_exit_status, "{}", r.stderr);
}

#[test]
fn test_mode_specific_flags() {
    let cases: &[(&str, &str, i32)] = &[
        ("agent", "--storage.agent.path", 0),
        ("server", "--storage.tsdb.path", 0),
        ("server", "--storage.agent.path", 3),
        ("agent", "--storage.tsdb.path", 3),
    ];
    let _g = serial();
    let mut failures = Vec::new();
    for (mode, arg, exit_status) in cases {
        let dir = TempDir::new();
        let port = Port::new();
        let mut args = vec![s(arg), dir.path().to_string_lossy().into_owned(), port.listen()];
        if *mode == "agent" {
            args.push(s("--agent"));
            args.push(format!("--config.file={}", agent_config()));
        } else {
            args.push(format!("--config.file={}", prom_config()));
        }
        let name = format!("{mode} mode with option {arg}");
        // Go reports these with t.Errorf: every case runs.
        let r = std::panic::catch_unwind(|| check_exit(&args, *exit_status, &name));
        if let Err(e) = r {
            failures.push(e.downcast_ref::<String>().cloned().unwrap_or_else(|| name.clone()));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn test_documentation() {
    let r = Command::new(prom_path()).arg("--write-documentation").stderr(Stdio::inherit()).output();
    let out = r.expect("failed to generate CLI documentation via --write-documentation");
    assert!(out.status.success(), "failed to generate CLI documentation via --write-documentation");
    let base = std::path::Path::new(prom_path()).file_name().unwrap().to_string_lossy().into_owned();
    let app_title = base.trim_end_matches(".test").to_string();
    let generated_content = String::from_utf8_lossy(&out.stdout).replace(&base, &app_title);
    let expected_content = std::fs::read_to_string(upstream().join("docs/command-line/prometheus.md")).unwrap();
    assert_eq!(
        expected_content.replace("\r\n", "\n"),
        generated_content,
        "Generated content does not match documentation. Hint: run `make cli-documentation`."
    );
}

#[test]
fn test_rw_proto_msg_flag_parser() {
    use server::flags::{WRITE_V1_MESSAGE_TYPE as V1, WRITE_V2_MESSAGE_TYPE as V2, rw_proto_msg_set};
    use server::kingpin::{App, Flag, GoParse, Kind};

    let default_opts = vec![s(V1), s(V2)];
    let cases: Vec<(Vec<&str>, Vec<String>, Option<&str>)> = vec![
        (vec![], default_opts.clone(), None),
        (
            vec!["--test-proto-msgs", "test"],
            vec![],
            Some("unknown type for remote write protobuf message test, supported: prometheus.WriteRequest, io.prometheus.write.v2.Request"),
        ),
        (vec!["--test-proto-msgs", "io.prometheus.write.v2.Request"], vec![s(V2)], None),
        (
            vec!["--test-proto-msgs", "io.prometheus.write.v2.Request", "--test-proto-msgs", "io.prometheus.write.v2.Request"],
            vec![],
            Some("duplicated io.prometheus.write.v2.Request flag value, got io.prometheus.write.v2.Request already"),
        ),
        (
            vec!["--test-proto-msgs", "io.prometheus.write.v2.Request", "--test-proto-msgs", "prometheus.WriteRequest"],
            vec![s(V2), s(V1)],
            None,
        ),
        (
            vec![
                "--test-proto-msgs",
                "io.prometheus.write.v2.Request",
                "--test-proto-msgs",
                "prometheus.WriteRequest",
                "--test-proto-msgs",
                "io.prometheus.write.v2.Request",
            ],
            vec![],
            Some(
                "duplicated io.prometheus.write.v2.Request flag value, got io.prometheus.write.v2.Request, prometheus.WriteRequest already",
            ),
        ),
    ];
    for (args, expected, expected_err) in cases {
        let mut f = Flag::new("test-proto-msgs", "", Kind::Custom(rw_proto_msg_set, true));
        f.default = default_opts.clone();
        let a = App::new("test", "", "", vec![f], Vec::new());
        let args: Vec<String> = args.iter().map(|x| s(x)).collect();
        let name = args.join(",");
        let mut out = Vec::new();
        match a.try_parse(&args, &mut out) {
            GoParse::Run(p) => {
                assert!(expected_err.is_none(), "{name}: expected error {expected_err:?}");
                assert_eq!(expected, p.strings("test-proto-msgs"), "{name}");
            }
            GoParse::ContextErr(e) | GoParse::ValueErr(e) => {
                let want = expected_err.unwrap_or_else(|| panic!("{name}: unexpected error {e}"));
                assert_eq!(want, e, "{name}");
            }
            GoParse::Exit(c) => panic!("{name}: exit {c}"),
        }
    }
}

/// HTTP POST of a form (Go `http.PostForm`).
fn post_form(url: &str, params: &[(&str, &str)]) -> Result<Resp, String> {
    fn enc(s: &str) -> String {
        let mut o = String::new();
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => o.push(b as char),
                b' ' => o.push('+'),
                _ => o.push_str(&format!("%{b:02X}")),
            }
        }
        o
    }
    let body: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", enc(k), enc(v))).collect();
    post(url, "application/x-www-form-urlencoded", body.join("&").as_bytes())
}

#[test]
fn test_runtime_gogc_config() {
    struct Case {
        name: &'static str,
        config: &'static str,
        gogc_env_var: &'static str,
        expected_gogc: f64,
    }
    let cases = [
        Case { name: "empty config file", config: "", gogc_env_var: "", expected_gogc: 75.0 },
        Case { name: "empty config file with GOGC env var set", config: "", gogc_env_var: "66", expected_gogc: 66.0 },
        Case { name: "gogc set through config", config: "\nruntime:\n  gogc: 77", gogc_env_var: "", expected_gogc: 77.0 },
        Case {
            name: "gogc set through config and env var",
            config: "\nruntime:\n  gogc: 77",
            gogc_env_var: "88",
            expected_gogc: 77.0,
        },
        Case { name: "incomplete runtime block", config: "\nruntime:", gogc_env_var: "", expected_gogc: 75.0 },
        Case {
            name: "incomplete runtime block and GOGC env var set",
            config: "\nruntime:",
            gogc_env_var: "88",
            expected_gogc: 88.0,
        },
        Case {
            name: "unrelated config and GOGC env var set",
            config: "\nglobal:\n  scrape_interval: 500ms",
            gogc_env_var: "80",
            expected_gogc: 80.0,
        },
    ];
    let _g = serial();
    for tc in cases {
        let tmp_dir = TempDir::new();
        let config_file = tmp_dir.join("prometheus.yml");
        let port = Port::new();
        std::fs::write(&config_file, tc.config).unwrap();
        let args = vec![
            format!("--config.file={config_file}"),
            port.listen(),
            format!("--storage.tsdb.path={}", tmp_dir.path().display()),
            s("--web.enable-lifecycle"),
        ];
        // Inject GOGC when set, and drop any inherited one otherwise, so that the environment the test runs in
        // cannot influence the cases expecting the default or the configured value.
        let set: Vec<(&str, &str)> = if tc.gogc_env_var.is_empty() { vec![] } else { vec![("GOGC", tc.gogc_env_var)] };
        let _prom = start_with_env(&args, &set, &["GOGC"]);

        // Wait for the initial configuration to be applied: /metrics is already served before that happens.
        wait_for_prometheus_ready(port.0);

        let ensure_gogc_value = |val: f64| {
            let r = get(&format!("http://127.0.0.1:{}/metrics", port.0)).unwrap();
            // Check the final GOGC that's set, consider go_gc_gogc_percent from /metrics as source of truth.
            let gogc = get_metric_value(&r.body, "go_gc_gogc_percent").unwrap_or_else(|e| panic!("{}: {e}", tc.name));
            assert_eq!(val, gogc, "{}", tc.name);
        };

        // The value is applied on startup.
        ensure_gogc_value(tc.expected_gogc);

        // After a reload with the same config, the value stays the same.
        let reload_url = format!("http://127.0.0.1:{}/-/reload", port.0);
        reload_prometheus_config(&reload_url);
        ensure_gogc_value(tc.expected_gogc);

        // After a reload with different config, the value gets updated.
        std::fs::write(&config_file, "\nruntime:\n  gogc: 99").unwrap();
        reload_prometheus_config(&reload_url);
        ensure_gogc_value(99.0);
    }
}

/// TestQueryCostLimitsConfig verifies that the query cost limits in the configuration file apply to queries only
/// with the query-cost feature enabled, and that a reload updates them.
#[test]
fn test_query_cost_limits_config() {
    struct QueryResponse {
        error_type: String,
        error: String,
        result_len: usize,
    }
    let _g = serial();
    for (name, enabled) in [("feature enabled", true), ("feature disabled", false)] {
        let tmp_dir = TempDir::new();
        let config_file = tmp_dir.join("prometheus.yml");
        let port = Port::new();
        // Prometheus scrapes itself, so the query below soon matches two series, one more than the configured
        // limit.
        let write_config = |max_series: i32| {
            let cfg = format!(
                "\nglobal:\n  scrape_interval: 100ms\n  query_max_series: {max_series}\nscrape_configs:\n  - job_name: prometheus\n    static_configs:\n      - targets: ['127.0.0.1:{}']\n",
                port.0
            );
            std::fs::write(&config_file, cfg).unwrap();
        };
        write_config(1);

        let mut args = vec![
            format!("--storage.tsdb.path={}", tmp_dir.path().display()),
            s("--web.enable-lifecycle"),
            s("--scrape.discovery-reload-interval=100ms"), // Reduce initial wait time from default of 5 seconds.
        ];
        if enabled {
            args.push(s("--enable-feature=query-cost"));
        }
        let _prom = prometheus_command_with_logging(&config_file, port.0, &args);
        wait_for_prometheus_ready(port.0);

        let query = |extra: &[(&str, &str)]| -> Result<(u16, QueryResponse), String> {
            let mut params = vec![("query", r#"{__name__=~"up|scrape_duration_seconds"}"#)];
            params.extend_from_slice(extra);
            let r = post_form(&format!("http://127.0.0.1:{}/api/v1/query", port.0), &params)?;
            let j = json::Json::parse(&r.body).map_err(|e| format!("{e:?}"))?;
            let body = QueryResponse {
                error_type: j.get("errorType").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                error: j.get("error").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                result_len: j.path("data.result").and_then(|v| v.as_arr()).map_or(0, |a| a.len()),
            };
            Ok((r.status, body))
        };
        // waitForQuery waits until both series have been scraped and the query gets the expected status.
        let wait_for_query = |want_status: u16| -> QueryResponse {
            let mut last = None;
            let ok = eventually(READY_TIMEOUT, Duration::from_millis(100), || {
                let Ok((status, body)) = query(&[]) else { return false };
                let r = if status == 200 { want_status == 200 && body.result_len == 2 } else { status == want_status };
                last = Some(body);
                r
            });
            assert!(ok, "{name}: query did not get status {want_status}");
            last.unwrap()
        };

        if !enabled {
            wait_for_query(200);
            continue;
        }

        let rejected = wait_for_query(422);
        assert_eq!("cost_limit", rejected.error_type, "{name}");
        // The samples-read limit also reports cost_limit, so check that the configured series limit is the one
        // that applied.
        assert!(rejected.error.contains("too many series"), "{name}: {}", rejected.error);
        // A request may only lower the configured ceiling.
        let (status, body) = query(&[("max_series", "2")]).unwrap();
        assert_eq!(400, status, "{name}");
        assert_eq!("bad_data", body.error_type, "{name}");

        write_config(0);
        reload_prometheus_config(&format!("http://127.0.0.1:{}/-/reload", port.0));
        wait_for_query(200);
    }
}

#[test]
fn test_reload_config_log_level() {
    use server::reload::{ReloadContext, Reloader, reload_config};
    let tmp_dir = TempDir::new();
    let config_file = tmp_dir.join("prometheus.yml");
    std::fs::write(&config_file, "runtime:\n  log_level: debug\n").unwrap();

    let output = util::logging::Buffer::new();
    let root = util::logging::Logger::new(util::logging::Config {
        level: util::logging::Level::parse("info").unwrap(),
        format: util::logging::Format::Logfmt,
        writer: Some(Box::new(output.clone())),
    });
    let logger = server::run::new_logger(root.clone());
    let interval = server::helpers::SafeNoStepSubqueryInterval::default();
    let rc = ReloadContext {
        filename: &config_file,
        enable_exemplar_storage: false,
        agent_mode: false,
        logger: &logger,
        no_step_subquery_interval: &interval,
        root_logger: &root,
    };
    let level = || root.level().map(|l| l.to_string()).unwrap_or_default();
    // Go `output.Reset()`: the buffer only grows, so remember where the output starts.
    let mark = std::cell::Cell::new(0usize);
    let reset = || mark.set(output.contents().len());
    let out = || output.contents()[mark.get()..].to_string();

    reload_config(&rc, &mut |_| {}, &[]).unwrap();
    assert_eq!("debug", level());
    reset();
    logger.debug("debug message", &[]);
    assert!(out().contains("debug message"));

    std::fs::write(&config_file, "runtime:\n  log_level: error\n").unwrap();
    let err = reload_config(&rc, &mut |_| {}, &[Reloader::new("failing", |_| Err("failed to apply".to_string()))]);
    assert!(err.is_err());
    assert_eq!("debug", level());
    reset();
    logger.debug("still visible", &[]);
    assert!(out().contains("still visible"));

    reload_config(&rc, &mut |_| {}, &[]).unwrap();
    assert_eq!("error", level());
    reset();
    logger.warn("hidden warning", &[]);
    assert!(out().is_empty(), "{}", out());
    logger.error("visible error", &[]);
    assert!(out().contains("visible error"));

    std::fs::write(&config_file, "{}\n").unwrap();
    reload_config(&rc, &mut |_| {}, &[]).unwrap();
    assert_eq!("info", level());
    reset();
    logger.info("visible info", &[]);
    assert!(out().contains("visible info"));
}

/// TestHeadCompactionWhileScraping verifies that running a head compaction concurrently with a scrape does not
/// trigger the data race described in https://github.com/prometheus/prometheus/issues/16490.
#[test]
fn test_head_compaction_while_scraping() {
    let _g = serial();
    // To increase the chance of reproducing the data race
    for i in 0..5 {
        let tmp_dir = TempDir::new();
        let config_file = tmp_dir.join("prometheus.yml");
        let port = Port::new();
        let config = format!(
            "\nscrape_configs:\n  - job_name: 'self1'\n    scrape_interval: 61ms\n    static_configs:\n      - targets: ['localhost:{p}']\n  - job_name: 'self2'\n    scrape_interval: 67ms\n    static_configs:\n      - targets: ['localhost:{p}']\n",
            p = port.0
        );
        std::fs::write(&config_file, config).unwrap();
        let prom = prometheus_command_with_logging(
            &config_file,
            port.0,
            &[
                format!("--storage.tsdb.path={}", tmp_dir.path().display()),
                s("--storage.tsdb.min-block-duration=100ms"),
                s("--scrape.discovery-reload-interval=100ms"), // Reduce initial wait time from default of 5 seconds.
            ],
        );
        let ok = eventually(Duration::from_secs(15), Duration::from_millis(500), || {
            let Ok(r) = get(&format!("http://127.0.0.1:{}/metrics", port.0)) else { return false };
            if r.status != 200 {
                return false;
            }
            // Wait for some compactions to run
            let Ok(compactions) = get_metric_value(&r.body, "prometheus_tsdb_compactions_total") else { return false };
            if compactions < 3.0 {
                return false;
            }
            // Sanity check: Some actual scraping was done.
            let series = get_metric_value(&r.body, "prometheus_tsdb_head_series_created_total").unwrap();
            assert_ne!(0.0, series, "{i}");
            // No compaction must have failed
            let failures = get_metric_value(&r.body, "prometheus_tsdb_compactions_failed_total").unwrap();
            assert_eq!(0.0, failures, "{i}");
            true
        });
        assert!(ok, "{i}: condition never satisfied\n{}", prom.logs());
        prom.signal(libc::SIGINT);
    }
}

/// A minimal `httptest.Server`: answers every request with 200 after calling `handler` with the request head
/// and body.
struct TestServer {
    url: String,
    stop: Arc<AtomicBool>,
    port: u16,
}

impl TestServer {
    fn new(handler: impl Fn(&str, &[u8]) + Send + Sync + 'static) -> TestServer {
        let port = random_port();
        let l = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind test server");
        l.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let st = stop.clone();
        let handler = Arc::new(handler);
        std::thread::spawn(move || {
            while !st.load(Ordering::SeqCst) {
                match l.accept() {
                    Ok((c, _)) => {
                        let handler = handler.clone();
                        let st = st.clone();
                        std::thread::spawn(move || serve_conn(c, &*handler, &st));
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        TestServer { url: format!("http://127.0.0.1:{port}"), stop, port }
    }
}

fn serve_conn(c: std::net::TcpStream, handler: &(dyn Fn(&str, &[u8]) + Send + Sync), stop: &AtomicBool) {
    let _ = c.set_nonblocking(false);
    let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
    let mut w = match c.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut r = BufReader::new(c);
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let mut head = String::new();
        let mut line = String::new();
        match r.read_line(&mut line) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(_) => return,
        }
        head.push_str(&line);
        let mut len = 0usize;
        loop {
            line.clear();
            if r.read_line(&mut line).unwrap_or(0) == 0 {
                return;
            }
            if line == "\r\n" || line == "\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case("content-length") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
            head.push_str(&line);
        }
        let mut body = vec![0u8; len];
        if r.read_exact(&mut body).is_err() {
            return;
        }
        handler(&head, &body);
        if w.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").is_err() {
            return;
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(600));
        release_port(self.port);
    }
}

/// This test verifies that metrics for the highest timestamps per queue account for relabelling.
/// See: https://github.com/prometheus/prometheus/pull/17065.
#[test]
fn test_remote_write_per_queue_metrics_after_relabeling() {
    let _g = serial();
    let tmp_dir = TempDir::new();
    let config_file = tmp_dir.join("prometheus.yml");
    let port = Port::new();
    let target_port = Port::new();

    let reached: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let rc = reached.clone();
    let server = TestServer::new(move |head, body| {
        *rc.lock().unwrap() = Some(format!(
            "should never be reached because the remote write relabeling shouldn't yield anything: header: {head}, body: {}",
            String::from_utf8_lossy(body)
        ));
    });

    // Simulate a remote write relabeling that doesn't yield any series.
    let config = format!(
        "\nglobal:\n  scrape_interval: 1s\nscrape_configs:\n  - job_name: 'self'\n    static_configs:\n      - targets: ['localhost:{}']\n  - job_name: 'target'\n    static_configs:\n      - targets: ['localhost:{}']\n\nremote_write:\n  - url: {}\n    write_relabel_configs:\n      - source_labels: [job,__name__]\n        regex: 'target,special_metric'\n        action: keep\n",
        port.0, target_port.0, server.url
    );
    std::fs::write(&config_file, config).unwrap();
    let prom = prometheus_command_with_logging(
        &config_file,
        port.0,
        &[format!("--storage.tsdb.path={}", tmp_dir.path().display())],
    );

    let ok = eventually(Duration::from_secs(10), Duration::from_millis(100), || {
        let Ok(r) = get(&format!("http://127.0.0.1:{}/metrics", port.0)) else { return false };
        if r.status != 200 {
            return false;
        }
        let m = &r.body;
        // The highest timestamp at storage level sees all samples, it should also consider the ones that are
        // filtered out by relabeling.
        match get_metric_value(m, "prometheus_remote_storage_highest_timestamp_in_seconds") {
            Ok(v) if v != 0.0 => {}
            _ => return false,
        }
        // The queue shouldn't see and send any sample, all samples are dropped due to relabeling, the metrics
        // should reflect that.
        match get_metric_value(m, "prometheus_remote_storage_samples_dropped_total") {
            Ok(v) if v != 0.0 => {}
            _ => return false,
        }
        let highest_timestamp = get_metric_value(m, "prometheus_remote_storage_queue_highest_timestamp_seconds").unwrap();
        assert_eq!(0.0, highest_timestamp);
        let highest_sent_timestamp =
            get_metric_value(m, "prometheus_remote_storage_queue_highest_sent_timestamp_seconds").unwrap();
        assert_eq!(0.0, highest_sent_timestamp);
        true
    });
    assert!(ok, "condition never satisfied\n{}", prom.logs());
    if let Some(e) = reached.lock().unwrap().take() {
        panic!("{e}");
    }
}

/// TestRemoteWrite_ReshardingWithoutDeadlock ensures that resharding (scaling up) doesn't block when the shards
/// are full. See: https://github.com/prometheus/prometheus/issues/17384.
#[test]
fn test_remote_write_resharding_without_deadlock() {
    let _g = serial();
    let tmp_dir = TempDir::new();
    let config_file = tmp_dir.join("prometheus.yml");
    let port = Port::new();

    let server = TestServer::new(|_, _| std::thread::sleep(Duration::from_secs(1)));

    let config = format!(
        "\nglobal:\n  # Using a smaller interval may cause the scrape to time out.\n  scrape_interval: 1s\nscrape_configs:\n  - job_name: 'self'\n    static_configs:\n      - targets: ['localhost:{}']\n\nremote_write:\n  - url: {}\n    queue_config:\n      # Speed up the queue being full.\n      capacity: 1\n      # Helps keep the \u{201c}time to send one sample\u{201d} low so it doesn\u{2019}t influence the resharding logic.\n      max_samples_per_send: 1\n",
        port.0, server.url
    );
    std::fs::write(&config_file, config).unwrap();
    let prom = prometheus_command_with_logging(
        &config_file,
        port.0,
        &[format!("--storage.tsdb.path={}", tmp_dir.path().display()), s("--log.level=debug")],
    );

    const DESIRED_SHARDS_METRIC: &str = "prometheus_remote_storage_shards_desired";
    let get_metrics = || -> Result<String, String> {
        let r = get(&format!("http://127.0.0.1:{}/metrics", port.0))?;
        if r.status != 200 {
            return Err(format!("unexpected status code: {}", r.status));
        }
        Ok(r.body)
    };

    // Ensure the initial desired shards is 1.
    let ok = eventually(Duration::from_secs(10), Duration::from_millis(100), || {
        let Ok(m) = get_metrics() else { return false };
        matches!(get_metric_value(&m, DESIRED_SHARDS_METRIC), Ok(v) if v == 1.0)
    });
    assert!(ok, "initial desired shards is not 1\n{}", prom.logs());

    // Ensure scaling up is triggered after some time.
    let ok = eventually(Duration::from_secs(30), Duration::from_secs(1), || {
        let Ok(m) = get_metrics() else { return false };
        matches!(get_metric_value(&m, DESIRED_SHARDS_METRIC), Ok(v) if v > 1.0)
        // 3*shardUpdateDuration to allow for the resharding logic to run.
    });
    assert!(ok, "desired shards never went above 1");
    drop(prom);
}

/// TestFeatureFlagsDocumented ensures the --enable-feature help text and the documented flags in
/// docs/feature_flags.md list the same set of flags.
// adapted: Go reads the help text from main.go; the Rust help text is `server::flags::ENABLE_FEATURE_HELP`
// (the string `app()` gives the flag), matched with the same expression.
#[test]
fn test_feature_flags_documented() {
    let h = server::flags::ENABLE_FEATURE_HELP;
    let flag_help = server::flags::app("prometheus")
        .flags
        .iter()
        .find(|f| f.name == "enable-feature")
        .map(|f| f.help.clone())
        .expect("enable-feature flag");
    assert_eq!(h, flag_help);
    let m = regex::Regex::new(r"(?s)^Comma separated feature names to enable. Valid options: (.+?)\.")
        .unwrap()
        .captures(h)
        .expect("help text matches");
    let help_flags: Vec<String> = m[1].split(',').map(|f| f.trim().to_string()).collect();
    assert!(!help_flags.is_empty());

    let d = std::fs::read_to_string(upstream().join("docs/feature_flags.md")).unwrap();
    let mut doc_flags: Vec<String> = Vec::new();
    for dm in regex::Regex::new(r"(?m)^`--enable-feature=(.+)`$").unwrap().captures_iter(&d) {
        doc_flags.push(dm[1].to_string());
    }
    assert!(!doc_flags.is_empty());
    assert!(help_flags.windows(2).all(|w| w[0] <= w[1]), "help flags are not sorted: {help_flags:?}");
    let mut a = help_flags.clone();
    let mut b = doc_flags.clone();
    a.sort();
    b.sort();
    assert_eq!(a, b);
}
