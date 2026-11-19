// Port of notifier/manager_test.go.
//
// adapted (whole file): Go's synctest bubbles (fake time) become real time with the same durations, and
// `synctest.Wait()` becomes a join of the threads the test owns or a sleep past the last deadline. Waits that
// only bound how long a test may hang (`require.Eventually`, `time.After`) are 10s instead of Go's 2s/5s,
// because the machine running the tests is heavily loaded; the checked values are unchanged.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use common::{alert, alerts_equal, decode_alerts, eventually, TestServer};
use config::{default_alertmanager_config, AlertmanagerConfig};
use discovery::manager::TargetSets;
use discovery::metrics::Registry;
use discovery::{CancelToken, Configs, Group, Logger, StaticConfig};
use model::labels::{ValidationScheme, ALERT_NAME};
use model::labelset::{LabelSet, ADDRESS_LABEL};
use model::relabel::{self, Action};
use notifier::{Alert, AlertMetrics, AlertmanagerSet, Context, Manager, Options, DEFAULT_MAX_BATCH_SIZE};

const WAIT: Duration = Duration::from_secs(10);
const TICK: Duration = Duration::from_millis(10);

/// Go's `errc` channel with capacity 1 and non-blocking sends.
#[derive(Clone, Default)]
struct ErrC(Arc<Mutex<Option<String>>>);
impl ErrC {
    fn put(&self, e: String) {
        let mut g = self.0.lock().unwrap();
        if g.is_none() {
            *g = Some(e);
        }
    }
    /// Go `checkNoErr`.
    fn check_no_err(&self) {
        if let Some(e) = self.0.lock().unwrap().take() {
            panic!("{e}");
        }
    }
}

type Expected = Arc<Mutex<Vec<Alert>>>;

/// Go `newTestHTTPServerBuilder`.
fn new_test_http_server_builder(expected: Expected, errc: ErrC, u: &str, p: &str, status: Arc<AtomicI32>) -> TestServer {
    let (u, p) = (u.to_string(), p.to_string());
    TestServer::new(move |r| {
        let (user, pass) = r.basic_auth();
        if user != u || pass != p {
            errc.put(format!("unexpected user/password: {user}/{pass} != {u}/{p}"));
            return 500;
        }
        let res = decode_alerts(&r.body).and_then(|alerts| alerts_equal(&expected.lock().unwrap(), &alerts));
        if let Err(e) = res {
            errc.put(e);
        }
        status.load(Ordering::SeqCst) as u16
    })
}

/// Go `newTestAlertmanagerSet`.
fn new_test_alertmanager_set(cfg: AlertmanagerConfig, client: Option<net::Client>, opts: Arc<Options>, metrics: Arc<AlertMetrics>, urls: &[&str]) -> Arc<AlertmanagerSet> {
    Arc::new(AlertmanagerSet::with_alertmanagers(cfg, client, opts, metrics, urls))
}

fn cfg_with_timeout(d: Duration) -> AlertmanagerConfig {
    let mut c = default_alertmanager_config();
    c.timeout = yaml::Duration(d.as_nanos() as i64);
    c
}

fn sets(v: Vec<(&str, Arc<AlertmanagerSet>)>) -> BTreeMap<String, Arc<AlertmanagerSet>> {
    v.into_iter().map(|(k, s)| (k.to_string(), s)).collect()
}

fn start_send_loops(m: &Manager) {
    for ams in m.alertmanager_sets().values() {
        ams.start_send_loops();
    }
}

/// Stops the send loops of all sets when dropped (Go's deferred `cleanSendLoops`).
struct CleanLoops<'a>(&'a Manager);
impl Drop for CleanLoops<'_> {
    fn drop(&mut self) {
        for ams in self.0.alertmanager_sets().values() {
            ams.clean_send_loops();
        }
    }
}

fn sent(m: &Manager, url: &str) -> f64 {
    m.metrics().sent.with_label_values(&[url]).get()
}
fn errors(m: &Manager, url: &str) -> f64 {
    m.metrics().errors.with_label_values(&[url]).get()
}

fn relabel_cfg(source: &str, action: Action, regex: &str, target: &str, replacement: &str) -> relabel::Config {
    relabel::Config {
        source_labels: Some(vec![source.to_string()]),
        action,
        regex: relabel::Regexp::must_new(regex),
        target_label: target.to_string(),
        replacement: replacement.to_string(),
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    }
}

fn load_config(s: &str) -> config::Config {
    let mut cfg = config::Config::default();
    yaml::unmarshal_into(s, &mut cfg, true).expect("Unable to load YAML config.");
    cfg
}

fn tgs(v: Vec<(&str, Group)>) -> TargetSets {
    v.into_iter().map(|(k, g)| (k.to_string(), vec![Arc::new(g)])).collect()
}

fn addr_group(addr: &str) -> Group {
    Group { targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, addr)])], ..Default::default() }
}

fn ok_response(req: net::Request) -> net::Response {
    net::Response { status: 200, status_text: "200 OK".to_string(), header: net::Header::new(), body: Vec::new(), request: req }
}

#[test]
fn test_handler_send_all() {
    let errc = ErrC::default();
    let expected: Expected = Arc::new(Mutex::new(Vec::new()));
    let (status1, status2, status3) = (Arc::new(AtomicI32::new(200)), Arc::new(AtomicI32::new(200)), Arc::new(AtomicI32::new(200)));

    let server1 = new_test_http_server_builder(expected.clone(), errc.clone(), "prometheus", "testing_password", status1.clone());
    let server2 = new_test_http_server_builder(expected.clone(), errc.clone(), "", "", status2.clone());
    let server3 = new_test_http_server_builder(expected.clone(), errc.clone(), "", "", status3.clone());

    let reg = Registry::new();
    let h = Manager::new(Options { registerer: Some(reg), ..Default::default() }, ValidationScheme::Utf8, None);

    let mut auth_cfg = net::HttpClientConfig::default_config();
    auth_cfg.basic_auth = Some(net::BasicAuth { username: "prometheus".to_string(), password: yaml::Secret("testing_password".to_string()), ..Default::default() });
    let auth_client = net::new_client_from_config(&auth_cfg, &net::HttpClientOptions::default()).unwrap();

    let opts = Arc::new(Options {
        do_fn: Some(Arc::new(notifier::send_loop::do_request)),
        queue_capacity: 10_000,
        max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        ..Default::default()
    });
    h.set_alertmanager_sets(sets(vec![
        ("1", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), Some(auth_client), opts.clone(), h.metrics(), &[&server1.url])),
        ("2", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, opts.clone(), h.metrics(), &[&server2.url, &server3.url])),
        ("3", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, opts.clone(), h.metrics(), &[])),
    ]));

    let mut alerts = Vec::new();
    for i in 0..DEFAULT_MAX_BATCH_SIZE {
        alerts.push(alert(&["alertname", &i.to_string()]));
        expected.lock().unwrap().push(alert(&["alertname", &i.to_string()]));
    }
    let n = DEFAULT_MAX_BATCH_SIZE as f64;

    // Start send loops.
    start_send_loops(&h);
    let _clean = CleanLoops(&h);

    h.send(alerts.clone());
    assert!(eventually(WAIT, TICK, || sent(&h, &server1.url) == n));
    errc.check_no_err();

    // The only am in set 1 is down.
    status1.store(404, Ordering::SeqCst);
    h.send(alerts.clone());
    // Wait for all send loops to process before changing any status.
    assert!(eventually(WAIT, TICK, || errors(&h, &server1.url) == n && sent(&h, &server2.url) == n * 2.0 && sent(&h, &server3.url) == n * 2.0));
    errc.check_no_err();

    // Fix the am.
    status1.store(200, Ordering::SeqCst);

    // Only one of the ams in set 2 is down.
    status2.store(500, Ordering::SeqCst);
    h.send(alerts.clone());
    // Wait for all send loops to either send or fail with errors depending on their status.
    assert!(eventually(WAIT, TICK, || errors(&h, &server2.url) == n && sent(&h, &server1.url) == n * 2.0 && sent(&h, &server3.url) == n * 3.0));
    errc.check_no_err();

    // Both ams in set 2 are down.
    status3.store(500, Ordering::SeqCst);
    h.send(alerts.clone());
    assert!(eventually(WAIT, TICK, || errors(&h, &server2.url) == n * 2.0 && errors(&h, &server3.url) == n));
    errc.check_no_err();
}

#[test]
fn test_handler_send_all_remap_per_am() {
    let errc = ErrC::default();
    let (expected1, expected2, expected3): (Expected, Expected, Expected) = Default::default();
    let (status1, status2, status3) = (Arc::new(AtomicI32::new(200)), Arc::new(AtomicI32::new(200)), Arc::new(AtomicI32::new(200)));

    let server1 = new_test_http_server_builder(expected1.clone(), errc.clone(), "", "", status1.clone());
    let server2 = new_test_http_server_builder(expected2.clone(), errc.clone(), "", "", status2.clone());
    let server3 = new_test_http_server_builder(expected3.clone(), errc.clone(), "", "", status3.clone());

    let reg = Registry::new();
    let h = Manager::new(Options { queue_capacity: 10_000, registerer: Some(reg), ..Default::default() }, ValidationScheme::Utf8, None);

    let am1_cfg = cfg_with_timeout(Duration::from_secs(1));
    let mut am2_cfg = cfg_with_timeout(Duration::from_secs(1));
    am2_cfg.alert_relabel_configs = vec![relabel_cfg("alertnamedrop", Action::Drop, ".+", "", "")];
    let mut am3_cfg = cfg_with_timeout(Duration::from_secs(1));
    am3_cfg.alert_relabel_configs = vec![relabel_cfg("alertname", Action::Drop, ".+", "", "")];

    h.set_alertmanager_sets(sets(vec![
        // Drop no alerts.
        ("1", new_test_alertmanager_set(am1_cfg, None, h.opts(), h.metrics(), &[&server1.url])),
        // Drop only alerts with the "alertnamedrop" label.
        ("2", new_test_alertmanager_set(am2_cfg, None, h.opts(), h.metrics(), &[&server2.url])),
        // Drop all alerts.
        ("3", new_test_alertmanager_set(am3_cfg, None, h.opts(), h.metrics(), &[&server3.url])),
        // Empty list of Alertmanager endpoints.
        ("4", new_test_alertmanager_set(default_alertmanager_config(), None, h.opts(), h.metrics(), &[])),
    ]));

    let mut alerts = Vec::new();
    for i in 0..DEFAULT_MAX_BATCH_SIZE / 2 {
        let s = i.to_string();
        alerts.push(alert(&["alertname", &s]));
        alerts.push(alert(&["alertname", "test", "alertnamedrop", &s]));
        expected1.lock().unwrap().push(alert(&["alertname", &s]));
        expected1.lock().unwrap().push(alert(&["alertname", "test", "alertnamedrop", &s]));
        expected2.lock().unwrap().push(alert(&["alertname", &s]));
    }

    // Start send loops.
    start_send_loops(&h);
    let _clean = CleanLoops(&h);

    // All ams are up.
    h.send(alerts.clone());
    assert!(eventually(WAIT, TICK, || sent(&h, &server1.url) == DEFAULT_MAX_BATCH_SIZE as f64));
    errc.check_no_err();

    // The only am in set 1 goes down.
    status1.store(500, Ordering::SeqCst);
    h.send(alerts.clone());
    // Wait for metrics to update.
    assert!(eventually(WAIT, TICK, || errors(&h, &server1.url) == DEFAULT_MAX_BATCH_SIZE as f64));
    errc.check_no_err();

    // Reset set 1.
    status1.store(200, Ordering::SeqCst);

    // Set 3 loses its only am, but all alerts were dropped so there was nothing to send, keeping sendAll true.
    status3.store(500, Ordering::SeqCst);
    h.send(alerts);
    errc.check_no_err();
}

#[test]
fn test_external_labels() {
    let reg = Registry::new();
    let h = Manager::new(
        Options {
            queue_capacity: 3 * DEFAULT_MAX_BATCH_SIZE,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            external_labels: model::labels::from_strings(&["a", "b"]),
            relabel_configs: vec![relabel_cfg("alertname", Action::Replace, "externalrelabelthis", "a", "c")],
            registerer: Some(reg),
            ..Default::default()
        },
        ValidationScheme::Utf8,
        None,
    );

    h.set_alertmanager_sets(sets(vec![("test", new_test_alertmanager_set(default_alertmanager_config(), None, h.opts(), h.metrics(), &["test"]))]));

    // This alert should get the external label attached.
    h.send(vec![alert(&["alertname", "test"])]);

    // This alert should get the external label attached, but then set to "c" through relabelling.
    h.send(vec![alert(&["alertname", "externalrelabelthis"])]);

    let expected = vec![alert(&["alertname", "test", "a", "b"]), alert(&["alertname", "externalrelabelthis", "a", "c"])];

    alerts_equal(&expected, &h.alertmanager_sets()["test"].send_loops()["test"].queue()).unwrap();
}

#[test]
fn test_handler_relabel() {
    let reg = Registry::new();
    let h = Manager::new(
        Options {
            queue_capacity: 3 * DEFAULT_MAX_BATCH_SIZE,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            relabel_configs: vec![relabel_cfg("alertname", Action::Drop, "drop", "", ""), relabel_cfg("alertname", Action::Replace, "rename", "alertname", "renamed")],
            registerer: Some(reg),
            ..Default::default()
        },
        ValidationScheme::Utf8,
        None,
    );

    h.set_alertmanager_sets(sets(vec![("test", new_test_alertmanager_set(default_alertmanager_config(), None, h.opts(), h.metrics(), &["test"]))]));

    // This alert should be dropped due to the configuration.
    h.send(vec![alert(&["alertname", "drop"])]);

    // This alert should be replaced due to the configuration.
    h.send(vec![alert(&["alertname", "rename"])]);

    let expected = vec![alert(&["alertname", "renamed"])];

    alerts_equal(&expected, &h.alertmanager_sets()["test"].send_loops()["test"].queue()).unwrap();
}

#[test]
fn test_handler_queuing() {
    // Rendezvous channels, like Go's unbuffered channels.
    let (called_tx, called_rx) = mpsc::sync_channel::<()>(0);
    let (expected_tx, expected_rx) = mpsc::sync_channel::<Vec<Alert>>(0);
    let (errc_tx, errc_rx) = mpsc::sync_channel::<Result<(), String>>(1);
    let (called_tx, expected_rx) = (Mutex::new(called_tx), Arc::new(Mutex::new(expected_rx)));

    let server = TestServer::new(move |r| {
        // Notify the test function that we have received something.
        if called_tx.lock().unwrap().send(()).is_err() {
            return 200;
        }
        // Wait for the test function to unblock us.
        let Ok(expected) = expected_rx.lock().unwrap().recv() else { return 200 };
        let res = decode_alerts(&r.body).and_then(|alerts| alerts_equal(&expected, &alerts));
        let _ = errc_tx.try_send(res);
        200
    });

    let reg = Registry::new();
    let h = Arc::new(Manager::new(
        Options { queue_capacity: 3 * DEFAULT_MAX_BATCH_SIZE, max_batch_size: DEFAULT_MAX_BATCH_SIZE, registerer: Some(reg), ..Default::default() },
        ValidationScheme::Utf8,
        None,
    ));
    h.set_alertmanager_sets(sets(vec![("1", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, h.opts(), h.metrics(), &[&server.url]))]));

    let h2 = h.clone();
    let run = std::thread::spawn(move || h2.run(None));

    // Start send loops.
    start_send_loops(&h);

    let alerts: Vec<Alert> = (0..20 * DEFAULT_MAX_BATCH_SIZE).map(|i| alert(&["alertname", &i.to_string()])).collect();
    let b = DEFAULT_MAX_BATCH_SIZE;

    let assert_alerts = |expected: &[Alert]| {
        let end = Instant::now() + WAIT;
        loop {
            if called_rx.try_recv().is_ok() {
                expected_tx.send(expected.to_vec()).unwrap();
            }
            if let Ok(res) = errc_rx.try_recv() {
                res.unwrap();
                return;
            }
            assert!(Instant::now() < end, "Alerts were not pushed.");
            std::thread::sleep(Duration::from_millis(1));
        }
    };

    // Send one batch, wait for it to arrive and block the server so the queue fills up.
    h.send(alerts[..b].to_vec());
    called_rx.recv_timeout(WAIT).expect("first request");

    // Send several batches while the server is still blocked so the queue fills up to its maximum capacity
    // (3*DEFAULT_MAX_BATCH_SIZE). Then check that the queue is truncated in the front.
    h.send(alerts[b..2 * b].to_vec()); // This batch should be dropped.
    h.send(alerts[2 * b..3 * b].to_vec());
    h.send(alerts[3 * b..4 * b].to_vec());

    // Send the batch that drops the first one.
    h.send(alerts[4 * b..5 * b].to_vec());

    // Unblock the server.
    expected_tx.send(alerts[..b].to_vec()).unwrap();
    match errc_rx.recv_timeout(WAIT) {
        Ok(res) => res.unwrap(),
        Err(_) => panic!("Alerts were not pushed."),
    }

    // Verify that we receive the last 3 batches.
    for i in 2..5 {
        assert_alerts(&alerts[i * b..(i + 1) * b]);
    }

    h.stop();
    drop(called_rx);
    drop(expected_tx);
    run.join().unwrap();
}

fn reload_first(n: &Manager, cfg: &config::Config, g: Group) {
    // Go takes the first key of the (random-order) ToMap; with one config it is "config-0".
    let k = cfg.alerting_config.alertmanager_configs_map().keys().next().unwrap().clone();
    n.reload(&tgs(vec![(&k, g)]));
}

#[test]
fn test_reload() {
    let tests = [(addr_group("alertmanager:9093"), "http://alertmanager:9093/api/v2/alerts")];

    let n = Manager::new(Options::default(), ValidationScheme::Utf8, None);
    let cfg = load_config("\nalerting:\n  alertmanagers:\n  - static_configs:\n");
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 1);

    n.apply_config(&cfg).expect("Error applying the config.");

    for (input, out) in tests {
        reload_first(&n, &cfg, input);
        let res = n.alertmanagers()[0].to_url_string();
        assert_eq!(out, res);
    }
}

#[test]
fn test_dropped_alertmanagers() {
    let tests = [(addr_group("alertmanager:9093"), "http://alertmanager:9093/api/v2/alerts")];

    let n = Manager::new(Options::default(), ValidationScheme::Utf8, None);
    let s = "
alerting:
  alertmanagers:
  - static_configs:
    relabel_configs:
      - source_labels: ['__address__']
        regex: 'alertmanager:9093'
        action: drop
";
    let cfg = load_config(s);
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 1);

    n.apply_config(&cfg).expect("Error applying the config.");

    for (input, out) in tests {
        reload_first(&n, &cfg, input);
        let res = n.dropped_alertmanagers()[0].to_url_string();
        assert_eq!(res, out);
    }
}

/// Ensures that the notifier takes SD changes into account even when there are queued alerts
/// (https://github.com/prometheus/prometheus/issues/13676 and #8768).
#[test]
fn test_hanging_notifier() {
    const BATCHES: usize = 100;
    const ALERTS_COUNT: usize = DEFAULT_MAX_BATCH_SIZE * BATCHES;
    const FAULTY_URL: &str = "http://faulty:9093/api/v2/alerts";
    const FUNCTIONAL_URL: &str = "http://functional:9093/api/v2/alerts";
    let send_timeout = Duration::from_millis(100);
    let sd_updatert = send_timeout / 2;

    // Track which alertmanagers have been called.
    let faulty_called = Arc::new(AtomicBool::new(false));
    let functional_called = Arc::new(AtomicBool::new(false));

    // Fake Do function that simulates the alertmanagers in-process.
    let (fc, gc) = (faulty_called.clone(), functional_called.clone());
    let fake_do = move |ctx: &Context, _c: Option<&net::Client>, req: net::Request| -> Result<net::Response, net::Error> {
        let url = req.url.to_url_string();
        if url.contains("faulty") {
            fc.store(true, Ordering::SeqCst);
            // The faulty alertmanager hangs until the context is done (by timeout).
            ctx.wait_done();
            return Err(ctx.err().unwrap_or_else(net::Error::timeout));
        }
        // The functional alertmanager responds successfully, after simulated network latency.
        gc.store(true, Ordering::SeqCst);
        std::thread::sleep(send_timeout / 2);
        Ok(ok_response(req))
    };

    // Initialize the discovery manager.
    let ctx = CancelToken::new();
    let reg = Registry::new();
    let sd_metrics = discovery::create_and_register_sd_metrics(&reg).unwrap();
    let sd_manager = discovery::Manager::new(
        &ctx,
        Logger::nop(),
        &reg,
        &sd_metrics,
        discovery::ManagerOptions { name: "sd-manager".to_string(), updatert: sd_updatert, ..Default::default() },
    )
    .unwrap();
    let sdm = sd_manager.clone();
    let sd_run = std::thread::spawn(move || sdm.run());

    // Set up the notifier with both faulty and functional Alertmanagers.
    let notifier = Arc::new(Manager::new(
        Options { queue_capacity: ALERTS_COUNT, registerer: Some(reg.clone()), do_fn: Some(Arc::new(fake_do)), ..Default::default() },
        ValidationScheme::Utf8,
        None,
    ));
    notifier.set_alertmanager_sets(sets(vec![(
        "config-0",
        new_test_alertmanager_set(cfg_with_timeout(send_timeout), None, notifier.opts(), notifier.metrics(), &[FAULTY_URL, FUNCTIONAL_URL]),
    )]));
    start_send_loops(&notifier);

    let n2 = notifier.clone();
    let rx = sd_manager.sync_ch();
    let run = std::thread::spawn(move || n2.run(Some(Box::new(rx))));

    assert_eq!(notifier.alertmanagers().len(), 2);

    // Enqueue the alerts.
    let alerts: Vec<Alert> = (0..ALERTS_COUNT).map(|i| alert(&["alertname", &i.to_string()])).collect();
    notifier.send(alerts);

    // Wait for the Alertmanagers to start receiving alerts.
    assert!(eventually(WAIT, sd_updatert, || faulty_called.load(Ordering::SeqCst) && functional_called.load(Ordering::SeqCst)));

    // Request to remove the faulty Alertmanager.
    let mut c = BTreeMap::new();
    c.insert("config-0".to_string(), Configs(vec![Box::new(StaticConfig(vec![addr_group("functional:9093")]))]));
    sd_manager.apply_config(&c);

    // Wait for the discovery update to be processed. The faulty alertmanager should be dropped without
    // waiting for its queue to drain.
    assert!(eventually(WAIT, Duration::from_millis(1), || notifier.alertmanagers().len() == 1));
    // The notifier should not wait until the alerts queue of the functional am is empty to apply the
    // discovery changes.
    assert_ne!(0, notifier.alertmanager_sets()["config-0"].send_loops()[FUNCTIONAL_URL].queue_len());

    notifier.stop();
    run.join().unwrap();
    ctx.cancel();
    let _ = sd_run.join();
    // Let in-flight request timeouts fire.
    std::thread::sleep(send_timeout * 2);
}

#[test]
fn test_stop_draining_disabled() {
    const URL: &str = "http://alertmanager:9093/api/v2/alerts";

    let (started_tx, started_rx) = mpsc::sync_channel::<()>(0);
    let started_tx = Mutex::new(started_tx);
    let alerts_received = Arc::new(AtomicI64::new(0));

    // Fake Do function that simulates a hanging alertmanager that times out.
    let ar = alerts_received.clone();
    let fake_do = move |ctx: &Context, _c: Option<&net::Client>, req: net::Request| -> Result<net::Response, net::Error> {
        let alerts = decode_alerts(&req.body).map_err(|e| net::Error::other(format!("unmarshal request body: {e}")))?;
        ar.fetch_add(alerts.len() as i64, Ordering::SeqCst);
        // Signal arrival, then block until the context times out.
        let _ = started_tx.lock().unwrap().send(());
        ctx.wait_done();
        Err(ctx.err().unwrap_or_else(net::Error::timeout))
    };

    let reg = Registry::new();
    let m = Arc::new(Manager::new(
        Options { queue_capacity: 10, drain_on_shutdown: false, registerer: Some(reg), do_fn: Some(Arc::new(fake_do)), ..Default::default() },
        ValidationScheme::Utf8,
        None,
    ));
    m.set_alertmanager_sets(sets(vec![("1", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, m.opts(), m.metrics(), &[URL]))]));
    start_send_loops(&m);

    let m2 = m.clone();
    let run = std::thread::spawn(move || m2.run(None));

    // Queue two alerts. The first should be immediately sent to the receiver, which blocks until it times out.
    m.send(vec![alert(&[ALERT_NAME, "alert-1"])]);

    // Wait for the receiver to get the request.
    started_rx.recv_timeout(WAIT).expect("request");

    m.send(vec![alert(&[ALERT_NAME, "alert-2"])]);

    // Stop the notification manager, then let the request timeout fire.
    m.stop();
    run.join().unwrap();
    std::thread::sleep(Duration::from_secs(1) + Duration::from_millis(500));

    // Confirm only the first notification was sent. The second notification should be dropped.
    assert_eq!(1, alerts_received.load(Ordering::SeqCst));
}

#[test]
fn test_stop_draining_enabled() {
    const URL: &str = "http://alertmanager:9093/api/v2/alerts";

    let (started_tx, started_rx) = mpsc::sync_channel::<()>(1);
    let started_tx = Mutex::new(started_tx);
    let alerts_received = Arc::new(AtomicI64::new(0));

    // Fake Do function that simulates an alertmanager responding slowly but successfully.
    let ar = alerts_received.clone();
    let fake_do = move |_ctx: &Context, _c: Option<&net::Client>, req: net::Request| -> Result<net::Response, net::Error> {
        let alerts = decode_alerts(&req.body).map_err(|e| net::Error::other(format!("unmarshal request body: {e}")))?;
        ar.fetch_add(alerts.len() as i64, Ordering::SeqCst);
        // Signal arrival.
        let _ = started_tx.lock().unwrap().send(());
        // Block to allow alert-2 to be queued while this request is in flight.
        std::thread::sleep(Duration::from_millis(100));
        Ok(ok_response(req))
    };

    let reg = Registry::new();
    let m = Arc::new(Manager::new(
        Options { queue_capacity: 10, drain_on_shutdown: true, registerer: Some(reg), do_fn: Some(Arc::new(fake_do)), ..Default::default() },
        ValidationScheme::Utf8,
        None,
    ));
    m.set_alertmanager_sets(sets(vec![("1", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, m.opts(), m.metrics(), &[URL]))]));
    start_send_loops(&m);

    let m2 = m.clone();
    let run = std::thread::spawn(move || m2.run(None));

    // Queue two alerts. The first should be immediately sent to the receiver.
    m.send(vec![alert(&[ALERT_NAME, "alert-1"])]);

    // Wait for the receiver to get the first request.
    started_rx.recv_timeout(WAIT).expect("request");

    // Send the second alert while the first is still being processed (fake_do has a 100ms delay).
    m.send(vec![alert(&[ALERT_NAME, "alert-2"])]);

    // Stop the notification manager. With drain_on_shutdown, the queue is drained, so both alerts are sent.
    m.stop();

    // Let in-flight requests complete.
    std::thread::sleep(Duration::from_secs(1));
    run.join().unwrap();

    // Confirm both notifications were sent.
    assert_eq!(2, alerts_received.load(Ordering::SeqCst));
}

/// Go `newImmediateAlertManager`: answers 200 and signals `done` once.
fn new_immediate_alert_manager(done: SyncSender<()>) -> TestServer {
    TestServer::new(move |_r| {
        let _ = done.try_send(());
        200
    })
}

/// Ensures that when an alertmanagerSet disappears after apply_config, its send loop queues are drained only
/// when drain_on_shutdown is set.
#[test]
fn test_queues_draining_on_apply_config() {
    let sub = |drain_on_shutdown: bool| {
        let (tx, alert_sent) = mpsc::sync_channel::<()>(1);
        let server = new_immediate_alert_manager(tx);

        let h = Manager::new(Options { queue_capacity: 10, drain_on_shutdown, ..Default::default() }, ValidationScheme::Utf8, None);
        h.set_alertmanager_sets(sets(vec![("1", new_test_alertmanager_set(cfg_with_timeout(Duration::from_secs(1)), None, h.opts(), h.metrics(), &[&server.url]))]));

        // The send loops were not started, nothing will be sent.
        h.send(vec![alert(&["alertname", "foo"])]);

        // Remove the alertmanagerSet.
        h.apply_config(&config::Config::default()).unwrap();

        match alert_sent.recv_timeout(Duration::from_millis(100)) {
            Ok(()) => assert!(drain_on_shutdown, "no alert should be sent"),
            Err(_) => assert!(!drain_on_shutdown, "alert wasn't received"),
        }
    };
    let t = std::thread::spawn(move || sub(false));
    sub(true);
    t.join().unwrap();
}

#[test]
fn test_apply_config() {
    let target_url = "alertmanager:9093";
    let target_group = addr_group(target_url);
    let alertmanager_url = format!("http://{target_url}/api/v2/alerts");

    let n = Manager::new(Options::default(), ValidationScheme::Utf8, None);
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
";
    // 1. Ensure known alertmanagers are not dropped during apply_config.
    let cfg = load_config(s);
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 1);

    // First, apply the config and reload.
    n.apply_config(&cfg).unwrap();
    n.reload(&tgs(vec![("config-0", target_group.clone())]));
    assert_eq!(n.alertmanagers().len(), 1);
    assert_eq!(alertmanager_url, n.alertmanagers()[0].to_url_string());

    // Reapply the config.
    n.apply_config(&cfg).unwrap();
    // Ensure the known alertmanagers are not dropped.
    assert_eq!(n.alertmanagers().len(), 1);
    assert_eq!(alertmanager_url, n.alertmanagers()[0].to_url_string());

    // 2. Ensure known alertmanagers are not dropped during apply_config even when the config order changes.
    let s = "
alerting:
  alertmanagers:
  - static_configs:
  - file_sd_configs:
    - files:
      - foo.json
";
    let cfg = load_config(s);
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 2);

    n.apply_config(&cfg).unwrap();
    assert_eq!(n.alertmanagers().len(), 1);
    // Ensure no unnecessary alertmanagers are injected.
    assert!(n.alertmanager_sets()["config-0"].ams().is_empty());
    // Ensure the config order is taken into account.
    let ams = n.alertmanager_sets()["config-1"].ams();
    assert_eq!(ams.len(), 1);
    assert_eq!(alertmanager_url, ams[0].url_string());

    // 3. Ensure known alertmanagers are reused for new config with identical AlertmanagerConfig.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
  - file_sd_configs:
    - files:
      - foo.json
";
    let cfg = load_config(s);
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 2);

    n.apply_config(&cfg).unwrap();
    assert_eq!(n.alertmanagers().len(), 2);
    for i in 0..2 {
        let ams = n.alertmanager_sets()[&format!("config-{i}")].ams();
        assert_eq!(ams.len(), 1);
        assert_eq!(alertmanager_url, ams[0].url_string());
    }

    // 4. Ensure known alertmanagers are reused only for identical AlertmanagerConfig.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
    path_prefix: /bar
  - file_sd_configs:
    - files:
      - foo.json
    relabel_configs:
    - source_labels: ['__address__']
      regex: 'doesntmatter:1234'
      action: drop
";
    let cfg = load_config(s);
    assert_eq!(cfg.alerting_config.alertmanager_configs.len(), 2);

    n.apply_config(&cfg).unwrap();
    assert!(n.alertmanagers().is_empty());
}

/// Checks that the manager copes with the discovery manager closing the target sets channel on shutdown: the
/// target update loop stops, while run keeps going until stop is called.
#[test]
fn test_manager_closed_target_sets_channel() {
    // target update loop returns
    {
        let m = Arc::new(Manager::new(Options::default(), ValidationScheme::Utf8, None));
        let (tx, rx) = mpsc::channel::<TargetSets>();
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let m2 = m.clone();
        std::thread::spawn(move || {
            let mut rx: Receiver<TargetSets> = rx;
            m2.target_update_loop(&mut rx);
            let _ = done_tx.send(());
        });
        drop(tx);
        match done_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => panic!("target update loop did not stop after the target sets channel was closed"),
        }
        m.stop();
    }

    // Run waits for Stop
    {
        let m = Arc::new(Manager::new(Options::default(), ValidationScheme::Utf8, None));
        let ams = new_test_alertmanager_set(default_alertmanager_config(), None, m.opts(), m.metrics(), &["http://alertmanager:9093/api/v2/alerts"]);
        m.set_alertmanager_sets(sets(vec![("test", ams.clone())]));
        ams.start_send_loops();

        let (tx, rx) = mpsc::channel::<TargetSets>();
        let m2 = m.clone();
        let run = std::thread::spawn(move || m2.run(Some(Box::new(rx))));
        drop(tx);
        // adapted: synctest.Wait() becomes a pause long enough for the target loop to see the closed channel.
        std::thread::sleep(Duration::from_millis(200));

        // Notifications must still be sent while rule evaluation finishes, so run may only return, and clean up
        // the send loops, once stop has been called.
        assert!(!run.is_finished(), "Run returned before Stop was called");
        assert_eq!(1, ams.send_loops().len(), "send loop was cleaned up before Stop was called");

        m.stop();
        run.join().unwrap();

        assert_eq!(0, ams.send_loops().len(), "send loop was not cleaned up after Stop was called");
    }
}

/// Ensures that a mutation by alert relabeling in one alertmanagerSet doesn't affect others
/// (https://github.com/prometheus/prometheus/pull/17063).
#[test]
fn test_alerst_relabeling_is_isolated() {
    let errc = ErrC::default();
    let (expected1, expected2): (Expected, Expected) = Default::default();
    let (status1, status2) = (Arc::new(AtomicI32::new(200)), Arc::new(AtomicI32::new(200)));

    let server1 = new_test_http_server_builder(expected1.clone(), errc.clone(), "", "", status1);
    let server2 = new_test_http_server_builder(expected2.clone(), errc.clone(), "", "", status2);

    let h = Manager::new(Options { queue_capacity: 10, ..Default::default() }, ValidationScheme::Utf8, None);

    let mut am1_cfg = cfg_with_timeout(Duration::from_secs(1));
    am1_cfg.alert_relabel_configs = vec![relabel_cfg("alertname", Action::Replace, "(.*)", "parasite", "yes")];
    let am2_cfg = cfg_with_timeout(Duration::from_secs(1));

    h.set_alertmanager_sets(sets(vec![
        ("am1", new_test_alertmanager_set(am1_cfg, None, h.opts(), h.metrics(), &[&server1.url])),
        ("am2", new_test_alertmanager_set(am2_cfg, None, h.opts(), h.metrics(), &[&server2.url])),
    ]));

    // Start send loops.
    start_send_loops(&h);
    let _clean = CleanLoops(&h);

    expected1.lock().unwrap().push(alert(&["alertname", "test", "parasite", "yes"]));
    // Am2 shouldn't get the parasite label.
    expected2.lock().unwrap().push(alert(&["alertname", "test"]));

    h.send(vec![alert(&["alertname", "test"])]);
    // adapted: Go checks errc right after Send (before the requests arrive); also wait for both deliveries so
    // the servers' checks have run.
    assert!(eventually(WAIT, TICK, || sent(&h, &server1.url) == 1.0 && sent(&h, &server2.url) == 1.0));
    errc.check_no_err();
}

/// Regression test for https://github.com/prometheus/prometheus/issues/7676: a black hole alertmanager that
/// never responds, with an (almost) infinite timeout, must not block the other alertmanagers.
#[test]
fn test_notifier_queue_independent_of_failed_alertmanager() {
    let stop_black_hole = Arc::new((Mutex::new(false), Condvar::new()));
    let sb = stop_black_hole.clone();
    let mut black_hole_am = TestServer::new(move |_r| {
        // Do nothing, wait to be canceled.
        let (m, cv) = &*sb;
        let mut g = m.lock().unwrap();
        while !*g {
            g = cv.wait(g).unwrap();
        }
        200
    });

    let (done_tx, done_alert_receive) = mpsc::sync_channel::<()>(1);
    let immediate_am = new_immediate_alert_manager(done_tx);

    let do_fn = |ctx: &Context, client: Option<&net::Client>, req: net::Request| notifier::send_loop::do_request(ctx, client, req);

    let reg = Registry::new();
    let h = Arc::new(Manager::new(
        Options { do_fn: Some(Arc::new(do_fn)), queue_capacity: 10, max_batch_size: DEFAULT_MAX_BATCH_SIZE, registerer: Some(reg), ..Default::default() },
        ValidationScheme::Utf8,
        None,
    ));

    let am_cfg = cfg_with_timeout(Duration::from_secs(3600 * 24 * 365));
    let default_client = Some(notifier::send_loop::default_client().clone());
    h.set_alertmanager_sets(sets(vec![
        ("1", new_test_alertmanager_set(am_cfg.clone(), default_client.clone(), h.opts(), h.metrics(), &[&black_hole_am.url])),
        ("2", new_test_alertmanager_set(am_cfg, default_client, h.opts(), h.metrics(), &[&immediate_am.url])),
    ]));

    start_send_loops(&h);

    let (send_done_tx, done_send_all) = mpsc::channel::<()>();
    let h2 = h.clone();
    std::thread::spawn(move || {
        h2.send(vec![alert(&["alertname", "test"])]);
        let _ = send_done_tx.send(());
    });

    // The happy case: the alert was received by the immediate alertmanager.
    if done_alert_receive.recv_timeout(WAIT).is_err() {
        panic!("Timeout waiting for alert to be received by immediate alertmanager");
    }
    // The happy case: send returned.
    if done_send_all.recv_timeout(WAIT).is_err() {
        panic!("Timeout waiting for sendAll to return");
    }

    {
        let (m, cv) = &*stop_black_hole;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }
    black_hole_am.close();
    for ams in h.alertmanager_sets().values() {
        ams.clean_send_loops();
    }
}

/// Reproduces a bug where send loops were stopped when the alertmanager config key changes but the config
/// content (and thus its hash) stays the same.
#[test]
fn test_apply_config_send_loops_not_stopped_on_key_change() {
    let (tx, alert_received) = mpsc::sync_channel::<()>(10);
    let server = TestServer::new(move |_r| {
        match tx.try_send(()) {
            Ok(()) | Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {}
        }
        200
    });

    let target_group = addr_group(&server.addr);

    let n = Manager::new(Options { queue_capacity: 10, ..Default::default() }, ValidationScheme::Utf8, None);

    // Initial config with TWO alertmanager configs: "config-0" (foo.json, hash X), "config-1" (bar.json, hash Y).
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
  - file_sd_configs:
    - files:
      - bar.json
";
    n.apply_config(&load_config(s)).unwrap();

    // Reload with target groups to discover alertmanagers.
    n.reload(&tgs(vec![("config-0", target_group.clone()), ("config-1", target_group.clone())]));
    assert_eq!(n.alertmanagers().len(), 2);

    // Verify send loops exist for both configs.
    assert_eq!(n.alertmanager_sets()["config-0"].send_loops().len(), 1);
    assert_eq!(n.alertmanager_sets()["config-1"].send_loops().len(), 1);

    // Start the send loops.
    start_send_loops(&n);
    let _clean = CleanLoops(&n);

    // Send an alert and verify it's received (twice, once per alertmanager set).
    n.send(vec![alert(&["alertname", "test1"])]);
    for _ in 0..2 {
        if alert_received.recv_timeout(WAIT).is_err() {
            panic!("timeout waiting for first alert");
        }
    }

    // Apply a new config that REVERSES the order of the alertmanager configs. The send loops should be
    // transferred based on hash matching.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - bar.json
  - file_sd_configs:
    - files:
      - foo.json
";
    n.apply_config(&load_config(s)).unwrap();

    // CRITICAL CHECK: after apply_config but BEFORE reload, the send loops should have been transferred based
    // on hash matching and NOT stopped.
    assert_eq!(n.alertmanager_sets()["config-0"].send_loops().len(), 1, "sendLoops should be transferred to config-0");
    assert_eq!(n.alertmanager_sets()["config-1"].send_loops().len(), 1, "sendLoops should be transferred to config-1");

    // Reload with target groups for the new config.
    n.reload(&tgs(vec![("config-0", target_group.clone()), ("config-1", target_group)]));

    // The alertmanagers should still be discoverable.
    assert_eq!(n.alertmanagers().len(), 2);

    // The critical test: send another alert and verify it's received by both.
    n.send(vec![alert(&["alertname", "test2"])]);
    for _ in 0..2 {
        if alert_received.recv_timeout(WAIT).is_err() {
            panic!("timeout waiting for second alert - sendLoops may have been incorrectly stopped");
        }
    }
}

/// Multiple new alertmanager configs with identical content (same hash) must not share the send loops.
#[test]
fn test_apply_config_duplicate_hash_shares_send_loops() {
    let n = Manager::new(Options { queue_capacity: 10, ..Default::default() }, ValidationScheme::Utf8, None);

    // Initial config with ONE alertmanager.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
";
    n.apply_config(&load_config(s)).unwrap();

    let target_group = addr_group("alertmanager:9093");
    n.reload(&tgs(vec![("config-0", target_group.clone())]));

    assert_eq!(n.alertmanager_sets()["config-0"].send_loops().len(), 1);

    // Apply a new config with TWO IDENTICAL alertmanager configs.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
  - file_sd_configs:
    - files:
      - foo.json
";
    n.apply_config(&load_config(s)).unwrap();

    // Reload with target groups for both configs - same alertmanager URL for both.
    n.reload(&tgs(vec![("config-0", target_group.clone()), ("config-1", target_group)]));

    // Both alertmanagerSets should have independent send loops.
    let send_loops0 = n.alertmanager_sets()["config-0"].send_loops();
    let send_loops1 = n.alertmanager_sets()["config-1"].send_loops();

    assert_eq!(send_loops0.len(), 1, "config-0 should have sendLoops");
    assert_eq!(send_loops1.len(), 1, "config-1 should have sendLoops");

    for (k, l0) in &send_loops0 {
        if let Some(l1) = send_loops1.get(k) {
            assert!(!Arc::ptr_eq(l0, l1), "config-0 and config-1 should have independent sendLoop instances, not shared references");
        }
    }

    for ams in n.alertmanager_sets().values() {
        ams.clean_send_loops();
    }
}

/// Send loops must be stopped when the config key stays the same but the config hash changes.
#[test]
fn test_apply_config_hash_change_leaks_send_loops() {
    let n = Manager::new(Options { queue_capacity: 10, ..Default::default() }, ValidationScheme::Utf8, None);

    // Initial config with one alertmanager.
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
";
    n.apply_config(&load_config(s)).unwrap();

    n.reload(&tgs(vec![("config-0", addr_group("alertmanager:9093"))]));

    // Capture the old send loop.
    let old_send_loops = n.alertmanager_sets()["config-0"].send_loops();
    assert_eq!(old_send_loops.len(), 1);
    let old_send_loop = old_send_loops.values().next().unwrap().clone();

    // Apply a new config with a DIFFERENT hash (added path_prefix).
    let s = "
alerting:
  alertmanagers:
  - file_sd_configs:
    - files:
      - foo.json
    path_prefix: /changed
";
    n.apply_config(&load_config(s)).unwrap();

    // The old send loop should have been stopped since the hash changed.
    assert!(old_send_loop.is_stopped(), "BUG: old sendLoop was not stopped when config hash changed - goroutine leak");
}
