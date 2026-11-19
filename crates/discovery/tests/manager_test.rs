//! Port of `discovery/manager_test.go`.
//!
//! Go's `TestMain` goroutine leak check (`testutil.TolerantVerifyLeak`) is Go-only. Every test here cancels its
//! context at the end (also on panic, see `TestCtx`) so the manager and provider threads stop.
//!
//! Go's `<-discoveryManager.SyncCh()` blocks until the test binary times out; here `recv_sync` waits at most
//! `SYNC_TIMEOUT`, so a missing update fails the test instead of hanging it.
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use discovery::manager::TargetSets;
use discovery::{CancelToken, Config, Configs, Discoverer, DiscovererOptions, Group, Logger, Manager, ManagerOptions, PoolKey, Registry, SdMetrics, Sender, StaticConfig};
use model::labelset::{ADDRESS_LABEL, LabelSet};

const SYNC_TIMEOUT: Duration = Duration::from_secs(30);

type Targets = BTreeMap<PoolKey, BTreeMap<String, Arc<Group>>>;

/// Go `t.Context()` / `context.WithCancel`: a token that is cancelled when the test ends (also on panic).
struct TestCtx(CancelToken);

impl TestCtx {
    fn new() -> TestCtx {
        TestCtx(CancelToken::new())
    }
}

impl Deref for TestCtx {
    type Target = CancelToken;
    fn deref(&self) -> &CancelToken {
        &self.0
    }
}

impl Drop for TestCtx {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Go `NewTestMetrics`.
fn new_test_metrics(reg: &Arc<Registry>) -> SdMetrics {
    let refresh_metrics = discovery::new_refresh_metrics(reg);
    let mechanism_metrics = discovery::register_sd_metrics(reg, refresh_metrics.clone()).expect("register SD metrics");
    SdMetrics { mechanism_metrics, refresh_manager: refresh_metrics }
}

/// Go `NewManager(ctx, logger, reg, sdMetrics)` + `require.NotNil` + `discoveryManager.updatert = updatert`.
fn new_manager(ctx: &CancelToken, reg: &Arc<Registry>, sd_metrics: &SdMetrics, updatert: Duration) -> Manager {
    let m = Manager::new(ctx, Logger::nop(), reg, sd_metrics, ManagerOptions::default()).expect("NewManager");
    m.set_updatert(updatert);
    m
}

/// Go `go discoveryManager.Run()`.
fn go_run(m: &Manager) -> thread::JoinHandle<()> {
    let m = m.clone();
    thread::spawn(move || {
        let _ = m.run();
    })
}

/// Go `<-discoveryManager.SyncCh()`.
fn recv_sync(m: &Manager) -> TargetSets {
    match m.sync_ch().recv_timeout(SYNC_TIMEOUT) {
        Ok(v) => v,
        Err(true) => panic!("no update on the sync channel within {SYNC_TIMEOUT:?}"),
        Err(false) => panic!("the sync channel is closed"),
    }
}

/// Joins a thread; fails instead of hanging when it does not finish within `d` (Go: the test would hang).
fn join_within<T: Send + 'static>(h: thread::JoinHandle<T>, d: Duration, what: &str) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(h.join());
    });
    match rx.recv_timeout(d) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => std::panic::resume_unwind(e),
        Err(_) => panic!("{what} did not finish within {d:?}"),
    }
}

/// A group of `__instance__` targets (the Go test literals `{Source: s, Targets: []model.LabelSet{{"__instance__": i}}}`).
fn tg(source: &str, instances: &[&str]) -> Group {
    Group { targets: instances.iter().map(|i| LabelSet::from_pairs(&[("__instance__", i)])).collect(), labels: LabelSet::new(), source: source.to_string() }
}

/// Go `assertEqualGroups`: sorts both by source (the received order is not guaranteed) and compares deeply.
fn assert_equal_groups(got: &[Arc<Group>], expected: &[Group]) {
    let mut got: Vec<Group> = got.iter().map(|g| (**g).clone()).collect();
    let mut expected: Vec<Group> = expected.to_vec();
    got.sort_by(|a, b| a.source.cmp(&b.source));
    expected.sort_by(|a, b| a.source.cmp(&b.source));
    assert_eq!(expected, got);
}

/// Go `staticConfig`.
fn static_config(addrs: &[&str]) -> StaticConfig {
    StaticConfig(
        addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| Group { source: i.to_string(), targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, addr)])], labels: LabelSet::new() })
            .collect(),
    )
}

fn configs(cs: Vec<Box<dyn Config>>) -> Configs {
    Configs(cs)
}

/// Go `verifySyncedPresence`.
fn verify_synced_presence(t_groups: &TargetSets, key: &str, label: &str, present: bool) {
    let Some(groups) = t_groups.get(key) else {
        panic!("'{key}' should be present in Group map keys: {t_groups:?}");
    };
    let mut matched = false;
    let mut merged_targets = String::new();
    for target_groups in groups {
        for l in &target_groups.targets {
            merged_targets = format!("{merged_targets} {l}");
            if l.to_string() == label {
                matched = true;
            }
        }
    }
    if matched != present {
        let msg = if !present { "not" } else { "" };
        panic!("{label:?} should {msg} be present in Group labels: {merged_targets:?}");
    }
}

/// Go `verifyPresence`.
fn verify_presence(t_sets: &Targets, pool_key: &PoolKey, label: &str, present: bool) {
    let Some(groups) = t_sets.get(pool_key) else {
        panic!("'{pool_key:?}' should be present in Pool keys: {t_sets:?}");
    };
    let mut matched = false;
    let mut merged_targets = String::new();
    for target_group in groups.values() {
        for l in &target_group.targets {
            merged_targets = format!("{merged_targets} {l}");
            if l.to_string() == label {
                matched = true;
            }
        }
    }
    if present {
        assert!(matched, "{label:?} must be present in Targets labels: {merged_targets:?}");
    } else {
        assert!(!matched, "{label:?} must be absent in Targets labels: {merged_targets:?}");
    }
}

/// Go `pk`.
fn pk(provider: &str, set_name: &str, n: usize) -> PoolKey {
    PoolKey::new(set_name, &format!("{provider}/{n}"))
}

// ---------------- mock providers ----------------

/// Go `update`.
#[derive(Clone)]
struct Update {
    target_groups: Vec<Group>,
    interval: Duration,
}

fn upd(interval_ms: u64, target_groups: Vec<Group>) -> Update {
    Update { target_groups, interval: Duration::from_millis(interval_ms) }
}

/// Go `mockdiscoveryProvider`.
#[derive(Clone)]
struct MockDiscoveryProvider {
    updates: Vec<Update>,
}

fn new_mock_discovery_provider(updates: Vec<Update>) -> MockDiscoveryProvider {
    MockDiscoveryProvider { updates }
}

impl Discoverer for MockDiscoveryProvider {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        for u in &self.updates {
            if !u.interval.is_zero() && ctx.wait_timeout(u.interval) {
                return;
            }
            let tgs: Vec<Arc<Group>> = u.target_groups.iter().cloned().map(Arc::new).collect();
            if !up.send(tgs) {
                return;
            }
        }
        ctx.wait();
    }
}

/// Go `onceProvider`: sends its groups once (if any) and closes the update channel.
struct OnceProvider {
    tgs: Vec<Arc<Group>>,
}

impl Discoverer for OnceProvider {
    fn run(&mut self, _ctx: &CancelToken, up: Sender) {
        if !self.tgs.is_empty() {
            up.send(self.tgs.clone());
        }
        // dropping `up` closes the channel (Go `close(ch)`)
    }
}

/// Go `errorConfig`.
#[derive(Clone, Debug, PartialEq)]
struct ErrorConfig {
    err: String,
}

impl Config for ErrorConfig {
    fn name(&self) -> &'static str {
        "error"
    }
    fn new_discoverer(&self, _opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Err(self.err.clone())
    }
}

/// Go `lockStaticConfig`: a static config whose discoverer holds `mu` while it sends.
#[derive(Clone, Debug)]
struct LockStaticConfig {
    mu: Arc<Mutex<()>>,
    config: StaticConfig,
}

// Go compares configs with reflect.DeepEqual: the same *sync.Mutex pointer and equal groups.
impl PartialEq for LockStaticConfig {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.mu, &o.mu) && self.config == o.config
    }
}

impl Config for LockStaticConfig {
    fn name(&self) -> &'static str {
        "lockstatic"
    }
    fn new_discoverer(&self, _opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(LockStaticDiscoverer(self.clone())))
    }
}

struct LockStaticDiscoverer(LockStaticConfig);

impl Discoverer for LockStaticDiscoverer {
    fn run(&mut self, _ctx: &CancelToken, up: Sender) {
        // Go: defer close(up); lock; select { <-ctx.Done() | up <- config } (the channel stops on cancel).
        let _g = self.0.mu.lock().unwrap_or_else(|e| e.into_inner());
        up.send(self.0.config.0.iter().cloned().map(Arc::new).collect());
    }
}

/// Go `testDiscoverer`: a config and a discoverer that can adjust targets with a simple function. Go uses one
/// pointer for both roles; here the clones share their state through an Arc and compare by pointer.
#[derive(Clone)]
struct TestDiscoverer(Arc<TdShared>);

struct TdShared {
    up: Mutex<Option<Sender>>,
    ready: Mutex<bool>,
    ready_cv: Condvar,
}

fn new_test_discoverer() -> TestDiscoverer {
    TestDiscoverer(Arc::new(TdShared { up: Mutex::new(None), ready: Mutex::new(false), ready_cv: Condvar::new() }))
}

impl fmt::Debug for TestDiscoverer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "testDiscoverer({:p})", Arc::as_ptr(&self.0))
    }
}

impl PartialEq for TestDiscoverer {
    fn eq(&self, o: &Self) -> bool {
        Arc::ptr_eq(&self.0, &o.0)
    }
}

impl Config for TestDiscoverer {
    fn name(&self) -> &'static str {
        "test"
    }
    fn new_discoverer(&self, _opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(self.clone()))
    }
}

impl Discoverer for TestDiscoverer {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        *self.0.up.lock().unwrap() = Some(up);
        *self.0.ready.lock().unwrap() = true; // Go close(t.ready)
        self.0.ready_cv.notify_all();
        ctx.wait();
    }
}

impl TestDiscoverer {
    fn update(&self, tgs: Vec<Arc<Group>>) {
        let mut r = self.0.ready.lock().unwrap();
        while !*r {
            r = self.0.ready_cv.wait(r).unwrap();
        }
        drop(r);
        let up = self.0.up.lock().unwrap().as_ref().expect("up is set when ready").clone();
        up.send(tgs);
    }
}

// ---------------- tests ----------------

/// Checks that the target updates are received in the expected order.
#[test]
fn test_target_updates_order() {
    // The order by which the updates are send is determined by the interval passed to the mock discovery adapter
    // Final targets array is ordered alphabetically by the name of the discoverer.
    // For example discoverer "A" with targets "t2,t3" and discoverer "B" with targets "t1,t2" will result in "t2,t3,t1,t2" after the merge.
    struct Case {
        title: &'static str,
        updates: Vec<(&'static str, Vec<Update>)>,
        expected_targets: Vec<Vec<Group>>,
    }
    let test_cases = vec![
        Case { title: "Single TP no updates", updates: vec![("tp1", vec![])], expected_targets: vec![] },
        Case { title: "Multiple TPs no updates", updates: vec![("tp1", vec![]), ("tp2", vec![]), ("tp3", vec![])], expected_targets: vec![] },
        Case { title: "Single TP empty initials", updates: vec![("tp1", vec![upd(5, vec![])])], expected_targets: vec![vec![]] },
        Case {
            title: "Multiple TPs empty initials",
            updates: vec![("tp1", vec![upd(5, vec![])]), ("tp2", vec![upd(200, vec![])]), ("tp3", vec![upd(100, vec![])])],
            expected_targets: vec![vec![], vec![], vec![]],
        },
        Case {
            title: "Single TP initials only",
            updates: vec![("tp1", vec![upd(0, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])])])],
            expected_targets: vec![vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]],
        },
        Case {
            title: "Multiple TPs initials only",
            updates: vec![
                ("tp1", vec![upd(0, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])])]),
                ("tp2", vec![upd(10, vec![tg("tp2_group1", &["3"])])]),
            ],
            expected_targets: vec![
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])],
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"]), tg("tp2_group1", &["3"])],
            ],
        },
        Case {
            title: "Single TP initials followed by empty updates",
            updates: vec![(
                "tp1",
                vec![upd(0, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]), upd(10, vec![tg("tp1_group1", &[]), tg("tp1_group2", &[])])],
            )],
            expected_targets: vec![vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])], vec![tg("tp1_group1", &[]), tg("tp1_group2", &[])]],
        },
        Case {
            title: "Single TP initials and new groups",
            updates: vec![(
                "tp1",
                vec![
                    upd(0, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]),
                    upd(10, vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"]), tg("tp1_group3", &["1"])]),
                ],
            )],
            expected_targets: vec![
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])],
                vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"]), tg("tp1_group3", &["1"])],
            ],
        },
        Case {
            title: "Multiple TPs initials and new groups",
            updates: vec![
                (
                    "tp1",
                    vec![
                        upd(10, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]),
                        upd(500, vec![tg("tp1_group3", &["3"]), tg("tp1_group4", &["4"])]),
                    ],
                ),
                (
                    "tp2",
                    vec![
                        upd(100, vec![tg("tp2_group1", &["5"]), tg("tp2_group2", &["6"])]),
                        upd(10, vec![tg("tp2_group3", &["7"]), tg("tp2_group4", &["8"])]),
                    ],
                ),
            ],
            expected_targets: vec![
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])],
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"]), tg("tp2_group1", &["5"]), tg("tp2_group2", &["6"])],
                vec![
                    tg("tp1_group1", &["1"]),
                    tg("tp1_group2", &["2"]),
                    tg("tp2_group1", &["5"]),
                    tg("tp2_group2", &["6"]),
                    tg("tp2_group3", &["7"]),
                    tg("tp2_group4", &["8"]),
                ],
                vec![
                    tg("tp1_group1", &["1"]),
                    tg("tp1_group2", &["2"]),
                    tg("tp1_group3", &["3"]),
                    tg("tp1_group4", &["4"]),
                    tg("tp2_group1", &["5"]),
                    tg("tp2_group2", &["6"]),
                    tg("tp2_group3", &["7"]),
                    tg("tp2_group4", &["8"]),
                ],
            ],
        },
        Case {
            title: "One TP initials arrive after other TP updates.",
            updates: vec![
                (
                    "tp1",
                    vec![
                        upd(10, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]),
                        upd(150, vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"])]),
                    ],
                ),
                (
                    "tp2",
                    vec![
                        upd(200, vec![tg("tp2_group1", &["5"]), tg("tp2_group2", &["6"])]),
                        upd(100, vec![tg("tp2_group1", &["7"]), tg("tp2_group2", &["8"])]),
                    ],
                ),
            ],
            expected_targets: vec![
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])],
                vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"])],
                vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"]), tg("tp2_group1", &["5"]), tg("tp2_group2", &["6"])],
                vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"]), tg("tp2_group1", &["7"]), tg("tp2_group2", &["8"])],
            ],
        },
        Case {
            title: "Single TP empty update in between",
            updates: vec![(
                "tp1",
                vec![
                    upd(30, vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])]),
                    upd(10, vec![tg("tp1_group1", &[]), tg("tp1_group2", &[])]),
                    upd(300, vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"])]),
                ],
            )],
            expected_targets: vec![
                vec![tg("tp1_group1", &["1"]), tg("tp1_group2", &["2"])],
                vec![tg("tp1_group1", &[]), tg("tp1_group2", &[])],
                vec![tg("tp1_group1", &["3"]), tg("tp1_group2", &["4"])],
            ],
        },
    ];

    // Go runs the subtests in parallel; each one runs on its own thread here.
    let handles: Vec<_> = test_cases
        .into_iter()
        .enumerate()
        .map(|(i, tc)| {
            thread::spawn(move || {
                // Go: context.WithTimeout(context.Background(), 5*time.Second)
                let ctx = TestCtx::new();
                let deadline = Instant::now() + Duration::from_secs(5);

                let reg = Registry::new();
                let sd_metrics = new_test_metrics(&reg);

                let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));

                let total_updates_count: usize = tc.updates.iter().map(|(_, up)| up.len()).sum();
                // Go: one buffered channel that all mock providers send on.
                let (prov_tx, prov_updates) = discovery::channel_with_cancel(&ctx);

                for (_, up) in &tc.updates {
                    let mut p = new_mock_discovery_provider(up.clone());
                    let (c, tx) = (ctx.0.clone(), prov_tx.clone());
                    thread::spawn(move || p.run(&c, tx));
                }
                drop(prov_tx);

                for x in 0..total_updates_count {
                    let left = deadline.saturating_duration_since(Instant::now());
                    match prov_updates.recv_timeout(left) {
                        discovery::Recv::Update(tgs) => {
                            discovery_manager.update_group(&PoolKey::new(&i.to_string(), tc.title), &tgs);
                            // As in Go, the manager has no providers here (no ApplyConfig), so allGroups is empty
                            // and this loop checks nothing.
                            for got in discovery_manager.all_groups().values() {
                                assert_equal_groups(got, &tc.expected_targets[x]);
                            }
                        }
                        other => panic!("{}: {x}: no update arrived within the timeout limit ({other:?})", tc.title),
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_target_set_target_groups_present_on_startup() {
    struct Case {
        name: &'static str,
        updatert: Duration,
        read_timeout: Duration,
        expected_targets: usize,
    }
    let test_cases = [
        Case { name: "startup wait with long interval times out", updatert: Duration::from_secs(100 * 3600), read_timeout: Duration::from_millis(10), expected_targets: 0 },
        Case { name: "startup wait with short interval succeeds", updatert: Duration::from_millis(10), read_timeout: Duration::from_millis(300), expected_targets: 1 },
        Case { name: "skip startup wait", updatert: Duration::from_secs(100 * 3600), read_timeout: Duration::from_millis(300), expected_targets: 1 },
    ];

    let handles: Vec<_> = test_cases
        .into_iter()
        .map(|tc| {
            thread::spawn(move || {
                // adapted: Go runs this case in a synctest bubble (fake time) and calls synctest.Wait() after
                // ApplyConfig, so the read window starts at fake time 0 and the first send (50-150ms after Run,
                // the backoff's first tick) is after a 10ms window and within a 300ms window. Here it runs in real
                // time with the same durations; the read window starts right after ApplyConfig (the static
                // provider delivers within that time).
                let ctx = TestCtx::new();

                let reg = Registry::new();
                let sd_metrics = new_test_metrics(&reg);

                let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, tc.updatert);
                go_run(&discovery_manager);

                let mut c = BTreeMap::new();
                c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
                discovery_manager.apply_config(&c);

                let timeout = Instant::now() + tc.read_timeout;
                let mut last_synced_targets: Option<TargetSets> = None;
                let sync_ch = discovery_manager.sync_ch();
                loop {
                    let left = timeout.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    match sync_ch.recv_timeout(left) {
                        Ok(v) => last_synced_targets = Some(v),
                        Err(true) => break,
                        Err(false) => panic!("{}: the sync channel is closed", tc.name),
                    }
                }

                if tc.expected_targets == 0 {
                    assert!(last_synced_targets.is_none(), "{}: {last_synced_targets:?}", tc.name);
                    return;
                }

                let last_synced_targets = last_synced_targets.unwrap_or_else(|| panic!("{}: nothing synced", tc.name));
                assert_eq!(last_synced_targets.len(), 1, "{}", tc.name);
                assert_eq!(last_synced_targets["prometheus"].len(), tc.expected_targets, "{}", tc.name);
                verify_synced_presence(&last_synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);

                let p = pk("static", "prometheus", 0);
                let targets = discovery_manager.targets();
                verify_presence(&targets, &p, "{__address__=\"foo:9090\"}", true);
                assert_eq!(targets.len(), 1, "{}", tc.name);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_target_set_target_groups_present_on_config_reload() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);

    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
}

#[test]
fn test_target_set_target_groups_present_on_config_rename() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);

    let v = c.remove("prometheus").unwrap();
    c.insert("prometheus2".to_string(), v);
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    let p = pk("static", "prometheus2", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus2", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus2"].len(), 1);
}

#[test]
fn test_target_set_target_groups_present_on_config_duplicate_and_delete_original() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);
    recv_sync(&discovery_manager);

    c.insert("prometheus2".to_string(), c["prometheus"].clone());
    discovery_manager.apply_config(&c);
    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(synced_targets.len(), 2);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    verify_synced_presence(&synced_targets, "prometheus2", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus2"].len(), 1);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 2);

    c.remove("prometheus");
    discovery_manager.apply_config(&c);
    let synced_targets = recv_sync(&discovery_manager);
    let p = pk("static", "prometheus2", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus2", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus2"].len(), 1);
}

#[test]
fn test_target_set_target_groups_present_on_config_change() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);

    let mu = Arc::new(Mutex::new(()));
    c.insert("prometheus2".to_string(), configs(vec![Box::new(LockStaticConfig { mu: mu.clone(), config: static_config(&["bar:9090"]) })]));
    let guard = mu.lock().unwrap();
    discovery_manager.apply_config(&c);

    // Original targets should be present as soon as possible.
    // An empty list should be sent for prometheus2 to drop any stale targets
    let synced_targets = recv_sync(&discovery_manager);
    drop(guard);
    assert_eq!(synced_targets.len(), 2);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    assert!(synced_targets["prometheus2"].is_empty());

    // prometheus2 configs should be ready on second sync.
    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(synced_targets.len(), 2);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    verify_synced_presence(&synced_targets, "prometheus2", "{__address__=\"bar:9090\"}", true);
    assert_eq!(synced_targets["prometheus2"].len(), 1);

    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    let p = pk("lockstatic", "prometheus2", 1);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"bar:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 2);

    // Delete part of config and ensure only original targets exist.
    c.remove("prometheus2");
    discovery_manager.apply_config(&c);
    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(discovery_manager.targets().len(), 1);
    verify_presence(&discovery_manager.targets(), &pk("static", "prometheus", 0), "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
}

#[test]
fn test_target_set_recreates_target_groups_on_config_change() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090", "bar:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"bar:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 1);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"bar:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 2);

    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);
    let synced_targets = recv_sync(&discovery_manager);
    assert_eq!(discovery_manager.targets().len(), 1);
    let p = pk("static", "prometheus", 1);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"bar:9090\"}", false);
    assert_eq!(discovery_manager.targets().len(), 1);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
}

#[test]
fn test_discoverer_configs() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090", "bar:9090"])), Box::new(static_config(&["baz:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"bar:9090\"}", true);
    let p = pk("static", "prometheus", 1);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"baz:9090\"}", true);
    assert_eq!(discovery_manager.targets().len(), 2);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"bar:9090\"}", true);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"baz:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 3);
}

/// Ensures that reloading a config file after removing all targets from the static_configs cleans the
/// corresponding targetGroups entries to avoid leaks and sends an empty update. The update is required to signal
/// the consumers that the previous targets should be dropped.
#[test]
fn test_target_set_recreates_empty_static_configs() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    let p = pk("static", "prometheus", 0);
    verify_presence(&discovery_manager.targets(), &p, "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets.len(), 1);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);

    c.insert("prometheus".to_string(), configs(vec![Box::new(StaticConfig(vec![Group::default()]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    let targets = discovery_manager.targets();
    assert_eq!(targets.len(), 1);
    let p = pk("static", "prometheus", 1);
    let target_groups = targets.get(&p).unwrap_or_else(|| panic!("'{p:?}' should be present in targets"));
    // Otherwise the targetGroups will leak, see https://github.com/prometheus/prometheus/issues/12436.
    assert!(target_groups.is_empty(), "'{p:?}' should no longer have any associated target groups");
    assert_eq!(synced_targets.len(), 1, "an update with no targetGroups should still be sent.");
    assert!(synced_targets["prometheus"].is_empty());
}

#[test]
fn test_identical_configurations_are_coalesced() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    // Go passes a nil logger.
    let discovery_manager = Manager::new(&ctx, Logger::default(), &reg, &sd_metrics, ManagerOptions::default()).expect("NewManager");
    discovery_manager.set_updatert(Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    c.insert("prometheus2".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let synced_targets = recv_sync(&discovery_manager);
    verify_presence(&discovery_manager.targets(), &pk("static", "prometheus", 0), "{__address__=\"foo:9090\"}", true);
    verify_presence(&discovery_manager.targets(), &pk("static", "prometheus2", 0), "{__address__=\"foo:9090\"}", true);
    assert_eq!(discovery_manager.providers().len(), 1, "Invalid number of providers.");
    assert_eq!(synced_targets.len(), 2);
    verify_synced_presence(&synced_targets, "prometheus", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus"].len(), 1);
    verify_synced_presence(&synced_targets, "prometheus2", "{__address__=\"foo:9090\"}", true);
    assert_eq!(synced_targets["prometheus2"].len(), 1);
}

#[test]
fn test_apply_config_does_not_modify_static_targets() {
    let original_config = configs(vec![Box::new(static_config(&["foo:9090", "bar:9090", "baz:9090"]))]);
    let processed_config = configs(vec![Box::new(static_config(&["foo:9090", "bar:9090", "baz:9090"]))]);
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut cfgs = BTreeMap::new();
    cfgs.insert("prometheus".to_string(), processed_config);
    discovery_manager.apply_config(&cfgs);
    recv_sync(&discovery_manager);

    for cfg in cfgs.values() {
        assert_eq!(&original_config, cfg);
    }
}

#[test]
fn test_gauge_failed_configs() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert(
        "prometheus".to_string(),
        configs(vec![
            Box::new(ErrorConfig { err: "tests error 0".into() }),
            Box::new(ErrorConfig { err: "tests error 1".into() }),
            Box::new(ErrorConfig { err: "tests error 2".into() }),
        ]),
    );
    discovery_manager.apply_config(&c);
    recv_sync(&discovery_manager);

    let failed_count = discovery_manager.metrics().failed_configs.get().get();
    assert_eq!(3.0, failed_count, "Expected to have 3 failed configs.");

    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);
    recv_sync(&discovery_manager);

    let failed_count = discovery_manager.metrics().failed_configs.get().get();
    assert_eq!(0.0, failed_count, "Expected to get no failed config.");
}

#[test]
fn test_coordination_with_receiver() {
    let update_delay = Duration::from_millis(100);

    struct Expect {
        delay: Duration,
        tgs: Vec<(&'static str, Vec<Group>)>,
    }
    struct Case {
        title: &'static str,
        providers: Vec<(&'static str, Box<dyn Discoverer>)>,
        expected: Vec<Expect>,
    }

    let test_cases = vec![
        Case {
            title: "Receiver should get all updates even when one provider closes its channel",
            providers: vec![
                ("once1", Box::new(OnceProvider { tgs: vec![Arc::new(tg("tg1", &["1"]))] })),
                ("mock1", Box::new(new_mock_discovery_provider(vec![Update { interval: 2 * update_delay, target_groups: vec![tg("tg2", &["2"])] }]))),
            ],
            expected: vec![
                Expect { delay: Duration::ZERO, tgs: vec![("once1", vec![tg("tg1", &["1"])]), ("mock1", vec![])] },
                Expect { delay: Duration::ZERO, tgs: vec![("once1", vec![tg("tg1", &["1"])]), ("mock1", vec![tg("tg2", &["2"])])] },
            ],
        },
        Case {
            title: "Receiver should get all updates even when the channel is blocked",
            providers: vec![(
                "mock1",
                Box::new(new_mock_discovery_provider(vec![
                    Update { interval: Duration::ZERO, target_groups: vec![tg("tg1", &["1"])] },
                    Update { interval: 4 * update_delay, target_groups: vec![tg("tg2", &["2"])] },
                ])),
            )],
            expected: vec![
                Expect { delay: 2 * update_delay, tgs: vec![("mock1", vec![tg("tg1", &["1"])])] },
                Expect { delay: 4 * update_delay, tgs: vec![("mock1", vec![tg("tg1", &["1"]), tg("tg2", &["2"])])] },
            ],
        },
    ];

    let handles: Vec<_> = test_cases
        .into_iter()
        .map(|tc| {
            thread::spawn(move || {
                // Go: context.WithTimeout(context.Background(), 5*time.Second)
                let ctx = TestCtx::new();
                let deadline = Instant::now() + Duration::from_secs(5);

                let reg = Registry::new();
                let sd_metrics = new_test_metrics(&reg);

                // Go passes a nil logger.
                let mgr = Manager::new(&ctx, Logger::default(), &reg, &sd_metrics, ManagerOptions::default()).expect("NewManager");
                mgr.set_updatert(update_delay);
                go_run(&mgr);

                for (name, p) in tc.providers {
                    mgr.start_custom_provider(&ctx, name, p);
                }

                for (i, expected) in tc.expected.iter().enumerate() {
                    thread::sleep(expected.delay);
                    let left = deadline.saturating_duration_since(Instant::now());
                    let tgs = match mgr.sync_ch().recv_timeout(left) {
                        Ok(v) => v,
                        Err(true) => panic!("{}: step {i}: no update received in the expected timeframe", tc.title),
                        Err(false) => panic!("{}: step {i}: discovery manager channel is closed", tc.title),
                    };
                    assert_eq!(tgs.len(), expected.tgs.len(), "{}: step {i}: targets mismatch", tc.title);

                    for (k, exp) in &expected.tgs {
                        let got = tgs.get(*k).unwrap_or_else(|| panic!("{}: step {i}: target group not found: {k}", tc.title));
                        assert_equal_groups(got, exp);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

/// Detects races when ApplyConfig happens at the same time as targets update.
#[test]
fn test_target_set_target_groups_update_during_apply_config() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let td = new_test_discoverer();

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(td.clone())]));
    discovery_manager.apply_config(&c);

    // Go: a WaitGroup of 2000 and a `start` channel that is closed to release all goroutines at once.
    let start = CancelToken::new();
    let mut wg = Vec::with_capacity(2000);
    for _ in 0..1000 {
        let (start, td) = (start.clone(), td.clone());
        wg.push(thread::spawn(move || {
            start.wait();
            td.update(vec![Arc::new(Group { targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "127.0.0.1:9090")])], ..Group::default() })]);
        }));
    }

    for i in 0..1000 {
        let (start, td, dm) = (start.clone(), td.clone(), discovery_manager.clone());
        wg.push(thread::spawn(move || {
            start.wait();
            let mut c = BTreeMap::new();
            c.insert(format!("prometheus-{i}"), configs(vec![Box::new(td)]));
            dm.apply_config(&c);
        }));
    }

    start.cancel();
    for h in wg {
        join_within(h, SYNC_TIMEOUT, "update or ApplyConfig");
    }
}

#[test]
fn test_unregister_metrics() {
    let reg = Registry::new();
    // Check that all metrics can be unregistered, allowing a second manager to be created.
    for _ in 0..2 {
        let ctx = TestCtx::new();

        let sd_metrics = new_test_metrics(&reg);

        // The manager is an error if its metrics could not be registered.
        let discovery_manager = Manager::new(&ctx, Logger::nop(), &reg, &sd_metrics, ManagerOptions::default()).expect("NewManager");
        // Unregister all metrics.
        discovery_manager.unregister_metrics();
        for sd_metric in sd_metrics.mechanism_metrics.values() {
            sd_metric.unregister();
        }
        sd_metrics.refresh_manager.unregister();
        ctx.cancel();
    }
}

/// Refresh and discovery metrics should be deleted for providers that are removed.
#[test]
fn test_metrics_cleanup_after_config_reload() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090", "bar:9090"]))]));
    c.insert("other".to_string(), configs(vec![Box::new(static_config(&["baz:9090"]))]));
    discovery_manager.apply_config(&c);
    recv_sync(&discovery_manager);

    // Manually instantiate refresh metrics to make them visible
    sd_metrics.refresh_manager.instantiate("static", "prometheus").failures.add(0.0);
    sd_metrics.refresh_manager.instantiate("static", "other").failures.add(0.0);

    assert_eq!(reg.gather_and_count("prometheus_sd_discovered_targets"), 2);
    assert_eq!(reg.gather_and_count("prometheus_sd_refresh_failures_total"), 2);
    assert_eq!(reg.gather_and_count("prometheus_sd_last_update_timestamp_seconds"), 2);

    // Simulate a config refresh.
    c.remove("prometheus");
    discovery_manager.apply_config(&c);
    recv_sync(&discovery_manager);

    // Ensure we still have metrics for the remaining provider.
    assert_eq!(reg.gather_and_count("prometheus_sd_discovered_targets"), 1);
    assert_eq!(reg.gather_and_count("prometheus_sd_refresh_failures_total"), 1);
    assert_eq!(reg.gather_and_count("prometheus_sd_last_update_timestamp_seconds"), 1);
}

/// Calling ApplyConfig() that removes providers at the same time as shutting down the manager should not hang.
#[test]
fn test_config_reload_and_shutdown_race() {
    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let mgr_ctx = TestCtx::new();
    let discovery_manager = new_manager(&mgr_ctx, &reg, &sd_metrics, Duration::from_millis(100));

    let wg_discovery = go_run(&discovery_manager);
    thread::sleep(Duration::from_millis(200));

    let update_chan = discovery_manager.sync_ch();
    let ctx = TestCtx::new();
    let bg1 = {
        let ctx = ctx.0.clone();
        // Go: select { case <-ctx.Done(): case <-updateChan: }
        thread::spawn(move || {
            while !ctx.is_cancelled() {
                match update_chan.recv_timeout(Duration::from_millis(5)) {
                    Ok(_) | Err(false) => return,
                    Err(true) => {}
                }
            }
        })
    };

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["bar:9090"]))]));
    discovery_manager.apply_config(&c);

    c.remove("prometheus");
    let bg2 = {
        let dm = discovery_manager.clone();
        thread::spawn(move || dm.apply_config(&c))
    };
    mgr_ctx.cancel();
    join_within(wg_discovery, SYNC_TIMEOUT, "Run");

    ctx.cancel();
    join_within(bg1, SYNC_TIMEOUT, "the sync channel reader");
    join_within(bg2, SYNC_TIMEOUT, "ApplyConfig");
}

#[test]
fn test_gauge_last_update_timestamp() {
    let ctx = TestCtx::new();

    let reg = Registry::new();
    let sd_metrics = new_test_metrics(&reg);

    let discovery_manager = new_manager(&ctx, &reg, &sd_metrics, Duration::from_millis(100));
    go_run(&discovery_manager);

    let mut c = BTreeMap::new();
    c.insert("prometheus".to_string(), configs(vec![Box::new(static_config(&["foo:9090"]))]));
    discovery_manager.apply_config(&c);

    let unix_secs = || SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as f64 / 1e9;
    let before = unix_secs();
    recv_sync(&discovery_manager);
    let after = unix_secs();

    let ts = discovery_manager.metrics().last_updated.with_label_values(&["prometheus"]).get();
    assert!(ts >= before, "last update timestamp should be >= time before sync");
    assert!(ts <= after, "last update timestamp should be <= time after sync");
}
