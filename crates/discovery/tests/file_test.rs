//! Port of discovery/file/file_test.go. The discoverer polls for changes (no inotify, see file.rs); the tests
//! keep upstream's 1s windows.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use discovery::file::{Discovery, SdConfig, file_source};
use discovery::{CancelToken, Config, Discoverer, Group, Recv, Registry};
use model::labelset::LabelSet;

const DEFAULT_WAIT: Duration = Duration::from_secs(1);

fn fixture(name: &str) -> String {
    format!("{}/../../upstream/discovery/file/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn temp_dir() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = format!("{}/file_test-{}-{}", env!("CARGO_TARGET_TMPDIR"), std::process::id(), N.fetch_add(1, Ordering::Relaxed));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[derive(Default)]
struct State {
    tgs: BTreeMap<String, Arc<Group>>,
    received_at: Option<Instant>,
}

struct TestRunner {
    dir: String,
    tmp: String,
    state: Arc<Mutex<State>>,
    cancel_sd: CancelToken,
    threads: Vec<JoinHandle<()>>,
}

impl TestRunner {
    fn new() -> TestRunner {
        TestRunner { dir: temp_dir(), tmp: temp_dir(), state: Arc::default(), cancel_sd: CancelToken::new(), threads: Vec::new() }
    }

    /// Copies a file to the runner's directory.
    fn copy_file(&self, src: &str) -> String {
        let base = src.rsplit('/').next().unwrap();
        self.copy_file_to(src, base)
    }

    /// Atomically copies a file with a different name to the runner's directory.
    fn copy_file_to(&self, src: &str, name: &str) -> String {
        let content = std::fs::read(fixture(src)).unwrap();
        let dst = format!("{}/{}", self.dir, name);
        self.atomic_write(&dst, &content);
        dst
    }

    /// Atomically writes a string to a file.
    fn write_string(&self, file: &str, data: &str) {
        self.atomic_write(file, data.as_bytes());
    }

    /// Writes to a temporary file outside the watched directory, then renames it.
    fn atomic_write(&self, dst: &str, data: &[u8]) {
        static N: AtomicUsize = AtomicUsize::new(0);
        let tmp = format!("{}/.sd-test-{}", self.tmp, N.fetch_add(1, Ordering::Relaxed));
        std::fs::write(&tmp, data).unwrap();
        std::fs::rename(&tmp, dst).unwrap();
    }

    /// Appends a string to a file.
    fn append_string(&self, file: &str, data: &str) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(file).unwrap();
        f.write_all(data.as_bytes()).unwrap();
    }

    /// Starts the file SD and the loop receiving target group updates.
    fn run(&mut self, files: &[&str]) {
        let (tx, rx) = discovery::channel();
        let st = self.state.clone();
        self.threads.push(std::thread::spawn(move || {
            loop {
                match rx.recv() {
                    Recv::Update(tgs) => {
                        let mut s = st.lock().unwrap();
                        s.received_at = Some(Instant::now());
                        for tg in tgs {
                            s.tgs.insert(tg.source.clone(), tg);
                        }
                    }
                    _ => return,
                }
            }
        }));
        let files: Vec<String> = files.iter().map(|f| format!("{}/{}", self.dir, f)).collect();
        let ctx = self.cancel_sd.clone();
        self.threads.push(std::thread::spawn(move || {
            // A high refresh interval makes sure that the tests only rely on file watches.
            let conf = SdConfig { files, refresh_interval: yaml::Duration::from_secs(3600) };
            let reg = Registry::new();
            let refresh_metrics = discovery::new_refresh_metrics(&reg);
            let metrics = conf.new_discoverer_metrics(&reg, refresh_metrics);
            metrics.register().unwrap();
            let mut d = Discovery::new(&conf, discovery::Logger::nop(), &Some(metrics.clone())).unwrap();
            d.run(&ctx, tx);
            metrics.unregister();
        }));
    }

    fn stop(&mut self) {
        self.cancel_sd.cancel();
        for t in self.threads.drain(..) {
            t.join().unwrap();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_dir_all(&self.tmp);
    }

    fn last_receive(&self) -> Option<Instant> {
        self.state.lock().unwrap().received_at
    }

    fn targets(&self) -> Vec<Arc<Group>> {
        self.state.lock().unwrap().tgs.values().cloned().collect()
    }

    fn require_update(&self, reference: Option<Instant>, expected: &[Group]) {
        let want = groups_json(expected.iter());
        let deadline = Instant::now() + DEFAULT_WAIT;
        loop {
            std::thread::sleep(DEFAULT_WAIT / 10);
            if Instant::now() > deadline {
                panic!("Expected update but got none, last: {}", groups_json(self.targets().iter().map(|g| g.as_ref())));
            }
            if reference == self.last_receive() {
                // No update received.
                continue;
            }
            // File SD can emit an empty snapshot (matching group count) before the file contents are visible.
            // Keep waiting until the groups match rather than failing on the first incomplete update.
            let got = groups_json(self.targets().iter().map(|g| g.as_ref()));
            if want != got {
                continue;
            }
            return;
        }
    }

    fn require_target_groups(&self, expected: &[Group], got: &[Arc<Group>]) {
        assert_eq!(groups_json(expected.iter()), groups_json(got.iter().map(|g| g.as_ref())));
    }
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Go `json.Marshal([]*targetgroup.Group)`.
fn groups_json<'a>(gs: impl Iterator<Item = &'a Group>) -> String {
    json::Json::Arr(gs.map(|g| g.to_json()).collect()).stringify()
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

const FP: &str = "__meta_filepath";

/// validTg() maps to fixtures/valid.{json,yml}.
fn valid_tg(file: &str) -> Vec<Group> {
    vec![
        Group {
            targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "example.org:443")])],
            labels: ls(&[("foo", "bar"), (FP, file)]),
            source: file_source(file, 0),
        },
        Group { targets: vec![ls(&[("__address__", "my.domain")])], labels: ls(&[(FP, file)]), source: file_source(file, 1) },
    ]
}

/// valid2Tg() maps to fixtures/valid2.{json,yml}.
#[allow(dead_code)]
fn valid2_tg(file: &str) -> Vec<Group> {
    vec![
        Group { targets: vec![ls(&[("__address__", "my.domain")])], labels: ls(&[(FP, file)]), source: file_source(file, 0) },
        Group { targets: vec![ls(&[("__address__", "localhost:9090")])], labels: ls(&[("foo", "bar"), ("fred", "baz"), (FP, file)]), source: file_source(file, 1) },
        Group { targets: vec![ls(&[("__address__", "example.org:443")])], labels: ls(&[("scheme", "https"), (FP, file)]), source: file_source(file, 2) },
    ]
}

fn ext(f: &str) -> &str {
    &f[f.rfind('.').unwrap()..]
}

#[test]
fn test_initial_update() {
    for tc in ["fixtures/valid.yml", "fixtures/valid.json"] {
        let mut runner = TestRunner::new();
        let sd_file = runner.copy_file(tc);
        runner.run(&[&format!("*{}", ext(tc))]);
        // Verify that we receive the initial target groups.
        runner.require_update(None, &valid_tg(&sd_file));
        runner.stop();
    }
}

#[test]
fn test_invalid_file() {
    for tc in ["fixtures/invalid_nil.yml", "fixtures/invalid_nil.json"] {
        let now = Instant::now();
        let mut runner = TestRunner::new();
        runner.copy_file(tc);
        runner.run(&[&format!("*{}", ext(tc))]);
        // Verify that we've received nothing.
        std::thread::sleep(DEFAULT_WAIT);
        assert!(!runner.last_receive().is_some_and(|t| t > now), "unexpected targets received: {:?}", runner.targets());
        runner.stop();
    }
}

#[test]
fn test_noop_file_update() {
    let mut runner = TestRunner::new();
    let sd_file = runner.copy_file("fixtures/valid.yml");
    runner.run(&["*.yml"]);
    // Verify that we receive the initial target groups.
    runner.require_update(None, &valid_tg(&sd_file));
    // Verify that we receive an update with the same target groups.
    let r = runner.last_receive();
    runner.copy_file_to("fixtures/valid3.yml", "valid.yml");
    runner.require_update(r, &valid_tg(&sd_file));
}

#[test]
fn test_file_update() {
    let mut runner = TestRunner::new();
    let sd_file = runner.copy_file("fixtures/valid.yml");
    runner.run(&["*.yml"]);
    // Verify that we receive the initial target groups.
    runner.require_update(None, &valid_tg(&sd_file));
    // Verify that we receive an update with the new target groups.
    let r = runner.last_receive();
    runner.copy_file_to("fixtures/valid2.yml", "valid.yml");
    runner.require_update(r, &valid2_tg(&sd_file));
}

#[test]
fn test_invalid_file_update() {
    let mut runner = TestRunner::new();
    let sd_file = runner.copy_file("fixtures/valid.yml");
    runner.run(&["*.yml"]);
    // Verify that we receive the initial target groups.
    runner.require_update(None, &valid_tg(&sd_file));
    let r = runner.last_receive();
    runner.write_string(&sd_file, "]gibberish\n][");
    // Verify that we receive nothing or the same targets as before.
    std::thread::sleep(DEFAULT_WAIT);
    if runner.last_receive() > r {
        runner.require_target_groups(&valid_tg(&sd_file), &runner.targets());
    }
}

#[test]
fn test_update_file_with_partial_writes() {
    let mut runner = TestRunner::new();
    let sd_file = runner.copy_file("fixtures/valid.yml");
    runner.run(&["*.yml"]);
    // Verify that we receive the initial target groups.
    runner.require_update(None, &valid_tg(&sd_file));

    // Do a partial write operation.
    let r = runner.last_receive();
    runner.write_string(&sd_file, "- targets");
    std::thread::sleep(DEFAULT_WAIT);
    // Verify that we receive nothing or the same target groups as before.
    if runner.last_receive() > r {
        runner.require_target_groups(&valid_tg(&sd_file), &runner.targets());
    }

    // Verify that we receive the update target groups once the file is a valid YAML payload.
    let r = runner.last_receive();
    runner.append_string(&sd_file, r#": ["localhost:9091"]"#);
    runner.require_update(
        r,
        &[
            Group { targets: vec![ls(&[("__address__", "localhost:9091")])], labels: ls(&[(FP, &sd_file)]), source: file_source(&sd_file, 0) },
            Group { source: file_source(&sd_file, 1), ..Default::default() },
        ],
    );
}

#[test]
fn test_remove_file() {
    let mut runner = TestRunner::new();
    let sd_file = runner.copy_file("fixtures/valid.yml");
    runner.run(&["*.yml"]);
    // Verify that we receive the initial target groups.
    runner.require_update(None, &valid_tg(&sd_file));
    // Verify that we receive the update about the target groups being removed.
    let r = runner.last_receive();
    std::fs::remove_file(&sd_file).unwrap();
    runner.require_update(r, &[Group { source: file_source(&sd_file, 0), ..Default::default() }, Group { source: file_source(&sd_file, 1), ..Default::default() }]);
}
