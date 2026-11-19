//! Shared helpers of the server tests (Go `cmd/prometheus` test helpers: `promPath`, `prometheusCommandWithLogging`,
//! `waitForPrometheusReady`, `testutil.RandomUnprivilegedPort`, `getMetricValue`, ...).
//!
//! The tests run the real `prometheus` binary as Go's tests run the test binary with `-test.main`.
//! Ports: the agent's port range is 9970-9999; [`random_port`] hands out free ports from 9972-9999 and never the
//! same port twice in one test process.
#![allow(dead_code)]

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Go `promPath`: the binary under test.
pub fn prom_path() -> &'static str {
    env!("CARGO_BIN_EXE_prometheus")
}

/// The upstream repository root (test data is read from there, as the Go tests do).
pub fn upstream() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../upstream")
}

/// Go `promConfig`: documentation/examples/prometheus.yml.
pub fn prom_config() -> String {
    upstream().join("documentation/examples/prometheus.yml").to_string_lossy().into_owned()
}

/// Go `agentConfig`: documentation/examples/prometheus-agent.yml.
pub fn agent_config() -> String {
    upstream().join("documentation/examples/prometheus-agent.yml").to_string_lossy().into_owned()
}

/// The directory of cmd/prometheus (Go's working directory in these tests).
pub fn cmd_dir() -> PathBuf {
    upstream().join("cmd/prometheus")
}

/// Go `startupTime`.
pub const STARTUP_TIME: Duration = Duration::from_secs(10);
/// Go `readyTimeout`.
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

static USED_PORTS: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// Go `testutil.RandomUnprivilegedPort`: a free TCP port (in this agent's range 9972-9999).
pub fn random_port() -> u16 {
    let mut used = USED_PORTS.lock().unwrap_or_else(|p| p.into_inner());
    let used = used.get_or_insert_with(HashSet::new);
    let start = (std::process::id() as u16) % 28;
    for i in 0..28u16 {
        let p = 9972 + (start + i) % 28;
        if used.contains(&p) {
            continue;
        }
        let free4 = std::net::TcpListener::bind(("0.0.0.0", p)).is_ok();
        let free6 = std::net::TcpListener::bind(("::1", p)).is_ok() || std::net::TcpListener::bind(("::", p)).is_ok();
        if free4 && free6 {
            used.insert(p);
            return p;
        }
    }
    panic!("no free port in 9972-9999");
}

/// A temporary directory under the agent's bench/tmp folder (/tmp has a small quota), removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> TempDir {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w4-server/tests");
        let p = base.join(format!("t{}-{}-{}", std::process::id(), n, Instant::now().elapsed().as_nanos()));
        std::fs::create_dir_all(&p).expect("create temp dir");
        TempDir(std::fs::canonicalize(&p).unwrap_or(p))
    }
    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
    pub fn join(&self, s: &str) -> String {
        self.0.join(s).to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A running server that is killed (and reaped) on drop (Go `t.Cleanup(cmd.Process.Kill(); cmd.Wait())`).
pub struct Proc {
    pub child: Child,
    pub log: std::sync::Arc<Mutex<String>>,
}

impl Proc {
    /// Sends a signal (`libc::SIGINT`, ...).
    pub fn signal(&self, sig: i32) {
        // SAFETY: kill(2) on our own child's pid.
        unsafe {
            libc::kill(self.child.id() as i32, sig);
        }
    }
    /// Waits up to `d` for the process to exit; `Some(status)` when it did.
    pub fn wait_timeout(&mut self, d: Duration) -> Option<std::process::ExitStatus> {
        let end = Instant::now() + d;
        loop {
            if let Ok(Some(s)) = self.child.try_wait() {
                return Some(s);
            }
            if Instant::now() >= end {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    pub fn logs(&self) -> String {
        self.log.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Go `commandWithLogging` + `Start`: runs the binary with `args`, collecting stdout and stderr lines.
pub fn start_with_logging(args: &[String]) -> Proc {
    start_with_env(args, &[], &[])
}

/// Like [`start_with_logging`], with environment variables to set and to remove.
pub fn start_with_env(args: &[String], set: &[(&str, &str)], remove: &[&str]) -> Proc {
    let mut cmd = Command::new(prom_path());
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped()).env("no_proxy", "localhost,127.0.0.1,0.0.0.0,:");
    for k in remove {
        cmd.env_remove(k);
    }
    for (k, v) in set {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("start prometheus");
    let log = std::sync::Arc::new(Mutex::new(String::new()));
    for r in [Box::new(child.stdout.take().unwrap()) as Box<dyn Read + Send>, Box::new(child.stderr.take().unwrap())] {
        let log = log.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(r).lines().map_while(Result::ok) {
                let mut l = log.lock().unwrap_or_else(|p| p.into_inner());
                l.push_str(&line);
                l.push('\n');
            }
        });
    }
    Proc { child, log }
}

/// Go `prometheusCommandWithLogging` + `Start`.
pub fn prometheus_command_with_logging(config_file: &str, port: u16, extra: &[String]) -> Proc {
    let mut args = vec![format!("--config.file={config_file}"), format!("--web.listen-address=0.0.0.0:{port}")];
    args.extend(extra.iter().cloned());
    start_with_logging(&args)
}

/// Go `require.Eventually`.
pub fn eventually(timeout: Duration, tick: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(tick);
    }
}

/// A plain HTTP response.
pub struct Resp {
    pub status: u16,
    pub body: String,
}

fn client() -> http::Client {
    http::Client::default()
}

/// HTTP GET (Go `http.Get`); Err when the connection fails.
pub fn get(url: &str) -> Result<Resp, String> {
    let r = client().request("GET", url, &[], b"", Duration::from_secs(30)).map_err(|e| format!("{e:?}"))?;
    Ok(Resp { status: r.status, body: String::from_utf8_lossy(&r.body).into_owned() })
}

/// HTTP POST with a body and content type.
pub fn post(url: &str, content_type: &str, body: &[u8]) -> Result<Resp, String> {
    let r = client()
        .request("POST", url, &[("Content-Type", content_type)], body, Duration::from_secs(30))
        .map_err(|e| format!("{e:?}"))?;
    Ok(Resp { status: r.status, body: String::from_utf8_lossy(&r.body).into_owned() })
}

/// Go `waitForPrometheusReady`.
pub fn wait_for_prometheus_ready(port: u16) {
    let ok = eventually(READY_TIMEOUT, Duration::from_millis(100), || {
        get(&format!("http://127.0.0.1:{port}/-/ready")).map(|r| r.status == 200).unwrap_or(false)
    });
    assert!(ok, "Prometheus didn't become ready in time");
}

/// Go `reloadPrometheusConfig`.
pub fn reload_prometheus_config(reload_url: &str) {
    let r = post(reload_url, "text/plain", b"").expect("Failed to reload Prometheus");
    assert_eq!(200, r.status, "Unexpected status code when reloading Prometheus");
}

/// Go `getMetricValue` (gauges and counters of the text exposition; the family must have exactly one metric).
pub fn get_metric_value(body: &str, metric_name: &str) -> Result<f64, String> {
    let mut found_family = false;
    let mut values = Vec::new();
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            if rest.split_whitespace().next() == Some(metric_name) {
                found_family = true;
            }
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let name_end = line.find(['{', ' ']).unwrap_or(line.len());
        if &line[..name_end] != metric_name {
            continue;
        }
        found_family = true;
        let v = line.rsplit(' ').next().unwrap_or("");
        let v = match v {
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            "NaN" => f64::NAN,
            v => v.parse().map_err(|_| format!("bad value {v}"))?,
        };
        values.push(v);
    }
    if !found_family {
        return Err("metric family not found".to_string());
    }
    if values.len() != 1 {
        return Err("metric not found".to_string());
    }
    Ok(values[0])
}

/// The exit code of a finished process (`None` when it was killed by a signal).
pub fn exit_code(s: &std::process::ExitStatus) -> Option<i32> {
    s.code()
}
