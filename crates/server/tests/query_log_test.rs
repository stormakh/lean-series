//! Port of `cmd/prometheus/query_log_test.go`.

mod common;

use std::io::{Seek, SeekFrom, Write};
use std::time::Duration;

use common::*;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Origin {
    Api,
    Console,
    Rule,
}

/// Go `queryLogTest`.
struct QueryLogTest {
    origin: Origin,
    prefix: String,
    host: String,
    port: u16,
    cwd: String,
    config_file: Option<std::fs::File>,
    enabled_at_start: bool,
}

impl QueryLogTest {
    /// Go `skip`: `Some(reason)` when the test is not needed or the prerequisites are not met.
    fn skip(&self) -> Option<&'static str> {
        if !self.prefix.is_empty() && self.origin == Origin::Rule {
            return Some("changing prefix has no effect on rules");
        }
        // Some systems don't support IPv4 or IPv6.
        let h = self.host.trim_start_matches('[').trim_end_matches(']');
        if std::net::TcpListener::bind((h, 0)).is_err() {
            return Some("ip version not supported");
        }
        None
    }

    /// Go `waitForPrometheus`.
    fn wait_for_prometheus(&self) {
        let url = format!("http://{}:{}{}/-/ready", self.host, self.port, self.prefix);
        let ok = eventually(Duration::from_secs(20), Duration::from_millis(500), || {
            get(&url).map(|r| r.status == 200).unwrap_or(false)
        });
        assert!(ok, "prometheus at {}:{} did not become ready in time", self.host, self.port);
    }

    /// Go `setQueryLog`.
    fn set_query_log(&mut self, query_log_file: &str) {
        let conf = self.configuration();
        let f = self.config_file.as_mut().unwrap();
        f.set_len(0).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        if !query_log_file.is_empty() {
            write!(f, "global:\n  query_log_file: {}\n", query_log_file).unwrap();
        }
        f.write_all(conf.as_bytes()).unwrap();
    }

    /// Go `query`.
    fn query(&self) {
        match self.origin {
            Origin::Api => {
                let r = get(&format!(
                    "http://{}:{}{}/api/v1/query_range?step=5&start=0&end=3600&query={}",
                    self.host,
                    self.port,
                    self.prefix,
                    yaml::url::query_escape("query_with_api"),
                ))
                .unwrap();
                assert_eq!(200, r.status);
            }
            Origin::Console => {
                let r = get(&format!("http://{}:{}{}/consoles/test.html", self.host, self.port, self.prefix)).unwrap();
                assert_eq!(200, r.status);
            }
            Origin::Rule => {
                // Poll the /api/v1/rules endpoint until a new rule evaluation is detected.
                let mut last_eval_time = web::gotime::Time::ZERO;
                loop {
                    let r = get(&format!("http://{}:{}/api/v1/rules", self.host, self.port)).unwrap();
                    // Parse the rules response to find the last evaluation time.
                    let new_eval_time = parse_last_evaluation(&r.body);
                    if new_eval_time > last_eval_time {
                        if !last_eval_time.is_zero() {
                            break;
                        }
                        last_eval_time = new_eval_time;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    /// Go `queryString`.
    fn query_string(&self) -> &'static str {
        match self.origin {
            Origin::Api => "query_with_api",
            Origin::Rule => "query_in_rule",
            Origin::Console => "query_in_console",
        }
    }

    /// Go `validateLastQuery`.
    fn validate_last_query(&self, ql: &[QueryLogLine]) {
        let q = ql.last().unwrap();
        assert_eq!(self.query_string(), q.query);

        match self.origin {
            Origin::Api => {
                assert_eq!(5, q.step);
                assert_eq!("1970-01-01T00:00:00.000Z", q.start);
                assert_eq!("1970-01-01T01:00:00.000Z", q.end);
            }
            _ => assert_eq!(0, q.step),
        }

        if self.origin != Origin::Rule {
            let mut host = self.host.as_str();
            if host == "[::1]" {
                host = "::1";
            }
            assert_eq!(host, q.client_ip);
        }

        match self.origin {
            Origin::Api => assert_eq!(format!("{}/api/v1/query_range", self.prefix), q.path),
            Origin::Console => assert_eq!(format!("{}/consoles/test.html", self.prefix), q.path),
            Origin::Rule => {
                assert_eq!("querylogtest", q.rule_group_name);
                assert_eq!(format!("{}/testdata/rules/test.yml", self.cwd), q.rule_group_file);
            }
        }
    }

    /// Go `String`.
    fn name(&self) -> String {
        let mut name = match self.origin {
            Origin::Api => "api queries",
            Origin::Console => "console queries",
            Origin::Rule => "rule queries",
        }
        .to_string();
        name = format!("{}, {}", name, self.host);
        if self.enabled_at_start {
            name += ", enabled at start";
        }
        if !self.prefix.is_empty() {
            name = format!("{}, with prefix {}", name, self.prefix);
        }
        name
    }

    /// Go `params`.
    fn params(&self) -> Vec<String> {
        let mut s = Vec::new();
        if !self.prefix.is_empty() {
            s.push(format!("--web.route-prefix={}", self.prefix));
        }
        if self.origin == Origin::Console {
            s.push(format!("--web.console.templates={}/testdata/consoles", self.cwd));
        }
        s
    }

    /// Go `configuration`.
    fn configuration(&self) -> String {
        match self.origin {
            Origin::Rule => format!("\nrule_files:\n- {}/testdata/rules/test.yml\n", self.cwd),
            _ => "\n".to_string(),
        }
    }

    /// Go `exactQueryCount`.
    fn exact_query_count(&self) -> bool {
        self.origin != Origin::Rule
    }

    /// Go `run`.
    fn run(&mut self) {
        if let Some(reason) = self.skip() {
            eprintln!("skip {}: {}", self.name(), reason);
            return;
        }

        // Setup temporary files for this test.
        let tmp = TempDir::new();
        let query_log_file = tmp.join("query");
        std::fs::File::create(&query_log_file).unwrap();
        let config_path = tmp.join("config");
        self.config_file = Some(std::fs::OpenOptions::new().create(true).truncate(true).read(true).write(true).open(&config_path).unwrap());

        if self.enabled_at_start {
            self.set_query_log(&query_log_file);
        } else {
            self.set_query_log("");
        }

        let dir = TempDir::new();

        let mut params = vec![
            format!("--config.file={}", config_path),
            "--web.enable-lifecycle".to_string(),
            format!("--web.listen-address={}:{}", self.host, self.port),
            format!("--storage.tsdb.path={}", dir.path().display()),
        ];
        params.extend(self.params());

        let reload_url = format!("http://{}:{}{}/-/reload", self.host, self.port, self.prefix);
        // Log stderr in case of failure (the Proc keeps the output; it is killed on drop).
        let prom = start_with_logging(&params);
        let _guard = LogOnPanic(&prom);
        self.wait_for_prometheus();

        if !self.enabled_at_start {
            self.query();
            assert!(read_query_log(&query_log_file).is_empty());
            self.set_query_log(&query_log_file);
            reload_prometheus_config(&reload_url);
        }

        self.query();

        // Wait for query log entry to be written (avoid race with file I/O).
        let ql = wait_for_query_log(&query_log_file, 1);
        let mut qc = ql.len();
        if self.exact_query_count() {
            assert_eq!(1, qc);
        } else {
            assert!(qc > 0, "no queries logged");
        }
        self.validate_last_query(&ql);

        self.set_query_log("");
        reload_prometheus_config(&reload_url);
        if !self.exact_query_count() {
            qc = read_query_log(&query_log_file).len();
        }

        self.query();

        let ql = read_query_log(&query_log_file);
        assert_eq!(qc, ql.len());

        qc = ql.len();
        self.set_query_log(&query_log_file);
        reload_prometheus_config(&reload_url);

        self.query();
        qc += 1;

        // Wait for query log entry to be written (avoid race with file I/O).
        let ql = wait_for_query_log(&query_log_file, qc);
        if self.exact_query_count() {
            assert_eq!(qc, ql.len());
        } else {
            assert!(ql.len() >= qc, "no queries logged");
        }
        self.validate_last_query(&ql);
        qc = ql.len();

        // Move the file, Prometheus should still write to the old file.
        let new_file = tmp.join("newLoc");
        std::fs::File::create(&new_file).unwrap();
        std::fs::rename(&query_log_file, &new_file).unwrap();
        let ql = read_query_log(&new_file);
        if self.exact_query_count() {
            assert_eq!(qc, ql.len());
        }
        self.validate_last_query(&ql);
        qc = ql.len();

        self.query();

        qc += 1;

        // Wait for query log entry to be written (avoid race with file I/O).
        let ql = wait_for_query_log(&new_file, qc);
        if self.exact_query_count() {
            assert_eq!(qc, ql.len());
        } else {
            assert!(ql.len() >= qc, "no queries logged");
        }
        self.validate_last_query(&ql);

        reload_prometheus_config(&reload_url);

        self.query();

        // Wait for query log entry to be written (avoid race with file I/O).
        let ql = wait_for_query_log(&query_log_file, 1);
        let qc = ql.len();
        if self.exact_query_count() {
            assert_eq!(1, qc);
        } else {
            assert!(qc > 0, "no queries logged");
        }
    }
}

/// Prints the server output when the test fails (Go `t.Log(stderr)`).
struct LogOnPanic<'a>(&'a Proc);

impl Drop for LogOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", self.0.logs());
        }
    }
}

/// Go `parseLastEvaluation`: the last evaluation timestamp of the /api/v1/rules response.
fn parse_last_evaluation(rules_body: &str) -> web::gotime::Time {
    let Ok(j) = json::Json::parse(rules_body) else {
        return web::gotime::Time::ZERO;
    };
    let groups = j.path("data.groups").and_then(|g| g.as_arr()).cloned().unwrap_or_default();
    for group in &groups {
        for rule in group.get("rules").and_then(|r| r.as_arr()).cloned().unwrap_or_default() {
            if let Some(s) = rule.get("lastEvaluation").and_then(|v| v.as_str()) {
                if let Ok(t) = web::gotime::Time::parse_rfc3339(s) {
                    return t;
                }
            }
        }
    }
    web::gotime::Time::ZERO
}

/// Go `queryLogLine` (the fields that the test reads).
#[derive(Default, Debug)]
struct QueryLogLine {
    query: String,
    step: i64,
    start: String,
    end: String,
    path: String,
    client_ip: String,
    rule_group_file: String,
    rule_group_name: String,
}

/// Go `readQueryLog`.
fn read_query_log(path: &str) -> Vec<QueryLogLine> {
    let data = std::fs::read_to_string(path).unwrap();
    let s = |j: &json::Json, p: &str| j.path(p).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let mut ql = Vec::new();
    for line in data.lines() {
        let j = json::Json::parse(line).unwrap_or_else(|e| panic!("bad query log line {line:?}: {e:?}"));
        ql.push(QueryLogLine {
            query: s(&j, "params.query"),
            step: j.path("params.step").and_then(|v| v.as_i64()).unwrap_or(0),
            start: s(&j, "params.start"),
            end: s(&j, "params.end"),
            path: s(&j, "httpRequest.path"),
            client_ip: s(&j, "httpRequest.clientIP"),
            rule_group_file: s(&j, "ruleGroup.file"),
            rule_group_name: s(&j, "ruleGroup.name"),
        });
    }
    ql
}

/// Go `waitForQueryLog`.
fn wait_for_query_log(path: &str, min_entries: usize) -> Vec<QueryLogLine> {
    let mut ql = Vec::new();
    let ok = eventually(Duration::from_secs(5), Duration::from_millis(100), || {
        ql = read_query_log(path);
        ql.len() >= min_entries
    });
    assert!(ok, "timed out waiting for query log to have at least {} entries, got {}", min_entries, ql.len());
    ql
}

#[test]
fn test_query_log() {
    // Go's working directory is cmd/prometheus.
    let cwd = std::fs::canonicalize(cmd_dir()).unwrap().to_string_lossy().into_owned();

    // adapted: Go runs the 24 subtests in parallel; they run one after another here (small port range).
    for host in ["127.0.0.1", "[::1]"] {
        for prefix in ["", "/foobar"] {
            for enabled_at_start in [true, false] {
                for origin in [Origin::Api, Origin::Console, Origin::Rule] {
                    let mut p = QueryLogTest {
                        origin,
                        host: host.to_string(),
                        enabled_at_start,
                        prefix: prefix.to_string(),
                        cwd: cwd.clone(),
                        port: 0,
                        config_file: None,
                    };
                    eprintln!("=== RUN {}", p.name());
                    p.port = random_port();
                    p.run();
                }
            }
        }
    }
}
