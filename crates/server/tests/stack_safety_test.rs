//! Not an upstream test: checks that deep and large API queries do not crash the server (queries run on the HTTP
//! connection threads, whose stack is `server::run::QUERY_STACK_SIZE`).

mod common;

use std::time::Duration;

#[test]
fn deep_and_large_queries_do_not_crash() {
    let dir = common::TempDir::new();
    let port = common::random_port();
    let cfg = dir.join("prometheus.yml");
    std::fs::write(
        &cfg,
        format!("global:\n  scrape_interval: 1s\nscrape_configs:\n  - job_name: self\n    static_configs:\n      - targets: ['127.0.0.1:{port}']\n"),
    )
    .unwrap();
    let mut prom = common::prometheus_command_with_logging(
        &cfg,
        port,
        &[format!("--storage.tsdb.path={}", dir.join("data")), "--scrape.discovery-reload-interval=100ms".to_string()],
    );
    common::wait_for_prometheus_ready(port);
    // Some samples to query.
    std::thread::sleep(Duration::from_secs(3));

    let base = format!("http://127.0.0.1:{port}/api/v1");
    for depth in [2000usize, 10000] {
        let q = format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
        let r = common::post(&format!("{base}/query"), "application/x-www-form-urlencoded", format!("query={}", urlenc(&q)).as_bytes())
            .expect("deep query must get an answer");
        assert!(r.status == 200 || r.status == 400 || r.status == 422, "depth {depth}: status {} {}", r.status, r.body);
    }
    // A deep binary expression tree.
    let q = vec!["up"; 3000].join(" + ");
    let r = common::post(&format!("{base}/query"), "application/x-www-form-urlencoded", format!("query={}", urlenc(&q)).as_bytes())
        .expect("wide query must get an answer");
    assert!(r.status == 200 || r.status == 400 || r.status == 422, "status {} {}", r.status, r.body);
    // A large range query: 11000 steps (the API maximum) over every series.
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let r = common::get(&format!(
        "{base}/query_range?query={}&start={}&end={}&step=1",
        urlenc("rate({__name__=~\".+\"}[10s])"),
        now - 10_999,
        now
    ))
    .expect("range query must get an answer");
    assert_eq!(r.status, 200, "{}", r.body);

    assert!(prom.wait_timeout(Duration::from_millis(100)).is_none(), "server exited: {}", prom.logs());
    common::wait_for_prometheus_ready(port);
}

fn urlenc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}
