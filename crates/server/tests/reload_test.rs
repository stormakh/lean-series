//! Port of `cmd/prometheus/reload_test.go`.

mod common;

use std::time::Duration;

use common::*;

const CONFIG_RELOAD_METRIC: &str = "prometheus_config_last_reload_successful";

struct Step {
    config_text: &'static str,
    expected_interval: &'static str,
    expected_metric: f64,
}

#[test]
fn test_auto_reload_config_valid_to_valid() {
    let steps = [
        Step { config_text: "\nglobal:\n  scrape_interval: 30s\n", expected_interval: "30s", expected_metric: 1.0 },
        Step { config_text: "\nglobal:\n  scrape_interval: 15s\n", expected_interval: "15s", expected_metric: 1.0 },
        Step { config_text: "\nglobal:\n  scrape_interval: 30s\n", expected_interval: "30s", expected_metric: 1.0 },
    ];
    run_test_steps(&steps);
}

#[test]
fn test_auto_reload_config_valid_to_invalid_to_valid() {
    let steps = [
        Step { config_text: "\nglobal:\n  scrape_interval: 30s\n", expected_interval: "30s", expected_metric: 1.0 },
        Step {
            config_text: "\nglobal:\n  scrape_interval: 15s\ninvalid_syntax\n",
            expected_interval: "30s",
            expected_metric: 0.0,
        },
        Step { config_text: "\nglobal:\n  scrape_interval: 30s\n", expected_interval: "30s", expected_metric: 1.0 },
        Step {
            config_text: "\nglobal:\n  scrape_interval: 15s\n  scrape_timeout: 30s\n",
            expected_interval: "30s",
            expected_metric: 0.0,
        },
        Step { config_text: "\nglobal:\n  scrape_interval: 30s\n", expected_interval: "30s", expected_metric: 1.0 },
    ];
    run_test_steps(&steps);
}

/// Go `runTestSteps`.
fn run_test_steps(steps: &[Step]) {
    let config_dir = TempDir::new();
    let config_file_path = config_dir.join("prometheus.yml");
    eprintln!("Config file path: {config_file_path}");

    std::fs::write(&config_file_path, steps[0].config_text).expect("Failed to write initial config file");

    let port = random_port();
    // adapted: Go's test binary writes its data to ./data of the test directory; the data dir goes to the temp
    // dir here so that the repository stays clean.
    let data = config_dir.join("data");
    let _prom = prometheus_command_with_logging(
        &config_file_path,
        port,
        &[
            "--config.auto-reload".to_string(),
            "--config.auto-reload-interval=1s".to_string(),
            format!("--storage.tsdb.path={data}"),
        ],
    );

    let base_url = format!("http://localhost:{port}");
    wait_for_prometheus_ready(port);

    for (i, step) in steps.iter().enumerate() {
        eprintln!("Step {i}");
        std::fs::write(&config_file_path, step.config_text).expect("Failed to write config file for step");

        let ok = eventually(Duration::from_secs(10), Duration::from_millis(500), || {
            verify_scrape_interval(&base_url, step.expected_interval)
                && verify_config_reload_metric(&base_url, step.expected_metric)
        });
        assert!(ok, "Prometheus config reload didn't happen in time");
    }
}

/// Go `verifyScrapeInterval`.
fn verify_scrape_interval(base_url: &str, expected_interval: &str) -> bool {
    let resp = get(&format!("{base_url}/api/v1/status/config")).unwrap();
    let config = json::Json::parse(&resp.body).unwrap();
    let yaml = config.path("data.yaml").and_then(|v| v.as_str()).unwrap_or("");
    yaml.contains(&format!("scrape_interval: {expected_interval}"))
}

/// Go `verifyConfigReloadMetric`.
fn verify_config_reload_metric(base_url: &str, expected_value: f64) -> bool {
    let resp = get(&format!("{base_url}/metrics")).unwrap();
    let mut actual_value = 0.0;
    let mut found = false;
    for line in resp.body.split('\n') {
        if line.starts_with(CONFIG_RELOAD_METRIC) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                actual_value = parts[1].parse::<f64>().unwrap();
                found = true;
                break;
            }
        }
    }
    found && actual_value == expected_value
}
