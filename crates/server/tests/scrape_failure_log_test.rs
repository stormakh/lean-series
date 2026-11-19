//! Port of `cmd/prometheus/scrape_failure_log_test.go`.

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use common::*;

#[test]
fn test_scrape_failure_log_file() {
    // Tracks the number of requests made to the mock server.
    let request_count = Arc::new(AtomicI32::new(0));

    // Starts a server that always returns HTTP 500 errors.
    let mock = start_garbage_server(request_count.clone());
    let mock_server_address = mock.address.clone();

    // Create a temporary directory for Prometheus configuration and logs.
    let temp_dir = TempDir::new();

    // Define file paths for the scrape failure log and Prometheus configuration.
    // Like other files, the scrape failure log file should be relative to the
    // config file. Therefore, we split the name we put in the file and the full
    // path used to check the content of the file.
    let scrape_failure_log_file_name = "scrape_failure.log";
    let scrape_failure_log_file = temp_dir.join(scrape_failure_log_file_name);
    let prom_config_file = temp_dir.join("prometheus.yml");

    // Step 1: Set up an initial Prometheus configuration that globally
    // specifies a scrape failure log file.
    let prom_config = format!(
        "
global:
  scrape_interval: 500ms
  scrape_failure_log_file: {scrape_failure_log_file_name}

scrape_configs:
  - job_name: 'test_job'
    static_configs:
      - targets: ['{mock_server_address}']
"
    );
    std::fs::write(&prom_config_file, prom_config).expect("Failed to write Prometheus configuration file");

    // Start Prometheus with the generated configuration and a random port, enabling the lifecycle API.
    let port = random_port();
    let params = vec![
        format!("--config.file={prom_config_file}"),
        format!("--storage.tsdb.path={}", temp_dir.join("data")),
        format!("--web.listen-address=127.0.0.1:{port}"),
        "--web.enable-lifecycle".to_string(),
    ];
    let _prom = start_with_logging(&params);

    // Wait until the mock server receives at least two requests from Prometheus.
    let ok = eventually(Duration::from_secs(30), Duration::from_millis(500), || request_count.load(Ordering::SeqCst) >= 2);
    assert!(ok, "Expected at least two requests to the mock server");

    // Verify that the scrape failures have been logged to the specified file.
    let content = std::fs::read_to_string(&scrape_failure_log_file).expect("Failed to read scrape failure log");
    assert!(
        content.contains("server returned HTTP status 500 Internal Server Error"),
        "Expected scrape failure log entry not found: {content}"
    );

    // Step 2: Update the Prometheus configuration to remove the scrape failure
    // log file setting.
    let prom_config = format!(
        "
global:
  scrape_interval: 1s

scrape_configs:
  - job_name: 'test_job'
    static_configs:
      - targets: ['{mock_server_address}']
"
    );
    std::fs::write(&prom_config_file, prom_config).expect("Failed to update Prometheus configuration file");

    // Reload Prometheus with the updated configuration.
    let reload_url = format!("http://127.0.0.1:{port}/-/reload");
    reload_prometheus_config(&reload_url);

    // Count the number of lines in the scrape failure log file before any
    // further requests.
    let pre_reload_log_line_count = count_lines_in_file(&scrape_failure_log_file);

    // Wait for at least two more requests to the mock server to ensure
    // Prometheus continues scraping.
    let requests_before_reload = request_count.load(Ordering::SeqCst);
    let ok = eventually(Duration::from_secs(30), Duration::from_millis(500), || {
        request_count.load(Ordering::SeqCst) >= requests_before_reload + 2
    });
    assert!(ok, "Expected two more requests to the mock server after configuration reload");

    // Ensure that no new lines were added to the scrape failure log file after
    // the configuration change.
    assert_eq!(
        pre_reload_log_line_count,
        count_lines_in_file(&scrape_failure_log_file),
        "No new lines should be added to the scrape failure log file after removing the log setting"
    );

    // Step 3: Re-add the scrape failure log file setting, but this time under
    // scrape_configs, and reload Prometheus.
    let prom_config = format!(
        "
global:
  scrape_interval: 1s

scrape_configs:
  - job_name: 'test_job'
    scrape_failure_log_file: {scrape_failure_log_file_name}
    static_configs:
      - targets: ['{mock_server_address}']
"
    );
    std::fs::write(&prom_config_file, prom_config).expect("Failed to update Prometheus configuration file");

    // Reload Prometheus with the updated configuration.
    reload_prometheus_config(&reload_url);

    // Wait for at least two more requests to the mock server and verify that
    // new log entries are created.
    let post_reload_log_line_count = count_lines_in_file(&scrape_failure_log_file);
    let requests_before_re_adding_log = request_count.load(Ordering::SeqCst);
    let ok = eventually(Duration::from_secs(30), Duration::from_millis(500), || {
        request_count.load(Ordering::SeqCst) >= requests_before_re_adding_log + 2
    });
    assert!(ok, "Expected two additional requests after re-adding the log setting");

    // Confirm that new lines were added to the scrape failure log file.
    assert!(
        count_lines_in_file(&scrape_failure_log_file) > post_reload_log_line_count,
        "New lines should be added to the scrape failure log file after re-adding the log setting"
    );
}

/// A mock HTTP server (Go `httptest.Server`), stopped on drop.
struct GarbageServer {
    address: String,
    stop: Arc<AtomicBool>,
}

impl Drop for GarbageServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake up the accept loop.
        let _ = std::net::TcpStream::connect(&self.address);
    }
}

/// Go `startGarbageServer`: a mock server that returns a 500 Internal Server Error for all requests. It also
/// increments the request count each time it's hit.
fn start_garbage_server(request_count: Arc<AtomicI32>) -> GarbageServer {
    // adapted: httptest picks any free port; this one stays in the agent's port range.
    let port = random_port();
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("listen");
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            if stop2.load(Ordering::SeqCst) {
                return;
            }
            let Ok(conn) = conn else { continue };
            let rc = request_count.clone();
            std::thread::spawn(move || serve_500(conn, &rc));
        }
    });
    GarbageServer { address: format!("127.0.0.1:{port}"), stop }
}

/// Answers every request on the connection with an empty 500 response.
fn serve_500(conn: std::net::TcpStream, request_count: &AtomicI32) {
    let _ = conn.set_read_timeout(Some(Duration::from_secs(30)));
    let mut w = match conn.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut r = BufReader::new(conn);
    loop {
        let mut content_length = 0usize;
        let mut line = String::new();
        // Request line.
        match r.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        // Headers.
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            let l = line.trim_end();
            if l.is_empty() {
                break;
            }
            if let Some((k, v)) = l.split_once(':') {
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
        }
        let mut body = vec![0u8; content_length];
        if r.read_exact(&mut body).is_err() {
            return;
        }
        request_count.fetch_add(1, Ordering::SeqCst);
        if w.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n").is_err() {
            return;
        }
    }
}

/// Go `countLinesInFile`: the number of lines in the file (0 when it can't be read).
fn count_lines_in_file(file_path: &str) -> usize {
    match std::fs::read(file_path) {
        Ok(data) => data.iter().filter(|&&b| b == b'\n').count(),
        Err(_) => 0,
    }
}
