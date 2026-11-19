//! Port of cmd/promtool/metrics_test.go.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use discovery::log::Logger;
use promtool::{FAILURE_EXIT_CODE, Io, SUCCESS_EXIT_CODE};
use remote::remoteapi::{MessageTypes, WriteHandler, WriteMessageType, WriteResponse, WriteStorage, new_write_handler};
use storage::{Context, Error};

/// What the mock storage saw (Go `mockStorage` fields).
#[derive(Default)]
struct MockState {
    called: bool,
    last_err: Option<String>,
    received_data: Vec<u8>,
    received_content_type: String,
}

/// Go `mockStorage`.
#[derive(Clone, Default)]
struct MockStorage(Arc<Mutex<MockState>>);

impl WriteStorage for MockStorage {
    fn store(&self, _ctx: &Context, req: &http::Request, _t: WriteMessageType) -> (Option<WriteResponse>, Option<Error>) {
        let mut m = self.0.lock().unwrap();
        m.called = true;
        // Capture content-type header.
        m.received_content_type = req.header("content-type").unwrap_or("").to_string();
        m.received_data = req.body.clone();
        if let Some(e) = &m.last_err {
            return (None, Some(Error::msg(e.clone())));
        }
        let mut resp = WriteResponse::new();
        resp.set_status_code(204);
        (Some(resp), None)
    }
}

struct Handler(WriteHandler);

impl http::server::Service for Handler {
    fn handle(&self, req: http::Request) -> http::Response {
        self.0.serve(&Context::background(), req)
    }
}

/// Go `setupTestServer`: a remote write handler with a mock storage behind an HTTP server on 127.0.0.1:0.
/// adapted: the server thread is not closed at the end of the test (the crates/http server runs forever).
fn setup_test_server(supported_types: MessageTypes) -> (MockStorage, yaml::url::GoUrl) {
    let store = MockStorage::default();
    let handler = new_write_handler(Box::new(store.clone()), supported_types, Logger::nop());
    let srv = http::server::Server::bind("127.0.0.1:0", http::server::ServerConfig::default()).expect("bind");
    let port = srv.local_port();
    thread::spawn(move || srv.run(Arc::new(Handler(handler))));
    let server_url = yaml::url::parse(&format!("http://127.0.0.1:{port}")).expect("parse url");
    (store, server_url)
}

/// Go `t.TempDir()`, under the target directory.
fn temp_dir(name: &str) -> std::path::PathBuf {
    let d = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("promtool-metrics-test")
        .join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Go `createMetricsFile`: a temporary file with the given metrics data.
fn create_metrics_file(name: &str, metrics_data: &str) -> String {
    let dir = temp_dir(name);
    std::fs::create_dir_all(&dir).unwrap();
    let tmp_file = dir.join("metrics.txt");
    std::fs::write(&tmp_file, metrics_data).unwrap();
    tmp_file.to_string_lossy().into_owned()
}

/// Go `PushMetrics(serverURL, http.DefaultTransport, {}, 30s, protoMsg, "/api/v1/write", {"job": "test"}, file)`.
fn push(server_url: &yaml::url::GoUrl, proto_msg: &str, file: &str) -> i32 {
    let mut stdin: &[u8] = b"";
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut io = Io { stdin: &mut stdin, stdout: &mut stdout, stderr: &mut stderr };
    let labels: BTreeMap<String, String> = [("job".to_string(), "test".to_string())].into_iter().collect();
    promtool::metrics::push_metrics(
        server_url,
        Arc::new(net::Transport::default_transport()),
        &BTreeMap::new(),
        Duration::from_secs(30),
        proto_msg,
        "/api/v1/write",
        &labels,
        &[file.to_string()],
        &mut io,
    )
}

#[test]
fn test_push_metrics() {
    struct Case {
        name: &'static str,
        metrics_data: &'static str,
        proto_msg: &'static str,
        server_types: MessageTypes,
    }
    let tests = [
        Case {
            name: "successful push with gauge metrics (V1)",
            metrics_data: "# HELP test_metric A test metric\n# TYPE test_metric gauge\ntest_metric{label=\"value1\"} 42.0\ntest_metric{label=\"value2\"} 43.0\n",
            proto_msg: "prometheus.WriteRequest",
            server_types: MessageTypes(vec![WriteMessageType::V1]),
        },
        Case {
            name: "successful push with counter metrics (V1)",
            metrics_data: "# HELP test_counter A test counter\n# TYPE test_counter counter\ntest_counter 100\n",
            proto_msg: "prometheus.WriteRequest",
            server_types: MessageTypes(vec![WriteMessageType::V1]),
        },
        Case {
            name: "successful push with gauge metrics (V2)",
            metrics_data: "# HELP test_metric A test metric\n# TYPE test_metric gauge\ntest_metric{label=\"value1\"} 42.0\ntest_metric{label=\"value2\"} 43.0\n",
            proto_msg: "io.prometheus.write.v2.Request",
            server_types: MessageTypes(vec![WriteMessageType::V2]),
        },
    ];

    for (i, tc) in tests.into_iter().enumerate() {
        let (store, server_url) = setup_test_server(tc.server_types);
        let tmp_file = create_metrics_file(&format!("push-{i}"), tc.metrics_data);

        let status = push(&server_url, tc.proto_msg, &tmp_file);

        assert_eq!(SUCCESS_EXIT_CODE, status, "{}", tc.name);
        let m = store.0.lock().unwrap();
        assert!(m.called, "{}: Handler should have been called", tc.name);
        assert!(m.last_err.is_none(), "{}: Handler should not have returned an error", tc.name);
        assert!(
            !m.received_data.is_empty(),
            "{}: Request should contain data (compression and decompression successful)",
            tc.name
        );
        assert!(
            m.received_content_type.contains("application/x-protobuf"),
            "{}: Content-Type should be protobuf",
            tc.name
        );
    }
}

#[test]
fn test_push_metrics_invalid_proto_msg() {
    let (store, server_url) = setup_test_server(MessageTypes(vec![WriteMessageType::V1]));
    let tmp_file = create_metrics_file("invalid-proto", "test_metric 1\n");

    let status = push(&server_url, "blablalba", &tmp_file); // Invalid protoMsg.

    assert_eq!(FAILURE_EXIT_CODE, status);
    assert!(!store.0.lock().unwrap().called, "Handler should not have been called for invalid protoMsg");
}
