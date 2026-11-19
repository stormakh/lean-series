//! Port of `scrape/target_test.go`.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use config::ScrapeConfig;
use discovery::targetgroup::Group;
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, EXPONENTIAL_SCHEMA_MAX, Histogram, Span, custom_values};
use model::labels::{Builder, Labels, from_map, from_strings};
use model::labelset::LabelSet;
use net::{BasicAuth, HttpClientConfig, HttpClientOptions, TlsConfig, new_client_from_config};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer};
use scrape::limits::{ERR_SAMPLE_LIMIT, Limits, LimitsV1, LimitsV2};
use scrape::target::{
    ADDRESS_LABEL, JOB_LABEL, METRICS_PATH_LABEL, SCHEME_LABEL, Target, UNIX_SOCKET_LABEL, targets_from_group,
};
use scrape::{Scraper, TargetScraper};
use storage::{AOptions, Appendable as _, AppendableV2 as _, Context};
use util::teststorage::Appendable;
use yaml::url::{GoUrl, encode_query};

/// Go's test files live in `upstream/scrape`; their "testdata/..." paths are relative to it.
fn testdata(name: &str) -> String {
    format!("{}/../../upstream/scrape/testdata/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn ca_cert_path() -> String {
    testdata("ca.cer")
}

fn scrape_config_with_params(params: &[(&str, &[&str])]) -> Arc<ScrapeConfig> {
    let mut m = BTreeMap::new();
    for (k, vs) in params {
        m.insert(k.to_string(), vs.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    }
    Arc::new(ScrapeConfig { params: m, ..Default::default() })
}

#[test]
fn test_target_log_value() {
    let target = Arc::new(Target::new(
        from_strings(&[SCHEME_LABEL, "http", ADDRESS_LABEL, "test.invalid:9090", METRICS_PATH_LABEL, "/metrics"]),
        scrape_config_with_params(&[("zone", &["a b", "c"])]),
        LabelSet::new(),
        LabelSet::new(),
    ));
    const WANT_URL: &str = "http://test.invalid:9090/metrics?zone=a+b&zone=c";

    let new_scraper = |t: Option<Arc<Target>>| {
        TargetScraper::new(
            t,
            new_client_from_config(&HttpClientConfig::default(), &HttpClientOptions::default()).unwrap(),
            Duration::from_secs(10),
            0,
            String::new(),
            String::new(),
            false,
            discovery::Logger::nop(),
            scrape::testing::new_test_scrape_metrics(),
        )
    };

    // adapted: Go checks that the slog.LogValuer of a target (and of a targetScraper, and of a nil target)
    // resolves to the URL string (nil for a nil target), that encoding/json marshals the target as `{}`
    // (no exported fields), and the output of promslog's json/logfmt handlers in slog and go-kit styles. The
    // JSON marshaling and the slog/promslog handler formats are Go-only facts. The Rust log value of a target
    // is its Display (the URL), and of a scraper `Scraper::target_string` (None for a scraper without target,
    // Go's nil), which is what the scrape loop and pool put under the "target" log key.
    // name: "target"
    assert_eq!(WANT_URL, target.to_string());
    assert_eq!(WANT_URL, target.url_string());
    // name: "target scraper"
    assert_eq!(Some(WANT_URL.to_string()), new_scraper(Some(target.clone())).target_string());
    // name: "nil target"
    assert_eq!(None, new_scraper(None).target_string());

    // The value logged under "target" through a logger, direct and with `With`.
    let got: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let g = got.clone();
    let logger = discovery::Logger::new(Arc::new(move |_, _, kv: &[(String, String)]| {
        g.lock().unwrap().extend(kv.iter().cloned());
    }));
    let v = new_scraper(Some(target.clone())).target_string().unwrap();
    logger.info("test", &[("target", &v)]);
    logger.with(&[("target", &v)]).info("test", &[]);
    assert_eq!(
        vec![("target".to_string(), WANT_URL.to_string()), ("target".to_string(), WANT_URL.to_string())],
        *got.lock().unwrap()
    );
}

#[test]
fn test_target_labels() {
    let target = new_test_target("example.com:80", Duration::ZERO, from_strings(&["job", "some_job", "foo", "bar"]));
    let want = from_strings(&[JOB_LABEL, "some_job", "foo", "bar"]);
    let got = target.labels();
    assert_eq!(want, got);
    let mut i = 0;
    target.labels_range(|n, v| {
        match i {
            0 => assert_eq!(("foo", "bar"), (n, v)),
            1 => assert_eq!((JOB_LABEL, "some_job"), (n, v)),
            _ => {}
        }
        i += 1;
    });
    assert_eq!(2, i);
}

#[test]
fn test_target_offset() {
    let interval = Duration::from_secs(10);
    let offset_seed = 0u64;

    // Calculate offsets for 10000 different targets.
    let mut offsets = vec![Duration::ZERO; 10000];
    for (i, o) in offsets.iter_mut().enumerate() {
        let target = new_test_target("example.com:80", Duration::ZERO, from_strings(&["label", &i.to_string()]));
        *o = target.offset(interval, offset_seed);
    }

    // Put the offsets into buckets and validate that they are all within bounds.
    let bucket_size = Duration::from_secs(1);
    let mut buckets = vec![0usize; (interval.as_nanos() / bucket_size.as_nanos()) as usize];

    for offset in &offsets {
        assert!(*offset <= interval, "Offset {offset:?} out of bounds.");
        let bucket = (offset.as_nanos() / bucket_size.as_nanos()) as usize;
        buckets[bucket] += 1;
    }

    println!("{buckets:?}");

    // Calculate whether the number of targets per bucket does not differ more than a given tolerance.
    let avg = offsets.len() / buckets.len();
    let tolerance = 0.15;

    for bucket in &buckets {
        let diff = (*bucket as i64 - avg as i64).abs();
        assert!(diff as f64 / avg as f64 <= tolerance, "Bucket out of tolerance bounds.");
    }
}

#[test]
fn test_target_url() {
    let scrape_config = scrape_config_with_params(&[("abc", &["foo", "bar", "baz"]), ("xyz", &["hoo"])]);
    let labels = from_map([
        (ADDRESS_LABEL, "example.com:1234"),
        (SCHEME_LABEL, "https"),
        (METRICS_PATH_LABEL, "/metricz"),
        ("__param_abc", "overwrite"),
        ("__param_cde", "huu"),
    ]);
    let target = Target::new(labels, scrape_config, LabelSet::new(), LabelSet::new());

    // The reserved labels are concatenated into a full URL. The first value for each URL query parameter can
    // be set/modified via labels as well.
    let expected_params: Vec<(String, String)> = [
        ("abc", "overwrite"),
        ("abc", "bar"),
        ("abc", "baz"),
        ("cde", "huu"),
        ("xyz", "hoo"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let expected_url = GoUrl {
        scheme: "https".to_string(),
        host: "example.com:1234".to_string(),
        path: "/metricz".to_string(),
        raw_query: encode_query(&expected_params),
        ..Default::default()
    };

    assert_eq!(expected_url, target.url());
}

#[test]
fn test_target_url_unix() {
    let labels = from_map([
        (ADDRESS_LABEL, "example.com:1234"),
        (SCHEME_LABEL, "https"),
        (METRICS_PATH_LABEL, "/metrics"),
        (UNIX_SOCKET_LABEL, "/tmp/sock"),
    ]);
    let target = Target::new(labels, Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new());

    // __unix_socket__ does not affect URL generation; the URL is built from __scheme__, __address__ and
    // __metrics_path__ as usual. The socket path is passed to the dialer via the request context.
    let expected_url = GoUrl {
        scheme: "https".to_string(),
        host: "example.com:1234".to_string(),
        path: "/metrics".to_string(),
        ..Default::default()
    };

    assert_eq!(expected_url, target.url());
}

#[test]
fn test_target_url_unix_empty_address() {
    let labels = from_map([
        (SCHEME_LABEL, "http"),
        (METRICS_PATH_LABEL, "/metrics"),
        (UNIX_SOCKET_LABEL, "/tmp/sock"),
        // No __address__ set.
    ]);
    let target = Target::new(labels, Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new());

    // When __unix_socket__ is set but __address__ is empty, "localhost" is used as a placeholder host to keep
    // the URL valid.
    let expected_url = GoUrl {
        scheme: "http".to_string(),
        host: "localhost".to_string(),
        path: "/metrics".to_string(),
        ..Default::default()
    };

    assert_eq!(expected_url, target.url());
}

/// Go `newTestTarget`.
fn new_test_target(target_url: &str, _: Duration, lbls: Labels) -> Target {
    let mut lb = Builder::new(lbls);
    lb.set(SCHEME_LABEL, "http");
    lb.set(ADDRESS_LABEL, target_url.strip_prefix("http://").unwrap_or(target_url));
    lb.set(METRICS_PATH_LABEL, "/metrics");
    Target::new(lb.labels(), Arc::new(ScrapeConfig::default()), LabelSet::new(), LabelSet::new())
}

// ---------------------------------------------------------------------------------------------------------------
// A small HTTP(S) test server (Go httptest.Server).

/// The request headers the server saw, one entry per request.
type Seen = Arc<Mutex<Vec<Vec<(String, String)>>>>;

struct TestServer {
    url: String,
    port: u16,
    seen: Seen,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

impl TestServer {
    /// A header of the first request (empty if missing).
    fn header(&self, name: &str) -> String {
        let seen = self.seen.lock().unwrap();
        let req = seen.first().expect("the server got no request");
        req.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.clone()).unwrap_or_default()
    }
}

/// Go `httptest.NewServer` (tls None) or `NewUnstartedServer` + `StartTLS`. Every response is
/// `Content-Type: text/plain; version=0.0.4` with an empty body.
fn new_server(tls: Option<Arc<rustls::ServerConfig>>) -> TestServer {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let (st, sn) = (stop.clone(), seen.clone());
    let scheme = if tls.is_some() { "https" } else { "http" };
    let accept = std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                break;
            }
            let Ok(c) = c else { continue };
            let _ = c.set_read_timeout(Some(Duration::from_secs(5)));
            let (tls, sn) = (tls.clone(), sn.clone());
            std::thread::spawn(move || match tls {
                None => serve(c, &sn),
                Some(cfg) => {
                    let Ok(sc) = rustls::ServerConnection::new(cfg) else { return };
                    serve(rustls::StreamOwned::new(sc, c), &sn);
                }
            });
        }
    });
    TestServer { url: format!("{scheme}://127.0.0.1:{port}"), port, seen, stop, accept: Some(accept) }
}

fn serve<S: Read + Write>(s: S, seen: &Seen) {
    let mut r = BufReader::new(s);
    let mut line = String::new();
    if r.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut headers = Vec::new();
    loop {
        line.clear();
        if r.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    seen.lock().unwrap().push(headers);
    let s = r.get_mut();
    let _ = s.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    let _ = s.flush();
}

fn load_certs(p: &str) -> Vec<CertificateDer<'static>> {
    let data = std::fs::read(p).unwrap_or_else(|e| panic!("Couldn't read cert {p}: {e}"));
    net::tls::pem_blocks(&data)
        .into_iter()
        .filter(|b| b.type_ == "CERTIFICATE")
        .map(|b| CertificateDer::from(b.der))
        .collect()
}

fn load_key(p: &str) -> PrivateKeyDer<'static> {
    let data = std::fs::read(p).unwrap_or_else(|e| panic!("Couldn't read key {p}: {e}"));
    let b = net::tls::pem_blocks(&data).into_iter().find(|b| b.type_.ends_with("PRIVATE KEY")).expect("key block");
    match b.type_.as_str() {
        "RSA PRIVATE KEY" => PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(b.der)),
        _ => PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(b.der)),
    }
}

/// Go `newTLSConfig`: the server certificate `testdata/<cert_name>.cer`; with `client_auth`, client
/// certificates are required and verified with the CA (Go `tls.RequireAndVerifyClientCert`, `ClientCAs =
/// RootCAs`).
fn new_tls_config(cert_name: &str, client_auth: bool) -> Arc<rustls::ServerConfig> {
    let provider = net::tls::provider();
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(&ca_cert_path()) {
        roots.add(c).unwrap();
    }
    let cert_path = testdata(&format!("{cert_name}.cer"));
    let key_path = testdata(&format!("{cert_name}.key"));
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone()).with_safe_default_protocol_versions().unwrap();
    let builder = if client_auth {
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider).build().unwrap();
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let cfg = builder.with_single_cert(load_certs(&cert_path), load_key(&key_path)).unwrap_or_else(|e| {
        panic!("Unable to use specified server cert ({cert_path}) & key ({key_path}): {e}")
    });
    Arc::new(cfg)
}

fn client_get(cfg: &HttpClientConfig, url: &str) -> Result<net::Response, net::Error> {
    let c = new_client_from_config(cfg, &HttpClientOptions { user_agent: "test".to_string(), ..Default::default() })
        .expect("NewClientFromConfig");
    c.get(url)
}

// adapted: Go's handlers assert inside the server goroutine; here the server records the request headers and the
// test asserts on them after the request.
#[test]
fn test_new_http_bearer_token() {
    let server = new_server(None);
    let cfg = HttpClientConfig { bearer_token: "1234".into(), ..Default::default() };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
    assert_eq!("Bearer 1234", server.header("Authorization"), "Authorization header was not set correctly.");
}

#[test]
fn test_new_http_bearer_token_file() {
    let server = new_server(None);
    let cfg = HttpClientConfig { bearer_token_file: testdata("bearertoken.txt"), ..Default::default() };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
    assert_eq!("Bearer 12345", server.header("Authorization"), "Authorization header was not set correctly.");
}

#[test]
fn test_new_http_basic_auth() {
    let server = new_server(None);
    let cfg = HttpClientConfig {
        basic_auth: Some(BasicAuth {
            username: "user".to_string(),
            password: "password123".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
    // Go `r.BasicAuth()`.
    let auth = server.header("Authorization");
    let basic = auth
        .strip_prefix("Basic ")
        .and_then(crypto::base64_decode)
        .and_then(|d| String::from_utf8(d).ok())
        .and_then(|d| d.split_once(':').map(|(u, p)| (u.to_string(), p.to_string())));
    assert!(basic.is_some(), "Basic authorization header was not set correctly.");
    let (username, password) = basic.unwrap();
    assert_eq!("user", username);
    assert_eq!("password123", password);
}

#[test]
fn test_new_httpca_cert() {
    let server = new_server(Some(new_tls_config("server", false)));
    let cfg = HttpClientConfig { tls_config: TlsConfig { ca_file: ca_cert_path(), ..Default::default() }, ..Default::default() };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_new_http_client_cert() {
    let server = new_server(Some(new_tls_config("server", true)));
    let cfg = HttpClientConfig {
        tls_config: TlsConfig {
            ca_file: ca_cert_path(),
            cert_file: testdata("client.cer"),
            key_file: testdata("client.key"),
            ..Default::default()
        },
        ..Default::default()
    };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_new_http_with_server_name() {
    let server = new_server(Some(new_tls_config("servername", false)));
    let cfg = HttpClientConfig {
        tls_config: TlsConfig {
            ca_file: ca_cert_path(),
            server_name: "prometheus.rocks".to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_ok(), "{:?}", r.err());
}

#[test]
fn test_new_http_with_bad_server_name() {
    let server = new_server(Some(new_tls_config("servername", false)));
    let cfg = HttpClientConfig {
        tls_config: TlsConfig { ca_file: ca_cert_path(), server_name: "badname".to_string(), ..Default::default() },
        ..Default::default()
    };
    let r = client_get(&cfg, &server.url);
    assert!(r.is_err());
}

#[test]
fn test_new_client_with_bad_tls_config() {
    let cfg = HttpClientConfig {
        tls_config: TlsConfig {
            ca_file: testdata("nonexistent_ca.cer"),
            cert_file: testdata("nonexistent_client.cer"),
            key_file: testdata("nonexistent_client.key"),
            ..Default::default()
        },
        ..Default::default()
    };
    let r = new_client_from_config(&cfg, &HttpClientOptions { user_agent: "test".to_string(), ..Default::default() });
    assert!(r.is_err());
}

#[test]
fn test_targets_from_group() {
    let expected_error = "instance 0 in group : no address";

    let cfg = Arc::new(ScrapeConfig {
        scrape_timeout: yaml::Duration::from_secs(10),
        scrape_interval: yaml::Duration::from_secs(60),
        ..Default::default()
    });
    let mut lb = Builder::new(Labels::default());
    let mut targets = Vec::new();
    let group = Group {
        targets: vec![LabelSet::new(), LabelSet::from_pairs(&[(ADDRESS_LABEL, "localhost:9090")])],
        ..Default::default()
    };
    let failures = targets_from_group(&group, &cfg, &mut targets, &mut lb);
    assert_eq!(1, targets.len());
    assert_eq!(1, failures.len());
    assert_eq!(expected_error, failures[0].to_string());
}

/// Go `loadConfiguration`.
fn load_configuration(c: &str) -> config::Config {
    config::load(c, &discovery::Logger::nop()).expect("config.Load")
}

// TestTargetsFromGroupWithLabelKeepDrop demonstrates and reinforces the current behavior: relabeling's
// "labelkeep" and "labeldrop" are applied to all labels of a target, including internal ones (labels starting
// with "__" such as "__address__").
#[test]
fn test_targets_from_group_with_label_keep_drop() {
    struct Case {
        name: &'static str,
        cfg_text: &'static str,
        targets: Vec<LabelSet>,
        should_drop_target: bool,
    }
    let tests = vec![
        Case {
            name: "no relabeling",
            cfg_text: r#"
global:
  metric_name_validation_scheme: legacy
scrape_configs:
  - job_name: job1
    static_configs:
      - targets: ["localhost:9090"]
"#,
            targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "localhost:9090")])],
            should_drop_target: false,
        },
        Case {
            name: "labelkeep",
            cfg_text: r#"
global:
  metric_name_validation_scheme: legacy
scrape_configs:
  - job_name: job1
    static_configs:
      - targets: ["localhost:9090"]
    relabel_configs:
      - regex: 'foo'
        action: labelkeep
"#,
            targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "localhost:9090")])],
            should_drop_target: true,
        },
        Case {
            name: "labeldrop",
            cfg_text: r#"
global:
  metric_name_validation_scheme: legacy
scrape_configs:
  - job_name: job1
    static_configs:
      - targets: ["localhost:9090"]
    relabel_configs:
      - regex: '__address__'
        action: labeldrop
"#,
            targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "localhost:9090")])],
            should_drop_target: true,
        },
    ];

    for tt in tests {
        let config = load_configuration(tt.cfg_text);
        let mut lb = Builder::new(Labels::default());
        let mut targets = Vec::new();
        let group = Group { targets: tt.targets.clone(), ..Default::default() };
        let failures = targets_from_group(&group, &Arc::new(config.scrape_configs[0].clone()), &mut targets, &mut lb);

        if tt.should_drop_target {
            assert_eq!(1, failures.len(), "{}", tt.name);
            assert_eq!("instance 0 in group : no address", failures[0].to_string(), "{}", tt.name);
            assert!(targets.is_empty(), "{}", tt.name);
        } else {
            assert!(failures.is_empty(), "{}: {failures:?}", tt.name);
            assert_eq!(1, targets.len(), "{}", tt.name);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Limit appenders. Go stacks one wrapper type per limit; the Rust `Limits` applies the enabled limits in one
// struct. A Go wrapper alone (for example `bucketLimitAppender{limit: n}`) is `Limits` with only that limit
// enabled: no sample limit, no time limit (max_time = i64::MAX), max schema at the exponential maximum.

fn only_bucket_limit(limit: usize) -> Limits {
    Limits { max_time: i64::MAX, sample_limit: 0, bucket_limit: limit, max_schema: EXPONENTIAL_SCHEMA_MAX, i: 0 }
}

fn only_max_schema(max_schema: i32) -> Limits {
    Limits { max_time: i64::MAX, sample_limit: 0, bucket_limit: 0, max_schema, i: 0 }
}

fn example_histogram() -> Histogram {
    Histogram {
        schema: 0,
        count: 21,
        sum: 33.0,
        zero_threshold: 0.001,
        zero_count: 3,
        positive_spans: vec![Span { offset: 0, length: 3 }],
        positive_buckets: vec![3, 0, 0],
        negative_spans: vec![Span { offset: 0, length: 3 }],
        negative_buckets: vec![3, 0, 0],
        ..Default::default()
    }
}

fn custom_buckets_histogram() -> Histogram {
    Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 9,
        sum: 33.0,
        positive_spans: vec![Span { offset: 0, length: 3 }],
        positive_buckets: vec![3, 0, 0],
        custom_values: custom_values(&[1.0, 2.0, 3.0]),
        ..Default::default()
    }
}

/// Runs one append through the limits in V1 (`app_v2` false) or V2 form, like Go's `appV2=false/true`
/// subtests. Returns the append error and the schema and bucket count of the (possibly modified) histogram.
fn append_with_limits(limits: Limits, h: &Histogram, float_histo: bool, app_v2: bool) -> (Option<storage::Error>, i32, usize) {
    let ctx = Context::background();
    let ts = Duration::from_secs(600).as_millis() as i64;
    let lbls = from_strings(&["__name__", "sparse_histogram_series"]);
    let a = Appendable::new();
    let mut fh = h.to_float();
    let mut h = h.clone();
    let err;
    if app_v2 {
        let mut app = a.appender_v2(&ctx);
        {
            let mut l = LimitsV2 { app: &mut *app, limits };
            err = if float_histo {
                l.append(0, &lbls, 0, ts, 0.0, None, Some(&mut fh), &AOptions::default()).err()
            } else {
                l.append(0, &lbls, 0, ts, 0.0, Some(&mut h), None, &AOptions::default()).err()
            };
        }
        assert!(app.commit().is_none());
    } else {
        let mut app = a.appender(&ctx);
        {
            let mut l = LimitsV1 { app: &mut *app, limits };
            err = if float_histo {
                l.append_histogram(0, &lbls, ts, None, Some(&mut fh)).err()
            } else {
                l.append_histogram(0, &lbls, ts, Some(&mut h), None).err()
            };
        }
        assert!(app.commit().is_none());
    }
    if float_histo {
        (err, fh.schema, fh.negative_buckets.len() + fh.positive_buckets.len())
    } else {
        (err, h.schema, h.negative_buckets.len() + h.positive_buckets.len())
    }
}

#[test]
fn test_bucket_limit_appender() {
    let example = example_histogram();

    let big_gap = Histogram {
        schema: 0,
        count: 21,
        sum: 33.0,
        zero_threshold: 0.001,
        zero_count: 3,
        positive_spans: vec![
            Span { offset: 1, length: 1 }, // in (1, 2]
            Span { offset: 2, length: 1 }, // in (8, 16]
        ],
        positive_buckets: vec![1, 0], // 1, 1
        ..Default::default()
    };

    let custom_buckets = custom_buckets_histogram();

    struct Case<'a> {
        h: &'a Histogram,
        limit: usize,
        expect_error: bool,
        expect_bucket_count: usize,
        expect_schema: i32,
    }
    let cases = [
        Case { h: &example, limit: 3, expect_error: true, expect_bucket_count: 0, expect_schema: 0 },
        Case { h: &example, limit: 4, expect_error: false, expect_bucket_count: 4, expect_schema: -1 },
        Case { h: &example, limit: 10, expect_error: false, expect_bucket_count: 6, expect_schema: 0 },
        Case { h: &big_gap, limit: 1, expect_error: false, expect_bucket_count: 1, expect_schema: -2 },
        Case { h: &custom_buckets, limit: 2, expect_error: true, expect_bucket_count: 0, expect_schema: 0 },
        Case {
            h: &custom_buckets,
            limit: 3,
            expect_error: false,
            expect_bucket_count: 3,
            expect_schema: CUSTOM_BUCKETS_SCHEMA,
        },
    ];

    for (ci, c) in cases.iter().enumerate() {
        for float_histo in [true, false] {
            for app_v2 in [false, true] {
                let name = format!("case {ci}/floatHistogram={float_histo}/appV2={app_v2}");
                let (err, schema, count) = append_with_limits(only_bucket_limit(c.limit), c.h, float_histo, app_v2);
                if c.expect_error {
                    assert!(err.is_some(), "{name}");
                } else {
                    assert_eq!(c.expect_schema, schema, "{name}");
                    assert_eq!(c.expect_bucket_count, count, "{name}");
                    assert!(err.is_none(), "{name}: {err:?}");
                }
            }
        }
    }
}

#[test]
fn test_max_schema_appender() {
    let example = example_histogram();
    let custom_buckets = custom_buckets_histogram();

    let cases: [(&Histogram, i32, i32); 3] =
        [(&example, -1, -1), (&example, 0, 0), (&custom_buckets, -1, CUSTOM_BUCKETS_SCHEMA)];

    for (ci, (h, max_schema, expect_schema)) in cases.iter().enumerate() {
        for float_histo in [true, false] {
            for app_v2 in [false, true] {
                let name = format!("case {ci}/floatHistogram={float_histo}/appV2={app_v2}");
                let (err, schema, _) = append_with_limits(only_max_schema(*max_schema), h, float_histo, app_v2);
                assert_eq!(*expect_schema, schema, "{name}");
                assert!(err.is_none(), "{name}: {err:?}");
            }
        }
    }
}

// Test sample_limit when a scrape contains Native Histograms.
#[test]
fn test_append_with_sample_limit_and_native_histogram() {
    let now = SystemTime::now();
    let ctx = Context::background();
    let ts = model::timestamp::from_time(now);
    // appV2=false
    {
        let a = Appendable::new();
        let mut inner = a.appender(&ctx);
        let mut app = LimitsV1 { app: &mut *inner, limits: Limits::new(SystemTime::now(), 2, 0, EXPONENTIAL_SCHEMA_MAX) };

        // sample_limit is set to 2, so first two scrapes should work.
        let r = app.append(0, &from_strings(&["__name__", "foo"]), ts, 1.0);
        assert!(r.is_ok(), "{:?}", r.err());

        // Second sample, should be ok.
        let mut h = Histogram::default();
        let r = app.append_histogram(0, &from_strings(&["__name__", "my_histogram1"]), ts, Some(&mut h), None);
        assert!(r.is_ok(), "{:?}", r.err());

        // This is third sample with sample_limit=2, it should trigger errSampleLimit.
        let mut h = Histogram::default();
        let r = app.append_histogram(0, &from_strings(&["__name__", "my_histogram2"]), ts, Some(&mut h), None);
        assert!(r.as_ref().is_err_and(|e| e.is(&ERR_SAMPLE_LIMIT)), "{r:?}");
    }
    // appV2=true
    {
        let a = Appendable::new();
        let mut inner = a.appender_v2(&ctx);
        let mut app =
            LimitsV2 { app: &mut *inner, limits: Limits::new(SystemTime::now(), 2, 0, EXPONENTIAL_SCHEMA_MAX) };

        // sample_limit is set to 2, so first two scrapes should work.
        let r = app.append(0, &from_strings(&["__name__", "foo"]), 0, ts, 1.0, None, None, &AOptions::default());
        assert!(r.is_ok(), "{:?}", r.err());

        // Second sample, should be ok.
        let mut h = Histogram::default();
        let r = app.append(
            0,
            &from_strings(&["__name__", "my_histogram1"]),
            0,
            ts,
            0.0,
            Some(&mut h),
            None,
            &AOptions::default(),
        );
        assert!(r.is_ok(), "{:?}", r.err());

        // This is third sample with sample_limit=2, it should trigger errSampleLimit.
        let mut h = Histogram::default();
        let r = app.append(
            0,
            &from_strings(&["__name__", "my_histogram2"]),
            0,
            ts,
            0.0,
            Some(&mut h),
            None,
            &AOptions::default(),
        );
        assert!(r.as_ref().is_err_and(|e| e.is(&ERR_SAMPLE_LIMIT)), "{r:?}");
    }
}
