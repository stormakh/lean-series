//! scrape_test.go part 3 (Go lines 4500-6499).
//!
//! Tests that need the real TSDB (teststorage.New queried after the scrape) are not written here:
//! TestScrapeLoopDiscardDuplicateLabels, TestScrapeLoopDiscardUnnamedMetrics, TestScrapeAddFast,
//! TestScrapeReportSingleAppender, TestScrapeReportLimit, TestScrapeUTF8, TestLeQuantileReLabel.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime};

use config::{EscapingScheme, ScrapeConfig, ScrapeProtocol};
use discovery::targetgroup::Group;
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, ValidationScheme, from_strings};
use model::labelset::LabelSet;
use model::metadata::Metadata;
use model::relabel;
use model::timestamp;
use model::value::is_stale_nan;
use net::Header;
use scrape::ctx::Ctx;
use scrape::pool::{ScrapeLoopOptions, new_scrape_client, new_unix_socket_scrape_client, reusable_cache};
use scrape::scrape_loop::{AppendErrors, LabelLimits, mutate_report_sample_labels, mutate_sample_labels};
use scrape::scraper::{ERR_BODY_SIZE_LIMIT, ZSTD_MAX_WINDOW_SIZE, accept_header};
use scrape::target::{CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL, SCRAPE_INTERVAL_LABEL, SCRAPE_TIMEOUT_LABEL, UNIX_SOCKET_LABEL};
use scrape::testing::{TestLoop, new_test_scrape_loop, new_test_scrape_metrics, new_test_scrape_pool};
use scrape::util::BufPool;
use scrape::{Loop, Options, ScrapePool, Scraper, Target, TargetScraper};
use storage::{
    AppendOptions, AppendV2Options, Appender, AppenderTransaction, AppenderV2, Context, ERR_CANCELED,
    ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_SAMPLE, Error, ExemplarAppenderV2,
    SeriesRef,
};
use util::teststorage::{Appendable, Sample, require_equal};

// ---------------- helpers ----------------

/// Go `foreachAppendable`.
fn foreach_appendable(f: impl Fn(bool)) {
    for app_v2 in [false, true] {
        f(app_v2);
    }
}

/// Go `withAppendable(app, appV2)`.
fn with_appendable(sl: &mut scrape::ScrapeLoop, app: &Appendable, app_v2: bool) {
    if app_v2 {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
    } else {
        sl.appendable = Some(Arc::new(app.clone()));
    }
}

/// Go `selectAppendable(app, appV2)`: (V1, V2).
fn select_appendable(
    app: &Appendable,
    app_v2: bool,
) -> (Option<Arc<dyn storage::Appendable>>, Option<Arc<dyn storage::AppendableV2>>) {
    if app_v2 { (None, Some(Arc::new(app.clone()))) } else { (Some(Arc::new(app.clone())), None) }
}

fn new_target(l: Labels, cfg: ScrapeConfig) -> Arc<Target> {
    Arc::new(Target::new(l, Arc::new(cfg), LabelSet::default(), LabelSet::default()))
}

/// Go `http.DefaultClient`.
fn default_client() -> net::Client {
    net::Client::new(Arc::new(
        net::Transport::new(Default::default(), None, Default::default(), Default::default()).expect("default transport"),
    ))
}

fn utf8_scrape_config(job: &str) -> ScrapeConfig {
    ScrapeConfig {
        job_name: job.to_string(),
        metric_name_validation_scheme: ValidationScheme::Utf8,
        metric_name_escaping_scheme: "allow-utf-8".to_string(),
        ..Default::default()
    }
}

fn dur(d: Duration) -> yaml::types::Duration {
    yaml::types::Duration(d.as_nanos() as i64)
}

fn upstream_testdata(name: &str) -> String {
    format!("{}/../../upstream/scrape/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
}

/// A request seen by the test server.
#[derive(Clone)]
struct SReq {
    host: String,
    header: Header,
}

/// A response of the test server.
struct SResp {
    status: u16,
    header: Header,
    body: Vec<u8>,
}

impl SResp {
    fn ok(content_type: &str, body: &[u8]) -> SResp {
        let mut header = Header::new();
        if !content_type.is_empty() {
            header.set("Content-Type", content_type);
        }
        SResp { status: 200, header, body: body.to_vec() }
    }
}

type Handler = Arc<dyn Fn(&SReq) -> SResp + Send + Sync>;

/// Go `httptest.NewServer` (HTTP/1.1, keep-alive) on 127.0.0.1.
struct TestServer {
    host: String,
    port: u16,
    stop: Arc<AtomicBool>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
    }
}

fn new_server(h: impl Fn(&SReq) -> SResp + Send + Sync + 'static) -> TestServer {
    let h: Handler = Arc::new(h);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                break;
            }
            let Ok(c) = c else { continue };
            let h = h.clone();
            std::thread::spawn(move || serve(c, &h));
        }
    });
    TestServer { host: format!("127.0.0.1:{port}"), port, stop }
}

/// An HTTP server on a unix socket (Go `http.Server.Serve(unixListener)`), optionally with TLS.
struct UnixServer {
    path: String,
    stop: Arc<AtomicBool>,
}

impl Drop for UnixServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = UnixStream::connect(&self.path);
    }
}

fn new_unix_server(
    path: &str,
    tls: Option<Arc<rustls::ServerConfig>>,
    h: impl Fn(&SReq) -> SResp + Send + Sync + 'static,
) -> UnixServer {
    let h: Handler = Arc::new(h);
    let l = UnixListener::bind(path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                break;
            }
            let Ok(c) = c else { continue };
            let h = h.clone();
            let tls = tls.clone();
            std::thread::spawn(move || match tls {
                None => serve(c, &h),
                Some(cfg) => {
                    let Ok(sc) = rustls::ServerConnection::new(cfg) else { return };
                    serve(rustls::StreamOwned::new(sc, c), &h)
                }
            });
        }
    });
    UnixServer { path: path.to_string(), stop }
}

/// Go `newTLSConfig("server", t)` on the server side: the server certificate and key of testdata.
fn server_tls_config(cert_name: &str) -> Arc<rustls::ServerConfig> {
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = net::tls::pem_blocks(
        &std::fs::read(upstream_testdata(&format!("{cert_name}.cer"))).expect("Couldn't read cert."),
    )
    .into_iter()
    .filter(|b| b.type_ == "CERTIFICATE")
    .map(|b| b.der.into())
    .collect();
    let key = net::tls::pem_blocks(&std::fs::read(upstream_testdata(&format!("{cert_name}.key"))).unwrap()).remove(0);
    let key = rustls::pki_types::PrivateKeyDer::try_from(key.der).unwrap();
    let cfg = rustls::ServerConfig::builder_with_provider(net::tls::provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    Arc::new(cfg)
}

fn serve<S: Read + Write>(s: S, h: &Handler) {
    let mut r = BufReader::new(s);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut header = Header::new();
        loop {
            let mut l = String::new();
            if r.read_line(&mut l).unwrap_or(0) == 0 {
                return;
            }
            let t = l.trim_end();
            if t.is_empty() {
                break;
            }
            if let Some((k, v)) = t.split_once(':') {
                header.add(k.trim(), v.trim());
            }
        }
        let n: usize = header.get("Content-Length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let mut body = vec![0u8; n];
        if r.read_exact(&mut body).is_err() {
            return;
        }
        let req = SReq { host: header.get_str("Host").to_string(), header };
        let resp = h(&req);
        let mut out = format!("HTTP/1.1 {} {}\r\n", resp.status, net::transport::status_text(resp.status));
        for (k, v) in resp.header.iter() {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str(&format!("Content-Length: {}\r\n\r\n", resp.body.len()));
        let w = r.get_mut();
        if w.write_all(out.as_bytes()).is_err() || w.write_all(&resp.body).is_err() || w.flush().is_err() {
            return;
        }
    }
}

/// Go `os.MkdirTemp("", prefix)` (short, below the unix socket path limit).
fn temp_dir(prefix: &str) -> String {
    let mut b = [0u8; 6];
    crypto::random_bytes(&mut b);
    let d = format!("/tmp/{prefix}{}", crypto::hex(&b));
    std::fs::create_dir_all(&d).unwrap();
    d
}

struct RemoveDir(String);
impl Drop for RemoveDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[allow(clippy::too_many_arguments)]
fn target_scraper(
    target: Option<Arc<Target>>,
    client: net::Client,
    timeout: Duration,
    body_size_limit: i64,
    accept: String,
    enable_zstd: bool,
) -> TargetScraper {
    TargetScraper::new(
        target,
        client,
        timeout,
        body_size_limit,
        accept,
        String::new(),
        enable_zstd,
        discovery::Logger::nop(),
        new_test_scrape_metrics(),
    )
}

fn response(encoding: &str, body: Vec<u8>) -> net::Response {
    let mut header = Header::new();
    header.set("Content-Encoding", encoding);
    net::Response {
        status: 200,
        status_text: "200 OK".to_string(),
        header,
        body,
        request: net::Request::get("http://example.invalid/").unwrap(),
    }
}

/// Go `timeLimitAppender` / `timeLimitAppenderV2` around a fresh `teststorage.NewAppendable()` appender.
struct TimeLimitAppendable {
    max_time: i64,
}

struct TimeLimitAppender {
    app: Box<dyn Appender>,
    max_time: i64,
}

struct TimeLimitAppenderV2 {
    app: Box<dyn AppenderV2>,
    max_time: i64,
}

impl storage::Appendable for TimeLimitAppendable {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        Box::new(TimeLimitAppender { app: storage::Appendable::appender(&Appendable::new(), ctx), max_time: self.max_time })
    }
}

impl storage::AppendableV2 for TimeLimitAppendable {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(TimeLimitAppenderV2 { app: Appendable::new().appender_v2(ctx), max_time: self.max_time })
    }
}

impl AppenderTransaction for TimeLimitAppender {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl Appender for TimeLimitAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        if t > self.max_time {
            return Err(ERR_OUT_OF_BOUNDS);
        }
        self.app.append(r, l, t, v)
    }
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.app.set_options(opts)
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.app.append_exemplar(r, l, e)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram(r, l, t, h, fh)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &Metadata) -> Result<SeriesRef, Error> {
        self.app.update_metadata(r, l, m)
    }
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app.append_st_zero_sample(r, l, t, st)
    }
}

impl AppenderTransaction for TimeLimitAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl AppenderV2 for TimeLimitAppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if t > self.max_time {
            return Err(ERR_OUT_OF_BOUNDS);
        }
        self.app.append(r, ls, st, t, v, h, fh, opts)
    }
    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        self.app.as_exemplar_appender_v2()
    }
}

// ---------------- tests ----------------

#[test]
fn test_scrape_loop_append_exemplar_series() {
    foreach_appendable(|app_v2| {
        let scrape_text = [
            "metric_total{n=\"1\"} 1 # {t=\"1\"} 1.0 10000\n# EOF",
            "metric_total{n=\"1\"} 2 # {t=\"2\"} 2.0 20000\n# EOF",
        ];
        let mut samples = vec![
            Sample {
                l: from_strings(&["__name__", "metric_total", "n", "1"]),
                v: 1.0,
                es: vec![Exemplar { labels: from_strings(&["t", "1"]), value: 1.0, ts: 10000000, has_ts: true }],
                ..Default::default()
            },
            Sample {
                l: from_strings(&["__name__", "metric_total", "n", "1"]),
                v: 2.0,
                es: vec![Exemplar { labels: from_strings(&["t", "2"]), value: 2.0, ts: 20000000, has_ts: true }],
                ..Default::default()
            },
        ];
        let discovery_labels = new_target(from_strings::<&str>(&[]), ScrapeConfig::default());

        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, app_v2);
            let d1 = discovery_labels.clone();
            sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &d1, false, &[]));
            let d2 = discovery_labels.clone();
            sl.report_sample_mutator = Arc::new(move |l| mutate_report_sample_labels(l, &d2));
            // This test does not care about metadata. Having this true would mean we need to add metadata to sample
            // expectations.
            sl.append_metadata_to_wal = false;
        });

        let now = SystemTime::now();
        for (i, s) in samples.iter_mut().enumerate() {
            let ts = now + Duration::from_secs(i as u64);
            s.t = timestamp::from_time(ts);
        }

        for (i, st) in scrape_text.iter().enumerate() {
            let mut app = sl.appender();
            let (_, err) = app.append(st.as_bytes(), "application/openmetrics-text", timestamp::time(samples[i].t));
            assert!(err.is_none(), "{err:?}");
            assert!(app.commit().is_none());
        }

        require_equal(&samples, &app_test.result_samples());
    });
}

#[test]
fn test_scrape_loop_run_reports_target_down_on_scrape_error() {
    foreach_appendable(|app_v2| {
        let ctx = Ctx::background();
        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, app_v2);
            sl.ctx = ctx.clone();
        });
        let cancel = ctx.clone();
        scraper.set_scrape_func(move |_, _| {
            cancel.cancel();
            Err(Error::msg("scrape failed"))
        });

        Arc::new(sl).run(None);
        assert_eq!(0.0, app_test.result_samples()[0].v, "bad 'up' value");
    });
}

#[test]
fn test_scrape_loop_run_reports_target_down_on_invalid_utf8() {
    foreach_appendable(|app_v2| {
        let ctx = Ctx::background();
        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, app_v2);
            sl.ctx = ctx.clone();
        });
        let cancel = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            cancel.cancel();
            w.extend_from_slice(b"a{l=\"\xff\"} 1\n");
            Ok(())
        });

        Arc::new(sl).run(None);
        assert_eq!(0.0, app_test.result_samples()[0].v, "bad 'up' value");
    });
}

#[test]
fn test_scrape_loop_append_gracefully_if_amend_or_out_of_order_or_out_of_bounds() {
    foreach_appendable(|app_v2| {
        let app_test = Appendable::new().with_errs(
            Some(Arc::new(|ls: &Labels| match ls.get("__name__") {
                "out_of_order" => Some(ERR_OUT_OF_ORDER_SAMPLE),
                "amend" => Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP),
                "out_of_bounds" => Some(ERR_OUT_OF_BOUNDS),
                _ => None,
            })),
            None,
            None,
        );
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, app_v2));

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let mut app = sl.appender();
        let (stats, err) = app.append(b"out_of_order 1\namend 1\nnormal 1\nout_of_bounds 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let want = vec![Sample { l: from_strings(&["__name__", "normal"]), t: timestamp::from_time(now), v: 1.0, ..Default::default() }];
        require_equal(&want, &app_test.result_samples());
        assert_eq!(4, stats.total);
        assert_eq!(4, stats.added);
        assert_eq!(1, stats.series_added);
    });
}

#[test]
fn test_scrape_loop_creates_stale_markers_on_ooo_append() {
    foreach_appendable(|app_v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, app_v2));

        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric1 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let metric1_calls = Arc::new(AtomicUsize::new(0));
        // The test storage doesn't track order, so it cannot generate OOO error,
        // we have to emulate it.
        let calls = metric1_calls.clone();
        app_test.with_errs(
            Some(Arc::new(move |ls: &Labels| match ls.get("__name__") {
                "metric1" => {
                    // Only return error on the first append, the second one will be the
                    // staleness marker.
                    if calls.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                        return Some(ERR_OUT_OF_ORDER_SAMPLE);
                    }
                    None
                }
                _ => None,
            })),
            None,
            None,
        );

        let now2 = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
        let mut app2 = sl.appender();
        let (_, err) = app2.append(b"metric1 2\n", "text/plain", now2);
        assert!(err.is_none(), "{err:?}");
        assert!(app2.commit().is_none());

        let found_stale =
            app_test.result_samples().iter().any(|s| s.l.get("__name__") == "metric1" && is_stale_nan(s.v));
        assert!(found_stale, "Stale NaN marker was expected for metric1 but was not found");
    });
}

#[test]
fn test_scrape_loop_out_of_bounds_time_error() {
    foreach_appendable(|app_v2| {
        let (sl, _) = new_test_scrape_loop(|sl| {
            let a = Arc::new(TimeLimitAppendable {
                max_time: timestamp::from_time(SystemTime::now() + Duration::from_secs(600)),
            });
            if app_v2 {
                sl.appendable_v2 = Some(a);
            } else {
                sl.appendable = Some(a);
            }
        });

        let now = SystemTime::now() + Duration::from_secs(1200);
        let mut app = sl.appender();
        let (stats, err) = app.append(b"normal 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        assert_eq!(1, stats.total);
        assert_eq!(1, stats.added);
        assert_eq!(0, stats.series_added);
    });
}

#[test]
fn test_accept_header() {
    let p = |s: &str| ScrapeProtocol::new(s);
    let tests: Vec<(&str, Vec<ScrapeProtocol>, EscapingScheme, &str)> = vec![
        (
            "default scrape protocols with underscore escaping",
            config::default_scrape_protocols(),
            EscapingScheme::UnderscoreEscaping,
            "application/openmetrics-text;version=1.0.0;escaping=underscores;q=0.7,application/openmetrics-text;version=0.0.1;q=0.6,text/plain;version=1.0.0;escaping=underscores;q=0.5,text/plain;version=0.0.4;q=0.4,*/*;q=0.3",
        ),
        (
            "default proto first scrape protocols with underscore escaping",
            config::default_proto_first_scrape_protocols(),
            EscapingScheme::DotsEscaping,
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,application/openmetrics-text;version=1.0.0;escaping=dots;q=0.6,application/openmetrics-text;version=0.0.1;q=0.5,text/plain;version=1.0.0;escaping=dots;q=0.4,text/plain;version=0.0.4;q=0.3,*/*;q=0.2",
        ),
        (
            "default scrape protocols with no escaping",
            config::default_scrape_protocols(),
            EscapingScheme::NoEscaping,
            "application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.7,application/openmetrics-text;version=0.0.1;q=0.6,text/plain;version=1.0.0;escaping=allow-utf-8;q=0.5,text/plain;version=0.0.4;q=0.4,*/*;q=0.3",
        ),
        (
            "default proto first scrape protocols with no escaping",
            config::default_proto_first_scrape_protocols(),
            EscapingScheme::NoEscaping,
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,application/openmetrics-text;version=1.0.0;escaping=allow-utf-8;q=0.6,application/openmetrics-text;version=0.0.1;q=0.5,text/plain;version=1.0.0;escaping=allow-utf-8;q=0.4,text/plain;version=0.0.4;q=0.3,*/*;q=0.2",
        ),
        (
            "openmetrics 2.0.0 first, with underscore escaping",
            vec![p(config::OPEN_METRICS_TEXT_2_0_0), p(config::OPEN_METRICS_TEXT_1_0_0)],
            EscapingScheme::UnderscoreEscaping,
            "application/openmetrics-text;version=2.0.0;escaping=underscores;q=0.7,application/openmetrics-text;version=1.0.0;escaping=underscores;q=0.6,*/*;q=0.5",
        ),
        (
            "openmetrics 2.0.0 only, with no escaping",
            vec![p(config::OPEN_METRICS_TEXT_2_0_0)],
            EscapingScheme::NoEscaping,
            "application/openmetrics-text;version=2.0.0;escaping=allow-utf-8;q=0.7,*/*;q=0.6",
        ),
    ];
    for (name, protocols, scheme, expected) in tests {
        assert_eq!(expected, accept_header(&protocols, scheme), "{name}");
    }
}

/// TestRequestTraceparentHeader verifies that the HTTP client used by the target scraper
/// propagates the OpenTelemetry "traceparent" header correctly.
// adapted: Go installs a global OTel TracerProvider with AlwaysSample and the TraceContext propagator
// (setupTracing); the Rust port has no tracing provider, so the header must come from the scrape client itself.
#[test]
#[ignore = "waits for tracing (OpenTelemetry export is out of scope)"]
fn test_request_traceparent_header() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let s2 = seen.clone();
    let server = new_server(move |r| {
        s2.lock().unwrap().push(r.header.get_str("traceparent").to_string());
        SResp::ok("", b"")
    });

    let client = new_scrape_client(&net::HttpClientConfig::default_config(), &net::HttpClientOptions::default()).unwrap();
    let ts = target_scraper(
        Some(new_target(from_strings(&["__scheme__", "http", "__address__", &server.host]), ScrapeConfig::default())),
        client,
        Duration::ZERO,
        0,
        String::new(),
        false,
    );

    let resp = ts.scrape(&Ctx::background()).expect("scrape");
    assert!(resp.is_some());
    let seen = seen.lock().unwrap();
    assert_eq!(1, seen.len());
    // the traceparent header is sent.
    assert!(!seen[0].is_empty(), "traceparent header not sent");
}

#[test]
fn test_target_scraper_scrape_ok() {
    const CONFIG_TIMEOUT: Duration = Duration::from_millis(1500);
    const EXPECTED_TIMEOUT: &str = "1.5";

    let protobuf_parsing = Arc::new(AtomicBool::new(false));
    let allow_utf8 = Arc::new(AtomicBool::new(false));
    // Assertion failures inside the handler (Go's require in the handler goroutine).
    let failures = Arc::new(Mutex::new(Vec::<String>::new()));

    let (pp, au, fl) = (protobuf_parsing.clone(), allow_utf8.clone(), failures.clone());
    let server = new_server(move |r| {
        let fail = |m: String| fl.lock().unwrap().push(m);
        let accept = r.header.get_str("Accept").to_string();
        let allow_utf8 = au.load(Ordering::SeqCst);
        if allow_utf8 {
            if !accept.contains("escaping=allow-utf-8") {
                fail(format!("Expected Accept header to allow utf8, got {accept:?}"));
            }
        } else if accept.contains("escaping=allow-utf-8") {
            fail(format!("Expected Accept header to not allow utf8, got {accept:?}"));
        }
        if pp.load(Ordering::SeqCst) && !accept.starts_with("application/vnd.google.protobuf;") {
            fail("Expected Accept header to prefer application/vnd.google.protobuf.".to_string());
        }

        for st in accept.split(',') {
            // Go: regexp `q=([0-9]+(\.\d+)?)`.
            let Some(i) = st.find("q=") else {
                fail(format!("no q value in {st:?}"));
                continue;
            };
            let rest = &st[i + 2..];
            let int_len = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
            if int_len == 0 {
                fail(format!("no q value in {st:?}"));
                continue;
            }
            let mut end = int_len;
            if rest[int_len..].starts_with('.') {
                let frac = rest[int_len + 1..].bytes().take_while(|b| b.is_ascii_digit()).count();
                if frac > 0 {
                    end = int_len + 1 + frac;
                }
            }
            let m1 = &rest[..end];
            match m1.parse::<f64>() {
                Err(e) => fail(format!("Error parsing q value: {e}")),
                Ok(q) => {
                    if !(0.0..=1.0).contains(&q) {
                        fail(format!("q value out of range: {q}"));
                    }
                }
            }
            if m1.split('.').nth(1).unwrap().len() > 3 {
                fail("q value should have at most 3 decimal places".to_string());
            }
        }

        let timeout = r.header.get_str("X-Prometheus-Scrape-Timeout-Seconds");
        if timeout != EXPECTED_TIMEOUT {
            fail(format!("Expected scrape timeout header: {timeout:?}"));
        }

        if allow_utf8 {
            SResp::ok("text/plain; version=1.0.0; escaping=allow-utf-8", b"metric_a 1\nmetric_b 2\n")
        } else {
            SResp::ok("text/plain; version=0.0.4", b"metric_a 1\nmetric_b 2\n")
        }
    });

    let run_test = |accept: String| {
        let ts = target_scraper(
            Some(new_target(from_strings(&["__scheme__", "http", "__address__", &server.host]), ScrapeConfig::default())),
            default_client(),
            CONFIG_TIMEOUT,
            0,
            accept,
            false,
        );
        let mut buf = Vec::new();
        let resp = ts.scrape(&Ctx::background()).expect("scrape");
        let content_type = ts.read_response(&Ctx::background(), resp, &mut buf).expect("read response");
        if allow_utf8.load(Ordering::SeqCst) {
            assert_eq!("text/plain; version=1.0.0; escaping=allow-utf-8", content_type);
        } else {
            assert_eq!("text/plain; version=0.0.4", content_type);
        }
        assert_eq!("metric_a 1\nmetric_b 2\n", String::from_utf8(buf).unwrap());
    };

    for (protocols, scheme, pb, utf8) in [
        (config::default_scrape_protocols(), EscapingScheme::UnderscoreEscaping, false, false),
        (config::default_proto_first_scrape_protocols(), EscapingScheme::UnderscoreEscaping, true, false),
        (config::default_scrape_protocols(), EscapingScheme::NoEscaping, false, true),
        (config::default_proto_first_scrape_protocols(), EscapingScheme::NoEscaping, true, true),
    ] {
        protobuf_parsing.store(pb, Ordering::SeqCst);
        allow_utf8.store(utf8, Ordering::SeqCst);
        run_test(accept_header(&protocols, scheme));
        let f = failures.lock().unwrap();
        assert!(f.is_empty(), "{f:?}");
    }
}

#[test]
fn test_target_scraper_scrape_over_unix_socket() {
    // t.TempDir can produce paths exceeding the macOS 104-char socket
    // path limit, so we create our own temp dir in /tmp.
    let temp = temp_dir("uds-scrape-test");
    let _cleanup = RemoveDir(temp.clone());
    let socket_path = format!("{temp}/s");

    let hosts = Arc::new(Mutex::new(Vec::<String>::new()));
    let h2 = hosts.clone();
    // Serve HTTP over the Unix socket.
    let _server = new_unix_server(&socket_path, None, move |r| {
        h2.lock().unwrap().push(r.host.clone());
        SResp::ok("text/plain; version=0.0.4", b"metric_a 1\nmetric_b 2\n")
    });

    // Create a client with a DialContext that routes to the unix socket.
    let client = new_unix_socket_scrape_client(
        &socket_path,
        &net::HttpClientConfig::default(),
        &net::HttpClientOptions::default(),
    )
    .expect("unix socket client");

    // No __address__ set — falls back to "localhost".
    let ts = target_scraper(
        Some(new_target(
            from_strings(&["__scheme__", "http", "__metrics_path__", "/metrics", UNIX_SOCKET_LABEL, &socket_path]),
            ScrapeConfig::default(),
        )),
        client,
        Duration::from_millis(1500),
        0,
        accept_header(&config::default_scrape_protocols(), EscapingScheme::UnderscoreEscaping),
        false,
    );

    let mut buf = Vec::new();
    let resp = ts.scrape(&Ctx::background()).expect("scrape");
    let content_type = ts.read_response(&Ctx::background(), resp, &mut buf).expect("read response");
    assert_eq!("text/plain; version=0.0.4", content_type);
    assert_eq!("metric_a 1\nmetric_b 2\n", String::from_utf8(buf).unwrap());
    assert_eq!(vec!["localhost".to_string()], *hosts.lock().unwrap());
}

#[test]
fn test_target_scraper_scrape_over_unix_socket_tls() {
    let temp = temp_dir("uds-tls-test");
    let _cleanup = RemoveDir(temp.clone());
    let socket_path = format!("{temp}/s");

    // Create a Unix domain socket listener and wrap it with TLS using
    // the pre-generated test certificates (CN=localhost).
    let hosts = Arc::new(Mutex::new(Vec::<String>::new()));
    let h2 = hosts.clone();
    // Serve HTTPS over the Unix socket. The pre-generated certificate
    // has SAN IP Address:127.0.0.1, so the __address__ must match.
    let _server = new_unix_server(&socket_path, Some(server_tls_config("server")), move |r| {
        h2.lock().unwrap().push(r.host.clone());
        SResp::ok("text/plain; version=0.0.4", b"metric_a 1\nmetric_b 2\n")
    });

    // Create a client configured to trust the test CA.
    let mut cfg = net::HttpClientConfig::default();
    cfg.tls_config.ca_file = upstream_testdata("ca.cer");
    let client =
        new_unix_socket_scrape_client(&socket_path, &cfg, &net::HttpClientOptions::default()).expect("unix socket client");

    let ts = target_scraper(
        Some(new_target(
            from_strings(&[
                "__scheme__",
                "https",
                "__address__",
                "127.0.0.1",
                "__metrics_path__",
                "/metrics",
                UNIX_SOCKET_LABEL,
                &socket_path,
            ]),
            ScrapeConfig::default(),
        )),
        client,
        Duration::from_millis(1500),
        0,
        accept_header(&config::default_scrape_protocols(), EscapingScheme::UnderscoreEscaping),
        false,
    );

    let mut buf = Vec::new();
    let resp = ts.scrape(&Ctx::background()).expect("scrape");
    let content_type = ts.read_response(&Ctx::background(), resp, &mut buf).expect("read response");
    assert_eq!("text/plain; version=0.0.4", content_type);
    assert_eq!("metric_a 1\nmetric_b 2\n", String::from_utf8(buf).unwrap());
    assert_eq!(vec!["127.0.0.1".to_string()], *hosts.lock().unwrap());
}

#[test]
fn test_target_scraper_unix_socket_connections_are_not_shared() {
    // Two targets sharing the same __address__ but scraped through different
    // unix sockets must never exchange pooled keep-alive connections,
    // otherwise one target would receive the other target's metrics.
    let temp = temp_dir("uds-pool-test");
    let _cleanup = RemoveDir(temp.clone());

    let mut payloads: HashMap<String, String> = HashMap::new();
    let mut targets = Vec::new();
    let mut servers = Vec::new();
    for i in 0..2 {
        let socket_path = format!("{temp}/s{i}");
        let payload = format!("metric_a {i}\n");
        payloads.insert(socket_path.clone(), payload.clone());
        servers.push(new_unix_server(&socket_path, None, move |_| SResp::ok("text/plain; version=0.0.4", payload.as_bytes())));

        targets.push(new_target(
            from_strings(&[
                "__scheme__",
                "http",
                "__address__",
                "localhost:9090",
                "__metrics_path__",
                "/metrics",
                SCRAPE_INTERVAL_LABEL,
                "1s",
                SCRAPE_TIMEOUT_LABEL,
                "1s",
                UNIX_SOCKET_LABEL,
                &socket_path,
            ]),
            ScrapeConfig::default(),
        ));
    }

    // adapted: Go type-asserts opts.scraper to *targetScraper; the Scraper trait object is used as is, with the
    // target of the loop options for the socket label.
    let scrapers: Arc<Mutex<Vec<(Arc<Target>, Arc<dyn Scraper>)>>> = Arc::new(Mutex::new(Vec::new()));
    let s2 = scrapers.clone();
    let sp = new_test_scrape_pool(
        None,
        None,
        Some(Box::new(move |opts: ScrapeLoopOptions| {
            s2.lock().unwrap().push((opts.target.clone(), opts.scraper.clone()));
            let l = TestLoop::default();
            *l.start_func.lock().unwrap() = Some(Box::new(|_, _, _| {}));
            *l.stop_func.lock().unwrap() = Some(Box::new(|| {}));
            Arc::new(l) as Arc<dyn Loop>
        })),
    );
    let client = new_scrape_client(&net::HttpClientConfig::default(), &net::HttpClientOptions::default()).unwrap();
    sp.mtx.lock().unwrap().client = client;

    sp.sync_targets(targets);
    let scrapers = scrapers.lock().unwrap().clone();
    assert_eq!(2, scrapers.len());

    // Scrape each target twice so the second round is served from pooled
    // keep-alive connections.
    for _ in 0..2 {
        for (t, ts) in &scrapers {
            let mut buf = Vec::new();
            let resp = ts.scrape(&Ctx::background()).expect("scrape");
            ts.read_response(&Ctx::background(), resp, &mut buf).expect("read response");
            assert_eq!(payloads[t.raw_labels().get(UNIX_SOCKET_LABEL)], String::from_utf8(buf).unwrap());
        }
    }

    sp.stop();
}

#[test]
fn test_target_scrape_scrape_cancel() {
    let (block_tx, block_rx) = mpsc::channel::<()>();
    let block_rx = Mutex::new(block_rx);
    let server = new_server(move |_| {
        let _ = block_rx.lock().unwrap().recv();
        SResp::ok("", b"")
    });

    let ts = Arc::new(target_scraper(
        Some(new_target(from_strings(&["__scheme__", "http", "__address__", &server.host]), ScrapeConfig::default())),
        default_client(),
        Duration::ZERO,
        0,
        accept_header(
            config::default_global_config().scrape_protocols.as_deref().unwrap_or(&[]),
            EscapingScheme::UnderscoreEscaping,
        ),
        false,
    ));
    let ctx = Ctx::background().child();

    let (errc_tx, errc_rx) = mpsc::sync_channel::<Option<String>>(1);

    let c2 = ctx.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        c2.cancel();
    });

    let c3 = ctx.clone();
    let ts2 = ts.clone();
    std::thread::spawn(move || {
        let r = ts2.scrape(&c3);
        let msg = match r {
            Ok(_) => Some("expected error but got nil".to_string()),
            Err(_) if !c3.err().is_some_and(|e| e.is(&ERR_CANCELED)) => {
                Some(format!("expected context cancellation error but got: {:?}", c3.err()))
            }
            Err(_) => None,
        };
        let _ = errc_tx.send(msg);
    });

    match errc_rx.recv_timeout(Duration::from_secs(5)) {
        Err(_) => {
            drop(block_tx);
            panic!("Scrape function did not return unexpectedly.");
        }
        Ok(err) => assert!(err.is_none(), "{err:?}"),
    }
    // If this is closed in a defer above the function the test server
    // doesn't terminate and the test doesn't complete.
    drop(block_tx);
}

#[test]
fn test_target_scrape_scrape_not_found() {
    let server = new_server(|_| SResp { status: 404, header: Header::new(), body: Vec::new() });

    let ts = target_scraper(
        Some(new_target(from_strings(&["__scheme__", "http", "__address__", &server.host]), ScrapeConfig::default())),
        default_client(),
        Duration::ZERO,
        0,
        accept_header(
            config::default_global_config().scrape_protocols.as_deref().unwrap_or(&[]),
            EscapingScheme::UnderscoreEscaping,
        ),
        false,
    );

    let resp = ts.scrape(&Ctx::background()).expect("scrape");
    let err = ts.read_response(&Ctx::background(), resp, &mut Vec::new()).unwrap_err();
    assert!(err.to_string().contains("404"), "Expected \"404 NotFound\" error but got: {err}");
}

#[test]
fn test_target_scraper_body_size_limit() {
    const BODY_SIZE_LIMIT: i64 = 15;
    const RESPONSE_BODY: &str = "metric_a 1\nmetric_b 2\n";

    for encoding in ["identity", "gzip", "zstd"] {
        let server = new_server(move |_| {
            let mut r = SResp::ok("text/plain; version=0.0.4", b"");
            match encoding {
                "gzip" => {
                    r.header.set("Content-Encoding", encoding);
                    let mut gw = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    gw.write_all(RESPONSE_BODY.as_bytes()).unwrap();
                    r.body = gw.finish().unwrap();
                }
                "zstd" => {
                    r.header.set("Content-Encoding", encoding);
                    r.body = zstd::stream::encode_all(RESPONSE_BODY.as_bytes(), 0).unwrap();
                }
                _ => r.body = RESPONSE_BODY.as_bytes().to_vec(),
            }
            r
        });

        let mut ts = target_scraper(
            Some(new_target(from_strings(&["__scheme__", "http", "__address__", &server.host]), ScrapeConfig::default())),
            default_client(),
            Duration::ZERO,
            BODY_SIZE_LIMIT,
            accept_header(
                config::default_global_config().scrape_protocols.as_deref().unwrap_or(&[]),
                EscapingScheme::UnderscoreEscaping,
            ),
            true,
        );
        let mut buf = Vec::new();

        let resp = ts.scrape(&Ctx::background()).unwrap_or_else(|e| panic!("{encoding}: scrape: {e}"));
        let err = ts.read_response(&Ctx::background(), resp, &mut buf).unwrap_err();
        assert!(err.is(&ERR_BODY_SIZE_LIMIT), "{encoding}: {err}");
        assert_eq!(BODY_SIZE_LIMIT as usize, buf.len(), "{encoding}");

        buf.clear();
        ts.body_size_limit = 0;
        let resp = ts.scrape(&Ctx::background()).unwrap_or_else(|e| panic!("{encoding}: scrape: {e}"));
        ts.read_response(&Ctx::background(), resp, &mut buf).unwrap_or_else(|e| panic!("{encoding}: {e}"));
        assert_eq!(RESPONSE_BODY, String::from_utf8(buf).unwrap(), "{encoding}");
    }
}

#[test]
fn test_target_scraper_malformed_compressed_response() {
    for encoding in ["gzip", "zstd"] {
        let resp = response(encoding, b"not compressed".to_vec());
        let ts = target_scraper(None, default_client(), Duration::ZERO, i64::MAX, String::new(), true);
        let r = ts.read_response(&Ctx::background(), Some(resp), &mut Vec::new());
        assert!(r.is_err(), "{encoding}");
    }
}

#[test]
fn test_target_scraper_zstd_window_limit() {
    let data = vec![b'a'; ZSTD_MAX_WINDOW_SIZE + 1];
    let mut encoder = zstd::bulk::Compressor::new(0).unwrap();
    // Go: zstd.WithWindowSize(2*zstdMaxWindowSize).
    encoder.set_parameter(zstd::zstd_safe::CParameter::WindowLog((2 * ZSTD_MAX_WINDOW_SIZE).trailing_zeros())).unwrap();
    let compressed = encoder.compress(&data).unwrap();
    let resp = response("zstd", compressed);
    let ts = target_scraper(None, default_client(), Duration::ZERO, i64::MAX, String::new(), true);

    let err = ts.read_response(&Ctx::background(), Some(resp), &mut Vec::new()).unwrap_err();
    assert!(err.to_string().contains("decompressed size exceeds configured limit"), "{err}");
}

#[test]
fn test_target_scraper_zstd_feature_flag() {
    const RESPONSE_BODY: &str = "metric_a 1\nmetric_b 2\n";
    let compressed = zstd::bulk::compress(RESPONSE_BODY.as_bytes(), 0).unwrap();

    for (name, enable_zstd, expect_err) in
        [("enabled decompresses the body", true, ""), ("disabled rejects the response", false, "zstd-scrape")]
    {
        let resp = response("zstd", compressed.clone());
        let ts = target_scraper(None, default_client(), Duration::ZERO, i64::MAX, String::new(), enable_zstd);

        let mut buf = Vec::new();
        let r = ts.read_response(&Ctx::background(), Some(resp), &mut buf);
        if !expect_err.is_empty() {
            let err = r.unwrap_err();
            assert!(err.to_string().contains(expect_err), "{name}: {err}");
            // The undecoded frame must not reach the parser.
            assert!(buf.is_empty(), "{name}");
            continue;
        }
        r.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(RESPONSE_BODY, String::from_utf8(buf).unwrap(), "{name}");
    }
}

#[test]
fn test_scrape_loop_respect_timestamps() {
    foreach_appendable(|app_v2| {
        let s = Arc::new(tsdb::teststorage::new_default());
        let app_test = Appendable::new().then(s.clone());
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, app_v2));

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a{a=\"1\",b=\"1\"} 1 0", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let want = vec![Sample { l: from_strings(&["__name__", "metric_a", "a", "1", "b", "1"]), t: 0, v: 1.0, ..Default::default() }];
        require_equal(&want, &app_test.result_samples());
    });
}

#[test]
fn test_scrape_loop_discard_timestamps() {
    foreach_appendable(|app_v2| {
        // adapted: as in test_scrape_loop_respect_timestamps, the unread TSDB behind the mock is left out.
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, app_v2);
            sl.honor_timestamps = false;
        });

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a{a=\"1\",b=\"1\"} 1 0", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let want = vec![Sample {
            l: from_strings(&["__name__", "metric_a", "a", "1", "b", "1"]),
            t: timestamp::from_time(now),
            v: 1.0,
            ..Default::default()
        }];
        require_equal(&want, &app_test.result_samples());
    });
}

#[test]
fn test_reusable_config() {
    let job = |name: &str| ScrapeConfig { job_name: name.to_string(), ..Default::default() };
    let variants = [
        ScrapeConfig { scrape_timeout: dur(Duration::from_secs(15)), ..job("prometheus") },
        ScrapeConfig { scrape_timeout: dur(Duration::from_secs(15)), ..job("httpd") },
        ScrapeConfig { scrape_timeout: dur(Duration::from_secs(5)), ..job("prometheus") },
        ScrapeConfig { metrics_path: "/metrics".to_string(), ..job("prometheus") },
        ScrapeConfig { metrics_path: "/metrics2".to_string(), ..job("prometheus") },
        ScrapeConfig {
            scrape_timeout: dur(Duration::from_secs(5)),
            metrics_path: "/metrics2".to_string(),
            ..job("prometheus")
        },
        ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(5)),
            metrics_path: "/metrics2".to_string(),
            ..job("prometheus")
        },
        ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(5)),
            sample_limit: 1000,
            metrics_path: "/metrics2".to_string(),
            ..job("prometheus")
        },
    ];

    let matches = [[0, 2], [4, 5], [4, 6], [4, 7], [5, 6], [5, 7], [6, 7]];
    let no_match = [[1, 2], [0, 4], [3, 4]];

    for (i, m) in matches.iter().enumerate() {
        assert!(reusable_cache(Some(&variants[m[0]]), Some(&variants[m[1]])), "match test {i}");
        assert!(reusable_cache(Some(&variants[m[1]]), Some(&variants[m[0]])), "match test {i}");
        assert!(reusable_cache(Some(&variants[m[1]]), Some(&variants[m[1]])), "match test {i}");
        assert!(reusable_cache(Some(&variants[m[0]]), Some(&variants[m[0]])), "match test {i}");
    }
    for (i, m) in no_match.iter().enumerate() {
        assert!(!reusable_cache(Some(&variants[m[0]]), Some(&variants[m[1]])), "not match test {i}");
        assert!(!reusable_cache(Some(&variants[m[1]]), Some(&variants[m[0]])), "not match test {i}");
    }
}

#[test]
fn test_reuse_scrape_cache() {
    foreach_appendable(|app_v2| {
        let app = Appendable::new();
        let base = |job: &str| ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(5)),
            scrape_timeout: dur(Duration::from_secs(15)),
            metrics_path: "/metrics2".to_string(),
            ..utf8_scrape_config(job)
        };
        let cfg = ScrapeConfig {
            scrape_timeout: dur(Duration::from_secs(5)),
            scrape_interval: dur(Duration::from_secs(5)),
            metrics_path: "/metrics".to_string(),
            ..utf8_scrape_config("Prometheus")
        };
        let (v1, v2) = select_appendable(&app, app_v2);
        let sp = ScrapePool::new(
            Arc::new(cfg),
            v1,
            v2,
            0,
            discovery::Logger::nop(),
            None,
            Arc::new(Options::default()),
            new_test_scrape_metrics(),
        )
        .unwrap();
        let t1 = new_target(
            from_strings(&["labelNew", "nameNew", "labelNew1", "nameNew1", "labelNew2", "nameNew2"]),
            utf8_scrape_config(""),
        );
        let proxy_url = yaml::types::Url::parse("http://localhost:2128").unwrap();
        sp.sync_targets(vec![t1]);

        let mut proxied = ScrapeConfig { honor_timestamps: true, sample_limit: 400, ..base("Prometheus") };
        proxied.http_client_config.proxy_config.proxy_url = proxy_url;

        let steps: Vec<(bool, ScrapeConfig)> = vec![
            (
                true,
                ScrapeConfig {
                    scrape_interval: dur(Duration::from_secs(5)),
                    scrape_timeout: dur(Duration::from_secs(5)),
                    metrics_path: "/metrics".to_string(),
                    ..utf8_scrape_config("Prometheus")
                },
            ),
            (false, base("Prometheus")),
            (true, ScrapeConfig { sample_limit: 400, ..base("Prometheus") }),
            (false, ScrapeConfig { honor_timestamps: true, sample_limit: 400, ..base("Prometheus") }),
            (true, proxied),
            (
                false,
                ScrapeConfig { honor_timestamps: true, honor_labels: true, sample_limit: 400, ..base("Prometheus") },
            ),
            (false, ScrapeConfig { metrics_path: "/metrics".to_string(), label_limit: 1, ..base("Prometheus") }),
            (false, ScrapeConfig { metrics_path: "/metrics".to_string(), label_limit: 15, ..base("Prometheus") }),
            (
                false,
                ScrapeConfig {
                    metrics_path: "/metrics".to_string(),
                    label_limit: 15,
                    label_name_length_limit: 5,
                    ..base("Prometheus")
                },
            ),
            (
                false,
                ScrapeConfig {
                    metrics_path: "/metrics".to_string(),
                    label_limit: 15,
                    label_name_length_limit: 5,
                    label_value_length_limit: 7,
                    ..base("Prometheus")
                },
            ),
        ];

        // Go fmt.Sprintf("%p", l.getCache()).
        let cache_addr = |sp: &ScrapePool| -> HashMap<u64, usize> {
            sp.loops()
                .iter()
                .map(|(fp, l)| (*fp, l.get_cache().map(|c| Arc::as_ptr(&c) as usize).unwrap_or(0)))
                .collect()
        };

        for (i, (keep, new_config)) in steps.into_iter().enumerate() {
            let new_config = Arc::new(new_config);
            let init_cache_addr = cache_addr(&sp);
            assert!(sp.reload(new_config.clone()).is_ok());
            for (fp, new_cache_addr) in cache_addr(&sp) {
                if keep {
                    assert_eq!(init_cache_addr[&fp], new_cache_addr, "step {i}: old cache and new cache are not the same");
                } else {
                    assert_ne!(init_cache_addr[&fp], new_cache_addr, "step {i}: old cache and new cache are the same");
                }
            }
            let init_cache_addr = cache_addr(&sp);
            assert!(sp.reload(new_config.clone()).is_ok());
            for (fp, new_cache_addr) in cache_addr(&sp) {
                assert_eq!(
                    init_cache_addr[&fp], new_cache_addr,
                    "step {i}: reloading the exact config invalidates the cache"
                );
            }
        }
        sp.stop();
    });
}

#[test]
fn test_reuse_cache_race() {
    foreach_appendable(|app_v2| {
        let cfg = ScrapeConfig {
            scrape_timeout: dur(Duration::from_secs(5)),
            scrape_interval: dur(Duration::from_secs(5)),
            metrics_path: "/metrics".to_string(),
            ..utf8_scrape_config("Prometheus")
        };
        let buffers = Arc::new(BufPool::new(1000, 100_000_000, 3));
        let (v1, v2) = select_appendable(&Appendable::new(), app_v2);
        let sp = ScrapePool::new(
            Arc::new(cfg),
            v1,
            v2,
            0,
            discovery::Logger::nop(),
            Some(buffers),
            Arc::new(Options::default()),
            new_test_scrape_metrics(),
        )
        .unwrap();
        let t1 = new_target(from_strings(&["labelNew", "nameNew"]), ScrapeConfig::default());
        sp.sync_targets(vec![t1]);

        let start = Instant::now();
        let mut i: usize = 1;
        while i > 0 {
            if start.elapsed() > Duration::from_secs(5) {
                break;
            }
            let r = sp.reload(Arc::new(ScrapeConfig {
                scrape_timeout: dur(Duration::from_millis(1)),
                scrape_interval: dur(Duration::from_millis(1)),
                metrics_path: "/metrics".to_string(),
                sample_limit: i,
                ..utf8_scrape_config("Prometheus")
            }));
            assert!(r.is_ok(), "{r:?}");
            i = i.wrapping_add(1);
        }
        sp.stop();
    });
}

#[test]
fn test_check_add_error() {
    let mut app_errs = AppendErrors::default();
    let (sl, _) = new_test_scrape_loop(|_| {});
    // TODO: Check err etc
    let _ = sl.check_add_error(b"", &[], Some(ERR_OUT_OF_ORDER_SAMPLE), &mut None, &mut None, &mut app_errs);
    assert_eq!(1, app_errs.num_out_of_order);
    // TODO(bwplotka): Test partial error check and other cases
}

#[test]
fn test_scrape_loop_label_limit() {
    foreach_appendable(|app_v2| {
        struct Case {
            title: &'static str,
            scrape_labels: &'static str,
            discovery_labels: Vec<&'static str>,
            label_limits: LabelLimits,
            expect_err: bool,
        }
        let ll = |label_limit, label_name_length_limit, label_value_length_limit| LabelLimits {
            label_limit,
            label_name_length_limit,
            label_value_length_limit,
        };
        let cases = vec![
            Case {
                title: "Valid number of labels",
                scrape_labels: r#"metric{l1="1", l2="2"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(5, 0, 0),
                expect_err: false,
            },
            Case {
                title: "Too many labels",
                scrape_labels: r#"metric{l1="1", l2="2", l3="3", l4="4", l5="5", l6="6"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(5, 0, 0),
                expect_err: true,
            },
            Case {
                title: "Too many labels including discovery labels",
                scrape_labels: r#"metric{l1="1", l2="2", l3="3", l4="4"} 0"#,
                discovery_labels: vec!["l5", "5", "l6", "6"],
                label_limits: ll(5, 0, 0),
                expect_err: true,
            },
            Case {
                title: "Valid labels name length",
                scrape_labels: r#"metric{l1="1", l2="2"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(0, 10, 0),
                expect_err: false,
            },
            Case {
                title: "Label name too long",
                scrape_labels: r#"metric{label_name_too_long="0"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(0, 10, 0),
                expect_err: true,
            },
            Case {
                title: "Discovery label name too long",
                scrape_labels: r#"metric{l1="1", l2="2"} 0"#,
                discovery_labels: vec!["label_name_too_long", "0"],
                label_limits: ll(0, 10, 0),
                expect_err: true,
            },
            Case {
                title: "Valid labels value length",
                scrape_labels: r#"metric{l1="1", l2="2"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(0, 0, 10),
                expect_err: false,
            },
            Case {
                title: "Label value too long",
                scrape_labels: r#"metric{l1="label_value_too_long"} 0"#,
                discovery_labels: vec![],
                label_limits: ll(0, 0, 10),
                expect_err: true,
            },
            Case {
                title: "Discovery label value too long",
                scrape_labels: r#"metric{l1="1", l2="2"} 0"#,
                discovery_labels: vec!["l1", "label_value_too_long"],
                label_limits: ll(0, 0, 10),
                expect_err: true,
            },
        ];
        for test in cases {
            let discovery_labels = new_target(from_strings(&test.discovery_labels), ScrapeConfig::default());
            let app = Appendable::new();
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &app, app_v2);
                let d1 = discovery_labels.clone();
                sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &d1, false, &[]));
                let d2 = discovery_labels.clone();
                sl.report_sample_mutator = Arc::new(move |l| mutate_report_sample_labels(l, &d2));
                sl.label_limits = Some(test.label_limits);
            });

            let mut a = sl.appender();
            let (_, err) = a.append(test.scrape_labels.as_bytes(), "text/plain", SystemTime::now());

            if test.expect_err {
                assert!(err.is_some(), "Test:{}", test.title);
            } else {
                assert!(err.is_none(), "Test:{}: {err:?}", test.title);
                assert!(a.commit().is_none(), "Test:{}", test.title);
            }
        }
    });
}

fn relabel_replace(source: &str, regex: &str, replacement: &str, target: &str) -> relabel::Config {
    relabel::Config {
        source_labels: Some(vec![source.to_string()]),
        regex: relabel::Regexp::must_new(regex),
        replacement: replacement.to_string(),
        target_label: target.to_string(),
        action: relabel::Action::Replace,
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    }
}

fn address_group(addr: &str) -> Vec<Arc<Group>> {
    vec![Arc::new(Group {
        targets: vec![LabelSet(BTreeMap::from([("__address__".to_string(), addr.to_string())]))],
        ..Default::default()
    })]
}

#[test]
fn test_target_scrape_interval_and_timeout_relabel() {
    foreach_appendable(|app_v2| {
        let cfg = ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(2)),
            scrape_timeout: dur(Duration::from_millis(500)),
            relabel_configs: vec![
                relabel_replace(SCRAPE_INTERVAL_LABEL, "2s", "3s", SCRAPE_INTERVAL_LABEL),
                relabel_replace(SCRAPE_TIMEOUT_LABEL, "500ms", "750ms", SCRAPE_TIMEOUT_LABEL),
            ],
            ..utf8_scrape_config("")
        };
        let (v1, v2) = select_appendable(&Appendable::new(), app_v2);
        let sp = ScrapePool::new(
            Arc::new(cfg),
            v1,
            v2,
            0,
            discovery::Logger::nop(),
            None,
            Arc::new(Options::default()),
            new_test_scrape_metrics(),
        )
        .unwrap();

        sp.sync_groups(&address_group("127.0.0.1:9090"));

        assert_eq!("3s", sp.active_targets()[0].raw_labels().get(SCRAPE_INTERVAL_LABEL));
        assert_eq!("750ms", sp.active_targets()[0].raw_labels().get(SCRAPE_TIMEOUT_LABEL));
        sp.stop();
    });
}

#[test]
fn test_target_scrape_convert_classic_histograms_to_nhcb_relabel() {
    foreach_appendable(|app_v2| {
        let cfg = ScrapeConfig {
            scrape_interval: dur(Duration::from_secs(2)),
            scrape_timeout: dur(Duration::from_millis(500)),
            relabel_configs: vec![relabel_replace(
                CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL,
                "false",
                "true",
                CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL,
            )],
            ..utf8_scrape_config("")
        };
        let (v1, v2) = select_appendable(&Appendable::new(), app_v2);
        let sp = ScrapePool::new(
            Arc::new(cfg),
            v1,
            v2,
            0,
            discovery::Logger::nop(),
            None,
            Arc::new(Options::default()),
            new_test_scrape_metrics(),
        )
        .unwrap();

        sp.sync_groups(&address_group("127.0.0.1:9090"));

        assert_eq!("true", sp.active_targets()[0].raw_labels().get(CONVERT_CLASSIC_HISTOGRAMS_TO_NHCB_LABEL));

        {
            let ts = sp.target_mtx.lock().unwrap();
            assert_eq!(1, ts.loops.len());
            for l in ts.loops.values() {
                assert!(l.as_scrape_loop().expect("scrape loop").convert_classic_hist_to_nhcb);
            }
        }
        sp.stop();
    });
}
