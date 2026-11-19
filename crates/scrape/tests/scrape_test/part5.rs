//! scrape_test.go: the tests that write to and read from the real TSDB (`teststorage.New`), from all Go line
//! ranges. The other parts list them as moved here.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chunkenc::{ChunkIterator, ValueType};
use config::ScrapeConfig;
use discovery::targetgroup::Group;
use model::exemplar::{Exemplar, QueryResult};
use model::histogram::{CUSTOM_BUCKETS_SCHEMA, Histogram};
use model::labels::{Labels, ValidationScheme, from_strings};
use model::labelset::LabelSet;
use model::matcher::{MatchType, Matcher};
use model::metadata::{Metadata, MetricType};
use model::relabel;
use model::timestamp::from_time;
use model::value::is_stale_nan;
use scrape::ctx::Ctx;
use scrape::manager::TargetSets;
use scrape::scrape_loop::ERR_NAME_LABEL_MANDATORY;
use scrape::target::ADDRESS_LABEL;
use scrape::testing::dto::proto_marshal_delimited;
use scrape::testing::{new_test_scrape_loop, new_test_scrape_metrics};
use scrape::{Manager, Options, ScrapeLoop, ScrapePool};
use storage::{BoxSeriesSet, Context, ERR_OUT_OF_ORDER_SAMPLE, Error};
use tsdb::teststorage::{self, Shared, StorageOption, TestStorage};
use util::teststorage::{Appendable, Sample, require_equal_msg, require_not_equal};

// ---------------- helpers ----------------

/// Go `foreachAppendable`.
fn foreach_appendable(f: impl Fn(bool)) {
    for v2 in [false, true] {
        f(v2);
    }
}

/// Go `withAppendable(app, appV2)` for the mock.
fn with_appendable(sl: &mut ScrapeLoop, app: &Appendable, v2: bool) {
    if v2 {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
    } else {
        sl.appendable = Some(Arc::new(app.clone()));
    }
}

/// Go `withAppendable(s, appV2)` for the real TSDB.
fn with_storage(sl: &mut ScrapeLoop, s: &Arc<TestStorage>, v2: bool) {
    if v2 {
        sl.appendable_v2 = Some(s.clone());
    } else {
        sl.appendable = Some(s.clone());
    }
}

type Apps = (Option<Arc<dyn storage::Appendable>>, Option<Arc<dyn storage::AppendableV2>>);

/// Go `selectAppendable(s, appV2)`.
fn select_storage(s: &Arc<TestStorage>, v2: bool) -> Apps {
    if v2 { (None, Some(s.clone())) } else { (Some(s.clone()), None) }
}

fn new_storage() -> Shared {
    teststorage::new_shared_default()
}

fn new_storage_with(f: impl FnOnce(&mut tsdb::db::Options) + 'static) -> Shared {
    let o: StorageOption = Box::new(f);
    teststorage::new_shared(vec![o])
}

/// Go `time.Time{}.UnixNano()`: year 1 does not fit in int64 nanoseconds; Go's arithmetic wraps to this value.
const GO_ZERO_TIME_UNIX_NANO: i64 = -6795364578871345152;

/// Go `time.Time{}` (year 1).
fn go_zero_time() -> SystemTime {
    UNIX_EPOCH - Duration::from_secs(62_135_596_800)
}

fn now_unix_nano() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
}

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn querier(s: &TestStorage, mint: i64, maxt: i64) -> storage::BoxQuerier {
    storage::Queryable::querier(s, mint, maxt).expect("querier")
}

/// Reads the float samples of every series (Go: `for it.Next() == chunkenc.ValFloat`).
fn float_samples(series: &mut BoxSeriesSet) -> Vec<Sample> {
    let mut results = Vec::new();
    while series.next() {
        let s = series.at().clone();
        let mut it = s.iterator(None);
        while it.next() == ValueType::Float {
            let (t, v) = it.at();
            results.push(Sample { l: s.labels().clone(), t, v, ..Default::default() });
        }
        assert!(it.err().is_none(), "{:?}", it.err());
    }
    assert!(series.err().is_none(), "{:?}", series.err());
    results
}

fn utf8_cfg(job: &str) -> ScrapeConfig {
    ScrapeConfig {
        job_name: job.into(),
        scheme: "http".into(),
        scrape_interval: dur(Duration::from_millis(100)),
        scrape_timeout: dur(Duration::from_millis(100)),
        metric_name_validation_scheme: ValidationScheme::Utf8,
        metric_name_escaping_scheme: config::ALLOW_UTF8.into(),
        ..Default::default()
    }
}

fn dur(d: Duration) -> yaml::types::Duration {
    yaml::types::Duration(d.as_nanos() as i64)
}

/// Go `newScrapePool(cfg, sa.V1(), sa.V2(), 0, nil, nil, opts, newTestScrapeMetrics(t))`.
fn new_pool(cfg: ScrapeConfig, apps: Apps, opts: Options) -> Arc<ScrapePool> {
    ScrapePool::new(
        Arc::new(cfg),
        apps.0,
        apps.1,
        0,
        discovery::Logger::nop(),
        None,
        Arc::new(opts),
        new_test_scrape_metrics(),
    )
    .expect("new scrape pool")
}

fn target_group(host: &str, extra: &[(&str, &str)]) -> Group {
    let mut pairs = vec![(ADDRESS_LABEL, host)];
    pairs.extend_from_slice(extra);
    Group { targets: vec![LabelSet::from_pairs(&pairs)], labels: LabelSet::default(), source: String::new() }
}

/// Go `httptest.NewServer`: answers every request with `body` (one response per connection).
struct TestServer {
    host: String,
    stop: Arc<AtomicBool>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.host);
    }
}

fn new_server(content_type: Option<&'static str>, body: Vec<u8>, on_request: impl Fn() + Send + Sync + 'static) -> TestServer {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let host = l.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    let on_request = Arc::new(on_request);
    std::thread::spawn(move || {
        for c in l.incoming() {
            if st.load(Ordering::SeqCst) {
                return;
            }
            let Ok(mut c) = c else { continue };
            let body = body.clone();
            let on_request = on_request.clone();
            std::thread::spawn(move || {
                let _ = c.set_read_timeout(Some(Duration::from_secs(10)));
                let mut r = BufReader::new(c.try_clone().unwrap());
                loop {
                    let mut line = String::new();
                    if r.read_line(&mut line).unwrap_or(0) == 0 {
                        return;
                    }
                    if line.trim_end().is_empty() {
                        break;
                    }
                }
                let mut out = String::from("HTTP/1.1 200 OK\r\n");
                if let Some(ct) = content_type {
                    out.push_str(&format!("Content-Type: {ct}\r\n"));
                }
                out.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
                let _ = c.write_all(out.as_bytes());
                let _ = c.write_all(&body);
                let _ = c.flush();
                on_request();
            });
        }
    });
    TestServer { host, stop }
}

/// Go `newScrapableServer`: the receiver gets a value (Go closes the channel) after the second scrape.
fn new_scrapable_server(scrape_text: &str) -> (TestServer, Receiver<()>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(Some(tx));
    let scrapes = AtomicUsize::new(0);
    // Go net/http sniffs the written text and sends "Content-Type: text/plain; charset=utf-8".
    let srv = new_server(Some("text/plain; charset=utf-8"), scrape_text.as_bytes().to_vec(), move || {
        if scrapes.fetch_add(1, Ordering::SeqCst) + 1 == 2
            && let Some(tx) = tx.lock().unwrap().take()
        {
            let _ = tx.send(());
        }
    });
    (srv, rx)
}

#[track_caller]
fn require_sample(exp: &Sample, got: &Sample) {
    assert!(exp.equals(got), "sample not equal:\nexpected: {exp}\n     got: {got}");
}

fn exemplars_equal(a: &[Exemplar], b: &[Exemplar]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.equals(y) && x.has_ts == y.has_ts)
}

#[track_caller]
fn require_query_result(exp: &QueryResult, got: &QueryResult) {
    assert!(
        model::labels::equal(&exp.series_labels, &got.series_labels) && exemplars_equal(&exp.exemplars, &got.exemplars),
        "query result not equal:\nexpected: {exp:?}\n     got: {got:?}"
    );
}

/// Go `makeTestMetricFamily`.
fn make_test_metric_family(n: usize, name: &str, typ: &str) -> Vec<u8> {
    let mut sb = format!("# TYPE {name} {typ}\n# HELP {name} help text\n");
    for i in 0..n {
        sb.push_str(&format!("{name}{{foo=\"{i}\",bar=\"{}\"}} 1\n", i * 100));
    }
    sb.push_str("# EOF\n");
    sb.into_bytes()
}

/// Go `makeTestGauges`.
fn make_test_gauges(n: usize) -> Vec<u8> {
    make_test_metric_family(n, "metric_a", "gauge")
}

/// Go `readTextParseTestMetrics`.
fn read_text_parse_test_metrics() -> Vec<u8> {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/model/textparse/testdata/alltypes.237mfs.prom.txt");
    let b = std::fs::read(p).unwrap_or_else(|e| panic!("read {p}: {e}"));
    // Replace all Carriage Return chars that appear when testing on windows.
    b.into_iter().filter(|&c| c != b'\r').collect()
}

/// Go `makeTestHistogramsWithExemplars`.
fn make_test_histograms_with_exemplars(n: usize) -> Vec<u8> {
    const T: &str = r#"# HELP rpc_durations_histogram%d_seconds RPC latency distributions.
# TYPE rpc_durations_histogram%d_seconds histogram
rpc_durations_histogram%d_seconds_bucket{le="-0.00099"} 0
rpc_durations_histogram%d_seconds_bucket{le="-0.00089"} 1 # {dummyID="1242"} -0.00091 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0007899999999999999"} 1 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0006899999999999999"} 2 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0005899999999999998"} 3 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0004899999999999998"} 4 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0003899999999999998"} 5 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0002899999999999998"} 6 # {dummyID="17783"} -0.0003825067330956884 1.7268398142239082e+09
rpc_durations_histogram%d_seconds_bucket{le="-0.0001899999999999998"} 7 # {dummyID="84741"} -0.00020178290006788965 1.726839814829977e+09
rpc_durations_histogram%d_seconds_bucket{le="-8.999999999999979e-05"} 7
rpc_durations_histogram%d_seconds_bucket{le="1.0000000000000216e-05"} 8 # {dummyID="19206"} -4.6156147425468016e-05 1.7268398151337721e+09
rpc_durations_histogram%d_seconds_bucket{le="0.00011000000000000022"} 9 # {dummyID="3974"} 9.528436760156754e-05 1.726839814526797e+09
rpc_durations_histogram%d_seconds_bucket{le="0.00021000000000000023"} 11 # {dummyID="29640"} 0.00017459624183458996 1.7268398139220061e+09
rpc_durations_histogram%d_seconds_bucket{le="0.0003100000000000002"} 15 # {dummyID="9818"} 0.0002791130914009552 1.7268398149821382e+09
rpc_durations_histogram%d_seconds_bucket{le="0.0004100000000000002"} 15
rpc_durations_histogram%d_seconds_bucket{le="0.0005100000000000003"} 15
rpc_durations_histogram%d_seconds_bucket{le="0.0006100000000000003"} 15
rpc_durations_histogram%d_seconds_bucket{le="0.0007100000000000003"} 15
rpc_durations_histogram%d_seconds_bucket{le="0.0008100000000000004"} 15
rpc_durations_histogram%d_seconds_bucket{le="0.0009100000000000004"} 15
rpc_durations_histogram%d_seconds_bucket{le="+Inf"} 15
rpc_durations_histogram%d_seconds_sum -8.452185437166741e-05
rpc_durations_histogram%d_seconds_count 15
rpc_durations_histogram%d_seconds_created 1.726839813016302e+09
"#;
    let mut sb = String::new();
    for i in 0..n {
        sb.push_str(&T.replace("%d", &i.to_string()));
    }
    sb.push_str("# EOF\n");
    sb.into_bytes()
}

/// Go `seriesPerHistogramFor100HistsWithExemplars`.
fn series_per_histogram_for_100_hists_with_exemplars(app_v2: bool) -> usize {
    if app_v2 {
        // AppenderV2 with parseST enabled, uses _created lines for ST instead of samples.
        return 23;
    }
    24
}

fn eventually(mut f: impl FnMut() -> bool, wait: Duration, tick: Duration, msg: &str) {
    let end = Instant::now() + wait;
    loop {
        std::thread::sleep(tick);
        if f() {
            return;
        }
        if Instant::now() >= end {
            panic!("condition never satisfied: {msg}");
        }
    }
}

// ---------------- tests ----------------

#[test]
fn test_storage_handles_out_of_order_timestamps() {
    foreach_appendable(|v2| {
        // Test with default OutOfOrderTimeWindow (0)
        {
            let s = new_storage();
            run_scrape_loop_test(v2, &s, false);
        }
        // Test with specific OutOfOrderTimeWindow (600000)
        {
            let s = new_storage_with(|o| o.out_of_order_time_window = 600000);
            run_scrape_loop_test(v2, &s, true);
        }
    });
}

fn run_scrape_loop_test(v2: bool, s: &Arc<TestStorage>, expect_out_of_order: bool) {
    let (sl, _) = new_test_scrape_loop(|sl| with_storage(sl, s, v2));

    // Current time for generating timestamps.
    let now = SystemTime::now();
    // Calculate timestamps for the samples based on the current time.
    let secs = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let now = UNIX_EPOCH + Duration::from_secs(secs - secs % 60); // round down the now timestamp to the nearest minute
    let timestamp_inorder1 = now;
    let timestamp_out_of_order = now - Duration::from_secs(5 * 60);
    let timestamp_inorder2 = now + Duration::from_secs(5 * 60);

    let mut app = sl.appender();
    let (_, err) = app.append(br#"metric_total{a="1",b="1"} 1"#, "text/plain", timestamp_inorder1);
    assert!(err.is_none(), "{err:?}");
    let (_, err) = app.append(br#"metric_total{a="1",b="1"} 2"#, "text/plain", timestamp_out_of_order);
    assert!(err.is_none(), "{err:?}");
    let (_, err) = app.append(br#"metric_total{a="1",b="1"} 3"#, "text/plain", timestamp_inorder2);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Query the samples back from the storage.
    let q = querier(s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());
    // Use a matcher to filter the metric name.
    let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "metric_total")]);
    let results = float_samples(&mut series);

    // Define the expected results
    let want = vec![
        Sample {
            l: from_strings(&["__name__", "metric_total", "a", "1", "b", "1"]),
            t: from_time(timestamp_inorder1),
            v: 1.0,
            ..Default::default()
        },
        Sample {
            l: from_strings(&["__name__", "metric_total", "a", "1", "b", "1"]),
            t: from_time(timestamp_inorder2),
            v: 3.0,
            ..Default::default()
        },
    ];
    if expect_out_of_order {
        require_not_equal(&want, &results);
    } else {
        require_equal_msg(&want, &results, "Appended samples not as expected");
    }
}

#[test]
fn test_scrape_loop_append_with_storage() {
    let ts = SystemTime::now();
    let fts = from_time(ts);
    let mf = |v2: bool, name: &str| if v2 { name.to_string() } else { String::new() };

    for app_v2 in [false, true] {
        type CheckSamples = Box<dyn Fn(&[Sample])>;
        type CheckExemplars = Box<dyn Fn(&[QueryResult])>;
        struct Tc {
            name: &'static str,
            parsable_text: Vec<u8>,
            expected_samples_len: usize,
            test_appended_samples: CheckSamples,
            test_exemplars: Option<CheckExemplars>,
        }
        let gauge_meta = Metadata { typ: MetricType::Gauge, help: "help text".into(), ..Default::default() };
        let cases = vec![
            Tc {
                name: "1Fam2000Gauges",
                parsable_text: make_test_gauges(2000),
                expected_samples_len: 2000,
                test_appended_samples: Box::new(move |committed| {
                    // Only AppenderV2 supports metric family passing.
                    let expected_mf = mf(app_v2, "metric_a");
                    // Verify a few samples.
                    let s = |foo: &str, bar: &str| Sample {
                        mf: expected_mf.clone(),
                        m: gauge_meta.clone(),
                        l: from_strings(&["__name__", "metric_a", "foo", foo, "bar", bar]),
                        v: 1.0,
                        t: fts,
                        ..Default::default()
                    };
                    require_sample(&s("0", "0"), &committed[0]);
                    require_sample(&s("1245", "124500"), &committed[1245]);
                    require_sample(&s("1999", "199900"), &committed[committed.len() - 1]);
                }),
                test_exemplars: None,
            },
            Tc {
                name: "237FamsAllTypes",
                parsable_text: read_text_parse_test_metrics(),
                expected_samples_len: 1857,
                test_appended_samples: Box::new(move |committed| {
                    // Verify a few samples.
                    require_sample(
                        &Sample {
                            mf: mf(app_v2, "go_gc_gomemlimit_bytes"),
                            m: Metadata { typ: MetricType::Gauge, help: "Go runtime memory limit configured by the user, otherwise math.MaxInt64. This value is set by the GOMEMLIMIT environment variable, and the runtime/debug.SetMemoryLimit function. Sourced from /gc/gomemlimit:bytes".into(), ..Default::default() },
                            l: from_strings(&["__name__", "go_gc_gomemlimit_bytes"]),
                            v: 9.03676723e+08,
                            t: fts,
                            ..Default::default()
                        },
                        &committed[11],
                    );
                    require_sample(
                        &Sample {
                            // Only AppenderV2 supports metric family passing.
                            mf: mf(app_v2, "prometheus_http_request_duration_seconds"),
                            m: Metadata { typ: MetricType::Histogram, help: "Histogram of latencies for HTTP requests.".into(), ..Default::default() },
                            l: from_strings(&["__name__", "prometheus_http_request_duration_seconds_bucket", "handler", "/api/v1/query_range", "le", "120.0"]),
                            v: 118157.0,
                            t: fts,
                            ..Default::default()
                        },
                        &committed[448],
                    );
                    require_sample(
                        &Sample {
                            // Only AppenderV2 supports metric family passing.
                            mf: mf(app_v2, "promhttp_metric_handler_requests_total"),
                            m: Metadata { typ: MetricType::Counter, help: "Total number of scrapes by HTTP status code.".into(), ..Default::default() },
                            l: from_strings(&["__name__", "promhttp_metric_handler_requests_total", "code", "503"]),
                            v: 0.0,
                            t: fts,
                            ..Default::default()
                        },
                        &committed[committed.len() - 1],
                    );
                }),
                test_exemplars: None,
            },
            Tc {
                name: "100HistsWithExemplars",
                parsable_text: make_test_histograms_with_exemplars(100),
                expected_samples_len: series_per_histogram_for_100_hists_with_exemplars(app_v2) * 100,
                test_appended_samples: Box::new(move |committed| {
                    let st = if app_v2 { 1726839813016 } else { 0 };
                    // Verify a few samples.
                    let m = Metadata { typ: MetricType::Histogram, help: "RPC latency distributions.".into(), ..Default::default() };
                    require_sample(
                        &Sample {
                            // Only AppenderV2 supports metric family passing.
                            mf: mf(app_v2, "rpc_durations_histogram0_seconds"),
                            m: m.clone(),
                            l: from_strings(&["__name__", "rpc_durations_histogram0_seconds_bucket", "le", "0.0003100000000000002"]),
                            v: 15.0,
                            st,
                            t: fts,
                            es: vec![Exemplar {
                                labels: from_strings(&["dummyID", "9818"]),
                                value: 0.0002791130914009552,
                                ts: 1726839814982,
                                has_ts: true,
                            }],
                            ..Default::default()
                        },
                        &committed[13],
                    );
                    require_sample(
                        &Sample {
                            mf: mf(app_v2, "rpc_durations_histogram49_seconds"),
                            m: m.clone(),
                            l: from_strings(&["__name__", "rpc_durations_histogram49_seconds_sum"]),
                            v: -8.452185437166741e-05,
                            st,
                            t: fts,
                            ..Default::default()
                        },
                        &committed[series_per_histogram_for_100_hists_with_exemplars(app_v2) * 49 + 21],
                    );
                    if !app_v2 {
                        // This series does not have metadata, nor metric family, because of isSeriesPartOfFamily bug and OpenMetric 1.0 limitations around _created series.
                        // TODO(bwplotka): Fix with https://github.com/prometheus/prometheus/issues/17900
                        require_sample(
                            &Sample {
                                l: from_strings(&["__name__", "rpc_durations_histogram99_seconds_created"]),
                                v: 1.726839813016302e+09,
                                t: fts,
                                ..Default::default()
                            },
                            &committed[committed.len() - 1],
                        );
                    } else {
                        require_sample(
                            &Sample {
                                mf: "rpc_durations_histogram99_seconds".into(),
                                m: m.clone(),
                                l: from_strings(&["__name__", "rpc_durations_histogram99_seconds_count"]),
                                v: 15.0,
                                st,
                                t: fts,
                                ..Default::default()
                            },
                            &committed[committed.len() - 1],
                        );
                    }
                }),
                test_exemplars: Some(Box::new(|er| {
                    // 12 out of 23/24 histogram series have exemplars.
                    assert_eq!(er.len(), 12 * 100);
                    require_query_result(
                        &QueryResult {
                            series_labels: from_strings(&["__name__", "rpc_durations_histogram0_seconds_bucket", "le", "0.0003100000000000002"]),
                            exemplars: vec![Exemplar {
                                labels: from_strings(&["dummyID", "9818"]),
                                value: 0.0002791130914009552,
                                ts: 1726839814982,
                                has_ts: true,
                            }],
                        },
                        &er[10],
                    );
                    require_query_result(
                        &QueryResult {
                            series_labels: from_strings(&["__name__", "rpc_durations_histogram9_seconds_bucket", "le", "1.0000000000000216e-05"]),
                            exemplars: vec![Exemplar {
                                labels: from_strings(&["dummyID", "19206"]),
                                value: -4.6156147425468016e-05,
                                ts: 1726839815133,
                                has_ts: true,
                            }],
                        },
                        &er[er.len() - 1],
                    );
                })),
            },
        ];
        for tc in cases {
            let name = format!("appV2={app_v2}/data={}", tc.name);
            let s = new_storage_with(|o| o.enable_metadata_wal_records = true);

            let app_test = Appendable::new().then(s.clone());
            let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, app_v2));
            let mut app = sl.appender();

            let (_, err) = app.append(&tc.parsable_text, "application/openmetrics-text", ts);
            assert!(err.is_none(), "{name}: {err:?}");
            assert!(app.commit().is_none(), "{name}");

            // Check the recorded samples on the Appender layer.
            assert!(app_test.pending_samples().is_empty(), "{name}");
            assert!(app_test.rolledback_samples().is_empty(), "{name}");

            let got = app_test.result_samples();
            assert_eq!(got.len(), tc.expected_samples_len, "{name}");
            (tc.test_appended_samples)(&got);

            // Check basic storage stats.
            let stats = s.head().stats("__name__", 2000);
            assert_eq!(tc.expected_samples_len, stats.num_series as usize, "{name}");

            // Check exemplars.
            let eq = storage::ExemplarQueryable::exemplar_querier(&**s, &Context::background()).expect("exemplar querier");
            // Go passes one nil matcher set (a variadic nil).
            let er = eq.select(i64::MIN, i64::MAX, &[vec![]]).expect("exemplar select");

            match &tc.test_exemplars {
                Some(f) => f(&er),
                // Expect no exemplars.
                None => assert!(er.is_empty(), "{name}: {er:?} is not empty"),
            }
        }
    }
}

#[test]
fn test_scrape_loop_append_start_time_synthesis() {
    let ts = SystemTime::now();

    let require_sample = |s: &Sample, name: &str, val: f64, ts: i64, st: i64, is_nan: bool| {
        assert_eq!(name, s.l.get("__name__"));
        assert_eq!(ts, s.t);
        if is_nan {
            assert!(is_stale_nan(s.v));
        } else {
            assert_eq!(val, s.v);
        }
        assert_eq!(st, s.st);
    };

    let s = new_storage();

    let app_test = Appendable::new().then(s.clone());
    let (sl, _) = new_test_scrape_loop(|sl| {
        with_appendable(sl, &app_test, true);
        sl.synthesize_st = true;
        sl.parse_st = true;
    });

    // First Scrape: anchor the start time, append is skipped.
    let scrape_a = b"# TYPE test_metric counter\ntest_metric 10\n# TYPE test_gauge gauge\ntest_gauge 10\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_a, "application/openmetrics-text", ts);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Gauge should have 1 point, Counter should be skipped.
    let got = app_test.result_samples();
    assert_eq!(got.len(), 1);
    require_sample(&got[0], "test_gauge", 10.0, from_time(ts), 0, false);

    // Second Scrape: Counter should yield 1 point with delta = 5, and ST = ts.
    let ts2 = ts + Duration::from_secs(1);
    let scrape_b = b"# TYPE test_metric counter\ntest_metric 15\n# TYPE test_gauge gauge\ntest_gauge 12\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_b, "application/openmetrics-text", ts2);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 3);
    require_sample(&got[0], "test_gauge", 10.0, from_time(ts), 0, false);
    require_sample(&got[1], "test_metric", 5.0, from_time(ts2), from_time(ts), false);
    require_sample(&got[2], "test_gauge", 12.0, from_time(ts2), 0, false);

    // Third Scrape: Simulate a Reset (value drops from 15 to 4).
    let ts3 = ts2 + Duration::from_secs(1);
    let scrape_c = b"# TYPE test_metric counter\ntest_metric 4\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_c, "application/openmetrics-text", ts3);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 5);
    require_sample(&got[3], "test_metric", 4.0, from_time(ts3), from_time(ts3) - 1, false);
    require_sample(&got[4], "test_gauge", 0.0, from_time(ts3), 0, true);

    // Fourth Scrape: Counter provides an explicit ST via _created. Synthesis is skipped. The cache is cleared.
    let ts_d = ts3 + Duration::from_secs(1);
    let scrape_d = b"# TYPE test_metric counter\ntest_metric 20\ntest_metric_created 1726839813.016\n# TYPE test_gauge gauge\ntest_gauge 15\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_d, "application/openmetrics-text", ts_d);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 7);
    require_sample(&got[5], "test_metric", 20.0, from_time(ts_d), 1726839813016, false);
    require_sample(&got[6], "test_gauge", 15.0, from_time(ts_d), 0, false);

    // Fifth Scrape: No explicit ST provided. Since Scrape D reset the cache, this is a fresh start and is skipped.
    let ts_e = ts_d + Duration::from_secs(1);
    let scrape_e = b"# TYPE test_metric counter\ntest_metric 25\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_e, "application/openmetrics-text", ts_e);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 8);
    require_sample(&got[7], "test_gauge", 0.0, from_time(ts_e), 0, true);

    // Sixth Scrape: No explicit ST provided. Second sample after fresh start.
    let ts_f = ts_e + Duration::from_secs(1);
    let scrape_f = b"# TYPE test_metric counter\ntest_metric 32\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_f, "application/openmetrics-text", ts_f);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 9);
    require_sample(&got[8], "test_metric", 7.0, from_time(ts_f), from_time(ts_e), false);
}

#[test]
fn test_scrape_loop_append_start_time_synthesis_with_st_storage() {
    let ts = SystemTime::now();

    let s = new_storage_with(|o| {
        o.enable_st_storage = true;
        o.float_chunk_encoding = chunkenc::ENC_XOR2;
        o.enable_histogram_st_encoding = true;
    });

    let app_test = Appendable::new().then(s.clone());
    let (sl, _) = new_test_scrape_loop(|sl| {
        with_appendable(sl, &app_test, true);
        sl.synthesize_st = true;
        sl.parse_st = true;
    });

    // First Scrape: anchor the start time, append is skipped.
    let mut app = sl.appender();
    let (_, err) = app.append(b"# TYPE test_metric counter\ntest_metric 10\n# EOF\n", "application/openmetrics-text", ts);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Second Scrape: Counter should yield 1 point with delta = 5, and ST = ts.
    let ts2 = ts + Duration::from_secs(1);
    let mut app = sl.appender();
    let (_, err) = app.append(b"# TYPE test_metric counter\ntest_metric 15\n# EOF\n", "application/openmetrics-text", ts2);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Verify via querier that ST is actually stored.
    let mut q = tsdb::querier::new_block_querier(&**s.head(), i64::MIN, i64::MAX).expect("block querier");

    let mut ss = q.select(&Context::background(), false, None, &[m(MatchType::Equal, "__name__", "test_metric")]);
    assert!(ss.next());
    let series = ss.at().clone();
    assert!(ss.err().is_none());

    let mut series_it = series.iterator(None);
    assert_eq!(ValueType::Float, series_it.next());

    // Check sample value and timestamp
    let (t_val, v_val) = series_it.at();
    assert_eq!(from_time(ts2), t_val);
    assert_eq!(5.0, v_val);

    // Check Start Timestamp
    let st = series_it.at_st();
    assert_eq!(from_time(ts), st);
    assert_eq!(ValueType::None, series_it.next());
    drop(series_it);
    drop(ss);
    let _ = q.close();
}

#[test]
fn test_scrape_loop_append_start_time_synthesis_out_of_order() {
    let ts = SystemTime::now();

    let s = new_storage();
    // Simulate OOO error for "test_metric" on the second scrape.
    let return_ooo = Arc::new(AtomicBool::new(false));
    let r = return_ooo.clone();
    let app_test = Appendable::new()
        .with_errs(
            Some(Arc::new(move |ls: &Labels| {
                if r.load(Ordering::SeqCst) && ls.get("__name__") == "test_metric" {
                    return Some(ERR_OUT_OF_ORDER_SAMPLE);
                }
                None
            })),
            None,
            None,
        )
        .then(s.clone());

    let (sl, _) = new_test_scrape_loop(|sl| {
        with_appendable(sl, &app_test, true);
        sl.synthesize_st = true;
        sl.parse_st = true;
    });

    // First Scrape: anchor the start time, append is skipped.
    let mut app = sl.appender();
    let (_, err) = app.append(b"# TYPE test_metric counter\ntest_metric 10\n# EOF\n", "application/openmetrics-text", ts);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Counter should be skipped (anchored).
    let got = app_test.result_samples();
    assert!(got.is_empty(), "{got:?}");

    // Second Scrape: Counter should be rejected due to OOO.
    // stCache is now non-nil for this series.
    let ts2 = ts + Duration::from_secs(1);
    return_ooo.store(true, Ordering::SeqCst); // Now return OOO error
    let mut app = sl.appender();
    let (stats, err) = app.append(b"# TYPE test_metric counter\ntest_metric 15\n# EOF\n", "application/openmetrics-text", ts2);
    assert!(err.is_none(), "{err:?}"); // Scrape should not fail
    assert!(app.commit().is_none());

    // Counter should NOT be in the result samples because it was dropped.
    let got = app_test.result_samples();
    assert!(got.is_empty(), "{got:?}");

    assert_eq!(1, stats.total);
    assert_eq!(1, stats.added);
    assert_eq!(0, stats.series_added);

    // Third Scrape: Counter is missing.
    // Since it was never successfully appended (anchored on first scrape, failed with OOO on second),
    // it should NOT have been tracked for staleness. Thus, no StaleNaN should be appended.
    let ts3 = ts2 + Duration::from_secs(1);
    let mut app = sl.appender();
    let (_, err) = app.append(b"# TYPE test_metric counter\n# EOF\n", "application/openmetrics-text", ts3);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Verify that still no samples are appended (specifically no stale marker).
    let got = app_test.result_samples();
    assert!(
        got.is_empty(),
        "Expected no samples (specifically no stale markers) because the series was never tracked for staleness: {got:?}"
    );
}

#[test]
fn test_set_options_handling_staleness() {
    foreach_appendable(|v2| {
        let s = new_storage_with(|o| o.out_of_order_time_window = 600000);

        let (signal_tx, signal_rx) = sync_channel::<()>(1);
        let ctx = Ctx::background().child();

        // Function to run the scrape loop
        type Action = Box<dyn Fn(&Arc<ScrapeLoop>) + Send + Sync>;
        fn run_scrape_loop(s: Arc<TestStorage>, v2: bool, ctx: Ctx, cue: usize, action: Action) {
            let (sl, scraper) = new_test_scrape_loop(|sl| {
                with_storage(sl, &s, v2);
                sl.ctx = ctx.clone();
            });
            let sl = Arc::new(sl);
            let weak = Arc::downgrade(&sl);
            let num_scrapes = AtomicUsize::new(0);
            scraper.set_scrape_func(move |_, w| {
                let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
                if n == cue
                    && let Some(sl) = weak.upgrade()
                {
                    action(&sl);
                }
                w.extend_from_slice(format!("metric_a{{a=\"1\",b=\"1\"}} {}\n", 42 + n).as_bytes());
                Ok(())
            });
            sl.run(None);
        }

        let (s1, ctx1) = (s.clone(), ctx.clone());
        std::thread::spawn(move || {
            let (s2, ctx2) = (s1.clone(), ctx1.clone());
            run_scrape_loop(
                s1,
                v2,
                ctx1,
                2,
                Box::new(move |sl| {
                    let sl2 = sl.clone();
                    std::thread::spawn(move || sl2.stop());
                    // Wait a bit then start a new target.
                    std::thread::sleep(Duration::from_millis(100));
                    let (s3, ctx3, tx) = (s2.clone(), ctx2.clone(), signal_tx.clone());
                    std::thread::spawn(move || {
                        let c = ctx3.clone();
                        run_scrape_loop(s3, v2, ctx3, 4, Box::new(move |_| c.cancel()));
                        let _ = tx.send(());
                    });
                }),
            );
        });

        if signal_rx.recv_timeout(Duration::from_secs(10)).is_err() {
            panic!("Scrape wasn't stopped.");
        }

        let q = querier(&s, 0, now_unix_nano());
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "metric_a")]);
        let results = float_samples(&mut series);
        let c = results.iter().filter(|s| is_stale_nan(s.v)).count();
        assert_eq!(c, 0, "invalid count of staleness markers after stopping the engine");
    });
}

#[test]
fn test_scrape_loop_discard_duplicate_labels() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let app_test = Appendable::new().then(s.clone());
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));

        // We add a good and a bad metric to check that both are discarded.
        let mut app = sl.appender();
        let (_, err) = app.append(b"test_metric{le=\"500\"} 1\ntest_metric{le=\"600\",le=\"700\"} 1\n", "text/plain", go_zero_time());
        assert!(err.is_some());
        assert!(app.rollback().is_none());
        drop(app);
        // We need to cycle staleness cache maps after a manual rollback. Otherwise, they will have old entries in them,
        // which would cause ErrDuplicateSampleForTimestamp errors on the next append.
        {
            let mut c = sl.cache.series();
            sl.cache.iter_done(&mut c, true);
        }

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, 0);
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", ".*")]);
        assert!(!series.next(), "series found in tsdb");
        assert!(series.err().is_none());

        // We add a good metric to check that it is recorded.
        let mut app = sl.appender();
        let (_, err) = app.append(b"test_metric{le=\"500\"} 1\n", "text/plain", go_zero_time());
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, 0);
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Equal, "le", "500")]);
        assert!(series.next(), "series not found in tsdb");
        assert!(series.err().is_none());
        assert!(!series.next(), "more than one series found in tsdb");
    });
}

#[test]
fn test_scrape_loop_discard_unnamed_metrics() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let app_test = Appendable::new().then(s.clone());
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.sample_mutator = Arc::new(|l: Labels| {
                if l.has("drop") {
                    return from_strings(&["no", "name"]); // This label set will trigger an error.
                }
                l
            });
        });

        let mut app = sl.appender();
        let (_, err) = app.append(b"nok 1\nnok2{drop=\"drop\"} 1\n", "text/plain", go_zero_time());
        assert!(err.is_some());
        assert!(app.rollback().is_none());
        let err = err.unwrap();
        assert!(err.is(&ERR_NAME_LABEL_MANDATORY), "{err}");
        assert_eq!(ERR_NAME_LABEL_MANDATORY.to_string(), err.to_string());

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, 0);
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", ".*")]);
        assert!(!series.next(), "series found in tsdb");
        assert!(series.err().is_none());
    });
}

#[test]
fn test_scrape_add_fast() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let (sl, _) = new_test_scrape_loop(|sl| with_storage(sl, &s, v2));

        let mut app = sl.appender();
        let (_, err) = app.append(b"up 1\n", "text/plain", go_zero_time());
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        drop(app);

        // Poison the cache. There is just one entry, and one series in the
        // storage. Changing the ref will create a 'not found' error.
        sl.get_cache().series().for_each_series_entry_mut(|v| v.ref_ += 1);

        let mut app = sl.appender();
        let (_, err) = app.append(b"up 1\n", "text/plain", go_zero_time() + Duration::from_secs(1));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
    });
}

#[test]
fn test_scrape_report_single_appender() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let ctx = Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_storage(sl, &s, v2);
            sl.ctx = ctx.clone();
            // Since we're writing samples directly below we need to provide a protocol fallback.
            sl.fallback_scrape_protocol = "text/plain".into();
        });

        let num_scrapes = AtomicUsize::new(0);
        scraper.set_scrape_func(move |_, w| {
            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            if n % 4 == 0 {
                return Err(Error::msg("scrape failed"));
            }
            w.extend_from_slice(b"metric_a 44\nmetric_b 44\nmetric_c 44\nmetric_d 44\n");
            Ok(())
        });

        let sl = Arc::new(sl);
        let (signal_tx, signal_rx) = mpsc::channel();
        let sl2 = sl.clone();
        std::thread::spawn(move || {
            sl2.run(None);
            let _ = signal_tx.send(());
        });

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let mut q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());
            let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", ".+")]);

            let mut c = 0;
            while series.next() {
                let se = series.at().clone();
                let mut i = se.iterator(None);
                while i.next() != ValueType::None {
                    c += 1;
                }
            }

            assert_eq!(0, c % 9, "Appended samples not as expected: {c}");
            drop(series);
            assert!(q.close().is_none());
        }
        ctx.cancel();

        if signal_rx.recv_timeout(Duration::from_secs(5)).is_err() {
            panic!("Scrape wasn't stopped.");
        }
    });
}

#[test]
fn test_scrape_report_limit() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let cfg = ScrapeConfig { sample_limit: 5, ..utf8_cfg("test") };

        let (ts, scraped_twice) = new_scrapable_server("metric_a 44\nmetric_b 44\nmetric_c 44\nmetric_d 44\n");

        let sp = new_pool(cfg, select_storage(&s, v2), Options::default());

        sp.sync_targets_from_groups(&[target_group(&ts.host, &[])]);

        if scraped_twice.recv_timeout(Duration::from_secs(5)).is_err() {
            sp.stop();
            panic!("target was not scraped twice");
        }
        // If the target has been scraped twice, report samples from the first
        // scrape have been inserted in the database.

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "up")]);

        let mut found = false;
        while series.next() {
            let se = series.at().clone();
            let mut i = se.iterator(None);
            while i.next() == ValueType::Float {
                let (_, v) = i.at();
                assert_eq!(1.0, v);
                found = true;
            }
        }
        sp.stop();

        assert!(found);
    });
}

#[test]
fn test_scrape_utf8() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let cfg = utf8_cfg("test");
        let (ts, scraped_twice) = new_scrapable_server("{\"with.dots\"} 42\n");

        let sp = new_pool(cfg, select_storage(&s, v2), Options::default());

        sp.sync_targets_from_groups(&[target_group(&ts.host, &[])]);

        if scraped_twice.recv_timeout(Duration::from_secs(5)).is_err() {
            sp.stop();
            panic!("target was not scraped twice");
        }
        // If the target has been scraped twice, report samples from the first
        // scrape have been inserted in the database.

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());
        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "with.dots")]);
        let found = series.next();
        sp.stop();

        assert!(found, "series not found in tsdb");
    });
}

fn replace_config(source: &[&str], regex: &str, target: &str, replacement: &str, separator: &str) -> relabel::Config {
    relabel::Config {
        source_labels: Some(source.iter().map(|s| s.to_string()).collect()),
        regex: relabel::Regexp::must_new(regex),
        replacement: replacement.into(),
        separator: separator.into(),
        target_label: target.into(),
        action: relabel::Action::Replace,
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    }
}

/// Go `checkValues` of TestLeQuantileReLabel.
fn check_values(label_name: &str, expected_values: &[&str], series: &mut BoxSeriesSet) {
    let mut found_le_values: HashMap<String, bool> = HashMap::new();

    while series.next() {
        let v = series.at().labels().get(label_name).to_string();
        assert!(!found_le_values.contains_key(&v), "duplicate label value found");
        found_le_values.insert(v, true);
    }

    assert_eq!(found_le_values.len(), expected_values.len(), "number of label values not as expected");
    for v in expected_values {
        assert!(found_le_values.contains_key(*v), "label value not found");
    }
}

#[test]
fn test_le_quantile_re_label() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let d = relabel::default_relabel_config();
        let cfg = ScrapeConfig {
            metric_relabel_configs: vec![
                replace_config(&["le", "__name__"], "(\\d+)\\.0+;.*_bucket", "le", &d.replacement, &d.separator),
                replace_config(&["quantile"], "(\\d+)\\.0+", "quantile", &d.replacement, &d.separator),
            ],
            sample_limit: 100,
            ..utf8_cfg("test")
        };

        let metrics_text = r#"
# HELP test_histogram This is a histogram with default buckets
# TYPE test_histogram histogram
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.005"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.01"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.025"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.05"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.1"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.25"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="0.5"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="1.0"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="2.5"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="5.0"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="10.0"} 0
test_histogram_bucket{address="0.0.0.0",port="5001",le="+Inf"} 0
test_histogram_sum{address="0.0.0.0",port="5001"} 0
test_histogram_count{address="0.0.0.0",port="5001"} 0
# HELP test_summary Number of inflight requests sampled at a regular interval. Quantile buckets keep track of inflight requests over the last 60s.
# TYPE test_summary summary
test_summary{quantile="0.5"} 0
test_summary{quantile="0.9"} 0
test_summary{quantile="0.95"} 0
test_summary{quantile="0.99"} 0
test_summary{quantile="1.0"} 1
test_summary_sum 1
test_summary_count 199
"#;

        // The expected "le" values do not have the trailing ".0".
        let expected_le_values = ["0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"];

        // The expected "quantile" values do not have the trailing ".0".
        let expected_quantile_values = ["0.5", "0.9", "0.95", "0.99", "1"];

        let (ts, scraped_twice) = new_scrapable_server(metrics_text);

        let sp = new_pool(cfg, select_storage(&s, v2), Options::default());

        sp.sync_targets_from_groups(&[target_group(&ts.host, &[])]);
        assert_eq!(sp.active_targets().len(), 1);

        if scraped_twice.recv_timeout(Duration::from_secs(5)).is_err() {
            sp.stop();
            panic!("target was not scraped");
        }

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());

        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "test_histogram_bucket")]);
        check_values("le", &expected_le_values, &mut series);

        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "test_summary")]);
        check_values("quantile", &expected_quantile_values, &mut series);
        sp.stop();
    });
}

// Testing whether we can automatically convert scraped classic histograms into native histograms with custom buckets.
#[test]
fn test_convert_classic_histograms_to_nhcb() {
    foreach_appendable(test_convert_classic_histograms_to_nhcb_v);
}

fn test_convert_classic_histograms_to_nhcb_v(app_v2: bool) {
    let gen_test_counter_text = |name: &str| {
        format!(
            "\n# HELP {name} some help text\n# TYPE {name} counter\n{name}{{address=\"0.0.0.0\",port=\"5001\"}} 1\n"
        )
    };
    let gen_test_hist_text = |name: &str| {
        format!(
            r#"
# HELP {name} This is a histogram with default buckets
# TYPE {name} histogram
{name}_bucket{{address="0.0.0.0",port="5001",le="0.005"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.01"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.025"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.05"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.1"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.25"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="0.5"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="1"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="2.5"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="5"}} 0
{name}_bucket{{address="0.0.0.0",port="5001",le="10"}} 1
{name}_bucket{{address="0.0.0.0",port="5001",le="+Inf"}} 1
{name}_sum{{address="0.0.0.0",port="5001"}} 10
{name}_count{{address="0.0.0.0",port="5001"}} 1
"#
        )
    };
    let gen_test_counter_proto = |name: &str| {
        format!(
            r#"
name: "{name}"
help: "some help text"
type: COUNTER
metric: <
	label: <
		name: "address"
		value: "0.0.0.0"
	>
	label: <
		name: "port"
		value: "5001"
	>
	counter: <
		value: {}
	>
>
"#,
            1
        )
    };
    let gen_test_hist_proto = |name: &str, has_classic: bool, has_exponential: bool| {
        let mut classic = String::new();
        if has_classic {
            for (c, ub) in [
                (0, "0.005"),
                (0, "0.01"),
                (0, "0.025"),
                (0, "0.05"),
                (0, "0.1"),
                (0, "0.25"),
                (0, "0.5"),
                (0, "1"),
                (0, "2.5"),
                (0, "5"),
                (1, "10"),
            ] {
                classic.push_str(&format!("\nbucket: <\n\tcumulative_count: {c}\n\tupper_bound: {ub}\n>"));
            }
        }
        let mut expo = String::new();
        if has_exponential {
            expo = "\nschema: 3\nzero_threshold: 2.938735877055719e-39\nzero_count: 0\npositive_span: <\n\toffset: 2\n\tlength: 1\n>\npositive_delta: 1".to_string();
        }
        format!(
            r#"
name: "{name}"
help: "This is a histogram with default buckets"
type: HISTOGRAM
metric: <
	label: <
		name: "address"
		value: "0.0.0.0"
	>
	label: <
		name: "port"
		value: "5001"
	>
	histogram: <
		sample_count: 1
		sample_sum: 10
		{classic}
		{expo}
	>
>
"#
        )
    };
    let gen_test_hist_om2 = |name: &str, has_classic: bool, has_exponential: bool| {
        let mut fields = vec!["count:1", "sum:10"];
        if has_exponential {
            fields.extend(["schema:3", "zero_threshold:2.938735877055719e-39", "zero_count:0", "positive_spans:[2:1]", "positive_buckets:[1]"]);
        }
        if has_classic {
            fields.push("bucket:[0.005:0,0.01:0,0.025:0,0.05:0,0.1:0,0.25:0,0.5:0,1.0:0,2.5:0,5.0:0,10.0:1,+Inf:1]");
        }
        format!(
            "\n# HELP {name} This is a histogram with default buckets\n# TYPE {name} histogram\n{name}{{address=\"0.0.0.0\",port=\"5001\"}} {{{}}}\n",
            fields.join(",")
        )
    };

    struct MetricsText {
        text: Vec<String>,
        content_type: &'static str,
        has_classic: bool,
        has_exponential: bool,
    }
    // The families of one block: test_metric_i, _count, _sum, _bucket as counters, then the histogram (or the
    // histogram first).
    let counters = |i: usize, gen_fn: &dyn Fn(&str) -> String| -> Vec<String> {
        ["", "_count", "_sum", "_bucket"].iter().map(|suf| gen_fn(&format!("test_metric_{i}{suf}"))).collect()
    };
    let block = |hist_first: &[bool; 3], gen_c: &dyn Fn(&str) -> String, gen_h: &dyn Fn(usize) -> String| {
        let mut v = Vec::new();
        for i in 1..=3 {
            if hist_first[i - 1] {
                v.push(gen_h(i));
                v.extend(counters(i, gen_c));
            } else {
                v.extend(counters(i, gen_c));
                v.push(gen_h(i));
            }
        }
        v
    };
    let ht = |i: usize| gen_test_hist_text(&format!("test_histogram_{i}"));
    let hp = |c: bool, e: bool| move |i: usize| gen_test_hist_proto(&format!("test_histogram_{i}"), c, e);
    let ho = |c: bool, e: bool| move |i: usize| gen_test_hist_om2(&format!("test_histogram_{i}"), c, e);
    let metrics_texts: Vec<(&str, MetricsText)> = vec![
        (
            "text",
            MetricsText { text: block(&[false; 3], &gen_test_counter_text, &ht), content_type: "", has_classic: true, has_exponential: false },
        ),
        (
            "text, in different order",
            MetricsText {
                text: block(&[false, false, true], &gen_test_counter_text, &ht),
                content_type: "",
                has_classic: true,
                has_exponential: false,
            },
        ),
        (
            "protobuf",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_proto, &hp(true, false)),
                content_type: "application/vnd.google.protobuf",
                has_classic: true,
                has_exponential: false,
            },
        ),
        (
            "protobuf, in different order",
            MetricsText {
                text: block(&[true; 3], &gen_test_counter_proto, &hp(true, false)),
                content_type: "application/vnd.google.protobuf",
                has_classic: true,
                has_exponential: false,
            },
        ),
        (
            "protobuf, with additional native exponential histogram",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_proto, &hp(true, true)),
                content_type: "application/vnd.google.protobuf",
                has_classic: true,
                has_exponential: true,
            },
        ),
        (
            "protobuf, with only native exponential histogram",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_proto, &hp(false, true)),
                content_type: "application/vnd.google.protobuf",
                has_classic: false,
                has_exponential: true,
            },
        ),
        (
            "openmetrics2",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_text, &ho(true, false)),
                content_type: "application/openmetrics-text; version=2.0.0",
                has_classic: true,
                has_exponential: false,
            },
        ),
        (
            "openmetrics2, in different order",
            MetricsText {
                text: block(&[true; 3], &gen_test_counter_text, &ho(true, false)),
                content_type: "application/openmetrics-text; version=2.0.0",
                has_classic: true,
                has_exponential: false,
            },
        ),
        (
            "openmetrics2, with additional native exponential histogram",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_text, &ho(true, true)),
                content_type: "application/openmetrics-text; version=2.0.0",
                has_classic: true,
                has_exponential: true,
            },
        ),
        (
            "openmetrics2, with only native exponential histogram",
            MetricsText {
                text: block(&[false; 3], &gen_test_counter_text, &ho(false, true)),
                content_type: "application/openmetrics-text; version=2.0.0",
                has_classic: false,
                has_exponential: true,
            },
        ),
    ];

    let check_bucket_values = |expected_count: usize, series: &mut BoxSeriesSet| {
        let label_name = "le";
        let expected_values: Vec<&str> = if expected_count > 0 {
            vec!["0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1.0", "2.5", "5.0", "10.0", "+Inf"]
        } else {
            vec![]
        };
        let mut found_le_values: HashMap<String, bool> = HashMap::new();
        while series.next() {
            let v = series.at().labels().get(label_name).to_string();
            assert!(!found_le_values.contains_key(&v), "duplicate label value found");
            found_le_values.insert(v, true);
        }
        assert_eq!(
            found_le_values.len(),
            expected_values.len(),
            "unexpected number of label values, expected {expected_values:?} but found {found_le_values:?}"
        );
        for v in &expected_values {
            assert!(found_le_values.contains_key(*v), "label value not found");
        }
    };

    // Checks that the expected series is present and runs a basic sanity check of the float values.
    let check_float_series = |series: &mut BoxSeriesSet, expected_count: usize, expected_float: f64| {
        let mut count = 0;
        while series.next() {
            let se = series.at().clone();
            let mut i = se.iterator(None);
            loop {
                match i.next() {
                    ValueType::None => break,
                    ValueType::Float => {
                        let (_, f) = i.at();
                        assert_eq!(expected_float, f);
                    }
                    ValueType::Histogram => panic!("unexpected value type: histogram"),
                    ValueType::FloatHistogram => panic!("unexpected value type: float histogram"),
                }
            }
            count += 1;
        }
        assert_eq!(expected_count, count, "number of float series not as expected");
    };

    // Checks that the expected series is present and runs a basic sanity check of the histogram values.
    let check_hist_series = |series: &mut BoxSeriesSet, expected_count: usize, expected_schema: i32| {
        let mut count = 0;
        while series.next() {
            let se = series.at().clone();
            let mut i = se.iterator(None);
            loop {
                match i.next() {
                    ValueType::None => break,
                    ValueType::Float => panic!("unexpected value type: float"),
                    ValueType::Histogram => {
                        let (_, h): (i64, Histogram) = i.at_histogram(None);
                        assert_eq!(expected_schema, h.schema);
                        assert_eq!(1u64, h.count);
                        assert_eq!(10.0, h.sum);
                    }
                    ValueType::FloatHistogram => {
                        let (_, h) = i.at_float_histogram(None);
                        assert_eq!(expected_schema, h.schema);
                        // Go compares uint64(1) with the float64 count: require.Equal fails on the type difference.
                        panic!("require.Equal(uint64(1), {}): types differ", h.count);
                    }
                }
            }
            count += 1;
        }
        assert_eq!(expected_count, count, "number of histogram series not as expected");
    };

    struct Tc {
        name: &'static str,
        always_scrape_classic_histograms: bool,
        convert_classic_hist_to_nhcb: bool,
    }
    let tcs = [
        Tc { name: "convert with scrape", always_scrape_classic_histograms: true, convert_classic_hist_to_nhcb: true },
        Tc { name: "convert without scrape", always_scrape_classic_histograms: false, convert_classic_hist_to_nhcb: true },
        Tc { name: "scrape without convert", always_scrape_classic_histograms: true, convert_classic_hist_to_nhcb: false },
        Tc { name: "scrape with nil convert", always_scrape_classic_histograms: true, convert_classic_hist_to_nhcb: false },
        Tc { name: "neither scrape nor convert", always_scrape_classic_histograms: false, convert_classic_hist_to_nhcb: false },
    ];

    for (metrics_text_name, metrics_text) in &metrics_texts {
        for tc in &tcs {
            let (mut expected_classic_hist_count, mut expected_native_hist_count) = (0, 0);
            let mut expect_custom_buckets = false;
            if metrics_text.has_exponential {
                expected_native_hist_count = 1;
                expect_custom_buckets = false;
                expected_classic_hist_count = 0;
                if metrics_text.has_classic && tc.always_scrape_classic_histograms {
                    expected_classic_hist_count = 1;
                }
            } else if metrics_text.has_classic {
                if !tc.convert_classic_hist_to_nhcb {
                    expected_classic_hist_count = 1;
                    expected_native_hist_count = 0;
                } else if tc.always_scrape_classic_histograms && tc.convert_classic_hist_to_nhcb {
                    expected_classic_hist_count = 1;
                    expected_native_hist_count = 1;
                    expect_custom_buckets = true;
                } else if !tc.always_scrape_classic_histograms && tc.convert_classic_hist_to_nhcb {
                    expected_classic_hist_count = 0;
                    expected_native_hist_count = 1;
                    expect_custom_buckets = true;
                }
            }

            let run_name = format!("appV2={app_v2}/{} with {metrics_text_name}", tc.name);
            let s = new_storage();

            let (sl, _) = new_test_scrape_loop(|sl| {
                with_storage(sl, &s, app_v2);
                sl.always_scrape_classic_hist = tc.always_scrape_classic_histograms;
                sl.convert_classic_hist_to_nhcb = tc.convert_classic_hist_to_nhcb;
                sl.enable_native_histogram_scraping = true;
                sl.enable_open_metrics2 = true;
            });

            let mut content_type = metrics_text.content_type;
            let content: Vec<u8> = match content_type {
                "application/vnd.google.protobuf" => {
                    let mut buf = Vec::new();
                    for text in &metrics_text.text {
                        // In case of protobuf, we have to create the binary representation.
                        buf.extend_from_slice(&proto_marshal_delimited(text));
                    }
                    buf
                }
                "application/openmetrics-text; version=2.0.0" => {
                    let mut b = String::new();
                    for text in &metrics_text.text {
                        b.push_str(text.trim_start_matches('\n'));
                    }
                    b.push_str("# EOF\n");
                    b.into_bytes()
                }
                "text/plain" | "" => {
                    // The input text fragments already have a newline at the
                    // end, so we just concatenate them without separator.
                    content_type = "text/plain";
                    metrics_text.text.concat().into_bytes()
                }
                _ => panic!("unexpected content type"),
            };
            let now = SystemTime::now();
            let mut app = sl.appender();
            let (_, err) = app.append(&content, content_type, now);
            assert!(err.is_none(), "{run_name}: {err:?}");
            assert!(app.commit().is_none(), "{run_name}");
            drop(app);

            let expected_schema = if expect_custom_buckets { CUSTOM_BUCKETS_SCHEMA } else { 3 };

            // Validated what was appended can be queried.
            let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());
            let ctx = Context::background();
            let sel = |name: String| q.select(&ctx, false, None, &[m(MatchType::Regexp, "__name__", &name)]);

            for i in 1..=3 {
                check_float_series(&mut sel(format!("test_metric_{i}")), 1, 1.0);
                check_float_series(&mut sel(format!("test_metric_{i}_count")), 1, 1.0);
                check_float_series(&mut sel(format!("test_metric_{i}_sum")), 1, 1.0);
                check_float_series(&mut sel(format!("test_metric_{i}_bucket")), 1, 1.0);
                check_float_series(&mut sel(format!("test_histogram_{i}_count")), expected_classic_hist_count, 1.0);
                check_float_series(&mut sel(format!("test_histogram_{i}_sum")), expected_classic_hist_count, 10.0);
                check_bucket_values(expected_classic_hist_count, &mut sel(format!("test_histogram_{i}_bucket")));
                check_hist_series(&mut sel(format!("test_histogram_{i}")), expected_native_hist_count, expected_schema);
            }
        }
    }
}

#[test]
fn test_type_unit_re_label() {
    foreach_appendable(|v2| {
        let s = new_storage();

        let cfg = ScrapeConfig {
            metric_relabel_configs: vec![
                replace_config(&["__name__"], ".*_total$", "__type__", "counter", ""),
                replace_config(&["__name__"], ".*_bytes$", "__unit__", "bytes", ""),
            ],
            sample_limit: 100,
            ..utf8_cfg("test")
        };

        let metrics_text = r#"
# HELP test_metric_1_total This is a counter
# TYPE test_metric_1_total counter
test_metric_1_total 123

# HELP test_metric_2_total This is a counter
# TYPE test_metric_2_total counter
test_metric_2_total 234

# HELP disk_usage_bytes This is a gauge
# TYPE disk_usage_bytes gauge
disk_usage_bytes 456
"#;

        let (ts, scraped_twice) = new_scrapable_server(metrics_text);

        let sp = new_pool(cfg, select_storage(&s, v2), Options::default());

        sp.sync_targets_from_groups(&[target_group(&ts.host, &[])]);
        assert_eq!(sp.active_targets().len(), 1);

        if scraped_twice.recv_timeout(Duration::from_secs(5)).is_err() {
            sp.stop();
            panic!("target was not scraped");
        }

        let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());

        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", ".*_total$")]);
        while series.next() {
            assert_eq!("counter", series.at().labels().get("__type__"));
        }

        let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Regexp, "__name__", "disk_usage_bytes")]);
        while series.next() {
            assert_eq!("bytes", series.at().labels().get("__unit__"));
        }
        sp.stop();
    });
}

#[test]
fn test_native_histogram_max_schema_set() {
    foreach_appendable(|v2| {
        for (name, min_bucket_factor, expected_schema) in [
            ("min factor not specified", "", 3),                                 // Factor 1.09.
            ("min factor 1", "native_histogram_min_bucket_factor: 1", 3),        // Factor 1.09.
            ("min factor 2", "native_histogram_min_bucket_factor: 2", 0),        // Factor 2.00.
        ] {
            test_native_histogram_max_schema_set_one(name, min_bucket_factor, expected_schema, v2);
        }
    });
}

fn test_native_histogram_max_schema_set_one(name: &str, min_bucket_factor: &str, expected_schema: i32, app_v2: bool) {
    // Create a ProtoBuf message to serve as a Prometheus metric.
    // adapted: Go builds a client_golang native histogram (bucket factor 1.1, max 100 buckets), observes 1, 1, 1,
    // 10 and 10, and gathers it; this is the MetricFamily that the gather returns (checked against client_golang
    // v1.25.0), with the created timestamp of now.
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let histogram_metric_family = format!(
        r#"name: "testing_example_native_histogram"
help: "This is used for testing"
type: HISTOGRAM
metric: <
  histogram: <
    sample_count: 5
    sample_sum: 23
    created_timestamp: < seconds: {} nanos: {} >
    schema: 3
    zero_threshold: 2.938735877055719e-39
    zero_count: 0
    positive_span: < offset: 0 length: 1 >
    positive_span: < offset: 26 length: 1 >
    positive_delta: 3
    positive_delta: -1
  >
>
"#,
        now.as_secs(),
        now.subsec_nanos()
    );
    let buffer = proto_marshal_delimited(&histogram_metric_family);

    // Create an HTTP server to serve /metrics via ProtoBuf
    let metrics_server = new_server(
        Some("application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited"),
        buffer,
        || {},
    );

    // Create a scrape loop with the HTTP server as the target.
    let config_str = format!(
        "
global:
  metric_name_validation_scheme: legacy
  scrape_interval: 50ms
  scrape_timeout: 25ms
scrape_configs:
  - job_name: test
    scrape_native_histograms: true
    {min_bucket_factor}
    static_configs:
      - targets: [{}]
",
        metrics_server.host
    );

    let s = new_storage();
    let reg = discovery::Registry::new();

    let (a1, a2) = select_storage(&s, app_v2);
    let mng = Manager::new(
        Some(Options { discovery_reload_interval: Duration::from_millis(10), ..Default::default() }),
        None,
        None,
        a1,
        a2,
        Some(reg),
    )
    .expect("NewManager");

    let cfg = config::load(&config_str, &discovery::Logger::nop()).unwrap_or_else(|e| panic!("{name}: {e}"));
    mng.apply_config(&cfg).expect("apply config");
    let (tsets_tx, tsets_rx) = sync_channel::<TargetSets>(0);
    let m2 = mng.clone();
    let run = std::thread::spawn(move || m2.run(tsets_rx));
    struct StopOnDrop(Arc<Manager>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.stop();
        }
    }
    let _stop = StopOnDrop(mng.clone());

    // Get the static targets and apply them to the scrape manager.
    assert_eq!(cfg.scrape_configs.len(), 1);
    let scrape_cfg = &cfg.scrape_configs[0];
    assert_eq!(scrape_cfg.service_discovery_configs.len(), 1);
    let static_discovery = scrape_cfg.service_discovery_configs[0]
        .as_any()
        .downcast_ref::<discovery::StaticConfig>()
        .expect("static config");
    assert_eq!(static_discovery.0.len(), 1);
    let mut ts = TargetSets::new();
    ts.insert("test".into(), static_discovery.0.iter().cloned().map(Arc::new).collect());
    tsets_tx.send(ts).unwrap();

    let select = || {
        let q = querier(&s, 0, i64::MAX);
        q.select(&Context::background(), false, None, &[m(MatchType::Equal, "__name__", "testing_example_native_histogram")])
    };

    // Wait for the scrape loop to scrape the target.
    eventually(
        || {
            let mut series_s = select();
            let mut count_series = 0;
            while series_s.next() {
                count_series += 1;
            }
            count_series > 0
        },
        Duration::from_secs(5),
        Duration::from_millis(100),
        name,
    );

    // Check that native histogram schema is as expected.
    let mut series_s = select();
    let mut histogram_samples: Vec<Histogram> = Vec::new();
    while series_s.next() {
        let series = series_s.at().clone();
        let mut it = series.iterator(None);
        loop {
            let vt = it.next();
            if vt == ValueType::None {
                break;
            }
            if vt != ValueType::Histogram {
                // don't care about other samples
                continue;
            }
            let (_, h) = it.at_histogram(None);
            histogram_samples.push(h);
        }
    }
    assert!(series_s.err().is_none());
    assert!(!histogram_samples.is_empty(), "{name}");
    for h in &histogram_samples {
        assert_eq!(expected_schema, h.schema, "{name}");
    }
    drop(_stop);
    drop(tsets_tx);
    let _ = run.join();
}

#[test]
fn test_new_scrape_loop_honor_labels_wiring() {
    foreach_appendable(|v2| {
        // Scraped metric has label "lbl" with value "scraped".
        // Discovery target has label "lbl" with value "discovery".
        // With honor_labels=true, the scraped value should win.
        // With honor_labels=false, the discovery value should win and scraped moves to exported_lbl.
        struct Tc {
            name: &'static str,
            honor_labels: bool,
            expected_lbl: &'static str,
            expected_exp_lbl: &'static str, // exported_lbl value, empty if not expected
        }
        let cases = [
            Tc { name: "honor_labels=true", honor_labels: true, expected_lbl: "scraped", expected_exp_lbl: "" },
            Tc { name: "honor_labels=false", honor_labels: false, expected_lbl: "discovery", expected_exp_lbl: "scraped" },
        ];
        for tc in cases {
            let (ts, scraped_twice) = new_scrapable_server("metric{lbl=\"scraped\"} 1");

            let s = new_storage();

            let cfg = ScrapeConfig {
                job_name: "test".into(),
                scheme: "http".into(),
                honor_labels: tc.honor_labels,
                honor_timestamps: !tc.honor_labels, // Opposite of HonorLabels to catch wiring bugs
                scrape_interval: dur(Duration::from_secs(1)),
                scrape_timeout: dur(Duration::from_millis(100)),
                metric_name_validation_scheme: ValidationScheme::Utf8,
                ..Default::default()
            };

            let sp = new_pool(cfg, select_storage(&s, v2), Options { skip_jitter_offsetting: true, ..Default::default() });

            // Sync with a target that has a conflicting label.
            sp.sync_targets_from_groups(&[target_group(&ts.host, &[("lbl", "discovery")])]);
            assert_eq!(sp.active_targets().len(), 1, "{}", tc.name);

            // Wait for scrape to complete.
            if scraped_twice.recv_timeout(Duration::from_secs(5)).is_err() {
                sp.stop();
                panic!("{}: scrape did not complete in time", tc.name);
            }

            // Query the storage to verify label values.
            let q = querier(&s, GO_ZERO_TIME_UNIX_NANO, now_unix_nano());

            let mut series = q.select(&Context::background(), false, None, &[m(MatchType::Equal, "__name__", "metric")]);
            let found = series.next();
            sp.stop();
            assert!(found, "{}: metric series not found", tc.name);
            assert_eq!(tc.expected_lbl, series.at().labels().get("lbl"), "{}", tc.name);
            assert_eq!(tc.expected_exp_lbl, series.at().labels().get("exported_lbl"), "{}", tc.name);
        }
    });
}
