//! Port of `storage/remote/client_test.go`.

mod common;

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chunkenc::{ChunkIterator as _, ValueType};
use common::*;
use remote::chunked::{ChunkedWriter, NopFlusher};
use remote::client::*;
use remote::codec::snappy_encode;
use remote::prompb::{self, Label, LabelMatcher, MatcherType, Message, Query, Sample, TimeSeries};
use storage::{Context, SeriesArc};

fn long_err_message() -> String {
    "error message".repeat(MAX_ERR_MSG_LEN)
}

fn write_client(url: &str, timeout: Duration) -> ClientConfig {
    ClientConfig { url: url.to_string(), timeout, ..Default::default() }
}

#[test]
fn test_store_http_error_handling() {
    let long = long_err_message();
    let cases: Vec<(u16, Option<(String, bool)>)> = vec![
        (200, None),
        (300, Some((format!("server returned HTTP status 300 Multiple Choices: {}", &long[..MAX_ERR_MSG_LEN]), false))),
        (404, Some((format!("server returned HTTP status 404 Not Found: {}", &long[..MAX_ERR_MSG_LEN]), false))),
        (500, Some((format!("server returned HTTP status 500 Internal Server Error: {}", &long[..MAX_ERR_MSG_LEN]), true))),
    ];
    for (code, want) in cases {
        let l = long.clone();
        let url = start_server(move |_| http_error(&l, code));
        let conf = write_client(&url, Duration::from_secs(1));
        // adapted: Go names the client with toHash(conf); the name does not matter here.
        let c = Client::new_write_client("hash", &conf).unwrap();
        let res = c.store(&Context::background(), &[], 0);
        match want {
            None => assert!(res.is_ok(), "{code}: {:?}", res.err()),
            Some((msg, recoverable)) => {
                let err = res.unwrap_err();
                assert_eq!(msg, err.to_string(), "{code}");
                match as_recoverable(&err) {
                    Some(r) => {
                        assert!(recoverable, "{code}: unexpected RecoverableError");
                        assert_eq!(DEFAULT_BACKOFF, r.retry_after);
                    }
                    None => assert!(!recoverable, "{code}: want RecoverableError"),
                }
            }
        }
    }
}

#[test]
fn test_client_retry_after() {
    let long = long_err_message();
    let cases: [(&str, u16, bool, bool, i64); 3] = [
        ("TooManyRequests - No Retry", 429, false, false, 0),
        ("TooManyRequests - With Retry", 429, true, true, 5_000_000_000),
        // HTTP 5xx errors do not depend on retryOnRateLimit.
        ("InternalServerError", 500, false, true, 5_000_000_000),
    ];
    for (name, status, retry_on_rate_limit, expected_recoverable, expected_retry_after) in cases {
        let l = long.clone();
        let url = start_server(move |_| http_error(&l, status).with_header("Retry-After", "5"));
        let conf = ClientConfig { retry_on_rate_limit, ..write_client(&url, Duration::from_secs(1)) };
        let c = Client::new_write_client("hash", &conf).unwrap();
        let err = c.store(&Context::background(), &[], 0).unwrap_err();
        let rec = as_recoverable(&err);
        assert_eq!(expected_recoverable, rec.is_some(), "{name}: Mismatch in expected recoverable error status.");
        if expected_recoverable {
            assert_eq!(expected_retry_after, rec.unwrap().retry_after, "{name}");
        }
    }
}

#[test]
fn test_retry_after_duration() {
    let cases = [
        ("seconds", "120", 120_000_000_000i64),
        // Expected layout is http.TimeFormat, hence an error.
        ("date-time default", "Mon, 02 Jan 2006 15:04:05 MST", DEFAULT_BACKOFF),
        // Expected layout is http.TimeFormat, hence an error.
        ("retry-after not provided", "", DEFAULT_BACKOFF),
    ];
    for (name, input, expected) in cases {
        assert_eq!(expected, retry_after_duration(input), "{name}");
    }
}

#[test]
fn test_client_custom_headers() {
    let headers_to_send: BTreeMap<String, String> =
        [("Foo", "Bar"), ("Baz", "qux")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let called = Arc::new(AtomicBool::new(false));
    let (c2, h2) = (called.clone(), headers_to_send.clone());
    let failures = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let f2 = failures.clone();
    let url = start_server(move |r| {
        c2.store(true, Ordering::SeqCst);
        for (name, value) in &h2 {
            let got: Vec<&str> = r.headers.iter().filter(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str()).collect();
            if got != vec![value.as_str()] {
                f2.lock().unwrap().push(format!("expected {headers:?} to be part of the received headers {got:?}", headers = h2));
            }
        }
        http::Response::new(200)
    });
    let conf = ClientConfig { headers: headers_to_send, ..write_client(&url, Duration::from_secs(1)) };
    let c = Client::new_write_client("c", &conf).unwrap();
    c.store(&Context::background(), &[], 0).unwrap();
    assert!(called.load(Ordering::SeqCst), "The remote server wasn't called");
    assert!(failures.lock().unwrap().is_empty(), "{:?}", failures.lock().unwrap());
}

fn sampled_response_http_handler(_: &http::Request) -> http::Response {
    let resp = prompb::ReadResponse {
        results: vec![prompb::QueryResult {
            timeseries: vec![
                TimeSeries {
                    labels: vec![Label::new("foo2", "bar")],
                    samples: vec![Sample { value: 1.0, timestamp: 0 }, Sample { value: 2.0, timestamp: 5 }],
                    exemplars: vec![],
                    ..Default::default()
                },
                TimeSeries {
                    labels: vec![Label::new("foo1", "bar")],
                    samples: vec![Sample { value: 3.0, timestamp: 0 }, Sample { value: 4.0, timestamp: 5 }],
                    exemplars: vec![],
                    ..Default::default()
                },
            ],
        }],
    };
    http::Response::text(200, "application/x-protobuf", snappy_encode(&resp.marshal()))
}

fn delayed_response_http_handler(delay: Duration) -> impl Fn(&http::Request) -> http::Response + Send + Sync {
    move |_| {
        std::thread::sleep(delay);
        http::Response::text(200, "application/x-protobuf", snappy_encode(&prompb::ReadResponse::default().marshal()))
    }
}

/// A body written with `ChunkedWriter` (Go writes it to the ResponseWriter and flushes each frame).
fn chunked_body(msgs: &[prompb::ChunkedReadResponse]) -> Vec<u8> {
    let mut cw = ChunkedWriter::new(Vec::new(), NopFlusher);
    for m in msgs {
        cw.write_all(&m.marshal()).unwrap();
    }
    cw.into_inner().0
}

const CHUNKED_CT: &str = "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse";

type Handler = Box<dyn Fn(&http::Request) -> http::Response + Send + Sync>;

#[test]
fn test_read_client() {
    struct Case {
        name: &'static str,
        query: Option<Query>,
        handler: Handler,
        timeout: Duration,
        expected_labels: Vec<Vec<(&'static str, &'static str)>>,
        expected_samples: Vec<Vec<(i64, f64)>>,
        expected_error_contains: &'static str,
        sort_series: bool,
        unwrap: bool,
    }
    let chunked_handler: Handler = Box::new(|_| {
        let l = vec![Label::new("foo", "bar")];
        let msgs: Vec<_> = build_test_chunks()
            .into_iter()
            .enumerate()
            .map(|(i, c)| prompb::ChunkedReadResponse {
                chunked_series: vec![prompb::ChunkedSeries { labels: l.clone(), chunks: vec![c] }],
                query_index: i as i64,
            })
            .collect();
        http::Response::text(200, CHUNKED_CT, chunked_body(&msgs))
    });
    let cases = vec![
        Case {
            name: "sorted sampled response",
            query: None,
            handler: Box::new(sampled_response_http_handler),
            timeout: Duration::ZERO,
            expected_labels: vec![vec![("foo1", "bar")], vec![("foo2", "bar")]],
            expected_samples: vec![vec![(0, 3.0), (5, 4.0)], vec![(0, 1.0), (5, 2.0)]],
            expected_error_contains: "",
            sort_series: true,
            unwrap: false,
        },
        Case {
            name: "unsorted sampled response",
            query: None,
            handler: Box::new(sampled_response_http_handler),
            timeout: Duration::ZERO,
            expected_labels: vec![vec![("foo2", "bar")], vec![("foo1", "bar")]],
            expected_samples: vec![vec![(0, 1.0), (5, 2.0)], vec![(0, 3.0), (5, 4.0)]],
            expected_error_contains: "",
            sort_series: false,
            unwrap: false,
        },
        Case {
            name: "chunked response",
            query: Some(Query { start_timestamp_ms: 4000, end_timestamp_ms: 12000, ..Default::default() }),
            handler: chunked_handler,
            timeout: Duration::ZERO,
            expected_labels: vec![vec![("foo", "bar")], vec![("foo", "bar")], vec![("foo", "bar")]],
            // This is the output of buildTestChunks minus the samples outside the query range.
            expected_samples: vec![
                vec![(4000, 4.0)],
                vec![(5000, 1.0), (6000, 2.0), (7000, 3.0), (8000, 4.0), (9000, 5.0)],
                vec![(10000, 2.0), (11000, 3.0), (12000, 4.0)],
            ],
            expected_error_contains: "",
            sort_series: false,
            unwrap: false,
        },
        Case {
            name: "unsupported content type",
            query: None,
            handler: Box::new(|_| http::Response::text(200, "foobar", Vec::new())),
            timeout: Duration::ZERO,
            expected_labels: vec![],
            expected_samples: vec![],
            expected_error_contains: "unsupported content type",
            sort_series: false,
            unwrap: false,
        },
        Case {
            name: "timeout",
            query: None,
            handler: Box::new(delayed_response_http_handler(Duration::from_millis(15))),
            timeout: Duration::from_millis(5),
            expected_labels: vec![],
            expected_samples: vec![],
            expected_error_contains: "context deadline exceeded: request timed out after 5ms",
            sort_series: false,
            unwrap: false,
        },
        Case {
            name: "unwrap error",
            query: None,
            handler: Box::new(|_| http_error("test error", 400)),
            timeout: Duration::ZERO,
            expected_labels: vec![],
            expected_samples: vec![],
            expected_error_contains: "test error",
            sort_series: false,
            unwrap: true,
        },
    ];

    for test in cases {
        let handler = test.handler;
        let url = start_server(move |r| handler(r));
        let timeout = if test.timeout.is_zero() { Duration::from_secs(5) } else { test.timeout };
        let conf = ClientConfig {
            url,
            timeout,
            chunked_read_limit: config::DEFAULT_CHUNKED_READ_LIMIT,
            ..Default::default()
        };
        let c = Client::new_read_client("test", &conf).unwrap();
        let query = test.query.unwrap_or_default();

        let res = c.read(&Context::background(), &query, test.sort_series);
        if !test.expected_error_contains.is_empty() {
            let err = match res {
                Err(e) => e,
                Ok(_) => panic!("{}: expected an error", test.name),
            };
            assert!(err.to_string().contains(test.expected_error_contains), "{}: {err}", test.name);
            if test.unwrap {
                let inner = match &err {
                    storage::Error::Wrapped(w) => w.1.to_string(),
                    e => panic!("{}: not a wrapped error: {e}", test.name),
                };
                assert_eq!(test.expected_error_contains, inner, "{}", test.name);
            }
            continue;
        }
        let mut ss = res.unwrap_or_else(|e| panic!("{}: {e}", test.name));
        let mut i = 0;
        while ss.next() {
            assert!(ss.err().is_none());
            let s: SeriesArc = ss.at().clone();
            let l = s.labels();
            assert_eq!(test.expected_labels[i].len(), l.len(), "{}", test.name);
            for (k, v) in &test.expected_labels[i] {
                assert!(l.has(k));
                assert_eq!(*v, l.get(k));
            }
            let mut it = s.iterator(None);
            let mut j = 0;
            loop {
                let vt = it.next();
                if vt == ValueType::None {
                    break;
                }
                assert!(it.err().is_none());
                let (ts, v) = it.at();
                let expected = test.expected_samples[i][j];
                assert_eq!(expected.0, ts, "{}", test.name);
                assert_eq!(expected.1, v, "{}", test.name);
                j += 1;
            }
            assert_eq!(test.expected_samples[i].len(), j, "{}", test.name);
            i += 1;
        }
        assert!(ss.err().is_none(), "{}: {:?}", test.name, ss.err().map(|e| e.to_string()));
    }
}

fn eq(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher { r#type: MatcherType::EQ, name: name.into(), value: value.into() }
}

fn re(name: &str, value: &str) -> LabelMatcher {
    LabelMatcher { r#type: MatcherType::RE, name: name.into(), value: value.into() }
}

fn q(start: i64, end: i64, matchers: Vec<LabelMatcher>) -> Query {
    Query { start_timestamp_ms: start, end_timestamp_ms: end, matchers, hints: None }
}

#[test]
fn test_read_multiple_error_handling() {
    let m = MockedRemoteClient::new(vec![TimeSeries { labels: vec![Label::new("job", "prometheus")], ..Default::default() }]);
    // Test with invalid matcher - should return error.
    let queries = vec![q(
        1000,
        2000,
        vec![LabelMatcher { r#type: MatcherType(999), name: "job".into(), value: "prometheus".into() }],
    )];
    let result = m.read_multiple(&Context::background(), &queries, true);
    assert!(result.is_err());
}

const SAMPLE_INTERVAL_MS: i64 = 250;

fn generate_series(labels: &[(&str, &str)], start_ms: i64, end_ms: i64) -> TimeSeries {
    let labels: Vec<Label> = labels.iter().map(|(n, v)| Label::new(*n, *v)).collect();
    // Helper function to calculate series multiplier based on labels.
    let mut label_hash: u64 = 0;
    for l in &labels {
        for b in l.name.chars().chain(l.value.chars()) {
            label_hash = label_hash.wrapping_mul(31).wrapping_add(b as u64);
        }
    }
    let mult = label_hash % SAMPLE_INTERVAL_MS as u64;
    let mut samples = Vec::new();
    let mut ts = start_ms;
    while ts <= end_ms {
        samples.push(Sample { timestamp: ts, value: (ts + mult as i64) as f64 });
        ts += SAMPLE_INTERVAL_MS;
    }
    TimeSeries { labels, samples, ..Default::default() }
}

fn collect(mut result: storage::BoxSeriesSet) -> Vec<TimeSeries> {
    let mut got = Vec::new();
    while result.next() {
        let series = result.at().clone();
        let mut samples = Vec::new();
        let mut it = series.iterator(None);
        while it.next() != ValueType::None {
            let (t, v) = it.at();
            samples.push(Sample { timestamp: t, value: v });
        }
        assert!(it.err().is_none());
        got.push(TimeSeries { labels: prompb::labels_to_label_protos(series.labels()), samples, ..Default::default() });
    }
    assert!(result.err().is_none());
    got
}

/// testify `ElementsMatch`.
fn elements_match(mut a: Vec<TimeSeries>, mut b: Vec<TimeSeries>, name: &str) {
    assert_eq!(a.len(), b.len(), "{name}: {a:?} vs {b:?}");
    let key = |t: &TimeSeries| format!("{t:?}");
    a.sort_by_key(key);
    b.sort_by_key(key);
    assert_eq!(a, b, "{name}");
}

#[test]
fn test_read_multiple() {
    let m = MockedRemoteClient::new(vec![
        generate_series(&[("job", "prometheus")], 0, 10000),
        generate_series(&[("job", "node_exporter")], 0, 10000),
        generate_series(&[("job", "cadvisor"), ("region", "us")], 0, 10000),
        generate_series(&[("instance", "localhost:9090")], 0, 10000),
    ]);
    let cases: Vec<(&str, Vec<Query>, Vec<TimeSeries>)> = vec![
        (
            "single query",
            vec![q(1000, 2000, vec![eq("job", "prometheus")])],
            vec![generate_series(&[("job", "prometheus")], 1000, 2000)],
        ),
        (
            "multiple queries - different matchers",
            vec![q(1000, 2000, vec![eq("job", "prometheus")]), q(1500, 2500, vec![eq("job", "node_exporter")])],
            vec![
                generate_series(&[("job", "node_exporter")], 1500, 2500),
                generate_series(&[("job", "prometheus")], 1000, 2000),
            ],
        ),
        (
            "multiple queries - overlapping results",
            vec![q(1000, 2000, vec![re("job", "prometheus|node_exporter")]), q(1500, 2500, vec![eq("region", "us")])],
            vec![
                generate_series(&[("job", "cadvisor"), ("region", "us")], 1500, 2500),
                generate_series(&[("job", "node_exporter")], 1000, 2000),
                generate_series(&[("job", "prometheus")], 1000, 2000),
            ],
        ),
        ("query with no results", vec![q(1000, 2000, vec![eq("job", "nonexistent")])], vec![]),
        ("empty query list", vec![], vec![]),
        (
            "three queries with mixed results",
            vec![
                q(1000, 2000, vec![eq("job", "prometheus")]),
                q(1500, 2500, vec![eq("job", "nonexistent")]),
                q(2000, 3000, vec![eq("instance", "localhost:9090")]),
            ],
            vec![
                generate_series(&[("instance", "localhost:9090")], 2000, 3000),
                generate_series(&[("job", "prometheus")], 1000, 2000),
            ],
        ),
        (
            "same matchers with overlapping time ranges",
            vec![q(1000, 5000, vec![eq("region", "us")]), q(3000, 8000, vec![eq("region", "us")])],
            vec![generate_series(&[("job", "cadvisor"), ("region", "us")], 1000, 8000)],
        ),
    ];
    for (name, queries, expected) in cases {
        m.reset();
        let result = m.read_multiple(&Context::background(), &queries, true).unwrap();
        // Verify the queries were stored correctly.
        assert_eq!(queries, *m.got_multiple.lock().unwrap(), "{name}");
        // Verify the combined result matches expected.
        elements_match(expected, collect(result), name);
    }
}

#[test]
fn test_read_multiple_sorting() {
    let m = MockedRemoteClient::new(vec![
        TimeSeries { labels: vec![Label::new("series", "ccc")], ..Default::default() },
        TimeSeries { labels: vec![Label::new("series", "aaa")], ..Default::default() },
        TimeSeries { labels: vec![Label::new("series", "bbb")], ..Default::default() },
    ]);
    let queries = vec![q(1000, 2000, vec![re("series", "ccc|bbb")]), q(1500, 2500, vec![re("series", "aaa|bbb")])];
    let cases = [
        ("multiple queries with sortSeries=true - should be sorted", true, vec!["aaa", "bbb", "ccc"]),
        ("multiple queries with sortSeries=false - concatenates without deduplication", false, vec!["ccc", "bbb", "aaa", "bbb"]),
    ];
    for (name, sort, expected_order) in cases {
        m.reset();
        let mut result = m.read_multiple(&Context::background(), &queries, sort).unwrap();
        let mut actual = Vec::new();
        while result.next() {
            actual.push(result.at().labels().get("series").to_string());
        }
        assert!(result.err().is_none());
        assert_eq!(expected_order, actual, "{name}");
    }
}

fn create_chunked_response_handler(queries: Vec<Query>) -> Handler {
    Box::new(move |_| {
        let mut msgs = Vec::new();
        // For each query, simulate multiple chunks.
        for query_index in 0..queries.len() {
            for (chunk_index, chunk) in build_test_chunks().into_iter().enumerate() {
                // Create unique labels for each series in each query.
                let labels = if query_index == 0 {
                    vec![Label::new("job", "prometheus"), Label::new("instance", format!("localhost:{}", 9090 + chunk_index))]
                } else {
                    vec![Label::new("job", "node_exporter"), Label::new("instance", format!("localhost:{}", 9100 + chunk_index))]
                };
                msgs.push(prompb::ChunkedReadResponse {
                    chunked_series: vec![prompb::ChunkedSeries { labels, chunks: vec![chunk] }],
                    query_index: query_index as i64,
                });
            }
        }
        http::Response::text(200, CHUNKED_CT, chunked_body(&msgs))
    })
}

fn create_sampled_response_handler(queries: Vec<Query>) -> Handler {
    Box::new(move |_| {
        let mut results = Vec::new();
        for (query_index, query) in queries.iter().enumerate() {
            let mut timeseries = Vec::new();
            // Create 2 series per query.
            for series_index in 0..2usize {
                let labels = if query_index == 0 {
                    vec![Label::new("job", "prometheus"), Label::new("instance", format!("localhost:{}", 9090 + series_index))]
                } else {
                    vec![Label::new("job", "node_exporter"), Label::new("instance", format!("localhost:{}", 9100 + series_index))]
                };
                // Create 2 samples per series within query time range.
                let samples = vec![
                    Sample { timestamp: query.start_timestamp_ms, value: (query_index * 10 + series_index) as f64 },
                    Sample { timestamp: query.end_timestamp_ms, value: (query_index * 10 + series_index + 1) as f64 },
                ];
                timeseries.push(TimeSeries { labels, samples, ..Default::default() });
            }
            results.push(prompb::QueryResult { timeseries });
        }
        let resp = prompb::ReadResponse { results };
        http::Response::text(200, "application/x-protobuf", snappy_encode(&resp.marshal()))
    })
}

fn create_overlapping_series_handler(queries: Vec<Query>) -> Handler {
    Box::new(move |_| {
        // Same series labels for both queries (will be merged).
        let common_labels = vec![Label::new("__name__", "up"), Label::new("job", "prometheus")];
        let msgs: Vec<_> = (0..queries.len())
            .map(|query_index| {
                // Use first chunk with 5 samples.
                let chunk = build_test_chunks().remove(0);
                prompb::ChunkedReadResponse {
                    chunked_series: vec![prompb::ChunkedSeries { labels: common_labels.clone(), chunks: vec![chunk] }],
                    query_index: query_index as i64,
                }
            })
            .collect();
        http::Response::text(200, CHUNKED_CT, chunked_body(&msgs))
    })
}

#[test]
fn test_read_multiple_with_chunks() {
    type Mk = fn(Vec<Query>) -> Handler;
    let cases: Vec<(&str, Vec<Query>, Mk, usize, Vec<usize>)> = vec![
        (
            "multiple queries with chunked responses",
            vec![q(1000, 5000, vec![eq("job", "prometheus")]), q(6000, 10000, vec![eq("job", "node_exporter")])],
            create_chunked_response_handler,
            6, // 3 chunks per query (2 queries * 3 series per query)
            vec![4, 5, 1, 4, 5, 1],
        ),
        (
            "sampled response multiple queries",
            vec![q(1000, 3000, vec![eq("job", "prometheus")]), q(4000, 6000, vec![eq("job", "node_exporter")])],
            create_sampled_response_handler,
            4, // 2 series per query * 2 queries
            vec![2, 2, 2, 2],
        ),
        (
            "single query with multiple chunks",
            vec![q(0, 15000, vec![eq("__name__", "cpu_usage")])],
            create_chunked_response_handler,
            3,
            vec![5, 5, 5],
        ),
        (
            "overlapping series from multiple queries",
            vec![q(1000, 5000, vec![eq("__name__", "up")]), q(3000, 7000, vec![eq("__name__", "up")])],
            create_overlapping_series_handler,
            2,           // Each query creates a separate series entry
            vec![4, 4], // Actual samples returned by handler
        ),
    ];
    for (name, queries, mk, expected_series_count, validate_sample_counts) in cases {
        let handler = mk(queries.clone());
        let url = start_server(move |r| handler(r));
        let cfg = ClientConfig {
            url,
            timeout: Duration::from_secs(5),
            chunked_read_limit: config::DEFAULT_CHUNKED_READ_LIMIT,
            ..Default::default()
        };
        let client = Client::new_read_client("test", &cfg).unwrap();
        let mut result = client.read_multiple(&Context::background(), &queries, false).unwrap();
        let mut n = 0;
        while result.next() {
            let series = result.at().clone();
            n += 1;
            // Verify we have some labels.
            assert!(series.labels().len() > 0, "{name}");
            let mut it = series.iterator(None);
            let mut sample_count = 0;
            while it.next() != ValueType::None {
                sample_count += 1;
            }
            assert!(it.err().is_none());
            assert_eq!(validate_sample_counts[n - 1], sample_count, "{name}: Series {n} sample count mismatch");
        }
        assert!(result.err().is_none(), "{name}");
        assert_eq!(expected_series_count, n, "{name}: Series count mismatch");
    }
}
