//! Helpers shared by the remote tests (the Go tests share them inside package `remote`): the package-level
//! fixtures of `codec_test.go`, the request builders of `queue_manager.go`, a test HTTP server (Go
//! `httptest.NewServer`), Go's `http.Error`, `mockedRemoteClient` (read_test.go) and `buildTestChunks`.
#![allow(dead_code)]

use model::histogram::{custom_values, Histogram, Span, CUSTOM_BUCKETS_SCHEMA};
use model::metadata::{Metadata, MetricType};
use remote::codec::snappy_encode;
use remote::prompb::{self, writev2, Message};

pub fn test_histogram() -> Histogram {
    Histogram {
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: 0,
        count: 3,
        sum: 20.0,
        positive_spans: vec![Span::new(0, 1)],
        positive_buckets: vec![1],
        negative_spans: vec![Span::new(0, 1)],
        negative_buckets: vec![2],
        ..Default::default()
    }
}

pub fn test_histogram_custom_buckets() -> Histogram {
    Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 16,
        sum: 20.0,
        positive_spans: vec![Span::new(1, 2)],
        positive_buckets: vec![10, -4], // 10 observations for upper bound 1.0 and 6 for upper bound +Inf.
        custom_values: custom_values(&[0.1, 1.0]), // +Inf is implied.
        ..Default::default()
    }
}

pub fn l(name: &str, value: &str) -> prompb::Label {
    prompb::Label::new(name, value)
}

fn fixture_labels() -> Vec<prompb::Label> {
    vec![l("__name__", "test_metric1"), l("b", "c"), l("baz", "qux"), l("d", "e"), l("foo", "bar")]
}

pub fn write_request_fixture() -> prompb::WriteRequest {
    let th = test_histogram();
    prompb::WriteRequest {
        timeseries: vec![
            prompb::TimeSeries {
                labels: fixture_labels(),
                samples: vec![prompb::Sample { value: 1.0, timestamp: 1 }],
                exemplars: vec![prompb::Exemplar { labels: vec![l("f", "g")], value: 1.0, timestamp: 1 }],
                histograms: vec![prompb::from_int_histogram(1, &th), prompb::from_float_histogram(2, &th.to_float())],
            },
            prompb::TimeSeries {
                labels: fixture_labels(),
                samples: vec![prompb::Sample { value: 2.0, timestamp: 2 }],
                exemplars: vec![prompb::Exemplar { labels: vec![l("h", "i")], value: 2.0, timestamp: 2 }],
                histograms: vec![prompb::from_int_histogram(3, &th), prompb::from_float_histogram(4, &th.to_float())],
            },
        ],
        metadata: Vec::new(),
    }
}

pub fn write_v2_request_series1_metadata() -> Metadata {
    Metadata {
        typ: MetricType::Gauge,
        help: "Test gauge for test purposes".to_string(),
        unit: "Maybe op/sec who knows (:".to_string(),
    }
}

pub fn write_v2_request_series2_metadata() -> Metadata {
    Metadata { typ: MetricType::Counter, help: "Test counter for test purposes".to_string(), unit: String::new() }
}

pub fn write_v2_request_fixture() -> writev2::Request {
    let th = test_histogram();
    let thc = test_histogram_custom_buckets();
    let syms = [
        "", "__name__", "test_metric1", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i",
        "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes",
    ];
    writev2::Request {
        symbols: syms.iter().map(|s| s.to_string()).collect(),
        timeseries: vec![
            writev2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::GAUGE, help_ref: 15, unit_ref: 16 },
                // ST needs to be lower than the sample's timestamp.
                samples: vec![writev2::Sample { value: 1.0, timestamp: 10, start_timestamp: 1 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![11, 12], value: 1.0, timestamp: 10 }],
                histograms: vec![
                    writev2::from_int_histogram(1, 10, &th),
                    writev2::from_float_histogram(2, 20, &th.to_float()),
                    writev2::from_int_histogram(11, 30, &thc),
                    writev2::from_float_histogram(21, 40, &thc.to_float()),
                ],
            },
            writev2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::COUNTER, help_ref: 17, unit_ref: 0 },
                samples: vec![writev2::Sample { value: 2.0, timestamp: 20, start_timestamp: 0 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![13, 14], value: 2.0, timestamp: 20 }],
                histograms: vec![
                    writev2::from_int_histogram(31, 50, &th),
                    writev2::from_float_histogram(41, 60, &th.to_float()),
                    writev2::from_int_histogram(51, 70, &thc),
                    writev2::from_float_histogram(61, 80, &thc.to_float()),
                ],
            },
        ],
    }
}

/// Go `buildWriteRequest(nil, series, metadata, nil, nil, nil, "snappy")` (queue_manager.go): the marshaled,
/// snappy-compressed request.
pub fn build_write_request(timeseries: Vec<prompb::TimeSeries>, metadata: Vec<prompb::MetricMetadata>) -> Vec<u8> {
    let req = prompb::WriteRequest { timeseries, metadata };
    snappy_encode(&req.marshal())
}

/// Go `buildV2WriteRequest(logger, series, symbols, nil, nil, nil, "snappy")`: optimized marshal, then snappy.
pub fn build_v2_write_request(timeseries: Vec<writev2::TimeSeries>, symbols: Vec<String>) -> Vec<u8> {
    let req = writev2::Request { symbols, timeseries };
    let mut buf = Vec::new();
    req.optimized_marshal(&mut buf);
    snappy_encode(&buf)
}

// ---------------------------------------------------------------- HTTP server, read client mock, chunks

use std::sync::{Arc, Mutex};
use std::thread;

use chunkenc::{new_empty_chunk, ENC_XOR};
use model::labels::ScratchBuilder;
use remote::client::{combine_query_results, ReadClient};
use remote::codec::{from_label_matchers, from_query_result};
use remote::prompb::{Query, QueryResult, TimeSeries};
use storage::{BoxSeriesSet, Context, Error};

struct FnService<F: Fn(&http::Request) -> http::Response + Send + Sync>(F);

impl<F: Fn(&http::Request) -> http::Response + Send + Sync> http::server::Service for FnService<F> {
    fn handle(&self, req: http::Request) -> http::Response {
        (self.0)(&req)
    }
}

/// An HTTP server on 127.0.0.1:0 (Go `httptest.NewServer`); returns "http://127.0.0.1:port".
/// adapted: the server thread is not closed at the end of the test (the crates/http server runs forever).
pub fn start_server(f: impl Fn(&http::Request) -> http::Response + Send + Sync + 'static) -> String {
    let srv = http::server::Server::bind("127.0.0.1:0", http::server::ServerConfig::default()).expect("bind");
    let port = srv.local_port();
    thread::spawn(move || srv.run(Arc::new(FnService(f))));
    format!("http://127.0.0.1:{port}")
}

/// Go `http.Error`.
pub fn http_error(msg: &str, code: u16) -> http::Response {
    http::Response::text(code, "text/plain; charset=utf-8", format!("{msg}\n")).with_header("X-Content-Type-Options", "nosniff")
}

pub const NUM_TEST_CHUNKS: usize = 3;
pub const NUM_SAMPLES_PER_TEST_CHUNK: usize = 5;

/// Go `buildTestChunks`.
pub fn build_test_chunks() -> Vec<prompb::Chunk> {
    let mut chks = Vec::with_capacity(NUM_TEST_CHUNKS);
    let mut time = 0i64;
    for i in 0..NUM_TEST_CHUNKS {
        let mut c = new_empty_chunk(ENC_XOR).unwrap();
        let mut a = c.appender().unwrap();
        let min_time_ms = time;
        for j in 0..NUM_SAMPLES_PER_TEST_CHUNK {
            a.append(c.as_mut(), 0, time, (i + j) as f64);
            time += 1000;
        }
        chks.push(prompb::Chunk {
            min_time_ms,
            max_time_ms: time,
            r#type: prompb::ChunkEncoding::XOR,
            data: c.bytes().to_vec(),
        });
    }
    chks
}

/// Go `mockedRemoteClient`.
#[derive(Default)]
pub struct MockedRemoteClient {
    pub got: Mutex<Option<Query>>,
    pub got_multiple: Mutex<Vec<Query>>,
    pub store: Vec<TimeSeries>,
}

impl MockedRemoteClient {
    pub fn new(store: Vec<TimeSeries>) -> MockedRemoteClient {
        MockedRemoteClient { store, ..Default::default() }
    }

    /// Go `reset`.
    pub fn reset(&self) {
        *self.got.lock().unwrap() = None;
        self.got_multiple.lock().unwrap().clear();
    }

    fn query_result(&self, query: &Query, skip_empty: bool) -> Result<QueryResult, Error> {
        let matchers = from_label_matchers(&query.matchers)?;
        let mut b = ScratchBuilder::new(0);
        let mut q = QueryResult::default();
        for s in &self.store {
            let l = s.to_labels(&mut b);
            let mut not_match = false;
            for m in &matchers {
                let v = l.get(&m.name);
                // Read checks only labels that are present; ReadMultiple checks every matcher.
                if skip_empty && v.is_empty() {
                    continue;
                }
                if !m.matches(v) {
                    not_match = true;
                    break;
                }
            }
            if not_match {
                continue;
            }
            // Filter samples by query time range.
            let filtered = s
                .samples
                .iter()
                .filter(|x| x.timestamp >= query.start_timestamp_ms && x.timestamp <= query.end_timestamp_ms)
                .copied()
                .collect();
            q.timeseries.push(TimeSeries { labels: s.labels.clone(), samples: filtered, ..Default::default() });
        }
        Ok(q)
    }
}

impl ReadClient for MockedRemoteClient {
    fn read(&self, _ctx: &Context, query: &Query, sort_series: bool) -> Result<BoxSeriesSet, Error> {
        {
            let mut got = self.got.lock().unwrap();
            if got.is_some() {
                return Err(Error::Msg(format!("expected only one call to remote client got: {query:?}").into()));
            }
            *got = Some(query.clone());
        }
        let q = self.query_result(query, true)?;
        Ok(from_query_result(sort_series, q))
    }

    fn read_multiple(&self, _ctx: &Context, queries: &[Query], sort_series: bool) -> Result<BoxSeriesSet, Error> {
        // Store the queries for verification.
        *self.got_multiple.lock().unwrap() = queries.to_vec();
        // Simulate the same behavior as the real client.
        let mut results = Vec::new();
        for query in queries {
            results.push(self.query_result(query, false)?);
        }
        // Use the same logic as the real client.
        combine_query_results(results, sort_series)
    }
}
