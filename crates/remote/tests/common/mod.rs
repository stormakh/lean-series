//! Helpers shared by the remote tests (the Go tests share them inside package `remote`): a test HTTP server
//! (Go `httptest.NewServer`), Go's `http.Error`, `mockedRemoteClient` (read_test.go) and `buildTestChunks`
//! (codec_test.go).
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::thread;

use chunkenc::{new_empty_chunk, ENC_XOR};
use model::labels::ScratchBuilder;
use remote::client::{combine_query_results, ReadClient};
use remote::codec::{from_label_matchers, from_query_result};
use remote::prompb::{self, Query, QueryResult, TimeSeries};
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
