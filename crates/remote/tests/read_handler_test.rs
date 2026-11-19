//! Port of `storage/remote/read_handler_test.go`.
//!
//! The storage is the real TSDB (`common::loaded_test_db`, Go `promqltest.LoadedStorage`).
//! Rust notes: the handler is called directly (no `httptest` server, no HTTP/2, TLS or otelhttp wrapper), and
//! streamed responses are one byte body, so a stream that fails after its first frame aborts the whole
//! response ([`AbortHandler`]) instead of truncating it.

mod common;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chunkenc::{ChunkIterator, ValueType};
use discovery::log::{Level, Logger};
use discovery::metrics::{MetricValue, Registry};
use model::histogram::CUSTOM_BUCKETS_SCHEMA;
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use common::{loaded_test_db, TestDb};
use tsdb::db::DB;
use remote::chunked::{ChunkedReader, ERR_EOF};
use remote::codec::{new_chunked_series_set, snappy_decode, snappy_encode, to_query, HttpError};
use remote::prompb::{self, BucketSpan, ChunkEncoding, Count, LabelMatcher, MatcherType, Message, ResponseType};
use remote::read_handler::{new_read_handler, AbortHandler, ExternalLabelsFn, ReadHandler};
use storage::{
    empty_series_set, AppendV2Options, Annotations, BoxChunkQuerier,
    BoxChunkSeriesSet, BoxQuerier, ChunkQuerier, ChunkQueryable, ChunkSeriesArc, ChunkSeriesSet, Context, Error,
    LabelHints, LabelQuerier, LabelResult, MockQuerier, Queryable, SampleAndChunkQueryable, SelectHints,
};

fn ext(ls: Labels) -> ExternalLabelsFn {
    Arc::new(move || ls.clone())
}

fn request(body: Vec<u8>) -> http::Request {
    http::Request { method: "POST".into(), path: "/api/v1/read".into(), body, ..Default::default() }
}

fn eq(n: &str, v: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, n, v)
}

fn add_native_histograms_to_test_suite(s: &TestDb, n: usize) {
    let lbls = labels::from_strings(&["__name__", "test_histogram_metric1", "baz", "qux"]);
    let mut app = storage::AppendableV2::appender_v2(&*s.db, &Context::background());
    for (i, fh) in storage::tsdbutil::generate_test_float_histograms(n).iter().enumerate() {
        app.append(0, &lbls, 0, i as i64 * 60_000, 0.0, None, Some(fh), &AppendV2Options::default()).unwrap();
    }
    let lbls = labels::from_strings(&["__name__", "test_nhcb_metric1", "baz", "qux"]);
    for (i, fh) in storage::tsdbutil::generate_test_custom_buckets_float_histograms(n).iter().enumerate() {
        app.append(0, &lbls, 0, i as i64 * 60_000, 0.0, None, Some(fh), &AppendV2Options::default()).unwrap();
    }
    assert!(app.commit().is_none());
}

fn execute_sampled_read(
    queryable: Arc<dyn SampleAndChunkQueryable>,
    external_labels: ExternalLabelsFn,
    req: &prompb::ReadRequest,
) -> (u16, prompb::ReadResponse) {
    let h = new_read_handler(Logger::nop(), None, queryable, external_labels, 1_000_000, 1, 0);
    let compressed = snappy_encode(&req.marshal());
    let resp = h.serve(&Context::background(), &request(compressed)).expect("not aborted");
    assert_eq!(2, resp.status / 100);
    assert_eq!(Some("application/x-protobuf"), resp.header("Content-Type"));
    assert_eq!(Some("snappy"), resp.header("Content-Encoding"));
    let uncompressed = snappy_decode(resp.body_bytes()).unwrap();
    (resp.status, prompb::ReadResponse::unmarshal(&uncompressed).unwrap())
}

fn pl(n: &str, v: &str) -> prompb::Label {
    prompb::Label::new(n, v)
}

#[test]
fn test_sampled_read_endpoint() {
    // with external labels
    {
        let store = loaded_test_db("load 1m\n\ttest_metric1{foo=\"bar\",baz=\"qux\"} 1\n");
        add_native_histograms_to_test_suite(&store, 1);

        let hints = SelectHints { step: 0, func: "avg".into(), ..Default::default() };
        let query1 = to_query(0, 1, &[eq("__name__", "test_metric1"), eq("d", "e")], Some(&hints)).unwrap();
        let query2 = to_query(0, 1, &[eq("__name__", "test_histogram_metric1"), eq("d", "e")], Some(&hints)).unwrap();
        let query3 = to_query(0, 1, &[eq("__name__", "test_nhcb_metric1"), eq("d", "e")], Some(&hints)).unwrap();

        let (_, resp) = execute_sampled_read(
            store.db.clone(),
            // We expect external labels to be added, with the source labels honored.
            ext(labels::from_strings(&["b", "c", "baz", "a", "d", "e"])),
            &prompb::ReadRequest { queries: vec![query1, query2, query3], ..Default::default() },
        );

        assert_eq!(3, resp.results.len(), "Expected 3 results.");

        assert_eq!(
            prompb::QueryResult {
                timeseries: vec![prompb::TimeSeries {
                    labels: vec![
                        pl("__name__", "test_metric1"),
                        pl("b", "c"),
                        pl("baz", "qux"),
                        pl("d", "e"),
                        pl("foo", "bar")
                    ],
                    samples: vec![prompb::Sample { value: 1.0, timestamp: 0 }],
                    ..Default::default()
                }],
            },
            resp.results[0]
        );

        assert_eq!(
            prompb::QueryResult {
                timeseries: vec![prompb::TimeSeries {
                    labels: vec![pl("__name__", "test_histogram_metric1"), pl("b", "c"), pl("baz", "qux"), pl("d", "e")],
                    histograms: vec![prompb::from_float_histogram(0, &storage::tsdbutil::generate_test_float_histogram(0))],
                    ..Default::default()
                }],
            },
            resp.results[1]
        );

        assert_eq!(
            prompb::QueryResult {
                timeseries: vec![prompb::TimeSeries {
                    labels: vec![pl("__name__", "test_nhcb_metric1"), pl("b", "c"), pl("baz", "qux"), pl("d", "e")],
                    histograms: vec![prompb::Histogram {
                        // Not prompb::from_float_histogram, as that is one of the things tested here.
                        schema: CUSTOM_BUCKETS_SCHEMA,
                        count: Some(Count::Float(5.0)),
                        sum: 18.4,
                        zero_count: Some(Count::Float(0.0)),
                        positive_spans: vec![BucketSpan { offset: 0, length: 2 }, BucketSpan { offset: 1, length: 2 }],
                        positive_counts: vec![1.0, 2.0, 1.0, 1.0],
                        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            },
            resp.results[2]
        );
    }

    // without external labels
    {
        let store = loaded_test_db("load 1m\n\ttest_metric{foo=\"bar\"} 1\n");
        let query = to_query(0, 1, &[eq("__name__", "test_metric")], None).unwrap();
        let (status, resp) = execute_sampled_read(
            store.db.clone(),
            ext(Labels::empty()),
            &prompb::ReadRequest { queries: vec![query], ..Default::default() },
        );
        assert_eq!(200, status);
        assert_eq!(
            prompb::ReadResponse {
                results: vec![prompb::QueryResult {
                    timeseries: vec![prompb::TimeSeries {
                        labels: vec![pl("__name__", "test_metric"), pl("foo", "bar")],
                        samples: vec![prompb::Sample { value: 1.0, timestamp: 0 }],
                        ..Default::default()
                    }],
                }],
            },
            resp
        );
    }
}

// The chunk bytes of the Go test (converted from its string literals, in order).
const CHUNK_0: &[u8] = b"\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe0\xd4\x03\xc2\x7c\x05\x94\x00\xc1\xac\x7d\xe9\x7a\x32\xd0\x4f\xed\xb4\x6e\x5b\x07\xce\x94\xa3\x6d\x64\xf9\xd0\xfd\x1a\x50\x6d\x0a\x53\x9d\x0e\x51\xad\x06\x50\xbd\xa8\xbf\xca\x81\x5a\x03\x28\x33\xa0\x52\x87\xda\x05\x28\x0f\xa0\xd2\x81\xda\x3d\x28\x13\xa0\x32\x83\x5a\x07\x28\x77\xa0\x32\x81\x5a\x0f\x28\x13\xb5\x97\xf4\x50\x1b\x40\xa5\x07\xf4\x0a\x50\x1b\x43\xa5\x02\x74\x1e\x50\x2b\x40\x65\x1e\xf4\x0e\x50\x6b\x40\x65\x02\x74\x3a\x50\x3b\x41\xa5\x01\xf4\x0a\x53\xfb\x40\xa5\x06\x74\x06\x50\x2b\x43\xe5\x02\xf4\x06\x50\x6b\x40\xe5\x1e\x74\x0a\x50\x1b\x41\xa5\x03\xf4\x3a\x50\x1b\x40\xa5\x06\x74\x0e\x5a\x4a\xff\x5c\x85\xcb\x88\xd7\xb8\x0f\xe5\x2b\x46\x5b\xc8\xe7\x45\x29\xed\x14\xa1\xf6\xe2\x7d\x28\x76\x8d\x28\x4e\x83\x29\xd6\x87\x28\xde\x83\x28\x56\xda\x57\xf2\x82\x74\x34\xa0\x6d\x05\x28\xff\x41\x4a\x06\xd0\xd2\x82\x74\xfc\xa0\xad\x03\x28\x6f\x41\xca\x3a\xd0\x32";
const CHUNK_1: &[u8] = b"\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe0\xd4\x03\xc2\x7c\x05\x94\x00\xc1\xac\x7d\xe9\x7a\x32\xd0\x4f\xed\xb4\x6e\x5b\x07\xce\x94\xa3\x6d\x64\xf9\xd0\xfd\x1a\x50\x6d\x0a\x53\x9d\x0e\x51\xad\x06\x50\xbd\xa8\xbf\xca\x81\x5a\x03\x28\x33\xa0\x52\x87\xda\x05\x28\x0f\xa0\xd2\x81\xda\x3d\x28\x13\xa0\x32\x83\x5a\x07\x28\x77\xa0\x32\x81\x5a\x0f\x28\x13\xb5\x97\xf4\x50\x1b\x40\xa5\x07\xf4\x0a\x50\x1b\x43\xa5\x02\x74\x1e\x50\x2b\x40\x65\x1e\xf4\x0e\x50\x6b\x40\x65\x02\x74\x3a\x50\x3b\x41\xa5\x01\xf4\x0a\x53\xfb\x40\xa5\x06\x74\x06\x50\x2b\x43\xe5\x02\xf4\x06\x50\x6b\x40\xe5\x1e\x74\x0a\x50\x1b\x41\xa5\x03\xf4\x3a\x50\x1b\x40\xa5\x06\x74\x0e\x5a\x4a\xff\x5c\x85\xcb\x88\xd7\xb8\x0f\xe5\x2b\x46\x5b\xc8\xe7\x45\x29\xed\x14\xa1\xf6\xe2\x7d\x28\x76\x8d\x28\x4e\x83\x29\xd6\x87\x28\xde\x83\x28\x56\xda\x57\xf2\x82\x74\x34\xa0\x6d\x05\x28\xff\x41\x4a\x06\xd0\xd2\x82\x74\xfc\xa0\xad\x03\x28\x6f\x41\xca\x3a\xd0\x32";
const CHUNK_2: &[u8] = b"\x00\x01\x80\xf4\xee\x06\x40\xc7\x70\x00\x00\x00\x00\x00";
const CHUNK_3: &[u8] = b"\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe0\xd4\x03\xc2\x7c\x05\x94\x00\xc1\xac\x7d\xe9\x7a\x32\xd0\x4f\xed\xb4\x6e\x5b\x07\xce\x94\xa3\x6d\x64\xf9\xd0\xfd\x1a\x50\x6d\x0a\x53\x9d\x0e\x51\xad\x06\x50\xbd\xa8\xbf\xca\x81\x5a\x03\x28\x33\xa0\x52\x87\xda\x05\x28\x0f\xa0\xd2\x81\xda\x3d\x28\x13\xa0\x32\x83\x5a\x07\x28\x77\xa0\x32\x81\x5a\x0f\x28\x13\xb5\x97\xf4\x50\x1b\x40\xa5\x07\xf4\x0a\x50\x1b\x43\xa5\x02\x74\x1e\x50\x2b\x40\x65\x1e\xf4\x0e\x50\x6b\x40\x65\x02\x74\x3a\x50\x3b\x41\xa5\x01\xf4\x0a\x53\xfb\x40\xa5\x06\x74\x06\x50\x2b\x43\xe5\x02\xf4\x06\x50\x6b\x40\xe5\x1e\x74\x0a\x50\x1b\x41\xa5\x03\xf4\x3a\x50\x1b\x40\xa5\x06\x74\x0e\x5a\x4a\xff\x5c\x85\xcb\x88\xd7\xb8\x0f\xe5\x2b\x46\x5b\xc8\xe7\x45\x29\xed\x14\xa1\xf6\xe2\x7d\x28\x76\x8d\x28\x4e\x83\x29\xd6\x87\x28\xde\x83\x28\x56\xda\x57\xf2\x82\x74\x34\xa0\x6d\x05\x28\xff\x41\x4a\x06\xd0\xd2\x82\x74\xfc\xa0\xad\x03\x28\x6f\x41\xca\x3a\xd0\x32";
const CHUNK_4: &[u8] = b"\x00\x78\x80\xf4\xee\x06\x40\xc7\x70\x00\x00\x00\x00\x00\xe0\xd4\x03\xe0\x3e\x94\xed\xb0\xbf\xd2\x80\xfa\x05\x28\x3d\xa0\x52\x87\x3a\x03\x28\x15\xa0\xf2\x81\x7a\x03\x28\xf5\xa0\x72\x83\x3a\x05\x28\x0d\xa1\xd2\x81\xfa\x0d\x28\x0d\xa0\x52\x9f\x3a\x07\x28\x35\xa0\x32\x81\x7a\x1f\x28\x15\xa0\x32\x83\x3a\x05\x28\xfd\xa0\x52\x80\xfa\x0d\x28\x1d\xa1\xd2\x81\x3a\x03\x28\x35\xa0\x72\xd6\x67\xf4\xb9\x8b\x97\x21\xab\x71\x1f\xca\x4e\xe0\x47\x4a\x1b\x45\x29\xfd\x14\xa1\xb6\xf2\x7d\x28\x4e\x8f\x28\x56\x83\x29\xde\x87\x28\xd6\x83\x28\x4e\xdc\x57\xd2\x83\xb4\x34\xa0\x7d\x05\x28\xfb\x41\x4a\x19\xd0\x32\x82\xb4\xfc\xa0\xbd\x03\x28\x6b\x41\xca\x39\xd0\x52\x81\xb4\x34\xa0\xfd\xb4\xbf\xd2\x80\xda\x05\x28\x33\xa0\x72\x87\x5a\x03\x28\x17\xa0\xf2\x81\x5a\x03\x28\xf3\xa0\x52\x83\xda\x05\x28\x0f\xa1\xd2\x81\xda\x0d\x28\x13\xa0\x32\x9f\x5a\x07\x28\x37\xa0\x32\x81\x5a\x1f\x28\x13\xa0\xd2\x80\xda\x05\x28\xff\xa0\x52\x80\xda\x0d\x20";
const CHUNK_5: &[u8] = b"\x00\x01\x80\xe8\xdd\x0d\x40\xd7\x70\x00\x00\x00\x00\x00";
const CHUNK_6: &[u8] = b"\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\xe0\xd4\x03\xc2\x7c\x05\x94\x00\xc1\xac\x7d\xe9\x7a\x32\xd0\x4f\xed\xb4\x6e\x5b\x07\xce\x94\xa3\x6d\x64\xf9\xd0\xfd\x1a\x50\x6d\x0a\x53\x9d\x0e\x51\xad\x06\x50\xbd\xa8\xbf\xca\x81\x5a\x03\x28\x33\xa0\x52\x87\xda\x05\x28\x0f\xa0\xd2\x81\xda\x3d\x28\x13\xa0\x32\x83\x5a\x07\x28\x77\xa0\x32\x81\x5a\x0f\x28\x13\xb5\x97\xf4\x50\x1b\x40\xa5\x07\xf4\x0a\x50\x1b\x43\xa5\x02\x74\x1e\x50\x2b\x40\x65\x1e\xf4\x0e\x50\x6b\x40\x65\x02\x74\x3a\x50\x3b\x41\xa5\x01\xf4\x0a\x53\xfb\x40\xa5\x06\x74\x06\x50\x2b\x43\xe5\x02\xf4\x06\x50\x6b\x40\xe5\x1e\x74\x0a\x50\x1b\x41\xa5\x03\xf4\x3a\x50\x1b\x40\xa5\x06\x74\x0e\x5a\x4a\xff\x5c\x85\xcb\x88\xd7\xb8\x0f\xe5\x2b\x46\x5b\xc8\xe7\x45\x29\xed\x14\xa1\xf6\xe2\x7d\x28\x76\x8d\x28\x4e\x83\x29\xd6\x87\x28\xde\x83\x28\x56\xda\x57\xf2\x82\x74\x34\xa0\x6d\x05\x28\xff\x41\x4a\x06\xd0\xd2\x82\x74\xfc\xa0\xad\x03\x28\x6f\x41\xca\x3a\xd0\x32";
const CHUNK_7: &[u8] = b"\x00\x19\x00\xff\x3f\x50\x62\x4d\xd2\xf1\xa9\xfc\x8c\xa4\x94\x65\x24\xa2\x40\x28\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x40\x32\x66\x66\x66\x66\x66\x66\x3f\xf0\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x3f\xf0\x00\x00\x00\x00\x00\x00\x3f\xf0\x00\x00\x00\x00\x00\x00\x3f\xf0\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x3f\xf0\x00\x00\x00\x00\x00\x00\x3f\xf0\x00\x00\x00\x00\x00\x00\xf8\xea\x60\xd6\x2f\x76\x03\xd2\x1f\xc2\x5f\xff\xd8\x0f\x09\x7f\xff\x09\x7f\xff\x09\x7f\xff\x60\x3c\x25\xff\xfc\x25\xff\xf4\xbd\x61\x7b\x34\x9f\xff\xff\xff\xff\xff\xfd\x80\xf5\x85\xec\x07\xb0\x1e\xc0\x7a\xc2\xf6\x03\xd8\x0d\xa4\x8f\xbd\xa0\xf5\xeb\x9f\xff\xff\xff\xff\xff\xfd\x61\x7b\x41\xeb\x0b\xd6\x17\xac\x2f\x68\x3d\x61\x7a\xc2\xd0\xbe\xc0\xb8\xac\xcc\xcc\xcc\xcc\xcc\xdb\x41\xec\x0b\xda\x0f\x68\x3d\xa0\xf6\x05\xed\x07\xb4\x1a\x09\x99\x93\x33\x33\x33\x33\x33\x3b\x02\xe7\x60\x5e\xc0\xbd\x81\x73\xb0\x2f\x60\x5a\x38\xd4\x27\xea\x1f\xbc\xea\x13\xe6\x67\x50\x9f\x32\xdc\x24\xee\x07\xbb\x29\xff\xff\xff\xff\xff\xff\x50\x9f\xb8\x1e\xa1\x3f\x50\x9f\xa8\x4f\xdc\x0f\x50\x9f\xa8\x4f\x6d\x17\xfb\x42\xf6\xe7\xb5\x55\x55\x55\x55\x55\x77\x03\xda\x17\xb8\x1e\xe0\x7b\x81\xed\x0b\xdc\x0f\x70\x34\x99\x9d\x99\x99\x99\x99\x99\x9e\xd0\xb9\xda\x17\xb4\x2f\x68\x5c\xed\x0b\xda\x16\xd8\x37\x7b\x03\xfb\x32\xe4\xcc\xcc\xcc\xcc\xcc\xe7\x60\x7e\x66\x76\x07\xe6\x51\x31\xdd\x95\xaa\xaa\xaa\xaa\xaa\xab\xb0\x3f\x1d\x81\xfd\x81\xfd\x81\xf8\xec\x0f\xec\x0f\xa5\xe7\xb7\x3c\xff\xff\xff\xff\xff\xff\x19\xc6\x31\x9c\x62\xd4\x4f\xc6\x3a\xf5\xef\xff\xff\xff\xff\xff\xfc\xe3\x39\xce\x33\x9a\x05\xeb\x13\xe0\xac\xcc\xcc\xcc\xcc\xcc\xc7\x58\x9f\x18\xc7\x58\x9f\x18\xa0\xce\xf0\x70\xc9\x99\x99\x99\x99\x99\xb5\x89\xfb\xc1\xeb\x13\xf5\x89\xfa\xc4\xfd\xe0\xf5\x89\xfa\xc4\xf4\x0f\xdc\x17\x07\xaa\xaa\xaa\xaa\xaa\xab\x78\x3d\xc1\x7b\xc1\xef\x07\xbc\x1e\xe0\xbd\xe0\xf7\x83\x43\x99\x8e\x7f\xff\xff\xff\xff\xff\xb8\x2e\x77\x05\xee\x0b\xdc\x17\x3b\x82\xf7\x05\xa0\x5e\xd0\xfc\x16\xaa\xaa\xaa\xaa\xaa\xa9\xda\x1f\x99\x9d\xa1\xf9\x94\x19\x8c\x2d\x99\x99\x99\x99\x99\x9d\xa1\xf8\xed\x0f\xed\x0f\xed\x0f\xc7\x68\x7f\x68\x7d\x1d\xe7\x3c\x99\x99\x99\x99\x99\x9a\x33\x8c\x63\x38\xc5\x02\x63\x05\xaa\xaa\xaa\xaa\xaa\xaa\x71\x9c\xe7\x19\xcd\x06\xf6\x09\xf0\xcf\xff\xff\xff\xff\xff\xe3\xb0\x4f\x8c\x63\xb0\x4f\x8c\x50\x26\x18\x3d\x55\x55\x55\x55\x55\x5b\x04\xf8\x76\x09\xfb\x04\xfd\x82\x7c\x3b\x04\xfd\x82\x7a\x1f\xc7\x1c\x99\x99\x99\x99\x99\x9a\x18\xe1\x86\x18\xe1\x84";


fn chunk(t: ChunkEncoding, min: i64, max: i64, data: &[u8]) -> prompb::Chunk {
    prompb::Chunk { min_time_ms: min, max_time_ms: max, r#type: t, data: data.to_vec() }
}

fn read_stream(body: &[u8]) -> Vec<prompb::ChunkedReadResponse> {
    let mut stream = ChunkedReader::new(body, config::DEFAULT_CHUNKED_READ_LIMIT, Vec::new());
    let mut results = Vec::new();
    loop {
        match stream.next_proto::<prompb::ChunkedReadResponse>() {
            Err(e) if e.is(&ERR_EOF) => break,
            Err(e) => panic!("{e}"),
            Ok(r) => results.push(r),
        }
    }
    results
}

fn lm(n: &str, v: &str) -> LabelMatcher {
    LabelMatcher { r#type: MatcherType::EQ, name: n.into(), value: v.into() }
}

/// Go `failingChunkQueryable`.
struct FailingChunkQueryable {
    store: Arc<DB>,
    fail_query: i64,
    fail_after: usize,
    err: Option<Error>,
    opened: Arc<AtomicI64>,
    closed: Arc<AtomicI64>,
}

impl Queryable for FailingChunkQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.store.querier(mint, maxt)
    }
}

impl ChunkQueryable for FailingChunkQueryable {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let q = self.store.chunk_querier(mint, maxt)?;
        let fail = self.opened.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_query;
        Ok(Box::new(FailingChunkQuerier {
            q,
            fail,
            fail_after: self.fail_after,
            err: self.err.clone(),
            closed: self.closed.clone(),
        }))
    }
}

struct FailingChunkQuerier {
    q: BoxChunkQuerier,
    fail: bool,
    fail_after: usize,
    err: Option<Error>,
    closed: Arc<AtomicI64>,
}

impl LabelQuerier for FailingChunkQuerier {
    fn label_values(&self, ctx: &Context, n: &str, h: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.q.label_values(ctx, n, h, ms)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.q.label_names(ctx, h, ms)
    }
    fn close(&mut self) -> Option<Error> {
        let err = self.q.close();
        self.closed.fetch_add(1, Ordering::SeqCst);
        err
    }
}

impl ChunkQuerier for FailingChunkQuerier {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, ms: &[Matcher]) -> BoxChunkSeriesSet {
        let set = self.q.select(ctx, sort, hints, ms);
        if !self.fail {
            return set;
        }
        Box::new(FailingChunkSeriesSet { set, remaining: self.fail_after, err: self.err.clone() })
    }
}

struct FailingChunkSeriesSet {
    set: BoxChunkSeriesSet,
    remaining: usize,
    err: Option<Error>,
}

impl ChunkSeriesSet for FailingChunkSeriesSet {
    fn next(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        if !self.set.next() {
            return false;
        }
        self.remaining -= 1;
        true
    }
    fn at(&self) -> &ChunkSeriesArc {
        self.set.at()
    }
    fn err(&self) -> Option<Error> {
        if self.remaining == 0 {
            return self.err.clone();
        }
        self.set.err()
    }
    fn warnings(&self) -> Annotations {
        self.set.warnings()
    }
}

fn gauge_value(reg: &Registry, name: &str) -> f64 {
    for f in reg.gather() {
        if f.name == name {
            assert_eq!(1, f.metrics.len());
            if let MetricValue::Gauge(v) = f.metrics[0].value {
                return v;
            }
            panic!("{name} is not a gauge");
        }
    }
    panic!("metric {name:?} not found");
}

fn histogram_count_sum(reg: &Registry, name: &str) -> (u64, f64) {
    for f in reg.gather() {
        if f.name == name {
            assert_eq!(1, f.metrics.len());
            if let MetricValue::Histogram { count, sum, .. } = f.metrics[0].value {
                return (count, sum);
            }
            panic!("{name} is not a histogram");
        }
    }
    panic!("metric {name:?} not found");
}

#[test]
fn test_stream_read_endpoint() {
    // successful response
    {
        // First with 120 float samples. We expect 1 frame with 1 chunk.
        // Second with 121 float samples. We expect 1 frame with 2 chunks.
        // Third with 241 float samples. We expect 1 frame with 2 chunks, and 1 frame with 1 chunk for the same
        // series due to the bytes limit.
        // Fourth with 25 histogram samples. We expect 1 frame with 1 chunk.
        let store = loaded_test_db(
            "load 1m\n\ttest_metric1{foo=\"bar1\",baz=\"qux\"} 0+100x119\n\ttest_metric1{foo=\"bar2\",baz=\"qux\"} 0+100x120\n\ttest_metric1{foo=\"bar3\",baz=\"qux\"} 0+100x240\n",
        );
        add_native_histograms_to_test_suite(&store, 25);

        let api = new_read_handler(
            Logger::nop(),
            None,
            store.db.clone(),
            // We expect external labels to be added, with the source labels honored.
            ext(labels::from_strings(&["baz", "a", "b", "c", "d", "e"])),
            1_000_000,
            1,
            // The label set has 57 bytes. A full chunk in the test data has about 240 bytes: at most 2 chunks.
            57 + 480,
        );

        let hints = SelectHints { step: 1, func: "avg".into(), start: 0, end: 14400001, ..Default::default() };
        let m1 = eq("__name__", "test_metric1");
        let query1 = to_query(0, 14400001, &[m1.clone(), eq("d", "e")], Some(&hints)).unwrap();
        let query2 = to_query(0, 14400001, &[m1, eq("foo", "bar1")], Some(&hints)).unwrap();
        let query3 = to_query(0, 14400001, &[eq("__name__", "test_histogram_metric1")], Some(&hints)).unwrap();

        let req = prompb::ReadRequest {
            queries: vec![query1, query2, query3],
            accepted_response_types: vec![ResponseType::STREAMED_XOR_CHUNKS],
        };
        let resp = api.serve(&Context::background(), &request(snappy_encode(&req.marshal()))).expect("not aborted");
        assert_eq!(2, resp.status / 100);
        assert_eq!(
            Some("application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"),
            resp.header("Content-Type")
        );
        assert!(resp.header("Content-Encoding").unwrap_or("").is_empty());

        let results = read_stream(resp.body_bytes());
        assert_eq!(6, results.len(), "Expected 6 results.");

        let lbls = |foo: &str| {
            vec![pl("__name__", "test_metric1"), pl("b", "c"), pl("baz", "qux"), pl("d", "e"), pl("foo", foo)]
        };
        let xor = ChunkEncoding::XOR;
        let cr = |labels: Vec<prompb::Label>, chunks: Vec<prompb::Chunk>, query_index: i64| prompb::ChunkedReadResponse {
            chunked_series: vec![prompb::ChunkedSeries { labels, chunks }],
            query_index,
        };
        let expected = vec![
            cr(lbls("bar1"), vec![chunk(xor, 0, 7140000, CHUNK_0)], 0),
            cr(lbls("bar2"), vec![chunk(xor, 0, 7140000, CHUNK_1), chunk(xor, 7200000, 7200000, CHUNK_2)], 0),
            cr(lbls("bar3"), vec![chunk(xor, 0, 7140000, CHUNK_3), chunk(xor, 7200000, 14340000, CHUNK_4)], 0),
            cr(lbls("bar3"), vec![chunk(xor, 14400000, 14400000, CHUNK_5)], 0),
            cr(lbls("bar1"), vec![chunk(xor, 0, 7140000, CHUNK_6)], 1),
            cr(
                vec![pl("__name__", "test_histogram_metric1"), pl("b", "c"), pl("baz", "qux"), pl("d", "e")],
                vec![chunk(ChunkEncoding::FLOAT_HISTOGRAM, 0, 1440000, CHUNK_7)],
                2,
            ),
        ];
        assert_eq!(expected, results);
    }

    // storage errors
    // adapted: Go runs each case over HTTP/1 and HTTP/2, with and without otelhttp, through a TLS test server;
    // here the handler is called directly once per case. A failure after the first frame is an abort
    // (AbortHandler) for both the buffered and the flushed cases, as the body is not streamed.
    {
        let store = loaded_test_db("load 1m\n test_metric{series=\"a\"} 1\n test_metric{series=\"b\"} 2\n");
        let storage_err = Error::msg("injected storage error");
        let http_err = Error::other(HttpError { msg: "injected HTTP error".into(), status: 400 });

        struct Tc {
            name: &'static str,
            fail_query: i64,
            fail_after: usize,
            err: Option<Error>,
            empty: bool,
            want_status: u16,
            want_series: usize,
        }
        let cases = vec![
            Tc { name: "success", fail_query: 0, fail_after: 0, err: None, empty: false, want_status: 200, want_series: 2 },
            Tc { name: "empty", fail_query: 0, fail_after: 0, err: None, empty: true, want_status: 200, want_series: 0 },
            Tc {
                name: "error before response",
                fail_query: 1,
                fail_after: 0,
                err: Some(storage_err.clone()),
                empty: false,
                want_status: 500,
                want_series: 0,
            },
            Tc {
                name: "HTTP error before response",
                fail_query: 1,
                fail_after: 0,
                err: Some(http_err.clone()),
                empty: false,
                want_status: 400,
                want_series: 0,
            },
            Tc {
                name: "buffered error",
                fail_query: 1,
                fail_after: 1,
                err: Some(storage_err.clone()),
                empty: false,
                want_status: 200,
                want_series: 1,
            },
            Tc {
                name: "flushed error",
                fail_query: 1,
                fail_after: 1,
                err: Some(storage_err.clone()),
                empty: false,
                want_status: 200,
                want_series: 1,
            },
            Tc {
                name: "later query error",
                fail_query: 2,
                fail_after: 0,
                err: Some(http_err.clone()),
                empty: false,
                want_status: 200,
                want_series: 2,
            },
        ];

        for tc in cases {
            let opened = Arc::new(AtomicI64::new(0));
            let closed = Arc::new(AtomicI64::new(0));
            let queryable = FailingChunkQueryable {
                store: store.db.clone(),
                fail_query: tc.fail_query,
                fail_after: tc.fail_after,
                err: tc.err.clone(),
                opened: opened.clone(),
                closed: closed.clone(),
            };
            let handler_log: Arc<Mutex<Vec<(Level, String)>>> = Arc::default();
            let hl = handler_log.clone();
            let logger = Logger::new(Arc::new(move |level, msg, kv| {
                let mut line = msg.to_string();
                for (k, v) in kv {
                    line.push_str(&format!(" {k}={v}"));
                }
                hl.lock().unwrap().push((level, line));
            }));
            let registry = Registry::new();
            let handler = new_read_handler(logger, Some(&registry), Arc::new(queryable), ext(Labels::empty()), 1_000_000, 1, 1 << 20);

            let metric = if tc.empty { "missing_metric" } else { "test_metric" };
            let query = prompb::Query {
                start_timestamp_ms: 0,
                end_timestamp_ms: 1,
                matchers: vec![lm("__name__", metric)],
                hints: None,
            };
            let mut queries = vec![query.clone()];
            if tc.fail_query == 2 {
                queries.push(query);
            }
            let data = prompb::ReadRequest { queries, accepted_response_types: vec![ResponseType::STREAMED_XOR_CHUNKS] }.marshal();
            let new_request = || {
                let mut r = request(snappy_encode(&data));
                r.headers.push(("content-type".into(), "application/x-protobuf".into()));
                r.headers.push(("content-encoding".into(), "snappy".into()));
                r
            };
            let check_cleanup = || {
                assert_eq!(opened.load(Ordering::SeqCst), closed.load(Ordering::SeqCst), "{}", tc.name);
                assert_eq!(0.0, gauge_value(&registry, "prometheus_remote_read_handler_queries"), "{}", tc.name);
            };

            let aborted = tc.err.is_some() && tc.want_status == 200;
            let res = handler.serve(&Context::background(), &new_request());
            if aborted {
                assert_eq!(Err(AbortHandler), res.map(|_| ()), "{}", tc.name);
            } else {
                let response = res.expect("not aborted");
                assert_eq!(tc.want_status, response.status, "{}", tc.name);
                if tc.want_status != 200 {
                    assert_eq!(Some("text/plain; charset=utf-8"), response.header("Content-Type"), "{}", tc.name);
                    let want = format!("{}\n", tc.err.as_ref().unwrap());
                    assert_eq!(want.as_bytes(), response.body_bytes(), "{}", tc.name);
                } else {
                    assert_eq!(
                        Some("application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"),
                        response.header("Content-Type")
                    );
                    let body = response.body_bytes().to_vec();
                    let mut series =
                        new_chunked_series_set(ChunkedReader::new(std::io::Cursor::new(body), config::DEFAULT_CHUNKED_READ_LIMIT, Vec::new()), 0, 1, |_| {});
                    let mut count = 0usize;
                    while series.next() {
                        assert!(count < tc.want_series, "{}", tc.name);
                        let s = series.at().clone();
                        let name = ((b'a' + count as u8) as char).to_string();
                        assert_eq!(&labels::from_strings(&["__name__", "test_metric", "series", &name]), s.labels());
                        let mut iter = s.iterator(None);
                        assert_eq!(ValueType::Float, iter.next());
                        assert_eq!((0, (count + 1) as f64), iter.at());
                        assert_eq!(ValueType::None, iter.next());
                        assert!(iter.err().is_none());
                        count += 1;
                    }
                    assert!(series.err().is_none(), "{}", tc.name);
                    assert_eq!(tc.want_series, count, "{}", tc.name);
                }
            }
            check_cleanup();

            // With a concurrency limit of one, another successful request proves the gate was released.
            let response = handler.serve(&Context::background(), &new_request());
            let response = response.expect("second request not aborted");
            assert_eq!(200, response.status, "{}", tc.name);
            check_cleanup();

            let log = handler_log.lock().unwrap();
            if aborted {
                assert_eq!(1, log.iter().filter(|(l, _)| *l == Level::Error).count(), "{}", tc.name);
                let want = tc.err.as_ref().unwrap().to_string();
                assert!(log.iter().any(|(_, line)| line.contains(&want)), "{}: {:?}", tc.name, *log);
            } else {
                assert!(log.is_empty(), "{}: {:?}", tc.name, *log);
            }
        }
    }

    // The "write errors" subtest checks how the handler reacts to a failing http.ResponseWriter (zero bytes and
    // partial writes, then panic(http.ErrAbortHandler)); this port builds the body in memory and writes
    // nothing itself, so there is no writer to fail (see tests-data/not-ported.md).
}

/// Go `gateWaitQueryable`.
struct GateWaitQueryable {
    before_query: Arc<dyn Fn() + Send + Sync>,
}

impl Queryable for GateWaitQueryable {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        (self.before_query)();
        Ok(Box::new(MockQuerier { select_mock_function: Box::new(|_, _, _| empty_series_set()) }))
    }
}

impl ChunkQueryable for GateWaitQueryable {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        Err(Error::msg("unexpected chunk query in gate wait test"))
    }
}

/// A one-shot flag (Go closed channel).
#[derive(Clone, Default)]
struct Flag(Arc<(Mutex<bool>, std::sync::Condvar)>);

impl Flag {
    fn set(&self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
    fn is_set(&self) -> bool {
        *self.0 .0.lock().unwrap()
    }
    fn wait(&self) {
        let mut g = self.0 .0.lock().unwrap();
        while !*g {
            g = self.0 .1.wait(g).unwrap();
        }
    }
    fn wait_timeout(&self, d: Duration) -> bool {
        let g = self.0 .0.lock().unwrap();
        let (g, _) = self.0 .1.wait_timeout_while(g, d, |v| !*v).unwrap();
        *g
    }
}

// adapted: Go runs this test in a synctest bubble (fake clock), so the observed waits are exactly 0s and 0.2s.
// Here the clock is real: waits are checked to be within 50ms above the expected value.
#[test]
fn test_remote_read_gate_wait_duration_observed() {
    const WAIT_DURATION: Duration = Duration::from_millis(200);
    const QUERY_DURATION: Duration = Duration::from_millis(100);
    const SLACK: f64 = 0.05;

    struct Tc {
        name: &'static str,
        concurrency_limit: usize,
        contended: bool,
        canceled: bool,
        want_status: u16,
    }
    let cases = [
        Tc { name: "successful acquisition", concurrency_limit: 1, contended: false, canceled: false, want_status: 200 },
        Tc { name: "canceled acquisition", concurrency_limit: 0, contended: false, canceled: true, want_status: 500 },
        Tc { name: "contended acquisition", concurrency_limit: 1, contended: true, canceled: false, want_status: 200 },
        Tc {
            name: "contended canceled acquisition",
            concurrency_limit: 1,
            contended: true,
            canceled: true,
            want_status: 500,
        },
    ];

    for tc in cases {
        let query_started = Flag::default();
        let query_continue = Flag::default();
        let (qs, qc) = (query_started.clone(), query_continue.clone());
        let queryable = GateWaitQueryable {
            before_query: Arc::new(move || {
                qs.set();
                qc.wait();
            }),
        };
        let reg = Registry::new();
        let h: Arc<ReadHandler> = Arc::new(new_read_handler(
            Logger::nop(),
            Some(&reg),
            Arc::new(queryable),
            ext(Labels::empty()),
            1_000_000,
            tc.concurrency_limit,
            0,
        ));

        let hints = SelectHints { step: 0, func: "avg".into(), ..Default::default() };
        let query = to_query(0, 1, &[eq("__name__", "test_metric1")], Some(&hints)).unwrap();
        let data = prompb::ReadRequest { queries: vec![query], ..Default::default() }.marshal();
        let req = request(snappy_encode(&data));

        let (ctx, cancel) = Context::background().with_cancel();

        let gate_held = Arc::new(AtomicBool::new(false));
        if tc.contended {
            h.gate().start(&Context::background()).unwrap();
            gate_held.store(true, Ordering::SeqCst);
        }
        let release_gate = |h: &ReadHandler| {
            if gate_held.swap(false, Ordering::SeqCst) {
                h.gate().done();
            }
        };
        if !tc.contended || tc.canceled {
            query_continue.set();
        }
        if tc.canceled && !tc.contended {
            // With zero capacity, only the canceled context lets Gate.start return.
            cancel.cancel();
        }

        let done = Flag::default();
        let status = Arc::new(Mutex::new(0u16));
        let (h2, d2, s2, ctx2) = (h.clone(), done.clone(), status.clone(), ctx.clone());
        let th = std::thread::spawn(move || {
            let r = h2.serve(&ctx2, &req).expect("not aborted");
            *s2.lock().unwrap() = r.status;
            d2.set();
        });

        let mut want_wait = 0.0;
        if tc.contended {
            std::thread::sleep(Duration::from_millis(20));
            assert!(!done.is_set(), "{}: request should be waiting on the gate", tc.name);
            assert!(!query_started.is_set(), "{}: storage should not be queried before acquisition", tc.name);
            let (count, sum) = histogram_count_sum(&reg, "prometheus_remote_read_handler_gate_wait_duration_seconds");
            assert_eq!(0, count, "{}", tc.name);
            assert_eq!(0.0, sum, "{}", tc.name);

            want_wait = WAIT_DURATION.as_secs_f64();
            std::thread::sleep(WAIT_DURATION - Duration::from_millis(20));
            if tc.canceled {
                // Keep the slot occupied until cancellation has completed.
                cancel.cancel();
            } else {
                release_gate(&h);
            }

            if !tc.canceled {
                assert!(query_started.wait_timeout(Duration::from_secs(5)), "{}: request should have entered query processing", tc.name);
                assert!(!done.is_set(), "{}: query processing should still be blocked", tc.name);
                let (count, sum) = histogram_count_sum(&reg, "prometheus_remote_read_handler_gate_wait_duration_seconds");
                assert_eq!(1, count, "{}", tc.name);
                assert!(sum >= want_wait && sum < want_wait + SLACK, "{}: {sum}", tc.name);

                // Query processing must not add to the already observed gate wait.
                std::thread::sleep(QUERY_DURATION);
                query_continue.set();
            }
        }

        assert!(done.wait_timeout(Duration::from_secs(5)), "{}: remote read handler should have finished", tc.name);
        th.join().unwrap();
        assert_eq!(tc.want_status, *status.lock().unwrap(), "{}", tc.name);
        assert_eq!(!tc.canceled, query_started.is_set(), "{}: canceled acquisition must not query storage", tc.name);
        let (count, sum) = histogram_count_sum(&reg, "prometheus_remote_read_handler_gate_wait_duration_seconds");
        assert_eq!(1, count, "{}", tc.name);
        assert!(sum >= want_wait && sum < want_wait + SLACK, "{}: {sum}", tc.name);
        assert_eq!(0.0, gauge_value(&reg, "prometheus_remote_read_handler_queries"), "{}", tc.name);

        if tc.concurrency_limit > 0 {
            release_gate(&h);
            let (gate_ctx, _c) = Context::background().with_timeout(Duration::from_secs(2));
            h.gate().start(&gate_ctx).unwrap();
            h.gate().done();
        }
        query_continue.set();
        cancel.cancel();
    }
}
