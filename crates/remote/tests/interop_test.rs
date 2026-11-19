//! Byte compatibility of the hand-written protobuf and the Snappy block format with upstream Go (gogo protobuf
//! and golang/snappy). `scripts/interop/remote/run.sh` runs the Go generator (`scripts/interop/remote/gen`), which
//! writes the encoded messages to `bench/tmp/w3-remote/interop/`. This test builds the same messages, asserts
//! identical bytes, decodes Go's bytes and re-encodes them, and writes its own files (`rust_*`) that the Go
//! generator then decodes. Skipped when the directory does not exist.

use std::path::PathBuf;

use model::histogram::{CounterResetHint, FloatHistogram, Histogram as MHist, Span, CUSTOM_BUCKETS_SCHEMA};
use remote::prompb::writev2::{self, SymbolsTable};
use remote::prompb::*;

fn int_hist() -> MHist {
    MHist {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: -2,
        count: 1234567,
        sum: -98.25,
        zero_threshold: 0.001,
        zero_count: 3,
        positive_spans: vec![Span::new(-3, 2), Span::new(5, 1)],
        positive_buckets: vec![100, -50, 7],
        negative_spans: vec![Span::new(0, 1)],
        negative_buckets: vec![-2],
        custom_values: None,
    }
}

fn float_hist() -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 12.5,
        sum: 1e300,
        positive_spans: vec![Span::new(0, 3)],
        positive_buckets: vec![1.5, 0.0, 11.0],
        custom_values: Some(std::sync::Arc::from(&[0.1, 1.0, 10.0][..])),
        ..Default::default()
    }
}

fn lbl(n: &str, v: &str) -> Label {
    Label::new(n, v)
}

fn v1_write() -> WriteRequest {
    WriteRequest {
        timeseries: vec![
            TimeSeries {
                labels: vec![lbl("__name__", "http_requests_total"), lbl("code", "200"), lbl("empty", ""), lbl("utf8", "Grüße ✓")],
                samples: vec![
                    Sample { value: 1.0, timestamp: 1700000000000 },
                    Sample { value: 0.0, timestamp: 1700000015000 },
                    Sample { value: -0.0, timestamp: 1700000030000 },
                    Sample { value: f64::from_bits(0x7ff0000000000002), timestamp: 1700000045000 },
                    Sample { value: f64::NEG_INFINITY, timestamp: -5 },
                    Sample { value: 3.5e-310, timestamp: 0 },
                ],
                exemplars: vec![
                    Exemplar { labels: vec![lbl("trace_id", "abc123")], value: 0.25, timestamp: 1700000000001 },
                    Exemplar { value: -1.0, ..Default::default() },
                ],
                ..Default::default()
            },
            TimeSeries {
                labels: vec![lbl("__name__", "latency")],
                histograms: vec![
                    from_int_histogram(1700000000000, &int_hist()),
                    from_float_histogram(-1, &float_hist()),
                    Histogram::default(),
                ],
                ..Default::default()
            },
            TimeSeries::default(),
        ],
        metadata: vec![
            MetricMetadata {
                r#type: MetricType::COUNTER,
                metric_family_name: "http_requests_total".into(),
                help: "Total requests.".into(),
                unit: "requests".into(),
            },
            MetricMetadata { r#type: MetricType::STATESET, metric_family_name: "x".into(), ..Default::default() },
            MetricMetadata::default(),
        ],
    }
}

fn v2_write() -> writev2::Request {
    let mut st = SymbolsTable::new();
    let refs1 = vec![st.symbolize("__name__"), st.symbolize("http_requests_total"), st.symbolize("code"), st.symbolize("200")];
    let help = st.symbolize("Total requests.");
    let unit = st.symbolize("requests");
    let refs2 = vec![st.symbolize("__name__"), st.symbolize("latency")];
    let ex_refs = vec![st.symbolize("trace_id"), st.symbolize("abc123")];
    let big: Vec<u32> = (0..300u32).map(|i| i * 1000).collect();
    writev2::Request {
        symbols: st.symbols().to_vec(),
        timeseries: vec![
            writev2::TimeSeries {
                labels_refs: refs1,
                samples: vec![
                    writev2::Sample { value: 1.0, timestamp: 1700000000000, start_timestamp: 1699999999000 },
                    writev2::Sample { value: f64::from_bits(0x7ff0000000000002), timestamp: 1700000015000, start_timestamp: 0 },
                    writev2::Sample { value: -7.25, timestamp: -3, start_timestamp: -9 },
                ],
                exemplars: vec![
                    writev2::Exemplar { labels_refs: ex_refs.clone(), value: 0.25, timestamp: 1700000000001 },
                    writev2::Exemplar::default(),
                ],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::COUNTER, help_ref: help, unit_ref: unit },
                ..Default::default()
            },
            writev2::TimeSeries {
                labels_refs: refs2,
                histograms: vec![
                    writev2::from_int_histogram(1699999999000, 1700000000000, &int_hist()),
                    writev2::from_float_histogram(0, -1, &float_hist()),
                ],
                exemplars: vec![writev2::Exemplar { labels_refs: ex_refs, value: 3.0, timestamp: 5 }],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::HISTOGRAM, ..Default::default() },
                ..Default::default()
            },
            writev2::TimeSeries { labels_refs: big, ..Default::default() },
            writev2::TimeSeries::default(),
        ],
    }
}

fn read_request() -> ReadRequest {
    ReadRequest {
        queries: vec![
            Query {
                start_timestamp_ms: 1700000000000,
                end_timestamp_ms: 1700000300000,
                matchers: vec![
                    LabelMatcher { r#type: MatcherType::EQ, name: "__name__".into(), value: "up".into() },
                    LabelMatcher { r#type: MatcherType::NRE, name: "job".into(), value: "a|b".into() },
                ],
                hints: Some(ReadHints {
                    step_ms: 15000,
                    func: "rate".into(),
                    start_ms: 1700000000000,
                    end_ms: 1700000300000,
                    grouping: vec!["job".into(), "".into()],
                    by: true,
                    range_ms: 300000,
                }),
            },
            Query { start_timestamp_ms: -1, hints: Some(ReadHints::default()), ..Default::default() },
        ],
        accepted_response_types: vec![ResponseType::STREAMED_XOR_CHUNKS, ResponseType::SAMPLES],
    }
}

fn read_response() -> ReadResponse {
    let w = v1_write();
    ReadResponse {
        results: vec![
            QueryResult { timeseries: vec![w.timeseries[0].clone(), w.timeseries[1].clone()] },
            QueryResult::default(),
            QueryResult {
                timeseries: vec![TimeSeries {
                    labels: vec![lbl("a", "b")],
                    samples: vec![Sample { value: 42.0, timestamp: 1 }],
                    ..Default::default()
                }],
            },
        ],
    }
}

fn chunked_response() -> ChunkedReadResponse {
    let data: Vec<u8> = (0..200usize).map(|i| (i * 7) as u8).collect();
    ChunkedReadResponse {
        chunked_series: vec![
            ChunkedSeries {
                labels: vec![lbl("__name__", "up"), lbl("job", "node")],
                chunks: vec![
                    Chunk { min_time_ms: 1700000000000, max_time_ms: 1700000100000, r#type: ChunkEncoding::XOR, data },
                    Chunk { min_time_ms: -10, max_time_ms: 0, r#type: ChunkEncoding::FLOAT_HISTOGRAM_ST, data: vec![0] },
                    Chunk::default(),
                ],
            },
            ChunkedSeries::default(),
        ],
        query_index: 3,
    }
}

fn snappy(b: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(b).unwrap()
}

fn check<M: Message>(dir: &PathBuf, name: &str, built: &M, eq_struct: bool) -> Vec<u8>
where
    M: PartialEq + std::fmt::Debug,
{
    let go = std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    let ours = built.marshal();
    assert_eq!(go, ours, "{name}: Rust encoding differs from Go");
    let decoded = M::unmarshal(&go).unwrap_or_else(|e| panic!("{name}: decode Go bytes: {e}"));
    assert_eq!(go, decoded.marshal(), "{name}: re-encode of decoded Go bytes differs");
    if eq_struct {
        assert_eq!(&decoded, built, "{name}: decoded struct differs");
    }
    ours
}

#[test]
fn interop_with_go() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w3-remote/interop");
    if !dir.join("v1_write.bin").exists() {
        println!("skipped: {} has no Go files; run scripts/interop/remote/run.sh", dir.display());
        return;
    }
    let v1 = check(&dir, "v1_write.bin", &v1_write(), false);
    let v2 = v2_write();
    let v2b = check(&dir, "v2_write.bin", &v2, false);
    let rr = check(&dir, "read_request.bin", &read_request(), true);
    let resp = check(&dir, "read_response.bin", &read_response(), false);
    let chunked = check(&dir, "chunked_response.bin", &chunked_response(), true);

    // OptimizedMarshal: exemplars before histograms.
    let go_opt = std::fs::read(dir.join("v2_write_opt.bin")).unwrap();
    let mut opt = Vec::new();
    v2.optimized_marshal(&mut opt);
    assert_eq!(go_opt, opt, "v2 OptimizedMarshal differs");
    assert_ne!(go_opt, v2b, "the fixture must have a series with both exemplars and histograms");
    assert_eq!(writev2::Request::unmarshal(&go_opt).unwrap().marshal(), v2b);

    // Snappy block format: same compressed bytes as golang/snappy, and Go's output decodes.
    for (name, raw) in [
        ("v1_write.snappy", &v1),
        ("v2_write_opt.snappy", &opt),
        ("read_response.snappy", &resp),
        ("chunked_response.snappy", &chunked),
    ] {
        let go = std::fs::read(dir.join(name)).unwrap();
        assert_eq!(&snap::raw::Decoder::new().decompress_vec(&go).unwrap(), raw, "{name}: decode Go snappy");
        assert_eq!(go, snappy(raw), "{name}: Rust snappy bytes differ from golang/snappy");
    }

    // Files for the Go side to decode.
    for (name, b) in [
        ("rust_v1_write.bin", &v1),
        ("rust_v2_write.bin", &v2b),
        ("rust_read_request.bin", &rr),
        ("rust_read_response.bin", &resp),
        ("rust_chunked_response.bin", &chunked),
    ] {
        std::fs::write(dir.join(name), b).unwrap();
    }
    std::fs::write(dir.join("rust_v1_write.snappy"), snappy(&v1)).unwrap();
    println!("interop: 5 messages, OptimizedMarshal and 4 snappy bodies identical to Go");
}
