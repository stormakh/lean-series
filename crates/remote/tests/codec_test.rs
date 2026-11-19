//! Port of `storage/remote/codec_test.go`. (The `prompb/codec_test.go` tests go into this file, too.)

mod common;

use std::io::{self, Cursor, Read, Write};
use std::sync::{Arc, Mutex};

use chunkenc::{ChunkIterator, Encoding, ValueType, XorChunk};
use common::*;
use model::histogram::{ErrorKind, Span, EXPONENTIAL_SCHEMA_MAX};
use model::labels::{self, ScratchBuilder};
use remote::chunked::{ChunkedReader, ChunkedWriter, NopFlusher};
use remote::codec::*;
use remote::prompb::{self, writev2, Message, ResponseType};
use storage::chunks::Meta;
use storage::{Annotations, ChunkSeriesArc, ChunkSeriesSet, Error, ListChunkSeries, Series, SeriesArc, SeriesSet};

#[test]
fn test_histogram_fixture_valid() {
    for ts in &write_request_fixture().timeseries {
        for h in &ts.histograms {
            if h.is_float_histogram() {
                h.to_float_histogram().validate().unwrap();
            } else {
                h.to_int_histogram().unwrap().validate().unwrap();
            }
        }
    }
    for ts in &write_v2_request_fixture().timeseries {
        for h in &ts.histograms {
            if h.is_float_histogram() {
                h.to_float_histogram().validate().unwrap();
            } else {
                h.to_int_histogram().unwrap().validate().unwrap();
            }
        }
    }
}

#[test]
fn test_write_v2_request_fixture() {
    // Generate the v2 fixture dynamically, reusing v1 fixture elements.
    let wrf = write_request_fixture();
    let mut st = writev2::SymbolsTable::new();
    let mut b = ScratchBuilder::new(0);
    let mut label_refs = Vec::new();
    st.symbolize_labels(&wrf.timeseries[0].to_labels(&mut b), &mut label_refs);
    let mut exemplar1_label_refs = Vec::new();
    st.symbolize_labels(&wrf.timeseries[0].exemplars[0].to_exemplar(&mut b).labels, &mut exemplar1_label_refs);
    let mut exemplar2_label_refs = Vec::new();
    st.symbolize_labels(&wrf.timeseries[1].exemplars[0].to_exemplar(&mut b).labels, &mut exemplar2_label_refs);
    let th = test_histogram();
    let thc = test_histogram_custom_buckets();
    let m1 = write_v2_request_series1_metadata();
    let m2 = write_v2_request_series2_metadata();
    let ts0 = writev2::TimeSeries {
        labels_refs: label_refs.clone(),
        metadata: writev2::Metadata {
            r#type: writev2::MetadataType::GAUGE,
            help_ref: st.symbolize(&m1.help),
            unit_ref: st.symbolize(&m1.unit),
        },
        samples: vec![writev2::Sample { value: 1.0, timestamp: 10, start_timestamp: 1 }],
        exemplars: vec![writev2::Exemplar { labels_refs: exemplar1_label_refs, value: 1.0, timestamp: 10 }],
        histograms: vec![
            writev2::from_int_histogram(1, 10, &th),
            writev2::from_float_histogram(2, 20, &th.to_float()),
            writev2::from_int_histogram(11, 30, &thc),
            writev2::from_float_histogram(21, 40, &thc.to_float()),
        ],
    };
    let ts1 = writev2::TimeSeries {
        labels_refs: label_refs,
        metadata: writev2::Metadata {
            r#type: writev2::MetadataType::COUNTER,
            help_ref: st.symbolize(&m2.help),
            unit_ref: 0, // No unit.
        },
        samples: vec![writev2::Sample { value: 2.0, timestamp: 20, start_timestamp: 0 }],
        exemplars: vec![writev2::Exemplar { labels_refs: exemplar2_label_refs, value: 2.0, timestamp: 20 }],
        histograms: vec![
            writev2::from_int_histogram(31, 50, &th),
            writev2::from_float_histogram(41, 60, &th.to_float()),
            writev2::from_int_histogram(51, 70, &thc),
            writev2::from_float_histogram(61, 80, &thc.to_float()),
        ],
    };
    let expected = writev2::Request { timeseries: vec![ts0, ts1], symbols: st.symbols().to_vec() };
    // Check that it matches the static fixture.
    assert_eq!(expected, write_v2_request_fixture());
}

#[test]
fn test_validate_labels_and_metric_name() {
    // Rows whose input holds invalid UTF-8 ("label name with \xff", "label value is an invalid UTF-8 value",
    // "metric name has invalid utf8") cannot be built: Rust strings are always valid UTF-8 (see
    // tests-data/not-ported.md).
    let tests: Vec<(Vec<prompb::Label>, &str, &str)> = vec![
        (vec![l("__name__", "name"), l("labelName", "labelValue")], "", "regular labels"),
        (vec![l("__name__", "name"), l("_labelName", "labelValue")], "", "label name with _"),
        (vec![l("__name__", "name"), l("", "labelValue")], "invalid label name: ", "label name is empty string"),
        (vec![l("__name__", "name1"), l("__name__", "name2")], "duplicate label with name: __name__", "duplicate label names"),
        (vec![l("label1", "name"), l("label2", "name")], "", "duplicate label values"),
        (vec![l("", "name"), l("label2", "name")], "invalid label name: ", "don't report as duplicate label name"),
    ];
    for (input, expected_err, description) in tests {
        let res = validate_labels_and_metric_name(&input);
        if !expected_err.is_empty() {
            assert_eq!(expected_err, res.expect_err(description).to_string(), "{description}");
        } else {
            assert!(res.is_ok(), "{description}: {:?}", res);
        }
    }
}

fn concrete(lbls: &[&str], floats: Vec<prompb::Sample>, histograms: Vec<prompb::Histogram>) -> ConcreteSeries {
    ConcreteSeries { labels: labels::from_strings(lbls), floats, histograms }
}

fn s(value: f64, timestamp: i64) -> prompb::Sample {
    prompb::Sample { value, timestamp }
}

#[test]
fn test_concrete_series_set() {
    let series1: SeriesArc = Arc::new(concrete(&["foo", "bar"], vec![s(1.0, 2)], vec![]));
    let series2: SeriesArc = Arc::new(concrete(&["foo", "baz"], vec![s(3.0, 4)], vec![]));
    let mut c = ConcreteSeriesSet::new(vec![series1.clone(), series2.clone()]);
    assert!(c.next(), "Expected Next() to be true.");
    assert!(Arc::ptr_eq(&series1, c.at()), "Unexpected series returned.");
    assert!(c.next(), "Expected Next() to be true.");
    assert!(Arc::ptr_eq(&series2, c.at()), "Unexpected series returned.");
    assert!(!c.next(), "Expected Next() to be false.");
}

#[test]
fn test_concrete_series_clones_labels() {
    let lbls = labels::from_strings(&["a", "b", "c", "d"]);
    let cs = concrete(&["a", "b", "c", "d"], vec![], vec![]);

    // adapted: Go's Labels() returns a copy that the caller may overwrite; here labels() lends the labels and
    // the caller overwrites its own copy.
    let mut got_labels = cs.labels().clone();
    assert_eq!(lbls, got_labels);

    got_labels.copy_from(&labels::from_strings(&["a", "foo", "c", "foo"]));

    let got_labels = cs.labels();
    assert_eq!(&lbls, got_labels);
}

#[test]
fn test_concrete_series_iterator_float_samples() {
    let series = concrete(
        &["foo", "bar"],
        vec![s(1.0, 1), s(1.5, 1), s(2.0, 2), s(3.0, 3), s(4.0, 4)],
        vec![],
    );
    let mut it = series.iterator(None);

    // Seek to the first sample with ts=1.
    assert_eq!(ValueType::Float, it.seek(1));
    assert_eq!((1, 1.0), it.at());

    // Seek one further, next sample still has ts=1.
    assert_eq!(ValueType::Float, it.next());
    assert_eq!((1, 1.5), it.at());

    // Seek again to 1 and make sure we stay where we are.
    assert_eq!(ValueType::Float, it.seek(1));
    assert_eq!((1, 1.5), it.at());

    // Another seek.
    assert_eq!(ValueType::Float, it.seek(3));
    assert_eq!((3, 3.0), it.at());

    // And we don't go back.
    assert_eq!(ValueType::Float, it.seek(2));
    assert_eq!((3, 3.0), it.at());

    // Seek beyond the end.
    assert_eq!(ValueType::None, it.seek(5));
    // And we don't go back. (This exposes issue #10027.)
    assert_eq!(ValueType::None, it.seek(2));
}

#[test]
fn test_concrete_series_iterator_histogram_samples() {
    let histograms = storage::tsdbutil::generate_test_histograms(5);
    let hist_protos: Vec<prompb::Histogram> = histograms
        .iter()
        .enumerate()
        .map(|(i, h)| {
            // Results in ts sequence of 1, 1, 2, 3, 4.
            let ts = if i == 0 { 1 } else { i as i64 };
            prompb::from_int_histogram(ts, h)
        })
        .collect();
    let series = concrete(&["foo", "bar"], vec![], hist_protos);
    let mut it = series.iterator(None);

    // Seek to the first sample with ts=1.
    assert_eq!(ValueType::Histogram, it.seek(1));
    assert_eq!((1, histograms[0].clone()), it.at_histogram(None));

    // Seek one further, next sample still has ts=1.
    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!((1, histograms[1].clone()), it.at_histogram(None));

    // Seek again to 1 and make sure we stay where we are.
    assert_eq!(ValueType::Histogram, it.seek(1));
    assert_eq!((1, histograms[1].clone()), it.at_histogram(None));

    // Another seek.
    assert_eq!(ValueType::Histogram, it.seek(3));
    assert_eq!((3, histograms[3].clone()), it.at_histogram(None));

    // And we don't go back.
    assert_eq!(ValueType::Histogram, it.seek(2));
    assert_eq!((3, histograms[3].clone()), it.at_histogram(None));

    // Seek beyond the end.
    assert_eq!(ValueType::None, it.seek(5));
    // And we don't go back. (This exposes issue #10027.)
    assert_eq!(ValueType::None, it.seek(2));
}

#[test]
fn test_concrete_series_iterator_float_and_histogram_samples() {
    // Histograms, then floats from ts=8 (overlap from 8 to 10), then histograms again from ts=16.
    let histograms = storage::tsdbutil::generate_test_histograms(15);
    let hist_protos: Vec<prompb::Histogram> = histograms
        .iter()
        .enumerate()
        .map(|(i, h)| {
            if i < 10 {
                prompb::from_int_histogram(i as i64 + 1, h)
            } else {
                prompb::from_int_histogram(i as i64 + 6, h)
            }
        })
        .collect();
    let series = concrete(
        &["foo", "bar"],
        vec![s(1.0, 8), s(2.0, 9), s(3.0, 10), s(4.0, 11), s(5.0, 12), s(6.0, 13), s(7.0, 14), s(8.0, 15)],
        hist_protos,
    );
    let mut it = series.iterator(None);

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!((1, histograms[0].clone()), it.at_histogram(None));

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!((2, histograms[1].clone()), it.at_histogram(None));

    // Seek to the first float/histogram sample overlap at ts=8 (should prefer the float sample).
    assert_eq!(ValueType::Float, it.seek(8));
    assert_eq!((8, 1.0), it.at());

    // Attempting to seek backwards should do nothing.
    assert_eq!(ValueType::Float, it.seek(1));
    assert_eq!((8, 1.0), it.at());

    // Seeking to 8 again should also do nothing.
    assert_eq!(ValueType::Float, it.seek(8));
    assert_eq!((8, 1.0), it.at());

    // Again, should prefer the float sample.
    assert_eq!(ValueType::Float, it.next());
    assert_eq!((9, 2.0), it.at());

    // Seek to ts=11 where there are only float samples.
    assert_eq!(ValueType::Float, it.seek(11));
    assert_eq!((11, 4.0), it.at());

    // Seek to ts=15 right before the transition back to histogram samples.
    assert_eq!(ValueType::Float, it.seek(15));
    assert_eq!((15, 8.0), it.at());

    assert_eq!(ValueType::Histogram, it.next());
    assert_eq!((16, histograms[10].clone()), it.at_histogram(None));

    // Getting a float histogram from an int histogram works.
    assert_eq!(ValueType::Histogram, it.next());
    let (ts, fh) = it.at_float_histogram(None);
    assert_eq!(17, ts);
    let expected = prompb::from_int_histogram(17, &histograms[11]).to_float_histogram();
    assert_eq!(expected, fh);

    // Keep calling Next() until the end.
    for _ in 0..3 {
        assert_eq!(ValueType::Histogram, it.next());
    }

    // The iterator is exhausted.
    assert_eq!(ValueType::None, it.next());
    assert_eq!(ValueType::None, it.next());
    // Should also not be able to seek backwards again.
    assert_eq!(ValueType::None, it.seek(1));
}

fn assert_err_is(it: &dyn ChunkIterator, kind: ErrorKind) {
    let err = it.err().expect("expected an error");
    assert!(err.is_histogram(kind), "unexpected error: {err}");
}

#[test]
fn test_concrete_series_iterator_histogram_samples_with_invalid_schema() {
    for schema in [-100, 100] {
        let th = test_histogram();
        let mut h = prompb::from_int_histogram(2, &th);
        h.schema = schema;
        let mut fh = prompb::from_float_histogram(4, &th.to_float());
        fh.schema = schema;
        let series = concrete(&["foo", "bar"], vec![s(1.0, 0), s(2.0, 3)], vec![h, fh]);
        let kind = ErrorKind::HistogramsUnknownSchema;

        let mut it = series.iterator(None);
        assert_eq!(ValueType::Float, it.next());
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_some());
        assert_err_is(&it, kind);

        let mut it = series.iterator(Some(it));
        assert_eq!(ValueType::Float, it.next());
        assert_eq!(ValueType::None, it.next());
        assert_err_is(&it, kind);

        let mut it = series.iterator(Some(it));
        assert_eq!(ValueType::None, it.seek(1));
        assert_err_is(&it, kind);

        let mut it = series.iterator(Some(it));
        assert_eq!(ValueType::Float, it.seek(3));
        assert_eq!(ValueType::None, it.next());
        assert_err_is(&it, kind);

        let mut it = series.iterator(Some(it));
        assert_eq!(ValueType::None, it.seek(4));
        assert_err_is(&it, kind);
    }
}

#[test]
fn test_concrete_series_iterator_histogram_samples_with_missing_bucket() {
    let mut mh = test_histogram().copy();
    mh.positive_spans = vec![Span::new(0, 2)];
    let h = prompb::from_int_histogram(2, &mh);
    let fh = prompb::from_float_histogram(4, &mh.to_float());
    let series = concrete(&["foo", "bar"], vec![s(1.0, 0), s(2.0, 3)], vec![h, fh]);
    let kind = ErrorKind::HistogramSpansBucketsMismatch;

    let mut it = series.iterator(None);
    assert_eq!(ValueType::Float, it.next());
    assert_eq!(ValueType::None, it.next());
    assert!(it.err().is_some());
    assert_err_is(&it, kind);

    let mut it = series.iterator(Some(it));
    assert_eq!(ValueType::Float, it.next());
    assert_eq!(ValueType::None, it.next());
    assert_err_is(&it, kind);

    let mut it = series.iterator(Some(it));
    assert_eq!(ValueType::None, it.seek(1));
    assert_err_is(&it, kind);

    let mut it = series.iterator(Some(it));
    assert_eq!(ValueType::Float, it.seek(3));
    assert_eq!(ValueType::None, it.next());
    assert_err_is(&it, kind);

    let mut it = series.iterator(Some(it));
    assert_eq!(ValueType::None, it.seek(4));
    assert_err_is(&it, kind);
}

#[test]
fn test_concrete_series_iterator_reduces_high_resolution_histograms() {
    for schema in [9, 52] {
        let mut h = test_histogram().copy();
        h.schema = schema;
        let fh = h.to_float();
        let series = concrete(
            &["foo", "bar"],
            vec![],
            vec![prompb::from_int_histogram(1, &h), prompb::from_float_histogram(2, &fh)],
        );
        let mut it = series.iterator(None);
        assert_eq!(ValueType::Histogram, it.next());
        let (_, got_h) = it.at_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, got_h.schema);
        let (_, got_fh) = it.at_float_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, got_fh.schema);
        assert_eq!(ValueType::FloatHistogram, it.next());
        let (_, got_fh) = it.at_float_histogram(None);
        assert_eq!(EXPONENTIAL_SCHEMA_MAX, got_fh.schema);
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none());
    }
}

#[test]
fn test_from_query_result_with_duplicates() {
    let ts1 = prompb::TimeSeries {
        labels: vec![l("foo", "bar"), l("foo", "def")],
        samples: vec![s(0.0, 0)],
        ..Default::default()
    };
    let res = prompb::QueryResult { timeseries: vec![ts1] };

    let mut series = from_query_result(false, res);

    // adapted: Go checks the concrete type errSeriesSet; here the set must hold no series and only the error.
    assert!(!series.next(), "Expected resulting series to be an errSeriesSet");
    let err_message = series.err().expect("expected an error").to_string();
    assert_eq!(
        "duplicate label with name: foo", err_message,
        "Expected error to be from duplicate label, but got: {err_message}"
    );
}

#[test]
fn test_negotiate_response_type() {
    let r = negotiate_response_type(&[ResponseType::STREAMED_XOR_CHUNKS, ResponseType::SAMPLES]).unwrap();
    assert_eq!(ResponseType::STREAMED_XOR_CHUNKS, r);

    let r2 = negotiate_response_type(&[ResponseType::SAMPLES, ResponseType::STREAMED_XOR_CHUNKS]).unwrap();
    assert_eq!(ResponseType::SAMPLES, r2);

    let r3 = negotiate_response_type(&[]).unwrap();
    assert_eq!(ResponseType::SAMPLES, r3);

    let err = negotiate_response_type(&[ResponseType(20)])
        .expect_err("expected error due to not supported requested response types");
    assert_eq!(
        "server does not support any of the requested response types: [20]; supported: map[SAMPLES:{} STREAMED_XOR_CHUNKS:{}]",
        err.to_string()
    );
}

#[test]
fn test_merge_labels() {
    let cases = [
        (
            vec![l("aaa", "foo"), l("bbb", "foo"), l("ddd", "foo")],
            vec![l("bbb", "bar"), l("ccc", "bar")],
            vec![l("aaa", "foo"), l("bbb", "foo"), l("ccc", "bar"), l("ddd", "foo")],
        ),
        (
            vec![l("bbb", "bar"), l("ccc", "bar")],
            vec![l("aaa", "foo"), l("bbb", "foo"), l("ddd", "foo")],
            vec![l("aaa", "foo"), l("bbb", "bar"), l("ccc", "bar"), l("ddd", "foo")],
        ),
    ];
    for (primary, secondary, expected) in cases {
        assert_eq!(expected, merge_labels(&primary, &secondary));
    }
}

#[test]
fn test_decode_read_request_too_large() {
    // 5-byte snappy stream whose header claims 256 MiB decoded length, well above decodeReadLimit (32 MiB).
    let bomb = vec![0x80, 0x80, 0x80, 0x80, 0x01];
    let req = http::Request { method: "POST".into(), path: "/".into(), body: bomb, ..Default::default() };
    let err = decode_read_request(&req).expect_err("expected an error");
    assert!(err.to_string().contains("exceeds limit"), "{err}");
}

#[test]
fn test_decode_write_request() {
    let buf = build_write_request(write_request_fixture().timeseries, Vec::new());
    let actual = decode_write_request(&buf).unwrap();
    assert_eq!(write_request_fixture(), actual);
}

#[test]
fn test_decode_write_v2_request() {
    let f = write_v2_request_fixture();
    let buf = build_v2_write_request(f.timeseries, f.symbols);
    let actual = decode_write_v2_request(&buf).unwrap();
    assert_eq!(write_v2_request_fixture(), actual);
}

/// Go `mockChunkSeriesSet`: chunk series made from proto chunked series.
struct MockChunkSeriesSet {
    series: Vec<ChunkSeriesArc>,
    index: isize,
}

fn new_mock_chunk_series_set(ss: &[prompb::ChunkedSeries]) -> MockChunkSeriesSet {
    let mut b = ScratchBuilder::new(0);
    let series = ss
        .iter()
        .map(|cs| {
            let metas = cs
                .chunks
                .iter()
                .map(|one| {
                    let c = chunkenc::from_data(Encoding(one.r#type.0 as u8), one.data.clone()).unwrap();
                    Meta::new(Arc::from(c), one.min_time_ms, one.max_time_ms)
                })
                .collect();
            Arc::new(ListChunkSeries::from_metas(cs.to_labels(&mut b), metas)) as ChunkSeriesArc
        })
        .collect();
    MockChunkSeriesSet { series, index: -1 }
}

impl ChunkSeriesSet for MockChunkSeriesSet {
    fn next(&mut self) -> bool {
        self.index += 1;
        (self.index as usize) < self.series.len()
    }
    fn at(&self) -> &ChunkSeriesArc {
        &self.series[self.index as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `mockWriter`: decodes each write as a `ChunkedReadResponse`.
#[derive(Default)]
struct MockWriter {
    actual: Vec<prompb::ChunkedSeries>,
}

impl Write for MockWriter {
    fn write(&mut self, p: &[u8]) -> io::Result<usize> {
        let cr = prompb::ChunkedReadResponse::unmarshal(p)
            .map_err(|e| io::Error::other(format!("unmarshaling: {e}")))?;
        self.actual.extend(cr.chunked_series);
        Ok(p.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_stream_response() {
    let lbs1 = prompb::labels_to_label_protos(&labels::from_strings(&["instance", "localhost1", "job", "demo1"]));
    let lbs2 = prompb::labels_to_label_protos(&labels::from_strings(&["instance", "localhost2", "job", "demo2"]));
    let chunk = prompb::Chunk { r#type: prompb::ChunkEncoding::XOR, data: vec![0; 100], ..Default::default() };
    let lb_size: usize = lbs1.iter().map(|l| l.encoded_len()).sum();
    let chunk_size = chunk.encoded_len();
    let max_bytes_in_frame = (lb_size + chunk_size * 2) as i64;
    let test_data = vec![
        prompb::ChunkedSeries { labels: lbs1.clone(), chunks: vec![chunk.clone(); 4] },
        prompb::ChunkedSeries { labels: lbs2.clone(), chunks: vec![chunk.clone(); 4] },
    ];
    let mut css = new_mock_chunk_series_set(&test_data);
    let mut writer = MockWriter::default();
    let (warning, err) = stream_chunked_read_responses(&mut writer, 0, &mut css, &[], max_bytes_in_frame);
    assert!(warning.is_empty());
    assert!(err.is_none(), "{err:?}");
    let expect_data = vec![
        prompb::ChunkedSeries { labels: lbs1.clone(), chunks: vec![chunk.clone(); 2] },
        prompb::ChunkedSeries { labels: lbs1, chunks: vec![chunk.clone(); 2] },
        prompb::ChunkedSeries { labels: lbs2.clone(), chunks: vec![chunk.clone(); 2] },
        prompb::ChunkedSeries { labels: lbs2, chunks: vec![chunk; 2] },
    ];
    assert_eq!(expect_data, writer.actual);
}

const NUM_TEST_CHUNKS: usize = 3;
const NUM_SAMPLES_PER_TEST_CHUNK: usize = 5;

fn build_test_chunks() -> Vec<prompb::Chunk> {
    let mut chks = Vec::with_capacity(NUM_TEST_CHUNKS);
    let mut time = 0i64;
    for i in 0..NUM_TEST_CHUNKS {
        let mut c = XorChunk::new();
        let mut a = c.appender().unwrap();
        let min_time_ms = time;
        for j in 0..NUM_SAMPLES_PER_TEST_CHUNK {
            a.append(&mut c, 0, time, (i + j) as f64);
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

#[test]
fn test_chunked_series_iterator() {
    // happy path
    {
        let chks = build_test_chunks();
        let mut it = new_chunked_series_iterator(&chks, 2000, 12000);

        assert!(it.err().is_none());
        assert!(it.has_cur());

        // Initial next; advance to first valid sample of first chunk.
        assert_eq!(ValueType::Float, it.next());
        assert!(it.err().is_none());
        assert_eq!((2000, 2.0), it.at());

        // Next to the second sample of the first chunk.
        assert_eq!(ValueType::Float, it.next());
        assert!(it.err().is_none());
        assert_eq!((3000, 3.0), it.at());

        // Attempt to seek to the first sample of the first chunk (should return current sample).
        assert_eq!(ValueType::Float, it.seek(0));
        assert_eq!((3000, 3.0), it.at());

        // Seek to the end of the first chunk.
        assert_eq!(ValueType::Float, it.seek(4000));
        assert_eq!((4000, 4.0), it.at());

        // Next to the first sample of the second chunk.
        assert_eq!(ValueType::Float, it.next());
        assert!(it.err().is_none());
        assert_eq!((5000, 1.0), it.at());

        // Seek to the second sample of the third chunk.
        assert_eq!(ValueType::Float, it.seek(10999));
        assert!(it.err().is_none());
        assert_eq!((11000, 3.0), it.at());

        // Attempt to seek to something past the last sample (should return false and exhaust the iterator).
        assert_eq!(ValueType::None, it.seek(99999));
        assert!(it.err().is_none());

        // Attempt to next past the last sample (should return false as the iterator is exhausted).
        assert_eq!(ValueType::None, it.next());
        assert!(it.err().is_none());
    }

    // invalid chunk encoding error
    {
        let mut chks = build_test_chunks();
        // Set chunk type to an invalid value.
        chks[0].r#type = prompb::ChunkEncoding(8);

        let mut it = new_chunked_series_iterator(&chks, 0, 14000);
        assert_eq!(ValueType::None, it.next());
        assert_eq!(ValueType::None, it.seek(1000));

        let err = it.err().expect("expected an error").to_string();
        assert!(err.contains("invalid chunk encoding"), "{err}");
        assert!(!it.has_cur());
    }

    // empty chunks
    {
        let empty_chunks: Vec<prompb::Chunk> = Vec::new();
        let mut it1 = new_chunked_series_iterator(&empty_chunks, 0, 1000);
        assert_eq!(ValueType::None, it1.next());
        assert_eq!(ValueType::None, it1.seek(1000));
        assert!(it1.err().is_none());
        // adapted: Go also checks a nil slice; Rust has one empty slice.
    }
}

#[test]
fn test_chunked_series() {
    let chks = build_test_chunks();
    let s = RemoteChunkedSeries::new(
        prompb::ChunkedSeries { labels: vec![l("foo", "bar"), l("asdf", "zxcv")], chunks: chks },
        0,
        0,
    );

    assert_eq!(&labels::from_strings(&["asdf", "zxcv", "foo", "bar"]), s.labels());

    let mut it = s.iterator(None);
    let res = it.next(); // Behavior is undefined w/o the initial call to Next.

    assert_eq!(ValueType::Float, res);
    assert!(it.err().is_none());
    assert_eq!((0, 0.0), it.at());
}

/// Go `oneShotCloser`: fails reads after close. Here the set drops the reader on close; a read through a
/// dropped reader cannot happen, so the shared flag records whether the reader was dropped.
struct OneShotCloser {
    r: Cursor<Vec<u8>>,
    closed: Arc<Mutex<bool>>,
}

impl Read for OneShotCloser {
    fn read(&mut self, p: &mut [u8]) -> io::Result<usize> {
        if *self.closed.lock().unwrap() {
            return Err(io::Error::other("already closed"));
        }
        self.r.read(p)
    }
}

impl Drop for OneShotCloser {
    fn drop(&mut self) {
        let mut c = self.closed.lock().unwrap();
        assert!(!*c, "already closed");
        *c = true;
    }
}

fn write_test_responses(corrupt: bool) -> Vec<u8> {
    let mut w = ChunkedWriter::new(Vec::new(), NopFlusher);
    let chks = build_test_chunks();
    let lbls = vec![l("foo", "bar")];
    for (i, c) in chks.into_iter().enumerate() {
        let read_resp = prompb::ChunkedReadResponse {
            chunked_series: vec![prompb::ChunkedSeries { labels: lbls.clone(), chunks: vec![c] }],
            query_index: i as i64,
        };
        let mut b = read_resp.marshal();
        if corrupt {
            b[0] = 0xFF; // Corruption!
        }
        w.write_frame(&b).unwrap();
    }
    w.into_inner().0
}

#[test]
fn test_chunked_series_set() {
    // happy path
    {
        let closed = Arc::new(Mutex::new(false));
        let wrapped_reader = OneShotCloser { r: Cursor::new(write_test_responses(false)), closed: closed.clone() };
        let r = ChunkedReader::new(wrapped_reader, config::DEFAULT_CHUNKED_READ_LIMIT, Vec::new());

        let mut ss = new_chunked_series_set(r, 0, 14000, |_| {});
        assert!(ss.err().is_none());
        assert!(ss.warnings().is_empty());

        assert!(ss.next());
        assert!(ss.err().is_none());

        let s = ss.at().clone();
        assert_eq!(1, s.labels().len());
        assert!(s.labels().has("foo"));
        assert_eq!("bar", s.labels().get("foo"));

        let mut it = s.iterator(None);
        it.next();
        assert_eq!((0, 0.0), it.at());

        let mut num_responses = 1;
        while ss.next() {
            num_responses += 1;
        }
        assert_eq!(NUM_TEST_CHUNKS, num_responses);
        assert!(ss.err().is_none());
        assert!(*closed.lock().unwrap());

        assert!(!ss.next(), "Next() should still return false after it previously returned false");
        assert!(
            ss.err().is_none(),
            "Err() should not return an error if Next() is called again after it previously returned false"
        );
    }

    // chunked reader error
    {
        let r = ChunkedReader::new(Cursor::new(write_test_responses(true)), config::DEFAULT_CHUNKED_READ_LIMIT, Vec::new());
        let mut ss = new_chunked_series_set(r, 0, 14000, |_| {});
        assert!(ss.err().is_none());
        assert!(ss.warnings().is_empty());

        assert!(!ss.next());
        let err = ss.err().expect("expected an error").to_string();
        assert!(err.contains("proto: illegal wireType 7"), "{err}");
    }
}
