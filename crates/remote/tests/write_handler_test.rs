//! Port of `storage/remote/write_handler_test.go`.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use common::*;
use discovery::log::Logger;
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram, Span, CUSTOM_BUCKETS_SCHEMA};
use model::labels::{from_strings, Labels, ScratchBuilder};
use model::metadata::Metadata;
use promql::promqltest::teststorage::ERR_INVALID_SAMPLE;
use remote::client::{Client, ClientConfig, WriteClient};
use remote::prompb::{self, writev2};
use remote::remoteapi::{MessageTypes, WriteHandler, WriteMessageType, VERSION20_HEADER_VALUE, VERSION_HEADER};
use remote::stats::{RW20_WRITTEN_EXEMPLARS_HEADER, RW20_WRITTEN_HISTOGRAMS_HEADER, RW20_WRITTEN_SAMPLES_HEADER};
use remote::write_handler::{new_write_handler, TimeLimitAppenderV2};
use storage::{
    AppendPartialError, AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, Error, ExemplarAppenderV2,
    ExemplarError, SeriesRef, ERR_DUPLICATE_EXEMPLAR, ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_NOT_FOUND, ERR_OUT_OF_BOUNDS,
    ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE,
};

const SNAPPY: &str = "snappy";

// ---------------------------------------------------------------- helpers

/// Go `http.NewRequest(http.MethodPost, "", bytes.NewReader(payload))` plus `req.Header.Set(k, v)` for each header.
fn new_request(payload: &[u8], headers: &[(&str, &str)]) -> http::Request {
    http::Request {
        method: "POST".to_string(),
        headers: headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), v.to_string())).collect(),
        body: payload.to_vec(),
        ..Default::default()
    }
}

fn handler(
    appendable: Arc<dyn AppendableV2>,
    accepted: Vec<WriteMessageType>,
    enable_type_and_unit_labels: bool,
    append_metadata: bool,
) -> WriteHandler {
    new_write_handler(Logger::nop(), None, appendable, MessageTypes(accepted), enable_type_and_unit_labels, append_metadata)
}

fn serve(h: &WriteHandler, req: http::Request) -> http::Response {
    h.serve(&Context::background(), req)
}

fn body_string(resp: &http::Response) -> String {
    String::from_utf8(resp.body_bytes().to_vec()).unwrap()
}

/// Go `expectHeaderValue`.
fn expect_header_value(expected: i64, got: Option<&str>) {
    let got = got.unwrap_or("");
    assert!(!got.is_empty(), "header is empty");
    let i: i64 = got.parse().expect("Atoi");
    assert_eq!(expected, i);
}

fn v2_headers() -> [(&'static str, &'static str); 3] {
    [
        ("Content-Type", WriteMessageType::V2.content_type_header()),
        ("Content-Encoding", SNAPPY),
        (VERSION_HEADER, VERSION20_HEADER_VALUE),
    ]
}

fn fixture_v1_payload() -> Vec<u8> {
    build_write_request(write_request_fixture().timeseries, Vec::new())
}

fn fixture_v2_payload() -> Vec<u8> {
    let f = write_v2_request_fixture();
    build_v2_write_request(f.timeseries, f.symbols)
}

fn symbolize_labels(st: &mut writev2::SymbolsTable, ls: &Labels) -> Vec<u32> {
    let mut buf = Vec::new();
    st.symbolize_labels(ls, &mut buf);
    buf
}

// ---------------------------------------------------------------- mockAppendable

#[derive(Clone, Debug, PartialEq)]
struct MockSample {
    l: Labels,
    st: i64,
    t: i64,
    v: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct MockExemplar {
    l: Labels,
    el: Labels,
    t: i64,
    v: f64,
}

#[derive(Clone, Debug, PartialEq)]
struct MockHistogram {
    l: Labels,
    st: i64,
    t: i64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
}

#[derive(Clone, Debug, PartialEq)]
struct MockMetadata {
    l: Labels,
    m: Metadata,
}

/// Go `mockAppendable` (the recorded data and the injected errors).
#[derive(Default)]
struct MockState {
    latest_sample: HashMap<u64, i64>,
    samples: Vec<MockSample>,
    latest_exemplar: HashMap<u64, i64>,
    exemplars: Vec<MockExemplar>,
    latest_histogram: HashMap<u64, i64>,
    latest_float_hist: HashMap<u64, i64>,
    histograms: Vec<MockHistogram>,
    metadata: Vec<MockMetadata>,

    ingest_st_zero_sample: bool,

    // Optional errors to inject.
    commit_err: Option<Error>,
    append_sample_err: Option<Error>,
    append_st_zero_sample_err: Option<Error>,
    append_histogram_err: Option<Error>,
    append_exemplar_err: Option<Error>,
    update_metadata_err: Option<Error>,
}

fn is_partial(err: &Error) -> bool {
    err.is(&Error::AppendPartial(Arc::new(AppendPartialError::default())))
}

fn has_duplicates(l: &Labels) -> bool {
    l.has_duplicate_label_names().is_some()
}

impl MockState {
    /// Go `mockAppendable.Append` (returns the reference next to the error, as Go does).
    fn append(&mut self, l: &Labels, t: i64, v: f64) -> (SeriesRef, Option<Error>) {
        if let Some(e) = &self.append_sample_err {
            if !is_partial(e) {
                return (0, Some(e.clone()));
            }
        }
        let hash = l.hash();
        let latest_ts = self.latest_sample.get(&hash).copied().unwrap_or(0);
        if t < latest_ts {
            return (0, Some(ERR_OUT_OF_ORDER_SAMPLE));
        }
        if t == latest_ts {
            return (0, Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP));
        }
        if l.is_empty() {
            return (0, Some(ERR_INVALID_SAMPLE));
        }
        if has_duplicates(l) {
            return (0, Some(ERR_INVALID_SAMPLE));
        }
        self.latest_sample.insert(hash, t);
        self.samples.push(MockSample { l: l.clone(), st: 0, t, v });
        (hash, self.append_sample_err.clone())
    }

    fn commit(&mut self) -> Option<Error> {
        if self.commit_err.is_some() {
            let _ = self.rollback(); // As per Commit method contract.
        }
        self.commit_err.clone()
    }

    fn rollback(&mut self) -> Option<Error> {
        self.samples.clear();
        self.exemplars.clear();
        self.histograms.clear();
        self.metadata.clear();
        None
    }

    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        if let Some(err) = &self.append_exemplar_err {
            return Err(err.clone());
        }
        let mut hash = r;
        if hash == 0 {
            hash = l.hash();
        }
        if let Some(&latest_ts) = self.latest_exemplar.get(&hash) {
            if e.ts < latest_ts {
                return Err(ERR_OUT_OF_ORDER_EXEMPLAR);
            }
            if e.ts == latest_ts {
                return Err(ERR_DUPLICATE_EXEMPLAR);
            }
        }
        self.latest_exemplar.insert(hash, e.ts);
        self.exemplars.push(MockExemplar { l: l.clone(), el: e.labels.clone(), t: e.ts, v: e.value });
        Ok(hash)
    }

    fn append_histogram(
        &mut self,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> (SeriesRef, Option<Error>) {
        if let Some(e) = &self.append_histogram_err {
            if !is_partial(e) {
                return (0, Some(e.clone()));
            }
        }
        let hash = l.hash();
        let latest_ts = if h.is_some() {
            self.latest_histogram.get(&hash).copied().unwrap_or(0)
        } else {
            self.latest_float_hist.get(&hash).copied().unwrap_or(0)
        };
        if t < latest_ts {
            return (0, Some(ERR_OUT_OF_ORDER_SAMPLE));
        }
        if t == latest_ts {
            return (0, Some(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP));
        }
        if l.is_empty() {
            return (0, Some(ERR_INVALID_SAMPLE));
        }
        if has_duplicates(l) {
            return (0, Some(ERR_INVALID_SAMPLE));
        }
        if h.is_some() {
            self.latest_histogram.insert(hash, t);
        } else {
            self.latest_float_hist.insert(hash, t);
        }
        self.histograms.push(MockHistogram { l: l.clone(), st: 0, t, h: h.cloned(), fh: fh.cloned() });
        (hash, self.append_histogram_err.clone())
    }

    fn append_histogram_st_zero_sample(
        &mut self,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
    ) -> Result<SeriesRef, Error> {
        if let Some(e) = &self.append_st_zero_sample_err {
            return Err(e.clone());
        }
        // Created Timestamp can't be higher than the original sample's timestamp.
        if st > t {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        let hash = l.hash();
        let latest_ts = if h.is_some() {
            self.latest_histogram.get(&hash).copied().unwrap_or(0)
        } else {
            self.latest_float_hist.get(&hash).copied().unwrap_or(0)
        };
        if st < latest_ts {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if st == latest_ts {
            return Err(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP);
        }
        if l.is_empty() {
            return Err(ERR_INVALID_SAMPLE);
        }
        if has_duplicates(l) {
            return Err(ERR_INVALID_SAMPLE);
        }
        if h.is_some() {
            self.latest_histogram.insert(hash, st);
            self.histograms.push(MockHistogram { l: l.clone(), st: 0, t: st, h: Some(Histogram::default()), fh: None });
        } else {
            self.latest_float_hist.insert(hash, st);
            self.histograms.push(MockHistogram { l: l.clone(), st: 0, t: st, h: None, fh: Some(FloatHistogram::default()) });
        }
        Ok(hash)
    }

    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, mp: &Metadata) -> Result<SeriesRef, Error> {
        if let Some(e) = &self.update_metadata_err {
            return Err(e.clone());
        }
        for md in self.metadata.iter().rev() {
            if md.l == *l {
                if md.m.equals(mp) {
                    return Ok(r);
                }
                break;
            }
        }
        self.metadata.push(MockMetadata { l: l.clone(), m: mp.clone() });
        Ok(r)
    }

    fn append_st_zero_sample(&mut self, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        if let Some(e) = &self.append_st_zero_sample_err {
            return Err(e.clone());
        }
        // Created Timestamp can't be higher than the original sample's timestamp.
        if st > t {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        let hash = l.hash();
        let latest_ts = self.latest_sample.get(&hash).copied().unwrap_or(0);
        if st < latest_ts {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if st == latest_ts {
            return Err(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP);
        }
        if l.is_empty() {
            return Err(ERR_INVALID_SAMPLE);
        }
        if has_duplicates(l) {
            return Err(ERR_INVALID_SAMPLE);
        }
        self.latest_sample.insert(hash, st);
        self.samples.push(MockSample { l: l.clone(), st: 0, t: st, v: 0.0 });
        Ok(hash)
    }

    fn series_exists(&self, hash: u64) -> bool {
        self.latest_sample.contains_key(&hash)
            || self.latest_histogram.contains_key(&hash)
            || self.latest_float_hist.contains_key(&hash)
    }

    /// Go `mockAppenderV2.AppendExemplars`.
    fn append_exemplars(&mut self, r: SeriesRef, l: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        let mut hash = r;
        if hash == 0 {
            hash = l.hash();
        }
        if !self.series_exists(hash) {
            return Err(Error::wrapped(format!("unknown series ref {}: {}", r, ERR_NOT_FOUND), ERR_NOT_FOUND));
        }
        let r = hash;
        let mut errs: Vec<ExemplarError> = Vec::new();
        for (i, e) in exemplars.iter().enumerate() {
            if let Err(err) = self.append_exemplar(r, l, e) {
                if !err.is(&ERR_DUPLICATE_EXEMPLAR) {
                    errs.push(ExemplarError { index: i, err });
                }
            }
        }
        if !errs.is_empty() {
            return Err(Error::AppendPartial(Arc::new(AppendPartialError { series_ref: r, exemplar_errors: errs })));
        }
        Ok(r)
    }
}

/// Go `mockAppendable`: shared by the test and the appenders it hands out.
#[derive(Clone, Default)]
struct MockAppendable(Arc<Mutex<MockState>>);

impl MockAppendable {
    fn new(s: MockState) -> MockAppendable {
        MockAppendable(Arc::new(Mutex::new(s)))
    }
    fn st(&self) -> MutexGuard<'_, MockState> {
        self.0.lock().unwrap()
    }
    fn arc(&self) -> Arc<dyn AppendableV2> {
        Arc::new(self.clone())
    }
}

impl AppendableV2 for MockAppendable {
    /// Returns a storage.AppenderV2 (which also implements storage.ExemplarAppenderV2) that records into the
    /// mockAppendable slices.
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(MockAppenderV2(self.0.clone()))
    }
}

/// Go `mockAppenderV2`.
struct MockAppenderV2(Arc<Mutex<MockState>>);

/// Go `return ref, err` where err may be an `*AppendPartialError`: the Rust partial error carries the reference.
fn to_result(r: SeriesRef, err: Option<Error>) -> Result<SeriesRef, Error> {
    match err {
        None => Ok(r),
        Some(e) => match e.as_append_partial() {
            Some(p) => Err(Error::AppendPartial(Arc::new(AppendPartialError {
                series_ref: r,
                exemplar_errors: p.exemplar_errors.clone(),
            }))),
            None => Err(e),
        },
    }
}

impl AppenderTransaction for MockAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.0.lock().unwrap().commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.lock().unwrap().rollback()
    }
}

impl AppenderV2 for MockAppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        let _ = r;
        let mut m = self.0.lock().unwrap();
        if m.ingest_st_zero_sample && st != 0 && t != 0 {
            // Mirror a real AppenderV2 implementation, which owns ST zero-sample injection internally and does
            // not surface its errors to the caller (e.g. TSDB head's bestEffortAppendSTZeroSample).
            if h.is_some() || fh.is_some() {
                let _ = m.append_histogram_st_zero_sample(l, t, st, h);
            } else {
                let _ = m.append_st_zero_sample(l, t, st);
            }
        }

        let (r, err);
        if h.is_some() || fh.is_some() {
            (r, err) = m.append_histogram(l, t, h, fh);
            if err.as_ref().is_none_or(is_partial) {
                m.histograms.last_mut().unwrap().st = st;
            }
        } else {
            (r, err) = m.append(l, t, v);
            if err.as_ref().is_none_or(is_partial) {
                m.samples.last_mut().unwrap().st = st;
            }
        }
        if let Some(e) = &err {
            if !is_partial(e) {
                return Err(e.clone());
            }
        }
        if let Some(md) = opts.metadata {
            if !md.is_empty() {
                // Metadata is best-effort within Append; errors are not surfaced to the caller.
                let _ = m.update_metadata(r, l, md);
            }
        }
        if !opts.exemplars.is_empty() {
            return m.append_exemplars(r, l, opts.exemplars);
        }
        to_result(r, err)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for MockAppenderV2 {
    fn append_exemplars(&mut self, r: SeriesRef, l: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        self.0.lock().unwrap().append_exemplars(r, l, exemplars)
    }
}

/// Go `mockAppendableV2Only`.
struct MockAppendableV2Only {
    rolled_back: Arc<AtomicBool>,
}

/// Go `mockAppenderV2Only`: embeds a nil storage.AppenderV2, so only Rollback works.
struct MockAppenderV2Only {
    rolled_back: Arc<AtomicBool>,
}

impl AppenderTransaction for MockAppenderV2Only {
    fn commit(&mut self) -> Option<Error> {
        panic!("nil storage.AppenderV2: Commit")
    }
    fn rollback(&mut self) -> Option<Error> {
        self.rolled_back.store(true, Ordering::SeqCst);
        None
    }
}

impl AppenderV2 for MockAppenderV2Only {
    fn append(
        &mut self,
        _r: SeriesRef,
        _l: &Labels,
        _st: i64,
        _t: i64,
        _v: f64,
        _h: Option<&Histogram>,
        _fh: Option<&FloatHistogram>,
        _opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        panic!("nil storage.AppenderV2: Append")
    }
}

impl AppendableV2 for MockAppendableV2Only {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(MockAppenderV2Only { rolled_back: self.rolled_back.clone() })
    }
}

/// Go `highSchemaHistogram`.
fn high_schema_histogram() -> Histogram {
    Histogram {
        schema: 10,
        positive_spans: vec![Span::new(-1, 2)],
        positive_buckets: vec![1, 2],
        negative_spans: vec![Span::new(0, 1)],
        negative_buckets: vec![1],
        ..Default::default()
    }
}

/// Go `reducedSchemaHistogram`.
fn reduced_schema_histogram() -> Histogram {
    Histogram {
        schema: 8,
        positive_spans: vec![Span::new(0, 1)],
        positive_buckets: vec![4],
        negative_spans: vec![Span::new(0, 1)],
        negative_buckets: vec![1],
        ..Default::default()
    }
}

// ---------------------------------------------------------------- tests

#[test]
fn test_remote_write_handler_headers_handling_v1_message() {
    let payload = fixture_v1_payload();
    let v1_ct = WriteMessageType::V1.content_type_header();
    let proto_yolo = format!("{};proto=yolo", remote::remoteapi::APP_PROTO_CONTENT_TYPE);

    // Generally Prometheus 1.0 Receiver never checked for existence of the headers, so we keep things permissive.
    let cases: Vec<(&str, Vec<(&str, &str)>, u16)> = vec![
        (
            "correct PRW 1.0 headers",
            vec![("Content-Type", v1_ct), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            204,
        ),
        ("missing remote write version", vec![("Content-Type", v1_ct), ("Content-Encoding", SNAPPY)], 204),
        ("no headers", vec![], 204),
        ("missing content-type", vec![("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)], 204),
        ("missing content-encoding", vec![("Content-Type", v1_ct), (VERSION_HEADER, VERSION20_HEADER_VALUE)], 204),
        (
            "wrong content-type",
            vec![("Content-Type", "yolo"), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
        ),
        (
            "wrong content-type2",
            vec![("Content-Type", &proto_yolo), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
        ),
        (
            "not supported content-encoding",
            vec![("Content-Type", v1_ct), ("Content-Encoding", "zstd"), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
        ),
    ];
    for (name, headers, expected_code) in cases {
        let req = new_request(&payload, &headers);
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
        let resp = serve(&h, req);
        assert_eq!(expected_code, resp.status, "{name}: {}", body_string(&resp));
    }
}

#[test]
fn test_remote_write_handler_headers_handling_v2_message() {
    let payload = fixture_v2_payload();
    let v2_ct = WriteMessageType::V2.content_type_header();
    let proto_yolo = format!("{};proto=yolo", remote::remoteapi::APP_PROTO_CONTENT_TYPE);

    let cases: Vec<(&str, Vec<(&str, &str)>, u16, &str)> = vec![
        (
            "correct PRW 2.0 headers",
            vec![("Content-Type", v2_ct), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            204,
            "",
        ),
        // We don't check for now.
        ("missing remote write version", vec![("Content-Type", v2_ct), ("Content-Encoding", SNAPPY)], 204, ""),
        (
            "no headers",
            vec![],
            415,
            "prometheus.WriteRequest protobuf message is not accepted by this server; only accepts io.prometheus.write.v2.Request",
        ),
        (
            // This only gives 415, because we explicitly only support 2.0. If we supported both (default) it
            // would be empty message parsed and ok response. This is perhaps better, than 415 for previously
            // working 1.0 flow with no content-type.
            "missing content-type",
            vec![("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
            "prometheus.WriteRequest protobuf message is not accepted by this server; only accepts io.prometheus.write.v2.Request",
        ),
        // Similar to 1.0 impl, we default to Snappy, so it works.
        ("missing content-encoding", vec![("Content-Type", v2_ct), (VERSION_HEADER, VERSION20_HEADER_VALUE)], 204, ""),
        (
            "wrong content-type",
            vec![("Content-Type", "yolo"), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
            "expected application/x-protobuf as the first (media) part, got yolo content-type",
        ),
        (
            "wrong content-type2",
            vec![("Content-Type", &proto_yolo), ("Content-Encoding", SNAPPY), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
            "got application/x-protobuf;proto=yolo content type; unknown type for remote write protobuf message yolo, supported: prometheus.WriteRequest, io.prometheus.write.v2.Request",
        ),
        (
            "not supported content-encoding",
            vec![("Content-Type", v2_ct), ("Content-Encoding", "zstd"), (VERSION_HEADER, VERSION20_HEADER_VALUE)],
            415,
            "zstd encoding (compression) is not accepted by this server; only snappy is acceptable",
        ),
    ];
    for (name, headers, expected_code, expected_error) in cases {
        let req = new_request(&payload, &headers);
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let resp = serve(&h, req);
        let out = body_string(&resp);
        assert_eq!(expected_code, resp.status, "{name}: {out}");
        if expected_code / 100 == 2 {
            continue;
        }

        // Invalid request case - no samples should be written.
        assert_eq!(expected_error, out.trim(), "{name}");
        let st = appendable.st();
        assert!(st.samples.is_empty(), "{name}");
        assert!(st.histograms.is_empty(), "{name}");
        assert!(st.exemplars.is_empty(), "{name}");
    }

    // unsupported v1 request
    {
        let payload = fixture_v1_payload();
        let req = new_request(
            &payload,
            &[("Content-Type", WriteMessageType::V1.content_type_header()), ("Content-Encoding", SNAPPY)],
        );
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let resp = serve(&h, req);
        let out = body_string(&resp);
        assert_eq!(415, resp.status, "{out}");
        assert_eq!(
            "prometheus.WriteRequest protobuf message is not accepted by this server; only accepts io.prometheus.write.v2.Request",
            out.trim()
        );
        let st = appendable.st();
        assert!(st.samples.is_empty());
        assert!(st.histograms.is_empty());
        assert!(st.exemplars.is_empty());
    }
}

#[test]
fn test_remote_write_handler_v1_message() {
    let payload = fixture_v1_payload();
    let req = new_request(&payload, &[]);

    // NOTE: Strictly speaking, even for 1.0 we require headers, but we never verified those in Prometheus, so
    // keeping like this to not break existing 1.0 clients.

    let appendable = MockAppendable::default();
    let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
    let resp = serve(&h, req);
    assert_eq!(204, resp.status);

    let st = appendable.st();
    let mut b = ScratchBuilder::new(0);
    let (mut i, mut j, mut k) = (0, 0, 0);
    for ts in &write_request_fixture().timeseries {
        let labels = ts.to_labels(&mut b);
        for s in &ts.samples {
            assert_eq!(MockSample { l: labels.clone(), st: 0, t: s.timestamp, v: s.value }, st.samples[i]);
            i += 1;
        }
        for e in &ts.exemplars {
            let exemplar_labels = e.to_exemplar(&mut b).labels;
            assert_eq!(MockExemplar { l: labels.clone(), el: exemplar_labels, t: e.timestamp, v: e.value }, st.exemplars[j]);
            j += 1;
        }
        for hp in &ts.histograms {
            if hp.is_float_histogram() {
                let fh = hp.to_float_histogram();
                assert_eq!(
                    MockHistogram { l: labels.clone(), st: 0, t: hp.timestamp, h: None, fh: Some(fh) },
                    st.histograms[k]
                );
            } else {
                let h = hp.to_int_histogram();
                assert_eq!(MockHistogram { l: labels.clone(), st: 0, t: hp.timestamp, h, fh: None }, st.histograms[k]);
            }
            k += 1;
        }
    }
}

#[derive(Default)]
struct V2Case {
    desc: &'static str,
    input: Vec<writev2::TimeSeries>,
    /// Custom symbol table for tests that need it.
    symbols: Option<Vec<String>>,
    expected_code: u16,
    expected_resp_body: &'static str,

    commit_err: Option<Error>,
    append_sample_err: Option<Error>,
    append_st_zero_sample_err: Option<Error>,
    append_histogram_err: Option<Error>,
    append_exemplar_err: Option<Error>,
    update_metadata_err: Option<Error>,

    ingest_st_zero_sample: bool,
    enable_type_and_unit_labels: bool,
    append_metadata: bool,
    /// For verifying type/unit labels.
    expected_labels: Labels,
}

/// The input series and symbols of the type-and-unit test cases: one series with `lbls`, metadata of type
/// `typ`, help and unit symbolized (in that order, when not empty), and one sample.
fn typed_series(lbls: &[&str], typ: writev2::MetadataType, help: &str, unit: &str, v: f64, t: i64) -> (Vec<writev2::TimeSeries>, Vec<String>) {
    let mut st = writev2::SymbolsTable::new();
    let labels_refs = symbolize_labels(&mut st, &from_strings(lbls));
    let help_ref = if help.is_empty() { 0 } else { st.symbolize(help) };
    let unit_ref = if unit.is_empty() { 0 } else { st.symbolize(unit) };
    let ts = vec![writev2::TimeSeries {
        labels_refs,
        metadata: writev2::Metadata { r#type: typ, help_ref, unit_ref },
        samples: vec![writev2::Sample { value: v, timestamp: t, start_timestamp: 0 }],
        ..Default::default()
    }];
    (ts, st.symbols().to_vec())
}

fn prepend(first: writev2::TimeSeries) -> Vec<writev2::TimeSeries> {
    let mut v = vec![first];
    v.extend(write_v2_request_fixture().timeseries);
    v
}

fn one_sample_series(labels_refs: Vec<u32>) -> writev2::TimeSeries {
    writev2::TimeSeries {
        labels_refs,
        samples: vec![writev2::Sample { value: 1.0, timestamp: 1, start_timestamp: 0 }],
        ..Default::default()
    }
}

#[test]
fn test_remote_write_handler_v2_message() {
    use writev2::MetadataType as MT;
    let fixture = || write_v2_request_fixture().timeseries;
    let typed = |lbls: &[&str], typ, help, unit, v, t, enable: bool, append_md: bool, expected: &[&str], desc| {
        let (input, symbols) = typed_series(lbls, typ, help, unit, v, t);
        V2Case {
            desc,
            input,
            symbols: Some(symbols),
            expected_code: 204,
            enable_type_and_unit_labels: enable,
            append_metadata: append_md,
            expected_labels: from_strings(expected),
            ..Default::default()
        }
    };

    // V2 supports partial writes for non-retriable errors, so test them.
    let cases = vec![
        V2Case { desc: "All timeseries accepted/ct_enabled", input: fixture(), expected_code: 204, ingest_st_zero_sample: true, ..Default::default() },
        V2Case { desc: "All timeseries accepted/ct_disabled", input: fixture(), expected_code: 204, ..Default::default() },
        V2Case {
            desc: "Partial write; first series with invalid labels (no metric name)",
            // Series with test_metric1="test_metric1" labels.
            input: prepend(one_sample_series(vec![2, 2])),
            expected_code: 400,
            expected_resp_body: "invalid metric name or labels, got {test_metric1=\"test_metric1\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with invalid labels (empty metric name)",
            // Series with __name__="" labels.
            input: prepend(one_sample_series(vec![1, 0])),
            expected_code: 400,
            expected_resp_body: "invalid metric name or labels, got {__name__=\"\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with duplicate labels",
            // Series with __name__="test_metric1",test_metric1="test_metric1",test_metric1="test_metric1" labels.
            input: prepend(one_sample_series(vec![1, 2, 2, 2, 2, 2])),
            expected_code: 400,
            expected_resp_body: "invalid labels for series, labels {__name__=\"test_metric1\", test_metric1=\"test_metric1\", test_metric1=\"test_metric1\"}, duplicated label test_metric1\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with odd number of label refs",
            input: prepend(one_sample_series(vec![1, 2, 3])),
            expected_code: 400,
            expected_resp_body: "parsing labels for series [1 2 3]: invalid labelRefs length 3\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with out-of-bounds symbol references",
            input: prepend(one_sample_series(vec![1, 999])),
            expected_code: 400,
            expected_resp_body: "parsing labels for series [1 999]: labelRefs 1 (name) = 999 (value) outside of symbols table (size 18)\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with out-of-bounds metadata unit ref",
            input: prepend(writev2::TimeSeries {
                metadata: writev2::Metadata { r#type: MT::GAUGE, help_ref: 0, unit_ref: 999 },
                ..one_sample_series(vec![1, 2])
            }),
            expected_code: 400,
            expected_resp_body: "parsing metadata for series [1 2]: metadata unit_ref 999 outside of symbols table (size 18)\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with out-of-bounds metadata help ref",
            input: prepend(writev2::TimeSeries {
                metadata: writev2::Metadata { r#type: MT::GAUGE, help_ref: 999, unit_ref: 0 },
                ..one_sample_series(vec![1, 2])
            }),
            expected_code: 400,
            expected_resp_body: "parsing metadata for series [1 2]: metadata help_ref 999 outside of symbols table (size 18)\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; TimeSeries with only exemplars (no samples or histograms)",
            // Series with only exemplars, no samples or histograms.
            input: prepend(writev2::TimeSeries {
                labels_refs: vec![1, 2],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![], value: 1.0, timestamp: 1 }],
                ..Default::default()
            }),
            expected_code: 400,
            expected_resp_body: "TimeSeries must contain at least one sample or histogram for series {__name__=\"test_metric1\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with one OOO sample",
            input: {
                let mut f = fixture();
                f[0].samples.push(writev2::Sample { value: 2.0, timestamp: 0, start_timestamp: 0 });
                f
            },
            expected_code: 400,
            expected_resp_body: "out of order sample for series {__name__=\"test_metric1\", b=\"c\", baz=\"qux\", d=\"e\", foo=\"bar\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with one dup sample",
            input: {
                let mut f = fixture();
                let s0 = f[0].samples[0];
                f[0].samples.push(s0);
                f
            },
            expected_code: 400,
            expected_resp_body: "duplicate sample for timestamp for series {__name__=\"test_metric1\", b=\"c\", baz=\"qux\", d=\"e\", foo=\"bar\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with one OOO histogram sample",
            input: {
                let mut f = fixture();
                f[0].histograms.push(writev2::from_float_histogram(0, 1, &test_histogram().to_float()));
                f
            },
            expected_code: 400,
            expected_resp_body: "out of order sample for series {__name__=\"test_metric1\", b=\"c\", baz=\"qux\", d=\"e\", foo=\"bar\"}\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; first series with one dup histogram sample",
            input: {
                let mut f = fixture();
                let last = f[0].histograms.last().unwrap().clone();
                f[0].histograms.push(last);
                f
            },
            expected_code: 400,
            expected_resp_body: "duplicate sample for timestamp for series {__name__=\"test_metric1\", b=\"c\", baz=\"qux\", d=\"e\", foo=\"bar\"}\n",
            ..Default::default()
        },
        // Non retriable errors from various parts.
        V2Case {
            desc: "Internal sample append error; rollback triggered",
            input: fixture(),
            append_sample_err: Some(Error::msg("some sample internal append error")),
            expected_code: 500,
            expected_resp_body: "some sample internal append error\n",
            ..Default::default()
        },
        V2Case {
            desc: "Internal histogram sample append error; rollback triggered",
            input: fixture(),
            append_histogram_err: Some(Error::msg("some histogram sample internal append error")),
            expected_code: 500,
            expected_resp_body: "some histogram sample internal append error\n",
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; skipped exemplar; exemplar storage errs are noop",
            input: fixture(),
            append_exemplar_err: Some(Error::msg("some exemplar internal append error")),
            expected_code: 204,
            ..Default::default()
        },
        V2Case {
            desc: "Partial write; skipped metadata; metadata storage errs are noop",
            input: fixture(),
            update_metadata_err: Some(Error::msg("some metadata update error")),
            expected_code: 204,
            ..Default::default()
        },
        V2Case {
            desc: "Internal commit error; rollback triggered",
            input: fixture(),
            commit_err: Some(Error::msg("storage error")),
            expected_code: 500,
            expected_resp_body: "storage error\n",
            ..Default::default()
        },
        // Type and unit labels tests.
        typed(
            &["__name__", "test_metric", "foo", "bar"], MT::COUNTER, "", "bytes", 1.0, 1000, true, false,
            &["__name__", "test_metric", "__type__", "counter", "__unit__", "bytes", "foo", "bar"],
            "Type and unit labels enabled with counter and bytes unit",
        ),
        typed(
            &["__name__", "test_metric", "foo", "bar"], MT::GAUGE, "", "seconds", 1.0, 1000, true, false,
            &["__name__", "test_metric", "__type__", "gauge", "__unit__", "seconds", "foo", "bar"],
            "Type and unit labels enabled with gauge and seconds unit",
        ),
        typed(
            &["__name__", "test_metric", "foo", "bar"], MT::COUNTER, "", "bytes", 1.0, 1000, false, false,
            &["__name__", "test_metric", "foo", "bar"],
            "Type and unit labels disabled - no metadata labels",
        ),
        typed(
            &["__name__", "test_metric_wal", "instance", "localhost"], MT::GAUGE, "Test metric for WAL verification", "seconds", 42.0, 2000, false, false,
            &["__name__", "test_metric_wal", "instance", "localhost"],
            "Metadata-wal-records disabled - metadata should not be stored in WAL",
        ),
        typed(
            &["__name__", "test_metric_wal", "instance", "localhost"], MT::GAUGE, "Test metric for WAL verification", "seconds", 42.0, 2000, false, true,
            &["__name__", "test_metric_wal", "instance", "localhost"],
            "Metadata-wal-records enabled - metadata stored via AppendV2Options",
        ),
        typed(
            &["__name__", "test_metric", "foo", "bar"], MT::UNSPECIFIED, "", "", 1.0, 1000, true, false,
            &["__name__", "test_metric", "foo", "bar"],
            "Type and unit labels enabled but no metadata",
        ),
        typed(
            &["__name__", "test_metric", "foo", "bar"], MT::UNSPECIFIED, "", "milliseconds", 1.0, 1000, true, false,
            &["__name__", "test_metric", "__unit__", "milliseconds", "foo", "bar"],
            "Type and unit labels enabled with only unit (no type)",
        ),
    ];

    let wf = write_v2_request_fixture();
    for tc in cases {
        let desc = tc.desc;
        let symbols = tc.symbols.clone().unwrap_or_else(|| wf.symbols.clone());
        let payload = build_v2_write_request(tc.input.clone(), symbols);
        let req = new_request(&payload, &v2_headers());

        let appendable = MockAppendable::new(MockState {
            ingest_st_zero_sample: tc.ingest_st_zero_sample,
            commit_err: tc.commit_err.clone(),
            append_sample_err: tc.append_sample_err.clone(),
            append_st_zero_sample_err: tc.append_st_zero_sample_err.clone(),
            append_histogram_err: tc.append_histogram_err.clone(),
            append_exemplar_err: tc.append_exemplar_err.clone(),
            update_metadata_err: tc.update_metadata_err.clone(),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], tc.enable_type_and_unit_labels, tc.append_metadata);
        let resp = serve(&h, req);
        assert_eq!(tc.expected_code, resp.status, "{desc}");
        assert_eq!(tc.expected_resp_body, body_string(&resp), "{desc}");

        let st = appendable.st();
        if tc.expected_code == 500 {
            // We don't expect writes for partial writes with retry-able code.
            expect_header_value(0, resp.header(RW20_WRITTEN_SAMPLES_HEADER));
            expect_header_value(0, resp.header(RW20_WRITTEN_HISTOGRAMS_HEADER));
            expect_header_value(0, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));

            assert!(st.samples.is_empty(), "{desc}");
            assert!(st.histograms.is_empty(), "{desc}");
            assert!(st.exemplars.is_empty(), "{desc}");
            assert!(st.metadata.is_empty(), "{desc}");
            continue;
        }

        if !tc.expected_labels.is_empty() {
            assert_eq!(1, st.samples.len(), "{desc}");
            assert_eq!(tc.expected_labels, st.samples[0].l, "{desc}");
            if tc.append_metadata && tc.update_metadata_err.is_none() {
                let expected_meta = tc.input[0].to_metadata(tc.symbols.as_deref().unwrap_or(&[])).unwrap();
                assert_eq!(vec![MockMetadata { l: tc.expected_labels.clone(), m: expected_meta }], st.metadata, "{desc}");
            } else if !tc.append_metadata {
                assert!(st.metadata.is_empty(), "{desc}");
            }
            continue;
        }

        // Double check mandatory 2.0 stats.
        // writeV2RequestFixture has 2 series with 1 sample, 2 histograms, 1 exemplar each.
        expect_header_value(2, resp.header(RW20_WRITTEN_SAMPLES_HEADER));
        expect_header_value(8, resp.header(RW20_WRITTEN_HISTOGRAMS_HEADER));
        if tc.append_exemplar_err.is_some() {
            expect_header_value(0, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));
        } else {
            expect_header_value(2, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));
        }

        // Double check what was actually appended.
        let mut b = ScratchBuilder::new(0);
        let (mut i, mut j, mut k, mut m) = (0, 0, 0, 0);
        for ts in &wf.timeseries {
            let ls = ts.to_labels(&mut b, &wf.symbols).unwrap();

            for s in &ts.samples {
                if s.start_timestamp != 0 && tc.ingest_st_zero_sample {
                    assert_eq!(MockSample { l: ls.clone(), st: 0, t: s.start_timestamp, v: 0.0 }, st.samples[i], "{desc}");
                    i += 1;
                }
                assert_eq!(
                    MockSample { l: ls.clone(), st: s.start_timestamp, t: s.timestamp, v: s.value },
                    st.samples[i],
                    "{desc}"
                );
                i += 1;
            }
            for hp in &ts.histograms {
                if hp.is_float_histogram() {
                    let fh = hp.to_float_histogram();
                    if hp.start_timestamp != 0 && tc.ingest_st_zero_sample {
                        assert_eq!(
                            MockHistogram { l: ls.clone(), st: 0, t: hp.start_timestamp, h: None, fh: Some(FloatHistogram::default()) },
                            st.histograms[k],
                            "{desc}"
                        );
                        k += 1;
                    }
                    assert_eq!(
                        MockHistogram { l: ls.clone(), st: hp.start_timestamp, t: hp.timestamp, h: None, fh: Some(fh) },
                        st.histograms[k],
                        "{desc}"
                    );
                } else {
                    let h = hp.to_int_histogram();
                    if hp.start_timestamp != 0 && tc.ingest_st_zero_sample {
                        assert_eq!(
                            MockHistogram { l: ls.clone(), st: 0, t: hp.start_timestamp, h: Some(Histogram::default()), fh: None },
                            st.histograms[k],
                            "{desc}"
                        );
                        k += 1;
                    }
                    assert_eq!(
                        MockHistogram { l: ls.clone(), st: hp.start_timestamp, t: hp.timestamp, h, fh: None },
                        st.histograms[k],
                        "{desc}"
                    );
                }
                k += 1;
            }
            if tc.append_exemplar_err.is_none() {
                for e in &ts.exemplars {
                    let ex = e.to_exemplar(&mut b, &wf.symbols).unwrap();
                    assert_eq!(
                        MockExemplar { l: ls.clone(), el: ex.labels, t: e.timestamp, v: e.value },
                        st.exemplars[j],
                        "{desc}"
                    );
                    j += 1;
                }
            }
            if tc.append_metadata && tc.update_metadata_err.is_none() {
                let expected_meta = ts.to_metadata(&wf.symbols).unwrap();
                assert_eq!(MockMetadata { l: ls.clone(), m: expected_meta }, st.metadata[m], "{desc}");
                m += 1;
            }
        }

        // Verify that when the feature flag is disabled, no metadata is stored in WAL.
        if !tc.append_metadata {
            assert!(
                st.metadata.is_empty(),
                "{desc}: metadata should not be stored when appendMetadata (metadata-wal-records) is false"
            );
        }
    }
}

/// Verifies that when type-and-unit-labels feature is enabled, the receiver correctly handles cases where
/// __type__ and __unit__ labels are already present in the incoming labels.
/// Regression test for https://github.com/prometheus/prometheus/issues/17480.
#[test]
fn test_remote_write_handler_v2_message_no_duplicate_type_and_unit_labels() {
    use writev2::MetadataType as MT;
    let cases: Vec<(&str, Labels, MT, Labels)> = vec![
        (
            "Labels with __type__ and __unit__ should not be duplicated",
            from_strings(&["__name__", "node_cpu_seconds_total", "__type__", "counter", "__unit__", "seconds", "cpu", "0", "mode", "idle"]),
            MT::COUNTER,
            from_strings(&["__name__", "node_cpu_seconds_total", "__type__", "counter", "__unit__", "seconds", "cpu", "0", "mode", "idle"]),
        ),
        (
            "Labels with __type__ only should not be duplicated",
            from_strings(&["__name__", "test_gauge", "__type__", "gauge", "instance", "localhost"]),
            MT::GAUGE,
            from_strings(&["__name__", "test_gauge", "__type__", "gauge", "instance", "localhost"]),
        ),
        (
            "Labels with __unit__ only should not be duplicated when metadata has unit",
            from_strings(&["__name__", "test_metric", "__unit__", "bytes", "job", "test"]),
            MT::GAUGE,
            from_strings(&["__name__", "test_metric", "__type__", "gauge", "__unit__", "bytes", "job", "test"]),
        ),
        (
            "Metadata type and unit override labels",
            from_strings(&["__name__", "test_metric", "__type__", "counter", "__unit__", "seconds", "job", "test"]),
            MT::GAUGE,
            from_strings(&["__name__", "test_metric", "__type__", "gauge", "__unit__", "seconds", "job", "test"]),
        ),
    ];
    for (desc, labels_to_send, metadata_type, expected_labels) in cases {
        let mut symbol_table = writev2::SymbolsTable::new();
        let labels_refs = symbolize_labels(&mut symbol_table, &labels_to_send);

        let mut unit_ref = 0u32;
        let unit = labels_to_send.get("__unit__");
        if !unit.is_empty() {
            unit_ref = symbol_table.symbolize(unit);
        }

        let ts = vec![writev2::TimeSeries {
            labels_refs,
            metadata: writev2::Metadata { r#type: metadata_type, help_ref: 0, unit_ref },
            samples: vec![writev2::Sample { value: 42.0, timestamp: 1000, start_timestamp: 0 }],
            ..Default::default()
        }];
        let payload = build_v2_write_request(ts, symbol_table.symbols().to_vec());
        let req = new_request(&payload, &v2_headers());

        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], true, false);
        let resp = serve(&h, req);
        assert_eq!(204, resp.status, "{desc}");

        let st = appendable.st();
        assert_eq!(1, st.samples.len(), "{desc}");
        let received_labels = &st.samples[0].l;

        let dup = received_labels.has_duplicate_label_names();
        assert!(
            dup.is_none(),
            "{desc}: Labels should NOT contain duplicates, but found duplicate label: {}\nReceived labels: {}",
            dup.unwrap_or(""),
            received_labels
        );

        assert_eq!(expected_labels.to_string(), received_labels.to_string(), "{desc}: Labels should match expected");

        if !expected_labels.get("__type__").is_empty() {
            assert!(!received_labels.get("__type__").is_empty(), "{desc}: __type__ should be present in labels");
        }
    }
}

// NOTE: V2 Message is tested in TestRemoteWriteHandler_V2Message.
#[test]
fn test_out_of_order_sample_v1_message() {
    for (name, timestamp) in [("historic", 0i64), ("future", i64::MAX)] {
        let payload = build_write_request(
            vec![prompb::TimeSeries {
                labels: vec![l("__name__", "test_metric")],
                samples: vec![prompb::Sample { value: 1.0, timestamp }],
                ..Default::default()
            }],
            Vec::new(),
        );
        let req = new_request(&payload, &[]);

        let appendable = MockAppendable::new(MockState {
            latest_sample: HashMap::from([(from_strings(&["__name__", "test_metric"]).hash(), 100)]),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
        let resp = serve(&h, req);
        assert_eq!(400, resp.status, "{name}");
    }
}

// This test case currently aims to verify that the WriteHandler endpoint don't fail on exemplar ingestion errors
// since the exemplar storage is still experimental.
// NOTE: V2 Message is tested in TestRemoteWriteHandler_V2Message.
#[test]
fn test_out_of_order_exemplar_v1_message() {
    for (name, timestamp) in [("historic", 0i64), ("future", i64::MAX)] {
        let payload = build_write_request(
            vec![prompb::TimeSeries {
                labels: vec![l("__name__", "test_metric")],
                exemplars: vec![prompb::Exemplar { labels: vec![l("foo", "bar")], value: 1.0, timestamp }],
                ..Default::default()
            }],
            Vec::new(),
        );
        let req = new_request(&payload, &[]);

        let hash = from_strings(&["__name__", "test_metric"]).hash();
        let appendable = MockAppendable::new(MockState {
            latest_sample: HashMap::from([(hash, 100)]),
            latest_exemplar: HashMap::from([(hash, 100)]),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
        let resp = serve(&h, req);
        // TODO: update to 409 (StatusConflict) once exemplar storage is not experimental.
        assert_eq!(204, resp.status, "{name}");
    }
}

// NOTE: V2 Message is tested in TestRemoteWriteHandler_V2Message.
#[test]
fn test_out_of_order_histogram_v1_message() {
    for (name, timestamp) in [("historic", 0i64), ("future", i64::MAX)] {
        let th = test_histogram();
        let payload = build_write_request(
            vec![prompb::TimeSeries {
                labels: vec![l("__name__", "test_metric")],
                histograms: vec![prompb::from_int_histogram(timestamp, &th), prompb::from_float_histogram(1, &th.to_float())],
                ..Default::default()
            }],
            Vec::new(),
        );
        let req = new_request(&payload, &[]);

        let appendable = MockAppendable::new(MockState {
            latest_sample: HashMap::from([(from_strings(&["__name__", "test_metric"]).hash(), 100)]),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
        let resp = serve(&h, req);
        assert_eq!(400, resp.status, "{name}");
    }
}

#[test]
fn test_commit_err_v1_message() {
    let payload = fixture_v1_payload();
    let req = new_request(&payload, &[]);

    let appendable = MockAppendable::new(MockState { commit_err: Some(Error::msg("commit error")), ..Default::default() });
    let h = handler(appendable.arc(), vec![WriteMessageType::V1], false, false);
    let resp = serve(&h, req);
    assert_eq!(500, resp.status);
    assert_eq!("commit error\n", body_string(&resp));
}

/// Go `t.TempDir()`: a new empty folder under bench/tmp (deleted by the caller).
fn temp_dir(name: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::path::PathBuf::from(format!(
        "{}/../../bench/tmp/w3-remote/handlers/wh-{}-{}-{}",
        env!("CARGO_MANIFEST_DIR"),
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

// Regression test for https://github.com/prometheus/prometheus/issues/17206
#[test]
fn test_histogram_validation_error_handling() {
    let test_cases: Vec<(&str, Histogram, &str)> = vec![
        (
            "count mismatch",
            Histogram {
                schema: 2,
                zero_threshold: 1e-128,
                zero_count: 1,
                count: 10,
                sum: 20.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![2],
                negative_spans: vec![Span::new(0, 1)],
                negative_buckets: vec![3],
                // Total: 1 (zero) + 2 (positive) + 3 (negative) = 6, but Count = 10
                ..Default::default()
            },
            "histogram's observation count should equal",
        ),
        (
            "custom buckets zero count",
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10,
                sum: 20.0,
                zero_count: 1, // Invalid: custom buckets must have zero count of 0
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![10],
                custom_values: model::histogram::custom_values(&[1.0]),
                ..Default::default()
            },
            "custom buckets: must have zero count of 0",
        ),
    ];

    for proto_msg in [WriteMessageType::V1, WriteMessageType::V2] {
        let proto_name = if proto_msg == WriteMessageType::V2 { "V2" } else { "V1" };

        for (desc, hist, expected) in &test_cases {
            let test_name = format!("{proto_name} {desc}");
            let dir = temp_dir("hist-validation");
            let db = tsdb::db::open(&dir, None, Some(tsdb::db::default_options()), None).expect("open tsdb");

            let head: Arc<dyn AppendableV2> = db.head().clone();
            let h = handler(head, vec![proto_msg], false, false);

            let buf = if proto_msg == WriteMessageType::V1 {
                build_write_request(
                    vec![prompb::TimeSeries {
                        labels: vec![l("__name__", "test")],
                        histograms: vec![prompb::from_int_histogram(1, hist)],
                        ..Default::default()
                    }],
                    Vec::new(),
                )
            } else {
                let mut st = writev2::SymbolsTable::new();
                let labels_refs = symbolize_labels(&mut st, &from_strings(&["__name__", "test"]));
                build_v2_write_request(
                    vec![writev2::TimeSeries {
                        labels_refs,
                        histograms: vec![writev2::from_int_histogram(0, 1, hist)],
                        ..Default::default()
                    }],
                    st.symbols().to_vec(),
                )
            };

            let mut req = new_request(&buf, &[("Content-Type", proto_msg.content_type_header()), ("Content-Encoding", "snappy")]);
            req.path = "/api/v1/write".to_string();
            let resp = serve(&h, req);
            let body = body_string(&resp);

            assert_eq!(400, resp.status, "{test_name}: {body}");
            assert!(body.contains(expected), "{test_name}: {body}");

            drop(h);
            assert!(db.close().is_ok(), "{test_name}: close");
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

#[test]
fn test_commit_err_v2_message() {
    let payload = fixture_v2_payload();
    let req = new_request(&payload, &v2_headers());

    let appendable = MockAppendable::new(MockState { commit_err: Some(Error::msg("commit error")), ..Default::default() });
    let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
    let resp = serve(&h, req);
    assert_eq!(500, resp.status);
    assert_eq!("commit error\n", body_string(&resp));
}

#[test]
fn test_histograms_reduction() {
    for proto_msg in [WriteMessageType::V1, WriteMessageType::V2] {
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![proto_msg], false, false);

        let hs = high_schema_histogram();
        let payload = if proto_msg == WriteMessageType::V1 {
            build_write_request(
                vec![
                    prompb::TimeSeries {
                        labels: vec![l("__name__", "test_metric1")],
                        histograms: vec![prompb::from_int_histogram(1, &hs)],
                        ..Default::default()
                    },
                    prompb::TimeSeries {
                        labels: vec![l("__name__", "test_metric2")],
                        histograms: vec![prompb::from_float_histogram(2, &hs.to_float())],
                        ..Default::default()
                    },
                ],
                Vec::new(),
            )
        } else {
            build_v2_write_request(
                vec![
                    writev2::TimeSeries {
                        labels_refs: vec![0, 1],
                        histograms: vec![writev2::from_int_histogram(0, 1, &hs)],
                        ..Default::default()
                    },
                    writev2::TimeSeries {
                        labels_refs: vec![0, 2],
                        histograms: vec![writev2::from_float_histogram(0, 2, &hs.to_float())],
                        ..Default::default()
                    },
                ],
                vec!["__name__".to_string(), "test_metric1".to_string(), "test_metric2".to_string()],
            )
        };

        let req = new_request(&payload, &[("Content-Type", proto_msg.content_type_header())]);
        let resp = serve(&h, req);
        assert_eq!(204, resp.status, "{proto_msg}");
        assert!(resp.body_bytes().is_empty(), "{proto_msg}");

        let st = appendable.st();
        assert_eq!(2, st.histograms.len(), "{proto_msg}");
        assert_eq!(1, st.histograms[0].t);
        assert_eq!(Some(reduced_schema_histogram()), st.histograms[0].h);
        assert_eq!(2, st.histograms[1].t);
        assert_eq!(Some(reduced_schema_histogram().to_float()), st.histograms[1].fh);
    }
}

// Regression test for https://github.com/prometheus/prometheus/issues/17659
#[test]
fn test_remote_write_handler_response_stats() {
    let payload_v1 = fixture_v1_payload();
    let payload_v2 = fixture_v2_payload();

    for (msg_type, payload, force_inject_headers, expect_headers) in [
        (WriteMessageType::V1, &payload_v1, false, false),
        (WriteMessageType::V1, &payload_v1, true, true),
        (WriteMessageType::V2, &payload_v2, false, true),
    ] {
        let name = format!("msg={msg_type}/force-inject-headers={force_inject_headers}");
        // Setup server side.
        let appendable = MockAppendable::default();
        let h = Arc::new(handler(appendable.arc(), vec![WriteMessageType::V1, WriteMessageType::V2], false, false));

        let url = start_server(move |r: &http::Request| {
            let req = http::Request {
                method: r.method.clone(),
                path: r.path.clone(),
                headers: r.headers.clone(),
                body: r.body.clone(),
                ..Default::default()
            };
            let mut resp = h.serve(&Context::background(), req);
            if force_inject_headers {
                // Inject response header. This simulates PRWv1 server that uses PRWv2 response headers for
                // confirmation of samples. This is not against spec and we support it.
                // adapted: Go sets the header before the handler runs, so a header the handler sets replaces it;
                // here it is added after the handler, only when the handler did not set it.
                if resp.header(RW20_WRITTEN_SAMPLES_HEADER).is_none() {
                    resp.set_header(RW20_WRITTEN_SAMPLES_HEADER, "2");
                }
            }
            resp
        });

        // Send message and do the parse response flow.
        let conf = ClientConfig {
            url,
            timeout: Duration::from_secs(5 * 60),
            write_proto_msg: msg_type.as_str().to_string(),
            ..Default::default()
        };
        let c = Client::new_write_client("test", &conf).unwrap();

        let stats = c.store(&Context::background(), payload, 0).unwrap_or_else(|e| panic!("{name}: {e}"));

        if expect_headers {
            assert!(stats.confirmed, "{name}");
            assert_eq!(appendable.st().samples.len() as i64, stats.samples, "{name}");
        } else {
            assert!(!stats.confirmed, "{name}");
        }
    }
}

#[test]
fn test_remote_write_handler_requires_exemplar_appender_v2() {
    let payload_v1 = fixture_v1_payload();
    let payload_v2 = fixture_v2_payload();

    for (name, msg_type, payload) in [("v1", WriteMessageType::V1, &payload_v1), ("v2", WriteMessageType::V2, &payload_v2)] {
        let rolled_back = Arc::new(AtomicBool::new(false));
        let h = handler(Arc::new(MockAppendableV2Only { rolled_back: rolled_back.clone() }), vec![msg_type], false, false);

        let req = new_request(payload, &[("Content-Type", msg_type.content_type_header()), ("Content-Encoding", SNAPPY)]);
        let resp = serve(&h, req);
        let body = body_string(&resp);

        assert_eq!(500, resp.status, "{name}");
        assert!(body.contains("requires a storage.ExemplarAppenderV2"), "{name}: {body}");
        assert!(rolled_back.load(Ordering::SeqCst), "{name}");
    }
}

#[test]
fn test_remote_write_handler_histogram_with_exemplars() {
    // Verify that for RW 1.0 and RW 2.0 requests containing only histograms and exemplars (no float samples),
    // histograms are appended before exemplars so that the series exists and its SeriesRef is passed to
    // AppendExemplars.
    for proto_msg in [WriteMessageType::V1, WriteMessageType::V2] {
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![proto_msg], false, false);

        let lbls = from_strings(&["__name__", "hist_only_metric"]);
        let hs = high_schema_histogram();
        let payload = if proto_msg == WriteMessageType::V1 {
            let mut labels = Vec::new();
            prompb::from_labels(&lbls, &mut labels);
            build_write_request(
                vec![prompb::TimeSeries {
                    labels,
                    histograms: vec![prompb::from_int_histogram(1000, &hs)],
                    exemplars: vec![prompb::Exemplar { labels: vec![l("trace_id", "abc")], value: 1.5, timestamp: 1000 }],
                    ..Default::default()
                }],
                Vec::new(),
            )
        } else {
            build_v2_write_request(
                vec![writev2::TimeSeries {
                    labels_refs: vec![0, 1],
                    histograms: vec![writev2::from_int_histogram(0, 1000, &hs)],
                    exemplars: vec![writev2::Exemplar { labels_refs: vec![2, 3], value: 1.5, timestamp: 1000 }],
                    ..Default::default()
                }],
                ["__name__", "hist_only_metric", "trace_id", "abc"].iter().map(|s| s.to_string()).collect(),
            )
        };

        let req = new_request(&payload, &[("Content-Type", proto_msg.content_type_header()), ("Content-Encoding", SNAPPY)]);
        let resp = serve(&h, req);
        assert_eq!(204, resp.status, "{proto_msg}");

        let st = appendable.st();
        assert_eq!(1, st.histograms.len(), "{proto_msg}");
        assert_eq!(1, st.exemplars.len(), "{proto_msg}");
        // Verify that AppendExemplar received the non-zero SeriesRef returned by AppendHistogram.
        assert_eq!(Some(&1000), st.latest_exemplar.get(&lbls.hash()), "{proto_msg}");
    }
}

#[test]
fn test_remote_write_handler_v2_exemplar_ordering_and_rejected_series() {
    let syms = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    let v2_req = |payload: &[u8]| {
        new_request(payload, &[("Content-Type", WriteMessageType::V2.content_type_header()), ("Content-Encoding", SNAPPY)])
    };

    // unsorted in-batch exemplars are sorted and ingested
    {
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let payload = build_v2_write_request(
            vec![writev2::TimeSeries {
                labels_refs: vec![0, 1],
                samples: vec![writev2::Sample { timestamp: 2000, value: 1.0, start_timestamp: 0 }],
                exemplars: vec![
                    writev2::Exemplar { labels_refs: vec![2, 3], value: 2.0, timestamp: 2000 },
                    writev2::Exemplar { labels_refs: vec![2, 4], value: 1.0, timestamp: 1000 },
                ],
                ..Default::default()
            }],
            syms(&["__name__", "test_metric", "trace_id", "t2", "t1"]),
        );
        let resp = serve(&h, v2_req(&payload));
        assert_eq!(204, resp.status);
        expect_header_value(2, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));
        let st = appendable.st();
        assert_eq!(2, st.exemplars.len());
        assert_eq!(1000, st.exemplars[0].t);
        assert_eq!(2000, st.exemplars[1].t);
    }

    // out of order exemplar vs earlier transaction returns 400
    {
        let lbls = from_strings(&["__name__", "test_metric"]);
        let appendable = MockAppendable::new(MockState {
            latest_sample: HashMap::from([(lbls.hash(), 1000)]),
            latest_exemplar: HashMap::from([(lbls.hash(), 1000)]),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let payload = build_v2_write_request(
            vec![writev2::TimeSeries {
                labels_refs: vec![0, 1],
                samples: vec![writev2::Sample { timestamp: 2000, value: 1.0, start_timestamp: 0 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![2, 3], value: 1.0, timestamp: 500 }],
                ..Default::default()
            }],
            syms(&["__name__", "test_metric", "trace_id", "t1"]),
        );
        let resp = serve(&h, v2_req(&payload));
        assert_eq!(400, resp.status);
        expect_header_value(1, resp.header(RW20_WRITTEN_SAMPLES_HEADER));
        expect_header_value(0, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));
    }

    // new series with rejected sample does not ingest exemplar
    {
        let appendable = MockAppendable::default();
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let payload = build_v2_write_request(
            vec![writev2::TimeSeries {
                labels_refs: vec![0, 1],
                samples: vec![writev2::Sample { timestamp: i64::MAX, value: 1.0, start_timestamp: 0 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![2, 3], value: 1.0, timestamp: 1000 }],
                ..Default::default()
            }],
            syms(&["__name__", "new_metric", "trace_id", "t1"]),
        );
        let resp = serve(&h, v2_req(&payload));
        assert_eq!(400, resp.status);
        expect_header_value(0, resp.header(RW20_WRITTEN_SAMPLES_HEADER));
        expect_header_value(0, resp.header(RW20_WRITTEN_EXEMPLARS_HEADER));
        assert!(appendable.st().exemplars.is_empty());
    }

    // AppendPartialError from Append still counts sample and histogram as written
    {
        let appendable = MockAppendable::new(MockState {
            append_sample_err: Some(Error::AppendPartial(Arc::new(AppendPartialError {
                series_ref: 0,
                exemplar_errors: vec![ExemplarError { index: 0, err: Error::msg("some non-OOO exemplar error") }],
            }))),
            append_histogram_err: Some(Error::AppendPartial(Arc::new(AppendPartialError {
                series_ref: 0,
                exemplar_errors: vec![ExemplarError { index: 0, err: ERR_OUT_OF_ORDER_EXEMPLAR }],
            }))),
            ..Default::default()
        });
        let h = handler(appendable.arc(), vec![WriteMessageType::V2], false, false);
        let payload = build_v2_write_request(
            vec![writev2::TimeSeries {
                labels_refs: vec![0, 1],
                samples: vec![writev2::Sample { timestamp: 1000, value: 1.0, start_timestamp: 0 }],
                histograms: vec![writev2::from_int_histogram(0, 1000, &high_schema_histogram())],
                ..Default::default()
            }],
            syms(&["__name__", "partial_err_metric"]),
        );
        let resp = serve(&h, v2_req(&payload));
        assert_eq!(400, resp.status);
        expect_header_value(1, resp.header(RW20_WRITTEN_SAMPLES_HEADER));
        expect_header_value(1, resp.header(RW20_WRITTEN_HISTOGRAMS_HEADER));
        let st = appendable.st();
        assert_eq!(1, st.samples.len());
        assert_eq!(1, st.histograms.len());
    }
}

#[test]
fn test_time_limit_appender_v2_limit_exemplars() {
    let lbls = from_strings(&["__name__", "test_metric"]);
    let ex = |id: &str, value: f64, ts: i64| Exemplar { labels: from_strings(&["id", id]), value, ts, has_ts: false };

    // Append with inlined exemplars
    {
        let appendable = MockAppendable::new(MockState {
            latest_exemplar: HashMap::from([(lbls.hash(), 500)]),
            ..Default::default()
        });
        let mut base = appendable.appender_v2(&Context::background());
        let mut app = TimeLimitAppenderV2::new(&mut *base, 1000);

        let input = vec![
            ex("1", 1.0, 400),  // out of order (< 500)
            ex("2", 2.0, 2000), // future (> maxTime)
            ex("3", 3.0, 900),  // valid (<= maxTime)
        ];
        let orig_first_id = input[0].labels.get("id").to_string();
        let orig_second_id = input[1].labels.get("id").to_string();
        let orig_third_id = input[2].labels.get("id").to_string();

        let err = app
            .append(0, &lbls, 0, 500, 1.0, None, None, &AppendV2Options { exemplars: &input, ..Default::default() })
            .unwrap_err();
        let p_err = err.as_append_partial().expect("AppendPartialError");
        assert_eq!(2, p_err.exemplar_errors.len());
        assert!(p_err.exemplar_errors[0].is(&ERR_OUT_OF_BOUNDS));
        assert_eq!(1, p_err.exemplar_errors[0].index);
        assert!(p_err.exemplar_errors[1].is(&ERR_OUT_OF_ORDER_EXEMPLAR));
        assert_eq!(0, p_err.exemplar_errors[1].index);

        // Caller's input slice must not be mutated in place.
        assert_eq!(orig_first_id, input[0].labels.get("id"));
        assert_eq!(orig_second_id, input[1].labels.get("id"));
        assert_eq!(orig_third_id, input[2].labels.get("id"));
        let st = appendable.st();
        assert_eq!(1, st.samples.len());
        assert_eq!(1, st.exemplars.len());
        assert_eq!(900, st.exemplars[0].t);
    }

    // AppendExemplars
    {
        let appendable = MockAppendable::default();
        let mut base = appendable.appender_v2(&Context::background());
        assert!(base.as_exemplar_appender_v2().is_some());
        let mut app = TimeLimitAppenderV2::new(&mut *base, 1000);

        let r = app.append(0, &lbls, 0, 500, 1.0, None, None, &AppendV2Options::default()).unwrap();

        let input = vec![
            ex("1", 1.0, 2000), // future (> maxTime)
            ex("2", 2.0, 900),  // valid (<= maxTime)
        ];
        let orig_first_id = input[0].labels.get("id").to_string();
        let orig_second_id = input[1].labels.get("id").to_string();

        let err = app.append_exemplars(r, &lbls, &input).unwrap_err();
        let p_err = err.as_append_partial().expect("AppendPartialError");
        assert_eq!(1, p_err.exemplar_errors.len());
        assert!(p_err.exemplar_errors[0].is(&ERR_OUT_OF_BOUNDS));

        // Caller's input slice must not be mutated in place.
        assert_eq!(orig_first_id, input[0].labels.get("id"));
        assert_eq!(orig_second_id, input[1].labels.get("id"));
        let st = appendable.st();
        assert_eq!(1, st.exemplars.len());
        assert_eq!(900, st.exemplars[0].t);
    }
}
