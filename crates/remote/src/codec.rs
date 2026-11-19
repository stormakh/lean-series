//! Port of `storage/remote/codec.go`: remote read request and response codecs, conversions between query
//! results and series sets, the streamed (chunked) remote read response writer and reader-side series set,
//! label matcher conversions, and the remote write request decoders.
//!
//! `DecodeOTLPWriteRequest` is ported by the OTLP part of this crate (owned by the OTLP helper), not here.

use std::fmt;
use std::io::{self, Read, Write};
use std::sync::Arc;

use chunkenc::{AnyIterator, ChunkIterator, Encoding, NopIterator, ValueType};
use model::histogram::{
    self as mhist, FloatHistogram, Histogram as ModelHistogram, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED,
};
use model::labels::{self, Labels, ScratchBuilder, ValidationScheme, METRIC_NAME};
use model::matcher::{MatchType, Matcher};
use storage::{
    Annotations, BoxSeriesSet, ChunkSeriesSet, Error, ErrSeriesSet, SelectHints, Series, SeriesArc, SeriesSet,
};

use crate::chunked::{ChunkedReader, ERR_EOF};
use crate::prompb::{self, writev2, LabelMatcher, MatcherType, Message, ResponseType};

/// Go `decodeReadLimit`: the maximum size of a read request body in bytes.
pub const DECODE_READ_LIMIT: usize = 32 * 1024 * 1024;

pub const PB_CONTENT_TYPE: &str = "application/x-protobuf";
pub const JSON_CONTENT_TYPE: &str = "application/json";

/// Go `HTTPError`: an error with the HTTP status to answer with. It travels as `storage::Error::Other`; find
/// it with [`as_http_error`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    pub msg: String,
    pub status: u16,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for HttpError {}

impl HttpError {
    pub fn status(&self) -> u16 {
        self.status
    }
}

/// Go `errors.As(err, &HTTPError{})`.
pub fn as_http_error(err: &Error) -> Option<&HttpError> {
    err.downcast_ref::<HttpError>()
}

// ---------------------------------------------------------------------------------------------------------------
// Snappy (block format), with the error texts of github.com/golang/snappy.

const SNAPPY_ERR_CORRUPT: &str = "snappy: corrupt input";
const SNAPPY_ERR_TOO_LARGE: &str = "snappy: decoded block is too large";

fn snappy_err(e: snap::Error) -> Error {
    match e {
        snap::Error::TooBig { .. } => Error::msg(SNAPPY_ERR_TOO_LARGE),
        _ => Error::msg(SNAPPY_ERR_CORRUPT),
    }
}

/// Go `snappy.DecodedLen`.
pub fn snappy_decoded_len(b: &[u8]) -> Result<usize, Error> {
    snap::raw::decompress_len(b).map_err(snappy_err)
}

/// Go `snappy.Decode(nil, b)`.
pub fn snappy_decode(b: &[u8]) -> Result<Vec<u8>, Error> {
    snap::raw::Decoder::new().decompress_vec(b).map_err(snappy_err)
}

/// Go `snappy.Encode(nil, b)`.
pub fn snappy_encode(b: &[u8]) -> Vec<u8> {
    // Encoding only fails for inputs above 4 GiB, which Go's snappy rejects with a panic, too.
    snap::raw::Encoder::new().compress_vec(b).expect("snappy: input too large")
}

fn proto_err(e: prompb::DecodeError) -> Error {
    Error::msg(e.to_string())
}

// ---------------------------------------------------------------------------------------------------------------
// Read request and response.

/// Go `DecodeReadRequest`: the snappy-compressed read request in the body (at most [`DECODE_READ_LIMIT`]
/// bytes are read, as Go's `io.LimitReader`).
pub fn decode_read_request(r: &http::Request) -> Result<prompb::ReadRequest, Error> {
    let compressed = &r.body[..r.body.len().min(DECODE_READ_LIMIT)];
    let decoded_len = snappy_decoded_len(compressed)?;
    if decoded_len > DECODE_READ_LIMIT {
        return Err(Error::msg(format!(
            "snappy: decoded length {} exceeds limit {}",
            decoded_len, DECODE_READ_LIMIT
        )));
    }
    let req_buf = snappy_decode(compressed)?;
    prompb::ReadRequest::unmarshal(&req_buf).map_err(proto_err)
}

/// Go `EncodeReadResponse`: the snappy-compressed response body.
pub fn encode_read_response(resp: &prompb::ReadResponse) -> Vec<u8> {
    snappy_encode(&resp.marshal())
}

/// Go `ToQuery`.
pub fn to_query(from: i64, to: i64, matchers: &[Matcher], hints: Option<&SelectHints>) -> Result<prompb::Query, Error> {
    let ms = to_label_matchers(matchers)?;
    let rp = hints.map(|h| prompb::ReadHints {
        start_ms: h.start,
        end_ms: h.end,
        step_ms: h.step,
        func: h.func.clone(),
        grouping: h.grouping.clone(),
        by: h.by,
        range_ms: h.range,
    });
    Ok(prompb::Query { start_timestamp_ms: from, end_timestamp_ms: to, matchers: ms, hints: rp })
}

/// Go `ToQueryResult`: reads the whole set into a query result. With `sample_limit > 0`, more samples than the
/// limit give an [`HttpError`] with status 400. On error the result is empty.
pub fn to_query_result(ss: &mut dyn SeriesSet, sample_limit: usize) -> (prompb::QueryResult, Annotations, Option<Error>) {
    let mut num_samples = 0usize;
    let mut resp = prompb::QueryResult::default();
    while ss.next() {
        let series = ss.at().clone();
        let mut iter = series.iterator(None);
        let mut samples = Vec::new();
        let mut histograms = Vec::new();
        loop {
            let vt = iter.next();
            if vt == ValueType::None {
                break;
            }
            num_samples += 1;
            if sample_limit > 0 && num_samples > sample_limit {
                return (
                    prompb::QueryResult::default(),
                    ss.warnings(),
                    Some(Error::other(HttpError {
                        msg: format!("exceeded sample limit ({})", sample_limit),
                        status: 400,
                    })),
                );
            }
            match vt {
                ValueType::Float => {
                    let (t, v) = iter.at();
                    samples.push(prompb::Sample { value: v, timestamp: t });
                }
                ValueType::Histogram => {
                    let (t, h) = iter.at_histogram(None);
                    histograms.push(prompb::from_int_histogram(t, &h));
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = iter.at_float_histogram(None);
                    histograms.push(prompb::from_float_histogram(t, &fh));
                }
                ValueType::None => unreachable!(),
            }
        }
        if let Some(e) = iter.err() {
            return (prompb::QueryResult::default(), ss.warnings(), Some(Error::from(e.clone())));
        }
        drop(iter);
        resp.timeseries.push(prompb::TimeSeries {
            labels: prompb::labels_to_label_protos(series.labels()),
            samples,
            histograms,
            ..Default::default()
        });
    }
    (resp, ss.warnings(), ss.err())
}

/// Go `FromQueryResult`: the series of a query result, sorted by labels if asked. Invalid labels give a set
/// with only the error.
pub fn from_query_result(sort_series: bool, res: prompb::QueryResult) -> BoxSeriesSet {
    let mut b = ScratchBuilder::new(0);
    let mut series: Vec<SeriesArc> = Vec::with_capacity(res.timeseries.len());
    for ts in res.timeseries {
        if let Err(e) = validate_labels_and_metric_name(&ts.labels) {
            return Box::new(ErrSeriesSet(Some(e)));
        }
        let lbls = ts.to_labels(&mut b);
        series.push(Arc::new(ConcreteSeries { labels: lbls, floats: ts.samples, histograms: ts.histograms }));
    }
    if sort_series {
        series.sort_by(|a, b| labels::compare(a.labels(), b.labels()).cmp(&0));
    }
    Box::new(ConcreteSeriesSet::new(series))
}

fn response_type_name(t: ResponseType) -> String {
    match t {
        ResponseType::SAMPLES => "SAMPLES".to_string(),
        ResponseType::STREAMED_XOR_CHUNKS => "STREAMED_XOR_CHUNKS".to_string(),
        ResponseType(n) => n.to_string(),
    }
}

/// Go `NegotiateResponseType`: the first accepted response type that this server supports. An empty list
/// means SAMPLES (backward compatibility).
pub fn negotiate_response_type(accepted: &[ResponseType]) -> Result<ResponseType, Error> {
    let samples = [ResponseType::SAMPLES];
    let accepted = if accepted.is_empty() { &samples[..] } else { accepted };
    for &t in accepted {
        if t == ResponseType::SAMPLES || t == ResponseType::STREAMED_XOR_CHUNKS {
            return Ok(t);
        }
    }
    let list: Vec<String> = accepted.iter().map(|&t| response_type_name(t)).collect();
    Err(Error::msg(format!(
        "server does not support any of the requested response types: [{}]; supported: map[SAMPLES:{{}} STREAMED_XOR_CHUNKS:{{}}]",
        list.join(" ")
    )))
}

/// Go `StreamChunkedReadResponses`: writes the chunks of every series to `stream` (one `ChunkedReadResponse`
/// per write), splitting a series over several frames when it has more than `max_bytes_in_frame` bytes. The
/// series must have populated chunks.
pub fn stream_chunked_read_responses<W: Write + ?Sized>(
    stream: &mut W,
    query_index: i64,
    ss: &mut dyn ChunkSeriesSet,
    sorted_external_labels: &[prompb::Label],
    max_bytes_in_frame: i64,
) -> (Annotations, Option<Error>) {
    let mut chks: Vec<prompb::Chunk> = Vec::new();
    let mut lbls: Vec<prompb::Label> = Vec::new();
    let mut buf: Vec<u8> = Vec::new();

    while ss.next() {
        let series = ss.at().clone();
        let mut iter = series.iterator(None);
        prompb::from_labels(series.labels(), &mut lbls);
        lbls = merge_labels(&lbls, sorted_external_labels);

        let mut max_data_length = max_bytes_in_frame;
        for l in &lbls {
            max_data_length -= l.encoded_len() as i64;
        }
        let mut frame_bytes_left = max_data_length;

        let mut is_next = iter.next();
        // Send at most one series per frame; a series may be split over several frames.
        while is_next {
            let chk = iter.at();
            let Some(c) = &chk.chunk else {
                return (
                    ss.warnings(),
                    Some(Error::msg(format!(
                        "StreamChunkedReadResponses: found not populated chunk returned by SeriesSet at ref: {}",
                        chk.chunk_ref
                    ))),
                );
            };
            let pc = prompb::Chunk {
                min_time_ms: chk.min_time,
                max_time_ms: chk.max_time,
                r#type: prompb::ChunkEncoding(c.encoding().0 as i32),
                data: c.bytes().to_vec(),
            };
            frame_bytes_left -= pc.encoded_len() as i64;
            chks.push(pc);

            // A frame may be bigger than the limit by at most one chunk.
            is_next = iter.next();
            if frame_bytes_left > 0 && is_next {
                continue;
            }

            let resp = prompb::ChunkedReadResponse {
                chunked_series: vec![prompb::ChunkedSeries { labels: lbls.clone(), chunks: std::mem::take(&mut chks) }],
                query_index,
            };
            resp.marshal_into(&mut buf);
            if let Err(e) = stream.write(&buf) {
                return (ss.warnings(), Some(Error::wrap("write to stream", crate::chunked::io_err(e))));
            }
            frame_bytes_left = max_data_length;
        }
        if let Some(e) = iter.err() {
            return (ss.warnings(), Some(e));
        }
    }
    (ss.warnings(), ss.err())
}

/// Go `MergeLabels`: merges two sorted label lists; on equal names `primary` wins.
pub fn merge_labels(primary: &[prompb::Label], secondary: &[prompb::Label]) -> Vec<prompb::Label> {
    let mut result = Vec::with_capacity(primary.len() + secondary.len());
    let (mut i, mut j) = (0, 0);
    while i < primary.len() && j < secondary.len() {
        match primary[i].name.cmp(&secondary[j].name) {
            std::cmp::Ordering::Less => {
                result.push(primary[i].clone());
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                result.push(secondary[j].clone());
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                result.push(primary[i].clone());
                i += 1;
                j += 1;
            }
        }
    }
    result.extend_from_slice(&primary[i..]);
    result.extend_from_slice(&secondary[j..]);
    result
}

// ---------------------------------------------------------------------------------------------------------------
// Concrete series (the samples of a query result).

/// Go `concreteSeriesSet`.
pub struct ConcreteSeriesSet {
    cur: usize,
    series: Vec<SeriesArc>,
}

impl ConcreteSeriesSet {
    pub fn new(series: Vec<SeriesArc>) -> ConcreteSeriesSet {
        ConcreteSeriesSet { cur: 0, series }
    }
}

impl SeriesSet for ConcreteSeriesSet {
    fn next(&mut self) -> bool {
        self.cur += 1;
        self.cur - 1 < self.series.len()
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.cur - 1]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `concreteSeries`: a series with the float samples and histograms of a query result.
#[derive(Clone, Debug, Default)]
pub struct ConcreteSeries {
    pub labels: Labels,
    pub floats: Vec<prompb::Sample>,
    pub histograms: Vec<prompb::Histogram>,
}

impl Series for ConcreteSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(ConcreteSeriesIterator::new(self)))
    }
}

/// Go `concreteSeriesIterator`. Floats win over histograms on equal timestamps.
pub struct ConcreteSeriesIterator<'a> {
    floats_cur: isize,
    histograms_cur: isize,
    cur_val_type: ValueType,
    series: &'a ConcreteSeries,
    err: Option<chunkenc::Error>,
    // Filled with the current model histogram when cur_val_type is Histogram or FloatHistogram.
    cur_h: Option<ModelHistogram>,
    cur_fh: Option<FloatHistogram>,
}

const NO_TS: i64 = i64::MAX;

/// Go `sort.Search(n, f)`.
fn search(n: usize, f: impl Fn(usize) -> bool) -> usize {
    let (mut i, mut j) = (0usize, n);
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

impl<'a> ConcreteSeriesIterator<'a> {
    pub fn new(series: &'a ConcreteSeries) -> ConcreteSeriesIterator<'a> {
        ConcreteSeriesIterator {
            floats_cur: -1,
            histograms_cur: -1,
            cur_val_type: ValueType::None,
            series,
            err: None,
            cur_h: None,
            cur_fh: None,
        }
    }

    fn float_ts(&self, i: isize) -> i64 {
        self.series.floats[i as usize].timestamp
    }

    fn hist_ts(&self, i: isize) -> i64 {
        self.series.histograms[i as usize].timestamp
    }

    /// Go `setCurrentHistogram`: converts and validates the current histogram, so that an invalid histogram
    /// from the remote source is found before `at_histogram`.
    fn set_current_histogram(&mut self) {
        let pb_h = &self.series.histograms[self.histograms_cur as usize];
        let schema = pb_h.schema;
        if !mhist::is_known_schema(schema) {
            self.err = Some(mhist::unknown_schema_error(schema).into());
            return;
        }
        if pb_h.is_float_histogram() {
            self.cur_val_type = ValueType::FloatHistogram;
            let mut m_fh = pb_h.to_float_histogram();
            if m_fh.schema > EXPONENTIAL_SCHEMA_MAX && m_fh.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
                // Slow path: a sample from a newer Prometheus with higher resolution.
                if let Err(e) = m_fh.reduce_resolution(EXPONENTIAL_SCHEMA_MAX) {
                    self.err = Some(e.into());
                    return;
                }
            }
            if let Err(e) = m_fh.validate() {
                self.err = Some(e.into());
                return;
            }
            self.cur_fh = Some(m_fh);
            return;
        }
        self.cur_val_type = ValueType::Histogram;
        let mut m_h = pb_h.to_int_histogram().expect("integer histogram");
        if m_h.schema > EXPONENTIAL_SCHEMA_MAX && m_h.schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            if let Err(e) = m_h.reduce_resolution(EXPONENTIAL_SCHEMA_MAX) {
                self.err = Some(e.into());
                return;
            }
        }
        if let Err(e) = m_h.validate() {
            self.err = Some(e.into());
            return;
        }
        self.cur_h = Some(m_h);
    }
}

impl ChunkIterator for ConcreteSeriesIterator<'_> {
    fn seek(&mut self, t: i64) -> ValueType {
        if self.err.is_some() {
            return ValueType::None;
        }
        if self.floats_cur == -1 {
            self.floats_cur = 0;
        }
        if self.histograms_cur == -1 {
            self.histograms_cur = 0;
        }
        let nf = self.series.floats.len() as isize;
        let nh = self.series.histograms.len() as isize;
        if self.floats_cur >= nf && self.histograms_cur >= nh {
            return ValueType::None;
        }

        // No-op check.
        if (self.cur_val_type == ValueType::Float && self.float_ts(self.floats_cur) >= t)
            || ((self.cur_val_type == ValueType::Histogram || self.cur_val_type == ValueType::FloatHistogram)
                && self.hist_ts(self.histograms_cur) >= t)
        {
            return self.cur_val_type;
        }

        self.cur_val_type = ValueType::None;

        // Binary search between the current position and the end, for floats and histograms.
        let fc = self.floats_cur as usize;
        let floats = &self.series.floats;
        self.floats_cur += search(floats.len() - fc, |n| floats[n + fc].timestamp >= t) as isize;
        let hc = self.histograms_cur as usize;
        let hists = &self.series.histograms;
        self.histograms_cur += search(hists.len() - hc, |n| hists[n + hc].timestamp >= t) as isize;

        if self.floats_cur < nf && self.histograms_cur < nh {
            // Overlapping timestamps prefer the float sample.
            let ft = self.float_ts(self.floats_cur);
            let ht = self.hist_ts(self.histograms_cur);
            self.cur_val_type = if ft <= ht { ValueType::Float } else { ValueType::Histogram };
            // Without overlap, the cursor of the other type went one too far.
            if ft != ht {
                if self.cur_val_type == ValueType::Float {
                    self.histograms_cur -= 1;
                } else {
                    self.floats_cur -= 1;
                }
            }
        } else if self.floats_cur < nf {
            self.cur_val_type = ValueType::Float;
        } else if self.histograms_cur < nh {
            self.cur_val_type = ValueType::Histogram;
        }
        if self.cur_val_type == ValueType::Histogram {
            self.set_current_histogram();
        }
        if self.err.is_some() {
            self.cur_val_type = ValueType::None;
        }
        self.cur_val_type
    }

    fn at(&self) -> (i64, f64) {
        if self.cur_val_type != ValueType::Float {
            panic!("iterator is not on a float sample");
        }
        let s = &self.series.floats[self.floats_cur as usize];
        (s.timestamp, s.value)
    }

    fn at_histogram(&self, _h: Option<ModelHistogram>) -> (i64, ModelHistogram) {
        if self.cur_val_type != ValueType::Histogram {
            panic!("iterator is not on an integer histogram sample");
        }
        (self.hist_ts(self.histograms_cur), self.cur_h.clone().unwrap_or_default())
    }

    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        match self.cur_val_type {
            ValueType::FloatHistogram => {
                (self.hist_ts(self.histograms_cur), self.cur_fh.clone().unwrap_or_default())
            }
            ValueType::Histogram => (
                self.hist_ts(self.histograms_cur),
                self.cur_h.as_ref().map(|h| h.to_float()).unwrap_or_default(),
            ),
            _ => panic!("iterator is not on a histogram sample"),
        }
    }

    fn at_t(&self) -> i64 {
        if self.cur_val_type == ValueType::Histogram || self.cur_val_type == ValueType::FloatHistogram {
            return self.hist_ts(self.histograms_cur);
        }
        self.float_ts(self.floats_cur)
    }

    /// Query results carry no start timestamps (Go has a TODO here, too).
    fn at_st(&self) -> i64 {
        0
    }

    fn next(&mut self) -> ValueType {
        if self.err.is_some() {
            return ValueType::None;
        }
        let nf = self.series.floats.len() as isize;
        let nh = self.series.histograms.len() as isize;
        let peek_float_ts = if self.floats_cur + 1 < nf { self.float_ts(self.floats_cur + 1) } else { NO_TS };
        let peek_hist_ts = if self.histograms_cur + 1 < nh { self.hist_ts(self.histograms_cur + 1) } else { NO_TS };
        self.cur_val_type = ValueType::None;
        if peek_float_ts < peek_hist_ts {
            self.floats_cur += 1;
            self.cur_val_type = ValueType::Float;
        } else if peek_hist_ts < peek_float_ts {
            self.histograms_cur += 1;
            self.cur_val_type = ValueType::Histogram;
        } else if peek_float_ts == NO_TS && peek_hist_ts == NO_TS {
            // Exhausted: put the cursors past the end so that seek returns nothing afterwards.
            self.floats_cur = nf;
            self.histograms_cur = nh;
        } else {
            // Floats win on a conflict; the histogram cursor moves too, else it is selected next.
            self.floats_cur += 1;
            self.histograms_cur += 1;
            self.cur_val_type = ValueType::Float;
        }
        if self.cur_val_type == ValueType::Histogram {
            self.set_current_histogram();
        }
        if self.err.is_some() {
            self.cur_val_type = ValueType::None;
        }
        self.cur_val_type
    }

    fn err(&self) -> Option<&chunkenc::Error> {
        self.err.as_ref()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunked series (streamed remote read responses).

/// Go `chunkedSeriesSet`: reads one `ChunkedReadResponse` per series from a chunked reader. The reader owns
/// the response body; on the end of the stream or an error the rest of the body is drained (on error), the
/// body is dropped (Go `Close`) and `cancel` runs with the error (`ERR_EOF` at a clean end).
pub struct ChunkedSeriesSet<R: Read + Send> {
    chunked_reader: Option<ChunkedReader<R>>,
    mint: i64,
    maxt: i64,
    cancel: Box<dyn FnMut(&Error) + Send>,
    current: Option<SeriesArc>,
    err: Option<Error>,
    exhausted: bool,
}

/// Go `NewChunkedSeriesSet`.
pub fn new_chunked_series_set<R: Read + Send + 'static>(
    chunked_reader: ChunkedReader<R>,
    mint: i64,
    maxt: i64,
    cancel: impl FnMut(&Error) + Send + 'static,
) -> BoxSeriesSet {
    Box::new(ChunkedSeriesSet::new(chunked_reader, mint, maxt, cancel))
}

impl<R: Read + Send> ChunkedSeriesSet<R> {
    pub fn new(
        chunked_reader: ChunkedReader<R>,
        mint: i64,
        maxt: i64,
        cancel: impl FnMut(&Error) + Send + 'static,
    ) -> ChunkedSeriesSet<R> {
        ChunkedSeriesSet {
            chunked_reader: Some(chunked_reader),
            mint,
            maxt,
            cancel: Box::new(cancel),
            current: None,
            err: None,
            exhausted: false,
        }
    }
}

impl<R: Read + Send> SeriesSet for ChunkedSeriesSet<R> {
    /// Blocks until the next series is available.
    fn next(&mut self) -> bool {
        if self.exhausted {
            // Never read again after the end (Go: avoids "read on closed response body").
            return false;
        }
        let Some(reader) = self.chunked_reader.as_mut() else {
            return false;
        };
        match reader.next_proto::<prompb::ChunkedReadResponse>() {
            Err(err) => {
                if !err.is(&ERR_EOF) {
                    self.err = Some(err.clone());
                    let _ = io::copy(reader.get_mut(), &mut io::sink());
                }
                // Close the body.
                self.chunked_reader = None;
                (self.cancel)(&err);
                self.exhausted = true;
                false
            }
            Ok(mut res) => {
                let cs = res.chunked_series.swap_remove(0);
                self.current = Some(Arc::new(RemoteChunkedSeries::new(cs, self.mint, self.maxt)));
                true
            }
        }
    }

    fn at(&self) -> &SeriesArc {
        self.current.as_ref().expect("At called before Next")
    }

    fn err(&self) -> Option<Error> {
        self.err.clone()
    }

    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `chunkedSeries`: a series of a streamed response, limited to [mint, maxt].
#[derive(Clone, Debug)]
pub struct RemoteChunkedSeries {
    labels: Labels,
    pub chunks: Vec<prompb::Chunk>,
    pub mint: i64,
    pub maxt: i64,
}

impl RemoteChunkedSeries {
    pub fn new(cs: prompb::ChunkedSeries, mint: i64, maxt: i64) -> RemoteChunkedSeries {
        let mut b = ScratchBuilder::new(0);
        RemoteChunkedSeries { labels: cs.to_labels(&mut b), chunks: cs.chunks, mint, maxt }
    }
}

impl Series for RemoteChunkedSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(new_chunked_series_iterator(&self.chunks, self.mint, self.maxt)))
    }
}

/// Go `chunkedSeriesIterator`: iterates the samples of the chunks within [mint, maxt].
pub struct ChunkedSeriesIterator<'a> {
    chunks: &'a [prompb::Chunk],
    idx: usize,
    cur: Option<AnyIterator<'a>>,
    val_type: ValueType,
    mint: i64,
    maxt: i64,
    err: Option<chunkenc::Error>,
}

/// Go `newChunkedSeriesIterator`.
pub fn new_chunked_series_iterator(chunks: &[prompb::Chunk], mint: i64, maxt: i64) -> ChunkedSeriesIterator<'_> {
    let mut it = ChunkedSeriesIterator { chunks: &[], idx: 0, cur: None, val_type: ValueType::None, mint, maxt, err: None };
    it.reset(chunks, mint, maxt);
    it
}

impl<'a> ChunkedSeriesIterator<'a> {
    /// Whether a chunk iterator is set (Go `it.cur != nil`).
    pub fn has_cur(&self) -> bool {
        self.cur.is_some()
    }

    fn cur(&self) -> &AnyIterator<'a> {
        self.cur.as_ref().expect("chunkedSeriesIterator: no current chunk")
    }

    fn reset_iterator(&mut self) {
        if self.idx < self.chunks.len() {
            let chunk = &self.chunks[self.idx];
            match AnyIterator::new(Encoding(chunk.r#type.0 as u8), &chunk.data) {
                Ok(it) => self.cur = Some(it),
                Err(e) => self.err = Some(e),
            }
        } else {
            self.cur = Some(AnyIterator::Nop(NopIterator));
        }
    }

    fn reset(&mut self, chunks: &'a [prompb::Chunk], mint: i64, maxt: i64) {
        self.chunks = chunks;
        self.mint = mint;
        self.maxt = maxt;
        self.idx = 0;
        if !chunks.is_empty() {
            self.reset_iterator();
        }
    }
}

impl ChunkIterator for ChunkedSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        if self.err.is_some() || self.chunks.is_empty() {
            return ValueType::None;
        }
        loop {
            self.val_type = self.cur.as_mut().expect("current chunk").next();
            if self.val_type == ValueType::None {
                break;
            }
            let at_t = self.at_t();
            if at_t > self.maxt {
                self.chunks = &[]; // Exhaust, so that later calls return fast.
                return ValueType::None;
            }
            if at_t >= self.mint {
                return self.val_type;
            }
        }
        if self.idx >= self.chunks.len() - 1 {
            self.val_type = ValueType::None;
        } else {
            self.idx += 1;
            self.reset_iterator();
            self.val_type = self.next();
        }
        self.val_type
    }

    fn seek(&mut self, t: i64) -> ValueType {
        if self.err.is_some() || self.chunks.is_empty() {
            return ValueType::None;
        }
        let start_idx = self.idx;
        let chunks = self.chunks;
        self.idx += search(chunks.len() - start_idx, |i| chunks[start_idx + i].max_time_ms >= t);
        if self.idx > start_idx {
            self.reset_iterator();
        } else {
            let ts = self.cur().at_t();
            if ts >= t {
                return self.val_type;
            }
        }
        loop {
            self.val_type = self.cur.as_mut().expect("current chunk").next();
            if self.val_type == ValueType::None {
                break;
            }
            let ts = self.cur().at_t();
            if ts > self.maxt {
                self.chunks = &[];
                return ValueType::None;
            }
            if ts >= t && ts >= self.mint {
                return self.val_type;
            }
        }
        self.val_type = ValueType::None;
        self.val_type
    }

    fn at(&self) -> (i64, f64) {
        self.cur().at()
    }
    fn at_histogram(&self, h: Option<ModelHistogram>) -> (i64, ModelHistogram) {
        self.cur().at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.cur().at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        self.cur().at_t()
    }
    fn at_st(&self) -> i64 {
        self.cur().at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.err.as_ref()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Labels and matchers.

/// Go `validateLabelsAndMetricName`: checks the names and values of remote read labels (UTF-8 validation)
/// and that there are no duplicate names.
///
/// Rust note: label strings are always valid UTF-8 here (the protobuf decoder replaces invalid bytes with
/// U+FFFD), so the UTF-8 checks only find empty names.
pub fn validate_labels_and_metric_name(ls: &[prompb::Label]) -> Result<(), Error> {
    for (i, l) in ls.iter().enumerate() {
        if l.name == METRIC_NAME && !ValidationScheme::Utf8.is_valid_metric_name(&l.value) {
            return Err(Error::msg(format!("invalid metric name: {}", l.value)));
        }
        if !ValidationScheme::Utf8.is_valid_label_name(&l.name) {
            return Err(Error::msg(format!("invalid label name: {}", l.name)));
        }
        if i > 0 && l.name == ls[i - 1].name {
            return Err(Error::msg(format!("duplicate label with name: {}", l.name)));
        }
    }
    Ok(())
}

/// Go `ToLabelMatchers`.
pub fn to_label_matchers(matchers: &[Matcher]) -> Result<Vec<LabelMatcher>, Error> {
    Ok(matchers
        .iter()
        .map(|m| LabelMatcher {
            r#type: match m.typ {
                MatchType::Equal => MatcherType::EQ,
                MatchType::NotEqual => MatcherType::NEQ,
                MatchType::Regexp => MatcherType::RE,
                MatchType::NotRegexp => MatcherType::NRE,
            },
            name: m.name.clone(),
            value: m.value.clone(),
        })
        .collect())
}

/// Go `FromLabelMatchers`.
pub fn from_label_matchers(matchers: &[LabelMatcher]) -> Result<Vec<Matcher>, Error> {
    let mut result = Vec::with_capacity(matchers.len());
    for m in matchers {
        let typ = match m.r#type {
            MatcherType::EQ => MatchType::Equal,
            MatcherType::NEQ => MatchType::NotEqual,
            MatcherType::RE => MatchType::Regexp,
            MatcherType::NRE => MatchType::NotRegexp,
            _ => return Err(Error::msg("invalid matcher type")),
        };
        result.push(Matcher::new(typ, &m.name, &m.value).map_err(|e| Error::msg(e.to_string()))?);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------------------------------------------
// Write requests.

/// Go `DecodeWriteRequest`: a snappy-compressed remote write 1.0 request.
pub fn decode_write_request(compressed: &[u8]) -> Result<prompb::WriteRequest, Error> {
    let req_buf = snappy_decode(compressed)?;
    prompb::WriteRequest::unmarshal(&req_buf).map_err(proto_err)
}

/// Go `DecodeWriteV2Request`: a snappy-compressed remote write 2.0 request.
pub fn decode_write_v2_request(compressed: &[u8]) -> Result<writev2::Request, Error> {
    let req_buf = snappy_decode(compressed)?;
    writev2::Request::unmarshal(&req_buf).map_err(proto_err)
}
