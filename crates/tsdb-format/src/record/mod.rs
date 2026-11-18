//! Port of `tsdb/record`: WAL record types, Encoder and Decoder (byte-identical to Go), buffer pools.
//!
//! Encoders append to a caller buffer (`&mut Vec<u8>`), decoders append to caller vectors (`&mut Vec<T>`), so a
//! caller that reuses its buffers allocates nothing per sample.

mod buffers;
#[cfg(test)]
mod go_golden;

pub use buffers::BuffersPool;

use std::fmt;
use std::sync::Arc;

use model::histogram::{
    self as histogram, CounterResetHint, EXPONENTIAL_SCHEMA_MAX, EXPONENTIAL_SCHEMA_MAX_RESERVED, FloatHistogram,
    Histogram, Span,
};
use model::labels::{Labels, ScratchBuilder};
use model::metadata::MetricType as ModelMetricType;

use crate::chunks::HeadSeriesRef;
use crate::encoding::{self, Decbuf, put_uvarint, put_varint};
use crate::tombstones::{Interval, Stone};

/// Go `chunks.ChunkDiskMapperRef`: a reference to a chunk in the head chunk files (segment << 32 | offset).
// TODO(chunks): use chunks::ChunkDiskMapperRef after merge.
pub type ChunkDiskMapperRef = u64;

// ---------------------------------------------------------------------------------------------------------------
// Types.

/// Go `Type`: the data type of a record (its first byte).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Type {
    /// Returned for unrecognised WAL record types.
    Unknown = 255,
    Series = 1,
    Samples = 2,
    Tombstones = 3,
    Exemplars = 4,
    /// OOO WBL records of type MmapMarkers.
    MmapMarkers = 5,
    Metadata = 6,
    HistogramSamples = 7,
    FloatHistogramSamples = 8,
    CustomBucketsHistogramSamples = 9,
    CustomBucketsFloatHistogramSamples = 10,
    /// Float samples with timestamp and an optional start timestamp (ST) per sample.
    SamplesV2 = 11,
    /// Histograms with a start time per sample.
    HistogramSamplesV2 = 12,
    /// Float histograms with a start time per sample.
    FloatHistogramSamplesV2 = 13,
    /// Checkpoint records that carry the mint a WAL truncation used. Only ever written into checkpoints.
    MinValidTime = 14,
}

impl Type {
    /// The record type of byte `b`; `Unknown` for unrecognised values.
    #[inline]
    pub fn from_u8(b: u8) -> Type {
        match b {
            1 => Type::Series,
            2 => Type::Samples,
            3 => Type::Tombstones,
            4 => Type::Exemplars,
            5 => Type::MmapMarkers,
            6 => Type::Metadata,
            7 => Type::HistogramSamples,
            8 => Type::FloatHistogramSamples,
            9 => Type::CustomBucketsHistogramSamples,
            10 => Type::CustomBucketsFloatHistogramSamples,
            11 => Type::SamplesV2,
            12 => Type::HistogramSamplesV2,
            13 => Type::FloatHistogramSamplesV2,
            14 => Type::MinValidTime,
            _ => Type::Unknown,
        }
    }

    /// Go `Type.String`.
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Series => "series",
            Type::Samples => "samples",
            Type::SamplesV2 => "samples_v2",
            Type::Tombstones => "tombstones",
            Type::Exemplars => "exemplars",
            Type::HistogramSamples => "histogram_samples",
            Type::FloatHistogramSamples => "float_histogram_samples",
            Type::CustomBucketsHistogramSamples => "custom_buckets_histogram_samples",
            Type::CustomBucketsFloatHistogramSamples => "custom_buckets_float_histogram_samples",
            Type::HistogramSamplesV2 => "histogram_samples_v2",
            Type::FloatHistogramSamplesV2 => "float_histogram_samples_v2",
            Type::MmapMarkers => "mmapmarkers",
            Type::Metadata => "metadata",
            Type::MinValidTime => "min_valid_time",
            Type::Unknown => "unknown",
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Go `MetricType`: the type of a series as stored in metadata records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    UnknownMt = 0,
    Counter = 1,
    Gauge = 2,
    HistogramSample = 3,
    GaugeHistogram = 4,
    Summary = 5,
    Info = 6,
    Stateset = 7,
}

/// Go `GetMetricType`.
pub fn get_metric_type(t: ModelMetricType) -> u8 {
    (match t {
        ModelMetricType::Counter => MetricType::Counter,
        ModelMetricType::Gauge => MetricType::Gauge,
        ModelMetricType::Histogram => MetricType::HistogramSample,
        ModelMetricType::GaugeHistogram => MetricType::GaugeHistogram,
        ModelMetricType::Summary => MetricType::Summary,
        ModelMetricType::Info => MetricType::Info,
        ModelMetricType::StateSet => MetricType::Stateset,
        _ => MetricType::UnknownMt,
    }) as u8
}

/// Go `ToMetricType`.
pub fn to_metric_type(m: u8) -> ModelMetricType {
    match m {
        1 => ModelMetricType::Counter,
        2 => ModelMetricType::Gauge,
        3 => ModelMetricType::Histogram,
        4 => ModelMetricType::GaugeHistogram,
        5 => ModelMetricType::Summary,
        6 => ModelMetricType::Info,
        7 => ModelMetricType::StateSet,
        _ => ModelMetricType::Unknown,
    }
}

const UNIT_META_NAME: &str = "UNIT";
const HELP_META_NAME: &str = "HELP";

/// Go `ErrNotFound` text ("Duplicate ErrNotFound from head.go").
pub const ERR_NOT_FOUND: &str = "not found";

// ---------------------------------------------------------------------------------------------------------------
// Errors.

/// Errors of the record decoder. `Display` is Go's text.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// A bare `tsdb/encoding` error (Go returns `dec.Err()` unwrapped).
    Encoding(encoding::Error),
    /// Go `fmt.Errorf("decode error after %d <what>: %w", n, err)`.
    DecodeAfter { n: usize, what: &'static str, err: encoding::Error },
    /// Go `fmt.Errorf("error reducing resolution of histogram #%d: %w", n, err)`.
    ReduceResolution { n: usize, err: histogram::Error },
    /// Any other error (Go `errors.New` / `fmt.Errorf` without `%w`).
    Msg(String),
}

impl Error {
    /// Go `errors.Is(err, e)` for an encoding sentinel.
    pub fn is(&self, e: &encoding::Error) -> bool {
        match self {
            Error::Encoding(x) | Error::DecodeAfter { err: x, .. } => x == e,
            _ => false,
        }
    }

    fn invalid_record_type() -> Error {
        Error::Msg("invalid record type".to_string())
    }

    fn bytes_left(n: usize) -> Error {
        Error::Msg(format!("unexpected {n} bytes left in entry"))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Encoding(e) => write!(f, "{e}"),
            Error::DecodeAfter { n, what, err } => write!(f, "decode error after {n} {what}: {err}"),
            Error::ReduceResolution { n, err } => write!(f, "error reducing resolution of histogram #{n}: {err}"),
            Error::Msg(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------------------------------------------
// Logger.

/// A minimal stand-in for Go's `*slog.Logger` (the decoder only logs warnings). Each call of the sink gets one
/// line in slog text form without the time, e.g. `level=WARN msg="..." schema=-100 timestamp=1234`.
/// The default logger discards everything (Go `promslog.NewNopLogger()`).
// TODO(util): use the shared logger after merge.
#[derive(Clone, Default)]
pub struct Logger(Option<Arc<dyn Fn(&str) + Send + Sync>>);

impl Logger {
    /// A logger that discards everything.
    pub fn nop() -> Logger {
        Logger(None)
    }

    /// A logger that sends each line to `sink`.
    pub fn new(sink: impl Fn(&str) + Send + Sync + 'static) -> Logger {
        Logger(Some(Arc::new(sink)))
    }

    fn log(&self, level: &str, msg: &str, kv: &[(&str, &dyn fmt::Display)]) {
        if let Some(s) = &self.0 {
            let mut line = format!("level={level} msg={msg:?}");
            for (k, v) in kv {
                line.push_str(&format!(" {k}={v}"));
            }
            s(&line);
        }
    }

    /// Logs at level WARN.
    pub fn warn(&self, msg: &str, kv: &[(&str, &dyn fmt::Display)]) {
        self.log("WARN", msg, kv);
    }

    /// Logs at level ERROR.
    pub fn error(&self, msg: &str, kv: &[(&str, &dyn fmt::Display)]) {
        self.log("ERROR", msg, kv);
    }
}

impl fmt::Debug for Logger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "Logger" } else { "Logger(nop)" })
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Record entries.

/// Go `RefSeries`: the series labels with the series ID.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RefSeries {
    pub r#ref: HeadSeriesRef,
    pub labels: Labels,
}

/// Go `RefSample`: a timestamp/start timestamp/value associated with a series reference.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RefSample {
    pub r#ref: HeadSeriesRef,
    pub st: i64,
    pub t: i64,
    pub v: f64,
}

/// Go `RefMetadata`: the metadata associated with a series ID.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefMetadata {
    pub r#ref: HeadSeriesRef,
    pub r#type: u8,
    pub unit: String,
    pub help: String,
}

/// Go `RefExemplar`: an exemplar with its labels, timestamp and value, and a series reference.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RefExemplar {
    pub r#ref: HeadSeriesRef,
    pub t: i64,
    pub v: f64,
    pub labels: Labels,
}

/// Go `RefHistogramSample`. The histogram is shared (Go `*histogram.Histogram`); clones are cheap.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RefHistogramSample {
    pub r#ref: HeadSeriesRef,
    pub st: i64,
    pub t: i64,
    pub h: Arc<Histogram>,
}

/// Go `RefFloatHistogramSample`. The histogram is shared (Go `*histogram.FloatHistogram`); clones are cheap.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RefFloatHistogramSample {
    pub r#ref: HeadSeriesRef,
    pub st: i64,
    pub t: i64,
    pub fh: Arc<FloatHistogram>,
}

/// Go `RefMmapMarker`: all the samples of the series until now have been m-mapped to disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefMmapMarker {
    pub r#ref: HeadSeriesRef,
    pub mmap_ref: ChunkDiskMapperRef,
}

// ---------------------------------------------------------------------------------------------------------------
// Decoder.

/// Go `Decoder`: decodes series, sample, metadata and tombstone records. Methods append to the given vector and
/// return `Ok(())`; on error the vector is truncated back to its length on entry (Go returns nil).
#[derive(Debug, Default)]
pub struct Decoder {
    builder: ScratchBuilder,
    logger: Logger,
}

#[inline]
fn bytes_str(b: &[u8]) -> std::borrow::Cow<'_, str> {
    // Go keeps the raw bytes (yoloString); label data written by Prometheus is valid UTF-8.
    String::from_utf8_lossy(b)
}

impl Decoder {
    /// Go `NewDecoder` (the symbol table argument is unused upstream too).
    pub fn new(logger: Logger) -> Decoder {
        Decoder { builder: ScratchBuilder::new(0), logger }
    }

    /// Go `Type`: the type of the record, `Unknown` if no valid record type is found.
    #[inline]
    pub fn r#type(&self, rec: &[u8]) -> Type {
        match rec.first() {
            Some(&b) => Type::from_u8(b),
            None => Type::Unknown,
        }
    }

    /// Go `Series`: appends the series in `rec` to `series`.
    pub fn series(&mut self, rec: &[u8], series: &mut Vec<RefSeries>) -> Result<(), Error> {
        let start = series.len();
        let r = self.series_inner(rec, series);
        if r.is_err() {
            series.truncate(start);
        }
        r
    }

    fn series_inner(&mut self, rec: &[u8], series: &mut Vec<RefSeries>) -> Result<(), Error> {
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::Series as u8 {
            return Err(Error::invalid_record_type());
        }
        while !dec.b.is_empty() && dec.e.is_none() {
            let r = dec.be64();
            let labels = self.decode_labels(&mut dec);
            series.push(RefSeries { r#ref: r, labels });
        }
        if let Some(e) = dec.e {
            return Err(Error::Encoding(e));
        }
        if !dec.b.is_empty() {
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(())
    }

    /// Go `MinValidTime`: decodes a min valid time record.
    pub fn min_valid_time(&self, rec: &[u8]) -> Result<i64, Error> {
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::MinValidTime as u8 {
            return Err(Error::invalid_record_type());
        }
        let mint = dec.be64int64();
        if let Some(e) = dec.e {
            return Err(Error::Encoding(e));
        }
        if !dec.b.is_empty() {
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(mint)
    }

    /// Go `Metadata`: appends the metadata in `rec` to `metadata`. Unknown fields are skipped.
    pub fn metadata(&self, rec: &[u8], metadata: &mut Vec<RefMetadata>) -> Result<(), Error> {
        let start = metadata.len();
        let r = Self::metadata_inner(rec, metadata);
        if r.is_err() {
            metadata.truncate(start);
        }
        r
    }

    fn metadata_inner(rec: &[u8], metadata: &mut Vec<RefMetadata>) -> Result<(), Error> {
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::Metadata as u8 {
            return Err(Error::invalid_record_type());
        }
        while !dec.b.is_empty() && dec.e.is_none() {
            let r = dec.uvarint64();
            let typ = dec.byte();
            let num_fields = dec.uvarint();

            // We're currently aware of two more metadata fields other than TYPE; that is UNIT and HELP. We can
            // skip the rest of the fields (if we encounter any), but we must decode them anyway so we can
            // correctly align with the start with the next metadata record.
            let mut unit = String::new();
            let mut help = String::new();
            let mut i = 0;
            while i < num_fields && dec.e.is_none() {
                let name = dec.uvarint_bytes();
                let value = dec.uvarint_bytes();
                if name == UNIT_META_NAME.as_bytes() {
                    unit = bytes_str(value).into_owned();
                } else if name == HELP_META_NAME.as_bytes() {
                    help = bytes_str(value).into_owned();
                }
                i += 1;
            }
            metadata.push(RefMetadata { r#ref: r, r#type: typ, unit, help });
        }
        if let Some(e) = dec.e {
            return Err(Error::Encoding(e));
        }
        if !dec.b.is_empty() {
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(())
    }

    /// Go `DecodeLabels`: decodes one set of labels from `dec`.
    pub fn decode_labels(&mut self, dec: &mut Decbuf) -> Labels {
        self.builder.reset();
        let n = dec.uvarint();
        let mut i = 0;
        // Go keeps adding empty labels after an error; they are discarded with the error, so stop early.
        while i < n && dec.e.is_none() {
            let name = dec.uvarint_bytes();
            let value = dec.uvarint_bytes();
            self.builder.add(&bytes_str(name), &bytes_str(value));
            i += 1;
        }
        self.builder.labels()
    }

    /// Go `Samples`: appends the samples in `rec` (a Samples or SamplesV2 record) to `samples`.
    pub fn samples(&self, rec: &[u8], samples: &mut Vec<RefSample>) -> Result<(), Error> {
        let start = samples.len();
        let mut dec = Decbuf::new(rec);
        let typ = dec.byte();
        let r = match Type::from_u8(typ) {
            Type::Samples => samples_v1(&mut dec, samples),
            Type::SamplesV2 => samples_v2(&mut dec, samples),
            _ => Err(Error::Msg(format!("invalid record type {typ}, expected Samples(2) or SamplesV2(11)"))),
        };
        if r.is_err() {
            samples.truncate(start);
        }
        r
    }

    /// Go `Tombstones`: appends the tombstones in `rec` to `tstones` (one stone per interval).
    pub fn tombstones(&self, rec: &[u8], tstones: &mut Vec<Stone>) -> Result<(), Error> {
        let start = tstones.len();
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::Tombstones as u8 {
            return Err(Error::invalid_record_type());
        }
        while !dec.b.is_empty() && dec.e.is_none() {
            let r = dec.be64();
            let mint = dec.varint64();
            let maxt = dec.varint64();
            tstones.push(Stone { r#ref: r, intervals: vec![Interval { mint, maxt }] });
        }
        if let Some(e) = dec.e {
            tstones.truncate(start);
            return Err(Error::Encoding(e));
        }
        if !dec.b.is_empty() {
            tstones.truncate(start);
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(())
    }

    /// Go `Exemplars`: appends the exemplars in `rec` to `exemplars`.
    pub fn exemplars(&mut self, rec: &[u8], exemplars: &mut Vec<RefExemplar>) -> Result<(), Error> {
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::Exemplars as u8 {
            return Err(Error::invalid_record_type());
        }
        self.exemplars_from_buffer(&mut dec, exemplars)
    }

    /// Go `ExemplarsFromBuffer`: appends the exemplars in `dec` to `exemplars`.
    pub fn exemplars_from_buffer(&mut self, dec: &mut Decbuf, exemplars: &mut Vec<RefExemplar>) -> Result<(), Error> {
        if dec.b.is_empty() {
            return Ok(());
        }
        let start = exemplars.len();
        let base_ref = dec.be64();
        let base_time = dec.be64int64();
        while !dec.b.is_empty() && dec.e.is_none() {
            let dref = dec.varint64();
            let dtime = dec.varint64();
            let val = dec.be64();
            let labels = self.decode_labels(dec);
            exemplars.push(RefExemplar {
                r#ref: base_ref.wrapping_add(dref as u64),
                t: base_time.wrapping_add(dtime),
                v: f64::from_bits(val),
                labels,
            });
        }
        if let Some(e) = dec.e.clone() {
            let n = exemplars.len();
            exemplars.truncate(start);
            return Err(Error::DecodeAfter { n, what: "exemplars", err: e });
        }
        if !dec.b.is_empty() {
            exemplars.truncate(start);
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(())
    }

    /// Go `MmapMarkers`: appends the mmap markers in `rec` to `markers`.
    pub fn mmap_markers(&self, rec: &[u8], markers: &mut Vec<RefMmapMarker>) -> Result<(), Error> {
        let mut dec = Decbuf::new(rec);
        if dec.byte() != Type::MmapMarkers as u8 {
            return Err(Error::invalid_record_type());
        }
        if dec.b.is_empty() {
            return Ok(());
        }
        let start = markers.len();
        while !dec.b.is_empty() && dec.e.is_none() {
            let r = dec.be64();
            let mmap_ref = dec.be64();
            markers.push(RefMmapMarker { r#ref: r, mmap_ref });
        }
        if let Some(e) = dec.e {
            let n = markers.len();
            markers.truncate(start);
            return Err(Error::DecodeAfter { n, what: "mmap markers", err: e });
        }
        if !dec.b.is_empty() {
            markers.truncate(start);
            return Err(Error::bytes_left(dec.b.len()));
        }
        Ok(())
    }

    /// Go `HistogramSamples`: appends the histogram samples in `rec` (HistogramSamples,
    /// CustomBucketsHistogramSamples or HistogramSamplesV2) to `histograms`.
    pub fn histogram_samples(&self, rec: &[u8], histograms: &mut Vec<RefHistogramSample>) -> Result<(), Error> {
        let start = histograms.len();
        let mut dec = Decbuf::new(rec);
        let typ = Type::from_u8(dec.byte());
        let r = match typ {
            Type::HistogramSamples | Type::CustomBucketsHistogramSamples => {
                self.histogram_samples_v1(&mut dec, histograms)
            }
            Type::HistogramSamplesV2 => self.histogram_samples_v2(&mut dec, histograms),
            _ => Err(Error::Msg(format!("invalid record type {typ}"))),
        };
        if r.is_err() {
            histograms.truncate(start);
        }
        r
    }

    /// Checks the schema of a decoded histogram: `Ok(false)` to skip it (unknown schema, logged), and reduces
    /// a too high resolution (from a newer Prometheus) to the maximum supported schema.
    fn check_histogram_schema<H: SchemaReducer>(&self, h: &mut H, t: i64, n: usize) -> Result<bool, Error> {
        let schema = h.schema();
        if !histogram::is_known_schema(schema) {
            self.logger.warn(
                "skipping histogram with unknown schema in WAL record",
                &[("schema", &schema), ("timestamp", &t)],
            );
            return Ok(false);
        }
        if schema > EXPONENTIAL_SCHEMA_MAX && schema <= EXPONENTIAL_SCHEMA_MAX_RESERVED {
            // This is a very slow path, but it should only happen if the record is from a newer Prometheus
            // version that supports higher resolution.
            if let Err(err) = h.reduce_resolution(EXPONENTIAL_SCHEMA_MAX) {
                return Err(Error::ReduceResolution { n: n + 1, err });
            }
        }
        Ok(true)
    }

    fn histogram_samples_v1(&self, dec: &mut Decbuf, histograms: &mut Vec<RefHistogramSample>) -> Result<(), Error> {
        if dec.b.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64();
        let base_time = dec.be64int64();
        while !dec.b.is_empty() && dec.e.is_none() {
            let dref = dec.varint64();
            let dtime = dec.varint64();
            let r = base_ref.wrapping_add(dref as u64);
            let t = base_time.wrapping_add(dtime);
            let mut h = Histogram::default();
            decode_histogram(dec, &mut h);
            if !self.check_histogram_schema(&mut h, t, histograms.len())? {
                continue;
            }
            histograms.push(RefHistogramSample { r#ref: r, st: 0, t, h: Arc::new(h) });
        }
        finish_decode(dec, histograms.len(), "histograms")
    }

    fn histogram_samples_v2(&self, dec: &mut Decbuf, histograms: &mut Vec<RefHistogramSample>) -> Result<(), Error> {
        if dec.b.is_empty() {
            return Ok(());
        }
        let first_ref = dec.varint64();
        let first_t = dec.varint64();
        let first_st = dec.varint64();
        let mut prev_ref: i64 = 0;
        let mut prev_st: i64 = 0;
        let mut has_prev = false;
        while !dec.b.is_empty() && dec.e.is_none() {
            let (r, t, st);
            if !has_prev {
                (r, t, st) = (first_ref, first_t, first_st);
                has_prev = true;
            } else {
                r = prev_ref.wrapping_add(dec.varint64());
                t = first_t.wrapping_add(dec.varint64());
                st = read_st_marker(dec, prev_st, first_st);
            }
            (prev_ref, prev_st) = (r, st);
            let mut h = Histogram::default();
            decode_histogram(dec, &mut h);
            if !self.check_histogram_schema(&mut h, t, histograms.len())? {
                continue;
            }
            histograms.push(RefHistogramSample { r#ref: r as HeadSeriesRef, st, t, h: Arc::new(h) });
        }
        finish_decode(dec, histograms.len(), "histograms")
    }

    /// Go `FloatHistogramSamples`: appends the float histogram samples in `rec` (FloatHistogramSamples,
    /// CustomBucketsFloatHistogramSamples or FloatHistogramSamplesV2) to `histograms`.
    pub fn float_histogram_samples(
        &self,
        rec: &[u8],
        histograms: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), Error> {
        let start = histograms.len();
        let mut dec = Decbuf::new(rec);
        let typ = Type::from_u8(dec.byte());
        let r = match typ {
            Type::FloatHistogramSamples | Type::CustomBucketsFloatHistogramSamples => {
                self.float_histogram_samples_v1(&mut dec, histograms)
            }
            Type::FloatHistogramSamplesV2 => self.float_histogram_samples_v2(&mut dec, histograms),
            _ => Err(Error::Msg(format!("invalid record type {typ}"))),
        };
        if r.is_err() {
            histograms.truncate(start);
        }
        r
    }

    fn float_histogram_samples_v1(
        &self,
        dec: &mut Decbuf,
        histograms: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), Error> {
        if dec.b.is_empty() {
            return Ok(());
        }
        let base_ref = dec.be64();
        let base_time = dec.be64int64();
        while !dec.b.is_empty() && dec.e.is_none() {
            let dref = dec.varint64();
            let dtime = dec.varint64();
            let r = base_ref.wrapping_add(dref as u64);
            let t = base_time.wrapping_add(dtime);
            let mut fh = FloatHistogram::default();
            decode_float_histogram(dec, &mut fh);
            if !self.check_histogram_schema(&mut fh, t, histograms.len())? {
                continue;
            }
            histograms.push(RefFloatHistogramSample { r#ref: r, st: 0, t, fh: Arc::new(fh) });
        }
        finish_decode(dec, histograms.len(), "histograms")
    }

    fn float_histogram_samples_v2(
        &self,
        dec: &mut Decbuf,
        histograms: &mut Vec<RefFloatHistogramSample>,
    ) -> Result<(), Error> {
        if dec.b.is_empty() {
            return Ok(());
        }
        let first_ref = dec.varint64();
        let first_t = dec.varint64();
        let first_st = dec.varint64();
        let mut prev_ref: i64 = 0;
        let mut prev_st: i64 = 0;
        let mut has_prev = false;
        while !dec.b.is_empty() && dec.e.is_none() {
            let (r, t, st);
            if !has_prev {
                (r, t, st) = (first_ref, first_t, first_st);
                has_prev = true;
            } else {
                r = prev_ref.wrapping_add(dec.varint64());
                t = first_t.wrapping_add(dec.varint64());
                st = read_st_marker(dec, prev_st, first_st);
            }
            (prev_ref, prev_st) = (r, st);
            let mut fh = FloatHistogram::default();
            decode_float_histogram(dec, &mut fh);
            if !self.check_histogram_schema(&mut fh, t, histograms.len())? {
                continue;
            }
            histograms.push(RefFloatHistogramSample { r#ref: r as HeadSeriesRef, st, t, fh: Arc::new(fh) });
        }
        finish_decode(dec, histograms.len(), "histograms")
    }
}

/// The common end of the sample decoders.
#[inline]
fn finish_decode(dec: &Decbuf, n: usize, what: &'static str) -> Result<(), Error> {
    if let Some(e) = &dec.e {
        return Err(Error::DecodeAfter { n, what, err: e.clone() });
    }
    if !dec.b.is_empty() {
        return Err(Error::bytes_left(dec.b.len()));
    }
    Ok(())
}

trait SchemaReducer {
    fn schema(&self) -> i32;
    fn reduce_resolution(&mut self, target: i32) -> Result<(), histogram::Error>;
}

impl SchemaReducer for Histogram {
    fn schema(&self) -> i32 {
        self.schema
    }
    fn reduce_resolution(&mut self, target: i32) -> Result<(), histogram::Error> {
        Histogram::reduce_resolution(self, target)
    }
}

impl SchemaReducer for FloatHistogram {
    fn schema(&self) -> i32 {
        self.schema
    }
    fn reduce_resolution(&mut self, target: i32) -> Result<(), histogram::Error> {
        FloatHistogram::reduce_resolution(self, target)
    }
}

/// Go `samplesV1`: ignores ST information.
fn samples_v1(dec: &mut Decbuf, samples: &mut Vec<RefSample>) -> Result<(), Error> {
    if dec.b.is_empty() {
        return Ok(());
    }
    let base_ref = dec.be64();
    let base_time = dec.be64int64();
    // Allow 1 byte for each varint and 8 for the value; the output slice must be at least that big.
    let min_size = dec.b.len() / (1 + 1 + 8);
    if samples.capacity() < min_size {
        // Go makes a new slice here (dropping what was in it).
        *samples = Vec::with_capacity(min_size);
    }
    while !dec.b.is_empty() && dec.e.is_none() {
        let dref = dec.varint64();
        let dtime = dec.varint64();
        let val = dec.be64();
        samples.push(RefSample {
            r#ref: (base_ref as i64).wrapping_add(dref) as HeadSeriesRef,
            st: 0,
            t: base_time.wrapping_add(dtime),
            v: f64::from_bits(val),
        });
    }
    finish_decode(dec, samples.len(), "samples")
}

/// Go `samplesV2`: see [`Encoder::samples`] for the format.
fn samples_v2(dec: &mut Decbuf, samples: &mut Vec<RefSample>) -> Result<(), Error> {
    if dec.b.is_empty() {
        return Ok(());
    }
    // Allow 1 byte for each varint and 8 for the value; the output slice must be at least that big.
    let min_size = dec.b.len() / (1 + 1 + 8);
    if samples.capacity() < min_size {
        // Go makes a new slice here (dropping what was in it).
        *samples = Vec::with_capacity(min_size);
    }
    let mut first_t: i64 = 0;
    let mut first_st: i64 = 0;
    while !dec.b.is_empty() && dec.e.is_none() {
        let (r, t, st);
        // Go checks the length of the output slice, including entries the caller passed in.
        match samples.last() {
            None => {
                r = dec.varint64();
                first_t = dec.varint64();
                t = first_t;
                st = dec.varint64();
                first_st = st;
            }
            Some(prev) => {
                let (prev_ref, prev_st) = (prev.r#ref, prev.st);
                r = (prev_ref as i64).wrapping_add(dec.varint64());
                t = first_t.wrapping_add(dec.varint64());
                st = read_st_marker(dec, prev_st, first_st);
            }
        }
        let val = dec.be64();
        samples.push(RefSample { r#ref: r as HeadSeriesRef, st, t, v: f64::from_bits(val) });
    }
    finish_decode(dec, samples.len(), "samples")
}

// Start timestamp marker values for indicating trivial cases.
/// Sample has no start time.
const NO_ST: u8 = 0;
/// Sample timestamp exists and is the same as the start time of the previous series.
const SAME_ST: u8 = 1;
/// Explicit start timestamp value, delta to first start time.
const EXPLICIT_ST: u8 = 2;

#[inline]
fn read_st_marker(buf: &mut Decbuf, prev_st: i64, first_st: i64) -> i64 {
    match buf.byte() {
        NO_ST => 0,
        SAME_ST => prev_st,
        _ => first_st.wrapping_add(buf.varint64()),
    }
}

#[inline]
fn write_st_marker(b: &mut Vec<u8>, st: i64, first_st: i64, prev_st: i64) {
    if st == 0 {
        b.push(NO_ST);
    } else if st == prev_st {
        b.push(SAME_ST);
    } else {
        b.push(EXPLICIT_ST);
        put_varint(b, st.wrapping_sub(first_st));
    }
}

/// Reads a uvarint count of following elements; sets `InvalidSize` instead of allocating when the count cannot
/// fit in the remaining bytes (each element takes at least one byte). Go would allocate and then fail the same way.
#[inline]
fn read_count(buf: &mut Decbuf) -> usize {
    let l = buf.uvarint64();
    if l > buf.b.len() as u64 {
        if buf.e.is_none() {
            buf.e = Some(encoding::Error::InvalidSize);
        }
        return 0;
    }
    l as usize
}

#[inline]
fn decode_spans(buf: &mut Decbuf, spans: &mut Vec<Span>) {
    let l = read_count(buf);
    spans.clear();
    spans.reserve_exact(l);
    for _ in 0..l {
        let offset = buf.varint64() as i32;
        let length = buf.uvarint32();
        spans.push(Span { offset, length });
    }
}

/// Go `DecodeHistogram`: decodes a Histogram from `buf`.
pub fn decode_histogram(buf: &mut Decbuf, h: &mut Histogram) {
    h.counter_reset_hint = CounterResetHint::from_u8(buf.byte());
    h.schema = buf.varint64() as i32;
    h.zero_threshold = buf.be64_float64();
    h.zero_count = buf.uvarint64();
    h.count = buf.uvarint64();
    h.sum = buf.be64_float64();

    decode_spans(buf, &mut h.positive_spans);
    decode_spans(buf, &mut h.negative_spans);

    let l = read_count(buf);
    h.positive_buckets.clear();
    h.positive_buckets.reserve_exact(l);
    for _ in 0..l {
        h.positive_buckets.push(buf.varint64());
    }
    let l = read_count(buf);
    h.negative_buckets.clear();
    h.negative_buckets.reserve_exact(l);
    for _ in 0..l {
        h.negative_buckets.push(buf.varint64());
    }

    if histogram::is_custom_buckets_schema(h.schema) {
        h.custom_values = decode_custom_values(buf);
    }
}

#[inline]
fn decode_custom_values(buf: &mut Decbuf) -> histogram::CustomValues {
    let l = read_count(buf);
    if l == 0 {
        return None;
    }
    Some((0..l).map(|_| buf.be64_float64()).collect())
}

/// Go `DecodeFloatHistogram`: decodes a FloatHistogram from `buf`.
pub fn decode_float_histogram(buf: &mut Decbuf, fh: &mut FloatHistogram) {
    fh.counter_reset_hint = CounterResetHint::from_u8(buf.byte());
    fh.schema = buf.varint64() as i32;
    fh.zero_threshold = buf.be64_float64();
    fh.zero_count = buf.be64_float64();
    fh.count = buf.be64_float64();
    fh.sum = buf.be64_float64();

    decode_spans(buf, &mut fh.positive_spans);
    decode_spans(buf, &mut fh.negative_spans);

    let l = read_count(buf);
    fh.positive_buckets.clear();
    fh.positive_buckets.reserve_exact(l);
    for _ in 0..l {
        fh.positive_buckets.push(buf.be64_float64());
    }
    let l = read_count(buf);
    fh.negative_buckets.clear();
    fh.negative_buckets.reserve_exact(l);
    for _ in 0..l {
        fh.negative_buckets.push(buf.be64_float64());
    }

    if histogram::is_custom_buckets_schema(fh.schema) {
        fh.custom_values = decode_custom_values(buf);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Encoder.

/// Go `Encoder`: encodes series, sample, and tombstones records. The default value is ready to use. Every method
/// appends the record to `b` (Go `Encbuf{B: b}`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Encoder {
    /// Enables the SamplesV2 (and histogram V2) encodings, which are more efficient than V1 and support a start
    /// time per sample.
    pub enable_st_storage: bool,
}

#[inline]
fn put_be64(b: &mut Vec<u8>, x: u64) {
    b.extend_from_slice(&x.to_be_bytes());
}

#[inline]
fn put_uvarint_str(b: &mut Vec<u8>, s: &str) {
    put_uvarint(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}

/// Go `EncodeLabels`: encodes the contents of `lbls` into `b`.
pub fn encode_labels(b: &mut Vec<u8>, lbls: &Labels) {
    put_uvarint(b, lbls.len() as u64);
    for (n, v) in lbls.iter() {
        put_uvarint_str(b, n);
        put_uvarint_str(b, v);
    }
}

#[inline]
fn encode_spans(b: &mut Vec<u8>, spans: &[Span]) {
    put_uvarint(b, spans.len() as u64);
    for s in spans {
        put_varint(b, s.offset as i64);
        put_uvarint(b, s.length as u64);
    }
}

#[inline]
fn encode_custom_values(b: &mut Vec<u8>, schema: i32, cv: &histogram::CustomValues) {
    if histogram::is_custom_buckets_schema(schema) {
        let cv: &[f64] = cv.as_deref().unwrap_or(&[]);
        put_uvarint(b, cv.len() as u64);
        for v in cv {
            put_be64(b, v.to_bits());
        }
    }
}

/// Go `EncodeHistogram`: encodes a Histogram (regular or custom buckets) into `b`.
pub fn encode_histogram(b: &mut Vec<u8>, h: &Histogram) {
    b.push(h.counter_reset_hint as u8);
    put_varint(b, h.schema as i64);
    put_be64(b, h.zero_threshold.to_bits());
    put_uvarint(b, h.zero_count);
    put_uvarint(b, h.count);
    put_be64(b, h.sum.to_bits());
    encode_spans(b, &h.positive_spans);
    encode_spans(b, &h.negative_spans);
    put_uvarint(b, h.positive_buckets.len() as u64);
    for &x in &h.positive_buckets {
        put_varint(b, x);
    }
    put_uvarint(b, h.negative_buckets.len() as u64);
    for &x in &h.negative_buckets {
        put_varint(b, x);
    }
    encode_custom_values(b, h.schema, &h.custom_values);
}

/// Go `EncodeFloatHistogram`: encodes a FloatHistogram into `b`.
pub fn encode_float_histogram(b: &mut Vec<u8>, h: &FloatHistogram) {
    b.push(h.counter_reset_hint as u8);
    put_varint(b, h.schema as i64);
    put_be64(b, h.zero_threshold.to_bits());
    put_be64(b, h.zero_count.to_bits());
    put_be64(b, h.count.to_bits());
    put_be64(b, h.sum.to_bits());
    encode_spans(b, &h.positive_spans);
    encode_spans(b, &h.negative_spans);
    put_uvarint(b, h.positive_buckets.len() as u64);
    for &x in &h.positive_buckets {
        put_be64(b, x.to_bits());
    }
    put_uvarint(b, h.negative_buckets.len() as u64);
    for &x in &h.negative_buckets {
        put_be64(b, x.to_bits());
    }
    encode_custom_values(b, h.schema, &h.custom_values);
}

impl Encoder {
    /// Go `Series`: appends the encoded series to `b`.
    pub fn series(&self, series: &[RefSeries], b: &mut Vec<u8>) {
        b.push(Type::Series as u8);
        for s in series {
            put_be64(b, s.r#ref);
            encode_labels(b, &s.labels);
        }
    }

    /// Go `MinValidTime`: appends the encoded min valid time record to `b`. Only ever written into checkpoints.
    pub fn min_valid_time(&self, mint: i64, b: &mut Vec<u8>) {
        b.push(Type::MinValidTime as u8);
        put_be64(b, mint as u64);
    }

    /// Go `Metadata`: appends the encoded metadata to `b`.
    pub fn metadata(&self, metadata: &[RefMetadata], b: &mut Vec<u8>) {
        b.push(Type::Metadata as u8);
        for m in metadata {
            put_uvarint(b, m.r#ref);
            b.push(m.r#type);
            put_uvarint(b, 2); // num_fields: We currently have two more metadata fields, UNIT and HELP.
            put_uvarint_str(b, UNIT_META_NAME);
            put_uvarint_str(b, &m.unit);
            put_uvarint_str(b, HELP_META_NAME);
            put_uvarint_str(b, &m.help);
        }
    }

    /// Go `Samples`: appends a Samples record, or a SamplesV2 record when `enable_st_storage` is set.
    ///
    /// V1: BE64 first ref, BE64 first T, then per sample varint (ref - first ref), varint (T - first T), BE64 V.
    /// V2: varint first ref, T, ST and BE64 V; then per sample varint (ref - previous ref), varint
    /// (T - first T), an ST marker byte (none, same as previous, or explicit + varint (ST - first ST)), BE64 V.
    #[inline]
    pub fn samples(&self, samples: &[RefSample], b: &mut Vec<u8>) {
        if self.enable_st_storage {
            Self::samples_v2(samples, b);
        } else {
            Self::samples_v1(samples, b);
        }
    }

    fn samples_v1(samples: &[RefSample], b: &mut Vec<u8>) {
        b.push(Type::Samples as u8);
        let Some(first) = samples.first() else { return };
        b.reserve(16 + samples.len() * 10);
        // Store base timestamp and base reference number of first sample. All samples encode their timestamp
        // and ref as delta to those.
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for s in samples {
            put_varint(b, (s.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, s.t.wrapping_sub(first.t));
            put_be64(b, s.v.to_bits());
        }
    }

    fn samples_v2(samples: &[RefSample], b: &mut Vec<u8>) {
        b.push(Type::SamplesV2 as u8);
        let Some(first) = samples.first() else { return };
        b.reserve(samples.len() * 11);
        // Store first ref, timestamp, ST, and value.
        put_varint(b, first.r#ref as i64);
        put_varint(b, first.t);
        put_varint(b, first.st);
        put_be64(b, first.v.to_bits());
        // Subsequent values are delta to the immediate previous values, and in the case of start timestamp,
        // use the marker byte to indicate what the value should be if it's one of the trivial cases.
        for w in samples.windows(2) {
            let (prev, s) = (&w[0], &w[1]);
            put_varint(b, (s.r#ref as i64).wrapping_sub(prev.r#ref as i64));
            put_varint(b, s.t.wrapping_sub(first.t));
            write_st_marker(b, s.st, first.st, prev.st);
            put_be64(b, s.v.to_bits());
        }
    }

    /// Go `Tombstones`: appends the encoded tombstones to `b` (one entry per interval).
    pub fn tombstones(&self, tstones: &[Stone], b: &mut Vec<u8>) {
        b.push(Type::Tombstones as u8);
        for s in tstones {
            for iv in &s.intervals {
                put_be64(b, s.r#ref);
                put_varint(b, iv.mint);
                put_varint(b, iv.maxt);
            }
        }
    }

    /// Go `Exemplars`: appends the encoded exemplars to `b`.
    pub fn exemplars(&self, exemplars: &[RefExemplar], b: &mut Vec<u8>) {
        b.push(Type::Exemplars as u8);
        if exemplars.is_empty() {
            return;
        }
        self.encode_exemplars_into_buffer(exemplars, b);
    }

    /// Go `EncodeExemplarsIntoBuffer`: appends the encoded exemplars (without a type byte) to `b`. Panics on an
    /// empty slice, as Go does.
    pub fn encode_exemplars_into_buffer(&self, exemplars: &[RefExemplar], b: &mut Vec<u8>) {
        // Store base timestamp and base reference number of first sample. All samples encode their timestamp
        // and ref as delta to those.
        let first = &exemplars[0];
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for ex in exemplars {
            put_varint(b, (ex.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, ex.t.wrapping_sub(first.t));
            put_be64(b, ex.v.to_bits());
            encode_labels(b, &ex.labels);
        }
    }

    /// Go `MmapMarkers`: appends the encoded mmap markers to `b`.
    pub fn mmap_markers(&self, markers: &[RefMmapMarker], b: &mut Vec<u8>) {
        b.push(Type::MmapMarkers as u8);
        for s in markers {
            put_be64(b, s.r#ref);
            put_be64(b, s.mmap_ref);
        }
    }

    /// Go `HistogramSamples`: appends the exponential histogram samples and returns the custom bucket histogram
    /// samples that must be encoded separately with [`Encoder::custom_buckets_histogram_samples`] (V1 only; V2
    /// encodes all of them and returns an empty vector). If all samples use custom buckets, `b` is cleared
    /// entirely (Go `buf.Reset()`).
    pub fn histogram_samples(&self, histograms: &[RefHistogramSample], b: &mut Vec<u8>) -> Vec<RefHistogramSample> {
        if self.enable_st_storage {
            Self::histogram_samples_v2(histograms, b);
            return Vec::new();
        }
        Self::histogram_samples_v1(histograms, b)
    }

    fn histogram_samples_v1(histograms: &[RefHistogramSample], b: &mut Vec<u8>) -> Vec<RefHistogramSample> {
        b.push(Type::HistogramSamples as u8);
        let Some(first) = histograms.first() else { return Vec::new() };
        let mut custom = Vec::new();
        // Store base timestamp and base reference number of first histogram. All histograms encode their
        // timestamp and ref as delta to those.
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for h in histograms {
            if h.h.uses_custom_buckets() {
                custom.push(h.clone());
                continue;
            }
            put_varint(b, (h.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            encode_histogram(b, &h.h);
        }
        // Reset buffer if only custom bucket histograms existed in list of histogram samples.
        if histograms.len() == custom.len() {
            b.clear();
        }
        custom
    }

    fn histogram_samples_v2(histograms: &[RefHistogramSample], b: &mut Vec<u8>) {
        b.push(Type::HistogramSamplesV2 as u8);
        let Some(first) = histograms.first() else { return };
        put_varint(b, first.r#ref as i64);
        put_varint(b, first.t);
        put_varint(b, first.st);
        encode_histogram(b, &first.h);
        for w in histograms.windows(2) {
            let (prev, h) = (&w[0], &w[1]);
            put_varint(b, (h.r#ref as i64).wrapping_sub(prev.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            write_st_marker(b, h.st, first.st, prev.st);
            encode_histogram(b, &h.h);
        }
    }

    /// Go `CustomBucketsHistogramSamples`: appends the encoded custom bucket histogram samples to `b`.
    pub fn custom_buckets_histogram_samples(&self, histograms: &[RefHistogramSample], b: &mut Vec<u8>) {
        if self.enable_st_storage {
            Self::histogram_samples_v2(histograms, b);
            return;
        }
        b.push(Type::CustomBucketsHistogramSamples as u8);
        let Some(first) = histograms.first() else { return };
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for h in histograms {
            put_varint(b, (h.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            encode_histogram(b, &h.h);
        }
    }

    /// Go `FloatHistogramSamples`: like [`Encoder::histogram_samples`] for float histograms.
    pub fn float_histogram_samples(
        &self,
        histograms: &[RefFloatHistogramSample],
        b: &mut Vec<u8>,
    ) -> Vec<RefFloatHistogramSample> {
        if self.enable_st_storage {
            Self::float_histogram_samples_v2(histograms, b);
            return Vec::new();
        }
        Self::float_histogram_samples_v1(histograms, b)
    }

    fn float_histogram_samples_v1(
        histograms: &[RefFloatHistogramSample],
        b: &mut Vec<u8>,
    ) -> Vec<RefFloatHistogramSample> {
        b.push(Type::FloatHistogramSamples as u8);
        let Some(first) = histograms.first() else { return Vec::new() };
        let mut custom = Vec::new();
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for h in histograms {
            if h.fh.uses_custom_buckets() {
                custom.push(h.clone());
                continue;
            }
            put_varint(b, (h.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            encode_float_histogram(b, &h.fh);
        }
        // Reset buffer if only custom bucket histograms existed in list of histogram samples.
        if histograms.len() == custom.len() {
            b.clear();
        }
        custom
    }

    fn float_histogram_samples_v2(histograms: &[RefFloatHistogramSample], b: &mut Vec<u8>) {
        b.push(Type::FloatHistogramSamplesV2 as u8);
        let Some(first) = histograms.first() else { return };
        put_varint(b, first.r#ref as i64);
        put_varint(b, first.t);
        put_varint(b, first.st);
        encode_float_histogram(b, &first.fh);
        for w in histograms.windows(2) {
            let (prev, h) = (&w[0], &w[1]);
            put_varint(b, (h.r#ref as i64).wrapping_sub(prev.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            write_st_marker(b, h.st, first.st, prev.st);
            encode_float_histogram(b, &h.fh);
        }
    }

    /// Go `CustomBucketsFloatHistogramSamples`: appends the encoded custom bucket float histogram samples to `b`.
    pub fn custom_buckets_float_histogram_samples(&self, histograms: &[RefFloatHistogramSample], b: &mut Vec<u8>) {
        if self.enable_st_storage {
            Self::float_histogram_samples_v2(histograms, b);
            return;
        }
        b.push(Type::CustomBucketsFloatHistogramSamples as u8);
        let Some(first) = histograms.first() else { return };
        put_be64(b, first.r#ref);
        put_be64(b, first.t as u64);
        for h in histograms {
            put_varint(b, (h.r#ref as i64).wrapping_sub(first.r#ref as i64));
            put_varint(b, h.t.wrapping_sub(first.t));
            encode_float_histogram(b, &h.fh);
        }
    }
}
