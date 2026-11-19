//! Port of `prompb/io/prometheus/write/v2` (`io.prometheus.write.v2`): the remote write 2.0 messages,
//! the symbols table (`symbols.go`), `OptimizedMarshal` (`custom.go`) and the model conversions (`codec.go`).

use std::collections::HashMap;

use model::exemplar::Exemplar as ModelExemplar;
use model::histogram::{FloatHistogram, Histogram as ModelHistogram};
use model::labels::{Labels as ModelLabels, ScratchBuilder};
use model::metadata::{Metadata as ModelMetadata, MetricType as ModelMetricType};

use super::proto::*;
use super::types::{put_msg, Message};
pub use super::types::{BucketSpan, Count, Histogram, ResetHint};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Request {
    pub symbols: Vec<String>,
    pub timeseries: Vec<TimeSeries>,
}

impl Message for Request {
    fn encoded_len(&self) -> usize {
        self.symbols.iter().map(|s| bytes_always_len(4, s.len())).sum::<usize>()
            + self.timeseries.iter().map(|t| message_len(5, t.encoded_len())).sum::<usize>()
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        for s in &self.symbols {
            put_bytes_always(buf, 4, s.as_bytes());
        }
        for t in &self.timeseries {
            put_msg(buf, 5, t);
        }
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Request")?;
            match f {
                4 => {
                    d.expect(wt, 2, "Symbols")?;
                    self.symbols.push(d.string()?);
                }
                5 => {
                    d.expect(wt, 2, "Timeseries")?;
                    let b = d.bytes()?;
                    let mut t = TimeSeries::default();
                    t.merge(b)?;
                    self.timeseries.push(t);
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

impl Request {
    /// Go `OptimizedMarshal`: same as `Marshal` except that each series writes its exemplars (field 4)
    /// before its histograms (field 3), as upstream's hand-written `OptimizedMarshalToSizedBuffer` does.
    /// Reuses `dst`.
    pub fn optimized_marshal(&self, dst: &mut Vec<u8>) {
        dst.clear();
        dst.reserve(self.encoded_len());
        for s in &self.symbols {
            put_bytes_always(dst, 4, s.as_bytes());
        }
        for t in &self.timeseries {
            put_message_header(dst, 5, t.encoded_len());
            t.encode_optimized(dst);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimeSeries {
    pub labels_refs: Vec<u32>,
    pub samples: Vec<Sample>,
    pub histograms: Vec<Histogram>,
    pub exemplars: Vec<Exemplar>,
    pub metadata: Metadata,
}

fn hist_len(h: &Histogram) -> usize {
    message_len(3, h.len_v(true))
}

impl TimeSeries {
    fn encode_optimized(&self, buf: &mut Vec<u8>) {
        put_packed_uint32(buf, 1, &self.labels_refs);
        for s in &self.samples {
            put_msg(buf, 2, s);
        }
        for e in &self.exemplars {
            put_msg(buf, 4, e);
        }
        for h in &self.histograms {
            put_message_header(buf, 3, h.len_v(true));
            h.encode_v(buf, true);
        }
        put_msg(buf, 5, &self.metadata);
    }

    /// Go `ToLabels`.
    pub fn to_labels(&self, b: &mut ScratchBuilder, symbols: &[String]) -> std::result::Result<ModelLabels, String> {
        desymbolize_labels(b, &self.labels_refs, symbols)
    }

    /// Go `ToMetadata`.
    pub fn to_metadata(&self, symbols: &[String]) -> std::result::Result<ModelMetadata, String> {
        let typ = match self.metadata.r#type {
            MetadataType::COUNTER => ModelMetricType::Counter,
            MetadataType::GAUGE => ModelMetricType::Gauge,
            MetadataType::HISTOGRAM => ModelMetricType::Histogram,
            MetadataType::GAUGEHISTOGRAM => ModelMetricType::GaugeHistogram,
            MetadataType::SUMMARY => ModelMetricType::Summary,
            MetadataType::INFO => ModelMetricType::Info,
            MetadataType::STATESET => ModelMetricType::StateSet,
            _ => ModelMetricType::Unknown,
        };
        if self.metadata.unit_ref as usize >= symbols.len() {
            return Err(format!(
                "metadata unit_ref {} outside of symbols table (size {})",
                self.metadata.unit_ref,
                symbols.len()
            ));
        }
        if self.metadata.help_ref as usize >= symbols.len() {
            return Err(format!(
                "metadata help_ref {} outside of symbols table (size {})",
                self.metadata.help_ref,
                symbols.len()
            ));
        }
        Ok(ModelMetadata {
            typ,
            unit: symbols[self.metadata.unit_ref as usize].clone(),
            help: symbols[self.metadata.help_ref as usize].clone(),
        })
    }
}

impl Message for TimeSeries {
    fn encoded_len(&self) -> usize {
        packed_uint32_len(1, &self.labels_refs)
            + self.samples.iter().map(|s| message_len(2, s.encoded_len())).sum::<usize>()
            + self.histograms.iter().map(hist_len).sum::<usize>()
            + self.exemplars.iter().map(|e| message_len(4, e.encoded_len())).sum::<usize>()
            + message_len(5, self.metadata.encoded_len())
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_packed_uint32(buf, 1, &self.labels_refs);
        for s in &self.samples {
            put_msg(buf, 2, s);
        }
        for h in &self.histograms {
            put_message_header(buf, 3, h.len_v(true));
            h.encode_v(buf, true);
        }
        for e in &self.exemplars {
            put_msg(buf, 4, e);
        }
        put_msg(buf, 5, &self.metadata);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("TimeSeries")?;
            match f {
                1 => {
                    let out = &mut self.labels_refs;
                    d.repeated_varint(wt, "LabelsRefs", |v| out.push(v as u32))?;
                }
                2 => {
                    d.expect(wt, 2, "Samples")?;
                    let b = d.bytes()?;
                    let mut s = Sample::default();
                    s.merge(b)?;
                    self.samples.push(s);
                }
                3 => {
                    d.expect(wt, 2, "Histograms")?;
                    let b = d.bytes()?;
                    let mut h = Histogram::default();
                    h.merge_v(b, true)?;
                    self.histograms.push(h);
                }
                4 => {
                    d.expect(wt, 2, "Exemplars")?;
                    let b = d.bytes()?;
                    let mut e = Exemplar::default();
                    e.merge(b)?;
                    self.exemplars.push(e);
                }
                5 => {
                    d.expect(wt, 2, "Metadata")?;
                    let b = d.bytes()?;
                    self.metadata.merge(b)?;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Exemplar {
    pub labels_refs: Vec<u32>,
    pub value: f64,
    pub timestamp: i64,
}

impl Message for Exemplar {
    fn encoded_len(&self) -> usize {
        packed_uint32_len(1, &self.labels_refs) + double_field_len(2, self.value) + int_field_len(3, self.timestamp)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_packed_uint32(buf, 1, &self.labels_refs);
        put_double_field(buf, 2, self.value);
        put_int_field(buf, 3, self.timestamp);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Exemplar")?;
            match f {
                1 => {
                    let out = &mut self.labels_refs;
                    d.repeated_varint(wt, "LabelsRefs", |v| out.push(v as u32))?;
                }
                2 => {
                    d.expect(wt, 1, "Value")?;
                    self.value = d.double()?;
                }
                3 => {
                    d.expect(wt, 0, "Timestamp")?;
                    self.timestamp = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

impl Exemplar {
    /// Go `ToExemplar`.
    pub fn to_exemplar(&self, b: &mut ScratchBuilder, symbols: &[String]) -> std::result::Result<ModelExemplar, String> {
        let labels = desymbolize_labels(b, &self.labels_refs, symbols)?;
        Ok(ModelExemplar { labels, value: self.value, ts: self.timestamp, has_ts: self.timestamp != 0 })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
    pub start_timestamp: i64,
}

impl Sample {
    pub fn t(&self) -> i64 {
        self.timestamp
    }
    pub fn v(&self) -> f64 {
        self.value
    }
}

impl Message for Sample {
    fn encoded_len(&self) -> usize {
        double_field_len(1, self.value) + int_field_len(2, self.timestamp) + int_field_len(3, self.start_timestamp)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_double_field(buf, 1, self.value);
        put_int_field(buf, 2, self.timestamp);
        put_int_field(buf, 3, self.start_timestamp);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Sample")?;
            match f {
                1 => {
                    d.expect(wt, 1, "Value")?;
                    self.value = d.double()?;
                }
                2 => {
                    d.expect(wt, 0, "Timestamp")?;
                    self.timestamp = d.varint()? as i64;
                }
                3 => {
                    d.expect(wt, 0, "StartTimestamp")?;
                    self.start_timestamp = d.varint()? as i64;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

/// Go `Metadata_MetricType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MetadataType(pub i32);

impl MetadataType {
    pub const UNSPECIFIED: MetadataType = MetadataType(0);
    pub const COUNTER: MetadataType = MetadataType(1);
    pub const GAUGE: MetadataType = MetadataType(2);
    pub const HISTOGRAM: MetadataType = MetadataType(3);
    pub const GAUGEHISTOGRAM: MetadataType = MetadataType(4);
    pub const SUMMARY: MetadataType = MetadataType(5);
    pub const INFO: MetadataType = MetadataType(6);
    pub const STATESET: MetadataType = MetadataType(7);
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    pub r#type: MetadataType,
    pub help_ref: u32,
    pub unit_ref: u32,
}

impl Message for Metadata {
    fn encoded_len(&self) -> usize {
        int_field_len(1, self.r#type.0 as i64) + uint_field_len(3, self.help_ref as u64) + uint_field_len(4, self.unit_ref as u64)
    }
    fn encode_raw(&self, buf: &mut Vec<u8>) {
        put_int_field(buf, 1, self.r#type.0 as i64);
        put_uint_field(buf, 3, self.help_ref as u64);
        put_uint_field(buf, 4, self.unit_ref as u64);
    }
    fn merge(&mut self, b: &[u8]) -> Result<()> {
        let mut d = Decoder::new(b);
        while !d.done() {
            let (f, wt, pre) = d.key("Metadata")?;
            match f {
                1 => {
                    d.expect(wt, 0, "Type")?;
                    self.r#type = MetadataType(d.varint()? as i32);
                }
                3 => {
                    d.expect(wt, 0, "HelpRef")?;
                    self.help_ref = d.varint()? as u32;
                }
                4 => {
                    d.expect(wt, 0, "UnitRef")?;
                    self.unit_ref = d.varint()? as u32;
                }
                _ => d.skip(pre)?,
            }
        }
        Ok(())
    }
}

impl Histogram {
    /// Encodes as `io.prometheus.write.v2.Histogram` (with field 17).
    pub fn marshal_v2(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.len_v(true));
        self.encode_v(&mut b, true);
        b
    }
    /// Decodes an `io.prometheus.write.v2.Histogram`.
    pub fn unmarshal_v2(b: &[u8]) -> Result<Histogram> {
        let mut h = Histogram::default();
        h.merge_v(b, true)?;
        Ok(h)
    }
}

// ---------------------------------------------------------------- symbols.go

/// Go `SymbolsTable`.
#[derive(Clone, Debug)]
pub struct SymbolsTable {
    strings: Vec<String>,
    symbols_map: HashMap<String, u32>,
}

impl Default for SymbolsTable {
    fn default() -> Self {
        SymbolsTable::new()
    }
}

impl SymbolsTable {
    /// Go `NewSymbolTable`.
    pub fn new() -> SymbolsTable {
        let mut symbols_map = HashMap::new();
        symbols_map.insert(String::new(), 0);
        SymbolsTable { strings: vec![String::new()], symbols_map }
    }

    /// Adds (if new) a string and returns its reference.
    pub fn symbolize(&mut self, s: &str) -> u32 {
        if let Some(&r) = self.symbols_map.get(s) {
            return r;
        }
        let r = self.strings.len() as u32;
        self.strings.push(s.to_string());
        self.symbols_map.insert(s.to_string(), r);
        r
    }

    /// Go `SymbolizeLabels`: clears `buf` and fills it with name/value references.
    pub fn symbolize_labels(&mut self, lbls: &ModelLabels, buf: &mut Vec<u32>) {
        buf.clear();
        for l in lbls.iter() {
            let n = self.symbolize(l.0);
            buf.push(n);
            let v = self.symbolize(l.1);
            buf.push(v);
        }
    }

    pub fn symbols(&self) -> &[String] {
        &self.strings
    }

    /// Clears the table, keeping the empty symbol.
    pub fn reset(&mut self) {
        self.strings.truncate(1);
        self.symbols_map.clear();
        self.symbols_map.insert(String::new(), 0);
    }
}

/// Go `desymbolizeLabels`.
pub fn desymbolize_labels(b: &mut ScratchBuilder, label_refs: &[u32], symbols: &[String]) -> std::result::Result<ModelLabels, String> {
    if label_refs.len() % 2 != 0 {
        return Err(format!("invalid labelRefs length {}", label_refs.len()));
    }
    b.reset();
    for p in label_refs.chunks_exact(2) {
        let (n, v) = (p[0], p[1]);
        if n as usize >= symbols.len() || v as usize >= symbols.len() {
            return Err(format!("labelRefs {} (name) = {} (value) outside of symbols table (size {})", n, v, symbols.len()));
        }
        b.add(&symbols[n as usize], &symbols[v as usize]);
    }
    b.sort();
    Ok(b.labels())
}

// ---------------------------------------------------------------- codec.go

/// Go `FromMetadataType`.
pub fn from_metadata_type(t: ModelMetricType) -> MetadataType {
    match t {
        ModelMetricType::Counter => MetadataType::COUNTER,
        ModelMetricType::Gauge => MetadataType::GAUGE,
        ModelMetricType::Histogram => MetadataType::HISTOGRAM,
        ModelMetricType::GaugeHistogram => MetadataType::GAUGEHISTOGRAM,
        ModelMetricType::Summary => MetadataType::SUMMARY,
        ModelMetricType::Info => MetadataType::INFO,
        ModelMetricType::StateSet => MetadataType::STATESET,
        _ => MetadataType::UNSPECIFIED,
    }
}

/// Go writev2 `FromIntHistogram`.
pub fn from_int_histogram(st: i64, timestamp: i64, h: &ModelHistogram) -> Histogram {
    let mut r = super::codec::from_int_histogram(timestamp, h);
    r.start_timestamp = st;
    r
}

/// Go writev2 `FromFloatHistogram`.
pub fn from_float_histogram(st: i64, timestamp: i64, fh: &FloatHistogram) -> Histogram {
    let mut r = super::codec::from_float_histogram(timestamp, fh);
    r.start_timestamp = st;
    r
}
