//! Port of `protobufparse.go`: presents the Prometheus protobuf format (varint-delimited `MetricFamily`
//! messages) through the text-style [`Parser`] interface.

use std::cell::Cell;

use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{Labels, ScratchBuilder};
use model::metadata::MetricType;

use crate::client_pb::{self, LabelAdder, MetricStreamingDecoder, MetricType as PbType};
use crate::convertnhcb::TempHistogram;
use crate::{Entry, Error, Parser, ParserKind};

/// Parses the Prometheus protobuf format (Go `ProtobufParser`).
pub struct ProtobufParser<'a> {
    dec: MetricStreamingDecoder<'a>,
    /// The series bytes of Series and Histogram, or the metric family name for Type, Unit and Help.
    entry_bytes: Vec<u8>,
    builder: ScratchBuilder,
    ex_builder: ScratchBuilder,
    /// Position within a summary or classic histogram: -2 is the count, -1 the sum, otherwise the index of the
    /// quantile or bucket.
    field_pos: isize,
    /// True if no more fields of a summary or classic histogram are to be processed. A `Cell` because `series()`
    /// (`&self`) sets it for a summary without quantiles, as Go's `Series` does.
    fields_done: Cell<bool>,
    redo_classic: bool,
    exemplar_pos: usize,
    exemplar_returned: bool,
    state: Entry,
    ignore_native_histograms: bool,
    parse_classic_histograms: bool,
    enable_type_and_unit_labels: bool,
    convert_classic_histograms_to_nhcb: bool,
    tmp_nhcb: TempHistogram,
    nhcb_h: Option<Histogram>,
    nhcb_fh: Option<FloatHistogram>,
    // Reused outputs of `histogram()`.
    h: Histogram,
    fh: FloatHistogram,
    name_buf: String,
    value_buf: String,
}

/// Which exemplar of the current metric `exemplar()` picked.
#[derive(Clone, Copy)]
enum ExRef {
    Counter,
    Hist(usize),
    Bucket(usize),
}

impl<'a> ProtobufParser<'a> {
    /// Go `NewProtobufParser`.
    pub fn new(
        b: &'a [u8],
        ignore_native_histograms: bool,
        parse_classic_histograms: bool,
        convert_classic_histograms_to_nhcb: bool,
        enable_type_and_unit_labels: bool,
    ) -> ProtobufParser<'a> {
        ProtobufParser {
            dec: MetricStreamingDecoder::new(b),
            entry_bytes: Vec::new(),
            builder: ScratchBuilder::new(16),
            ex_builder: ScratchBuilder::new(4),
            field_pos: 0,
            fields_done: Cell::new(false),
            redo_classic: false,
            exemplar_pos: 0,
            exemplar_returned: false,
            state: Entry::Invalid,
            ignore_native_histograms,
            parse_classic_histograms,
            enable_type_and_unit_labels,
            convert_classic_histograms_to_nhcb,
            tmp_nhcb: TempHistogram::new(),
            nhcb_h: None,
            nhcb_fh: None,
            h: Histogram::default(),
            fh: FloatHistogram::default(),
            name_buf: String::new(),
            value_buf: String::new(),
        }
    }

    #[inline]
    fn ts(&self) -> Option<i64> {
        // A zero timestamp means no timestamp (proto3 has no unset state for it).
        if self.dec.timestamp_ms != 0 { Some(self.dec.timestamp_ms) } else { None }
    }

    fn metric_type(&self) -> MetricType {
        match self.dec.get_type() {
            PbType::COUNTER => MetricType::Counter,
            PbType::GAUGE => MetricType::Gauge,
            PbType::HISTOGRAM => MetricType::Histogram,
            PbType::GAUGE_HISTOGRAM => MetricType::GaugeHistogram,
            PbType::SUMMARY => MetricType::Summary,
            _ => MetricType::Unknown,
        }
    }

    fn is_histogram_type(t: PbType) -> bool {
        t == PbType::HISTOGRAM || t == PbType::GAUGE_HISTOGRAM
    }

    fn is_classic(&self) -> bool {
        self.ignore_native_histograms || !is_native_histogram(&self.dec.histogram)
    }

    fn histogram_error(&self, e: String) -> Error {
        let mut m = String::from("histogram ");
        crate::strconv::append_quote_bytes(&mut m, self.dec.get_name());
        m.push_str(": ");
        m.push_str(&e);
        Error::Msg(m)
    }

    /// Go `getMagicName`, written into `name_buf`.
    fn set_magic_name(&mut self) {
        let t = self.dec.get_type();
        self.name_buf.clear();
        self.name_buf.push_str(&String::from_utf8_lossy(self.dec.get_name()));
        if self.state == Entry::Histogram || (!Self::is_histogram_type(t) && t != PbType::SUMMARY) {
            return;
        }
        if self.field_pos == -2 {
            self.name_buf.push_str("_count");
        } else if self.field_pos == -1 {
            self.name_buf.push_str("_sum");
        } else if Self::is_histogram_type(t) {
            self.name_buf.push_str("_bucket");
        }
    }

    /// Go `getMagicLabel`: the magic label name ("quantile" or "le") with its value in `value_buf`; also sets
    /// `fields_done`.
    fn magic_label(&mut self) -> Option<&'static str> {
        if self.state == Entry::Histogram || self.field_pos < 0 {
            return None;
        }
        let pos = self.field_pos as usize;
        match self.dec.get_type() {
            PbType::SUMMARY => {
                let qq = &self.dec.summary.quantile;
                if pos >= qq.len() {
                    self.fields_done.set(true);
                    return None;
                }
                let q = qq[pos].quantile;
                self.fields_done.set(pos == qq.len() - 1);
                self.value_buf.clear();
                model::labels::append_open_metrics_float(&mut self.value_buf, q);
                Some("quantile")
            }
            PbType::HISTOGRAM | PbType::GAUGE_HISTOGRAM => {
                let bb = &self.dec.histogram.bucket;
                if pos >= bb.len() {
                    self.fields_done.set(true);
                    self.value_buf.clear();
                    self.value_buf.push_str("+Inf");
                    return Some("le");
                }
                let ub = bb[pos].upper_bound;
                self.fields_done.set(ub == f64::INFINITY);
                self.value_buf.clear();
                model::labels::append_open_metrics_float(&mut self.value_buf, ub);
                Some("le")
            }
            _ => None,
        }
    }

    /// Go `onSeriesOrHistogramUpdate`: builds the labels and the series bytes.
    fn on_series_or_histogram_update(&mut self) -> Result<(), Error> {
        self.builder.reset();
        self.set_magic_name();
        if self.enable_type_and_unit_labels {
            let typ = self.metric_type();
            let type_set = typ != MetricType::Empty && typ != MetricType::Unknown;
            let unit = String::from_utf8_lossy(self.dec.get_unit());
            if !self.name_buf.is_empty() {
                self.builder.add("__name__", &self.name_buf);
            }
            if type_set {
                self.builder.add("__type__", typ.as_str());
            }
            if !unit.is_empty() {
                self.builder.add("__unit__", &unit);
            }
            let mut adder = IgnoreOverridden {
                b: &mut self.builder,
                name_empty: self.name_buf.is_empty(),
                type_empty: !type_set,
                unit_empty: unit.is_empty(),
            };
            self.dec.label(&mut adder).map_err(|e| Error::Msg(e.into_owned()))?;
        } else {
            self.builder.add("__name__", &self.name_buf);
            self.dec.label(&mut self.builder).map_err(|e| Error::Msg(e.into_owned()))?;
        }
        if let Some(name) = self.magic_label() {
            self.builder.add(name, &self.value_buf);
        }
        self.builder.sort();
        self.entry_bytes.clear();
        for (n, v) in self.builder.iter() {
            if n == "__name__" {
                self.entry_bytes.extend_from_slice(v.as_bytes());
                continue;
            }
            self.entry_bytes.push(0xff);
            self.entry_bytes.extend_from_slice(n.as_bytes());
            self.entry_bytes.push(0xff);
            self.entry_bytes.extend_from_slice(v.as_bytes());
        }
        Ok(())
    }

    fn convert_to_nhcb(&mut self, t: PbType) -> Result<(Option<Histogram>, Option<FloatHistogram>), Error> {
        let h = &self.dec.histogram;
        self.tmp_nhcb.reset();
        let mut v = h.sample_count_float;
        if v == 0.0 {
            v = h.sample_count as f64;
        }
        self.tmp_nhcb.set_count(v).map_err(Error::Msg)?;
        self.tmp_nhcb.set_sum(h.sample_sum).map_err(Error::Msg)?;
        for b in &h.bucket {
            let mut v = b.cumulative_count_float;
            if v == 0.0 {
                v = b.cumulative_count as f64;
            }
            self.tmp_nhcb.set_bucket_count(b.upper_bound, v).map_err(Error::Msg)?;
        }
        let (mut ch, mut cfh) = self.tmp_nhcb.convert().map_err(Error::Msg)?;
        if t == PbType::GAUGE_HISTOGRAM {
            if let Some(ch) = ch.as_mut() {
                ch.counter_reset_hint = CounterResetHint::GaugeType;
            } else if let Some(cfh) = cfh.as_mut() {
                cfh.counter_reset_hint = CounterResetHint::GaugeType;
            }
        }
        Ok((ch, cfh))
    }

    fn next_metric(&mut self) -> Result<bool, Error> {
        match self.dec.next_metric() {
            Ok(()) => Ok(true),
            Err(None) => Ok(false),
            Err(Some(e)) => Err(Error::Msg(e.into_owned())),
        }
    }
}

/// Go `IgnoreOverriddenMetadataLabelScratchBuilder` as a label adder.
struct IgnoreOverridden<'b> {
    b: &'b mut ScratchBuilder,
    name_empty: bool,
    type_empty: bool,
    unit_empty: bool,
}

impl LabelAdder for IgnoreOverridden<'_> {
    fn add(&mut self, name: &str, value: &str) {
        let empty_for = match name {
            "__name__" => self.name_empty,
            "__type__" => self.type_empty,
            "__unit__" => self.unit_empty,
            _ => true,
        };
        if !empty_for {
            return;
        }
        self.b.add(name, value);
    }
}

/// Go `isNativeHistogram`.
fn is_native_histogram(h: &client_pb::Histogram) -> bool {
    !h.positive_span.is_empty() || !h.negative_span.is_empty() || h.zero_threshold > 0.0 || h.zero_count > 0
}

/// Go `isFloatHistogram`.
fn is_float_histogram(h: &client_pb::Histogram) -> bool {
    h.sample_count_float > 0.0 || h.zero_count_float > 0.0 || !h.positive_count.is_empty() || !h.negative_count.is_empty()
}

/// Go `checkNativeHistogramConsistency`.
fn check_native_histogram_consistency(h: &client_pb::Histogram) -> Result<(), String> {
    let (pos, neg) = if is_float_histogram(h) {
        (h.positive_count.len(), h.negative_count.len())
    } else {
        (h.positive_delta.len(), h.negative_delta.len())
    };
    check_proto_span_bucket_consistency("positive", &h.positive_span, pos)?;
    check_proto_span_bucket_consistency("negative", &h.negative_span, neg)
}

fn check_proto_span_bucket_consistency(side: &str, spans: &[client_pb::BucketSpan], n: usize) -> Result<(), String> {
    let total: i64 = spans.iter().map(|s| s.length as i64).sum();
    if total != n as i64 {
        return Err(format!("{side} side: spans require {total} buckets, have {n}"));
    }
    Ok(())
}

fn copy_spans(dst: &mut Vec<Span>, src: &[client_pb::BucketSpan]) {
    dst.clear();
    dst.extend(src.iter().map(|s| Span { offset: s.offset, length: s.length }));
}

impl Parser for ProtobufParser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        let v = match self.dec.get_type() {
            PbType::COUNTER => self.dec.counter.value,
            PbType::GAUGE => self.dec.gauge.value,
            PbType::UNTYPED => self.dec.untyped.value,
            PbType::SUMMARY => {
                let s = &self.dec.summary;
                match self.field_pos {
                    -2 => s.sample_count as f64,
                    -1 => {
                        // Need to detect summaries without quantile here.
                        if s.quantile.is_empty() {
                            self.fields_done.set(true);
                        }
                        s.sample_sum
                    }
                    p => s.quantile[p as usize].value,
                }
            }
            PbType::HISTOGRAM | PbType::GAUGE_HISTOGRAM => {
                let h = &self.dec.histogram;
                let count = || if h.sample_count_float != 0.0 { h.sample_count_float } else { h.sample_count as f64 };
                match self.field_pos {
                    -2 => count(),
                    -1 => h.sample_sum,
                    p => {
                        let bb = &h.bucket;
                        if p as usize >= bb.len() {
                            count()
                        } else {
                            let b = &bb[p as usize];
                            if b.cumulative_count_float != 0.0 { b.cumulative_count_float } else { b.cumulative_count as f64 }
                        }
                    }
                }
            }
            _ => panic!("encountered unexpected metric type, this is a bug"),
        };
        (&self.entry_bytes, self.ts(), v)
    }

    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        let ts = self.ts();
        let h = &self.dec.histogram;
        if self.ignore_native_histograms || !is_native_histogram(h) {
            return (&self.entry_bytes, ts, self.nhcb_h.as_ref(), self.nhcb_fh.as_ref());
        }
        if self.parse_classic_histograms && !h.bucket.is_empty() {
            self.redo_classic = true;
        }
        let gauge = self.dec.get_type() == PbType::GAUGE_HISTOGRAM;
        if is_float_histogram(h) {
            let fh = &mut self.fh;
            fh.counter_reset_hint = if gauge { CounterResetHint::GaugeType } else { CounterResetHint::UnknownCounterReset };
            fh.schema = h.schema;
            fh.zero_threshold = h.zero_threshold;
            fh.zero_count = h.zero_count_float;
            fh.count = h.sample_count_float;
            fh.sum = h.sample_sum;
            copy_spans(&mut fh.positive_spans, &h.positive_span);
            copy_spans(&mut fh.negative_spans, &h.negative_span);
            fh.positive_buckets.clear();
            fh.positive_buckets.extend_from_slice(&h.positive_count);
            fh.negative_buckets.clear();
            fh.negative_buckets.extend_from_slice(&h.negative_count);
            fh.custom_values = None;
            fh.compact(0);
            return (&self.entry_bytes, ts, None, Some(&self.fh));
        }
        let sh = &mut self.h;
        sh.counter_reset_hint = if gauge { CounterResetHint::GaugeType } else { CounterResetHint::UnknownCounterReset };
        sh.schema = h.schema;
        sh.zero_threshold = h.zero_threshold;
        sh.zero_count = h.zero_count;
        sh.count = h.sample_count;
        sh.sum = h.sample_sum;
        copy_spans(&mut sh.positive_spans, &h.positive_span);
        copy_spans(&mut sh.negative_spans, &h.negative_span);
        sh.positive_buckets.clear();
        sh.positive_buckets.extend_from_slice(&h.positive_delta);
        sh.negative_buckets.clear();
        sh.negative_buckets.extend_from_slice(&h.negative_delta);
        sh.custom_values = None;
        sh.compact(0);
        (&self.entry_bytes, ts, Some(&self.h), None)
    }

    fn help(&mut self) -> (&[u8], &[u8]) {
        (&self.entry_bytes, self.dec.get_help())
    }

    fn typ(&self) -> (&[u8], MetricType) {
        (&self.entry_bytes, self.metric_type())
    }

    fn unit(&self) -> (&[u8], &[u8]) {
        (&self.entry_bytes, self.dec.get_unit())
    }

    fn comment(&self) -> &[u8] {
        &[]
    }

    fn labels(&mut self, l: &mut Labels) {
        self.builder.overwrite(l);
    }

    fn exemplar(&mut self, ex: &mut Exemplar) -> bool {
        if self.exemplar_returned && self.state == Entry::Series {
            return false;
        }
        let mut ex_ref: Option<ExRef> = None;
        match self.dec.get_type() {
            PbType::COUNTER => {
                if self.dec.counter.exemplar.is_some() {
                    ex_ref = Some(ExRef::Counter);
                }
            }
            PbType::HISTOGRAM | PbType::GAUGE_HISTOGRAM => {
                let is_classic = self.state == Entry::Series;
                let h = &self.dec.histogram;
                if !is_classic && !h.exemplars.is_empty() {
                    let exs = &h.exemplars;
                    while self.exemplar_pos < exs.len() {
                        ex_ref = Some(ExRef::Hist(self.exemplar_pos));
                        let e = &exs[self.exemplar_pos];
                        self.exemplar_pos += 1;
                        if e.timestamp.is_some() {
                            break;
                        }
                    }
                    if let Some(ExRef::Hist(i)) = ex_ref
                        && exs[i].timestamp.is_none() {
                            return false;
                        }
                } else {
                    let bb = &h.bucket;
                    if self.field_pos < 0 {
                        if is_classic {
                            return false;
                        }
                        self.field_pos = 0;
                    }
                    while (self.field_pos as usize) < bb.len() {
                        let p = self.field_pos as usize;
                        ex_ref = if bb[p].exemplar.is_some() { Some(ExRef::Bucket(p)) } else { None };
                        if is_classic {
                            break;
                        }
                        self.field_pos += 1;
                        if let Some(e) = &bb[p].exemplar
                            && e.timestamp.is_some() {
                                break;
                            }
                    }
                    if !is_classic {
                        let has_ts = match ex_ref {
                            Some(ExRef::Bucket(i)) => bb[i].exemplar.as_ref().is_some_and(|e| e.timestamp.is_some()),
                            _ => false,
                        };
                        if !has_ts {
                            return false;
                        }
                    }
                }
            }
            _ => return false,
        }
        let Some(r) = ex_ref else { return false };
        let e = match r {
            ExRef::Counter => self.dec.counter.exemplar.as_ref(),
            ExRef::Hist(i) => self.dec.histogram.exemplars.get(i),
            ExRef::Bucket(i) => self.dec.histogram.bucket[i].exemplar.as_ref(),
        };
        let Some(e) = e else { return false };
        ex.value = e.value;
        if let Some(ts) = e.timestamp {
            ex.has_ts = true;
            ex.ts = ts.seconds.wrapping_mul(1000).wrapping_add((ts.nanos / 1_000_000) as i64);
        }
        self.ex_builder.reset();
        let input = self.dec.input();
        for lp in &e.label {
            let n = String::from_utf8_lossy(&input[lp.name.clone()]);
            let v = String::from_utf8_lossy(&input[lp.value.clone()]);
            self.ex_builder.add(&n, &v);
        }
        self.ex_builder.sort();
        self.ex_builder.overwrite(&mut ex.labels);
        self.exemplar_returned = true;
        true
    }

    fn start_timestamp(&mut self) -> i64 {
        let st = match self.dec.get_type() {
            PbType::COUNTER => self.dec.counter.start_timestamp,
            PbType::SUMMARY => self.dec.summary.start_timestamp,
            PbType::HISTOGRAM | PbType::GAUGE_HISTOGRAM => self.dec.histogram.start_timestamp,
            _ => None,
        };
        match st {
            None => 0,
            Some(st) => st.seconds.wrapping_mul(1000).wrapping_add(st.nanos as i64 / 1_000_000),
        }
    }

    fn next(&mut self) -> Result<Entry, Error> {
        // Each `continue` is Go's tail call `return p.Next()`.
        loop {
            self.exemplar_returned = false;
            self.nhcb_h = None;
            self.nhcb_fh = None;
            match self.state {
                Entry::Invalid => {
                    self.exemplar_pos = 0;
                    self.field_pos = -2;
                    match self.dec.next_metric_family() {
                        Ok(()) => {}
                        Err(None) => return Err(Error::Eof),
                        Err(Some(e)) => return Err(Error::Msg(e.into_owned())),
                    }
                    if !self.next_metric()? {
                        // Skip empty metric families.
                        continue;
                    }
                    let name = self.dec.get_name();
                    if name.is_empty() || std::str::from_utf8(name).is_err() {
                        return Err(Error::Msg(format!("invalid metric name: {}", String::from_utf8_lossy(name))));
                    }
                    let help = self.dec.get_help();
                    if std::str::from_utf8(help).is_err() {
                        let mut m = String::from("invalid help for metric ");
                        crate::strconv::append_quote_bytes(&mut m, name);
                        m.push_str(": ");
                        m.push_str(&String::from_utf8_lossy(help));
                        return Err(Error::Msg(m));
                    }
                    let t = self.dec.get_type();
                    if !(0..=5).contains(&t.0) {
                        let mut m = String::from("unknown metric type for metric ");
                        crate::strconv::append_quote_bytes(&mut m, name);
                        m.push_str(": ");
                        m.push_str(&t.name());
                        return Err(Error::Msg(m));
                    }
                    self.entry_bytes.clear();
                    self.entry_bytes.extend_from_slice(name);
                    self.state = Entry::Help;
                }
                Entry::Help => {
                    self.state = if !self.dec.unit.is_empty() { Entry::Unit } else { Entry::Type };
                }
                Entry::Unit => {
                    self.state = Entry::Type;
                }
                Entry::Type => {
                    let t = self.dec.get_type();
                    if Self::is_histogram_type(t) {
                        if self.is_classic() {
                            self.state = Entry::Series;
                            self.field_pos = -3;
                            continue;
                        }
                        if let Err(e) = check_native_histogram_consistency(&self.dec.histogram) {
                            return Err(self.histogram_error(e));
                        }
                        self.state = Entry::Histogram;
                    } else {
                        self.state = Entry::Series;
                    }
                    self.on_series_or_histogram_update()?;
                }
                Entry::Series => {
                    let t = self.dec.get_type();
                    let mut decode_next = true;
                    let mut must_check_next_series_kind = false;
                    if t == PbType::SUMMARY || Self::is_histogram_type(t) {
                        let is_classic_histogram = Self::is_histogram_type(t) && self.is_classic();
                        let skip_series = self.convert_classic_histograms_to_nhcb
                            && is_classic_histogram
                            && !self.parse_classic_histograms;
                        if !self.fields_done.get() && !skip_series {
                            self.field_pos += 1;
                            self.on_series_or_histogram_update()?;
                            return Ok(self.state);
                        }
                        self.field_pos = -2;
                        self.fields_done.set(false);
                        self.exemplar_pos = 0;
                        if Self::is_histogram_type(t) {
                            if is_classic_histogram && self.convert_classic_histograms_to_nhcb {
                                let (h, fh) = self.convert_to_nhcb(t)?;
                                self.nhcb_h = h;
                                self.nhcb_fh = fh;
                                self.state = Entry::Histogram;
                                decode_next = false;
                            } else {
                                must_check_next_series_kind = true;
                            }
                        }
                    }
                    if decode_next {
                        if !self.next_metric()? {
                            self.state = Entry::Invalid;
                            continue;
                        }
                        if must_check_next_series_kind {
                            if self.is_classic() {
                                self.field_pos = -3;
                                self.fields_done.set(false);
                                continue;
                            }
                            if let Err(e) = check_native_histogram_consistency(&self.dec.histogram) {
                                return Err(self.histogram_error(e));
                            }
                            self.state = Entry::Histogram;
                        }
                    }
                    self.on_series_or_histogram_update()?;
                }
                Entry::Histogram => {
                    if self.redo_classic {
                        self.redo_classic = false;
                        self.field_pos = -3;
                        self.fields_done.set(false);
                        self.state = Entry::Series;
                        continue;
                    }
                    if !self.next_metric()? {
                        self.state = Entry::Invalid;
                        continue;
                    }
                    if self.is_classic() {
                        self.redo_classic = false;
                        self.field_pos = -3;
                        self.fields_done.set(false);
                        self.state = Entry::Series;
                        continue;
                    }
                    self.on_series_or_histogram_update()?;
                }
                s => return Err(Error::Msg(format!("invalid protobuf parsing state: {}", s as i8))),
            }
            return Ok(self.state);
        }
    }

    fn kind(&self) -> ParserKind {
        ParserKind::Protobuf
    }
}
