//! Port of `nhcbparse.go`: wraps a parser and converts classic histograms to native histograms with custom
//! buckets (NHCB).

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, ScratchBuilder};
use model::metadata::MetricType;

use crate::convertnhcb::{self, SuffixType, TempHistogram};
use crate::{Entry, Error, Parser, ParserKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollectionState {
    Start,
    Collecting,
    Emitting,
    /// Inhibiting NHCB, because there was an exponential histogram with the same labels.
    Inhibiting,
}

/// Wraps a parser and converts classic histograms to native histograms with custom buckets (Go `NHCBParser`).
/// Only series with the histogram metadata type are converted. The classic series are also returned when
/// `keep_classic_histograms` is set.
pub struct NHCBParser<'a> {
    parser: Box<dyn Parser + 'a>,
    keep_classic_histograms: bool,
    parse_st: bool,
    builder: ScratchBuilder,
    state: CollectionState,

    // Cached values of the wrapped parser. Series bytes and histograms are read from the wrapped parser, which
    // is still positioned at the cached entry whenever they are asked for.
    ts: Option<i64>,
    value: f64,
    lset: Labels,
    b_name: Vec<u8>,
    typ: MetricType,

    entry: Entry,
    err: Option<Error>,

    bytes_nhcb: Vec<u8>,
    h_nhcb: Option<Histogram>,
    fh_nhcb: Option<FloatHistogram>,
    lset_nhcb: Labels,
    st_nhcb: i64,
    ex_pos: usize,
    ex_end: usize,

    temp_lset_nhcb: Labels,
    temp_nhcb: TempHistogram,
    /// Backing store of the collected exemplars (Go `tempExemplars`, whose stale slots are reused like Go's).
    temp_exemplars: Vec<Exemplar>,
    temp_len: usize,
    temp_exemplar_count: usize,
    temp_st: i64,

    last_histogram_name: String,
    last_histogram_labels_hash: u64,
    h_buffer: Vec<u8>,
}

impl<'a> NHCBParser<'a> {
    /// Go `NewNHCBParser`.
    pub fn new(p: Box<dyn Parser + 'a>, keep_classic_histograms: bool, parse_st: bool) -> NHCBParser<'a> {
        NHCBParser {
            parser: p,
            keep_classic_histograms,
            parse_st,
            builder: ScratchBuilder::new(16),
            state: CollectionState::Start,
            ts: None,
            value: 0.0,
            lset: Labels::default(),
            b_name: Vec::new(),
            typ: MetricType::Empty,
            entry: Entry::Invalid,
            err: None,
            bytes_nhcb: Vec::new(),
            h_nhcb: None,
            fh_nhcb: None,
            lset_nhcb: Labels::default(),
            st_nhcb: 0,
            ex_pos: 0,
            ex_end: 0,
            temp_lset_nhcb: Labels::default(),
            temp_nhcb: TempHistogram::new(),
            temp_exemplars: Vec::new(),
            temp_len: 0,
            temp_exemplar_count: 0,
            temp_st: 0,
            last_histogram_name: String::new(),
            last_histogram_labels_hash: 0,
            h_buffer: Vec::new(),
        }
    }

    fn copy_labels(&mut self, from_nhcb: bool, l: &mut Labels) {
        let src = if from_nhcb { &self.lset_nhcb } else { &self.lset };
        if src.is_empty() {
            *l = Labels::default();
            return;
        }
        self.builder.reset();
        for (n, v) in src.iter() {
            self.builder.add(n, v);
        }
        self.builder.overwrite(l);
    }

    /// True if the labels changed and the NHCB should be emitted.
    fn different_metric(&mut self) -> bool {
        if self.typ != MetricType::Histogram {
            return true;
        }
        let (_, name) = convertnhcb::get_histogram_metric_base_name(self.lset.get("__name__"));
        if self.last_histogram_name != name {
            return true;
        }
        let next_hash = self.lset.hash_without_labels(&mut self.h_buffer, &["le"]);
        self.last_histogram_labels_hash != next_hash
    }

    fn store_classic_labels(&mut self, name: &str) {
        self.last_histogram_name.clear();
        self.last_histogram_name.push_str(name);
        self.last_histogram_labels_hash = self.lset.hash_without_labels(&mut self.h_buffer, &["le"]);
    }

    fn store_exponential_labels(&mut self) {
        self.last_histogram_name.clear();
        self.last_histogram_name.push_str(self.lset.get("__name__"));
        self.last_histogram_labels_hash = self.lset.hash_without_labels(&mut self.h_buffer, &[] as &[&str]);
    }

    /// Collates a classic histogram series (from `self.lset`); returns true if it was collated.
    fn handle_classic_histogram_series(&mut self) -> bool {
        if self.typ != MetricType::Histogram {
            return false;
        }
        let lset = self.lset.clone();
        let m_name = lset.get("__name__");
        let (suffix_type, name) = convertnhcb::get_histogram_metric_base_name(m_name);
        if name.as_bytes() != self.b_name.as_slice() {
            return false;
        }
        match suffix_type {
            SuffixType::Bucket => {
                if !lset.has("le") {
                    return false;
                }
                if let Ok(le) = crate::strconv::parse_float(lset.get("le").as_bytes()) {
                    if !le.is_nan() {
                        self.process_classic_histogram_series(&lset, name);
                        let v = self.value;
                        let _ = self.temp_nhcb.set_bucket_count(le, v);
                        return true;
                    }
                }
            }
            SuffixType::Count => {
                self.process_classic_histogram_series(&lset, name);
                let v = self.value;
                let _ = self.temp_nhcb.set_count(v);
                return true;
            }
            SuffixType::Sum => {
                self.process_classic_histogram_series(&lset, name);
                let v = self.value;
                let _ = self.temp_nhcb.set_sum(v);
                return true;
            }
            SuffixType::None => {}
        }
        false
    }

    fn process_classic_histogram_series(&mut self, lset: &Labels, name: &str) {
        if self.state != CollectionState::Collecting {
            self.store_classic_labels(name);
            self.temp_st = if self.parse_st { self.parser.start_timestamp() } else { 0 };
            self.state = CollectionState::Collecting;
            self.temp_lset_nhcb = convertnhcb::get_histogram_metric_base(lset, name);
        }
        self.store_exemplars();
    }

    fn store_exemplars(&mut self) {
        loop {
            // Go nextExemplarPtr.
            if !(self.temp_len > 0 && self.temp_exemplar_count == self.temp_len - 1) {
                self.temp_len += 1;
                if self.temp_len > self.temp_exemplars.len() {
                    self.temp_exemplars.push(Exemplar::default());
                }
            }
            let ex = &mut self.temp_exemplars[self.temp_len - 1];
            if !self.parser.exemplar(ex) {
                break;
            }
            self.temp_exemplar_count += 1;
        }
    }

    fn swap_exemplars(&mut self) {
        self.ex_pos = 0;
        self.ex_end = self.temp_exemplar_count;
        self.temp_len = 0;
    }

    /// Converts the collated classic histogram; true if the conversion was successful.
    fn process_nhcb(&mut self) -> bool {
        if self.state != CollectionState::Collecting {
            return false;
        }
        let res = self.temp_nhcb.convert();
        let ok = res.is_ok();
        if let Ok((h, fh)) = res {
            self.h_nhcb = h;
            self.fh_nhcb = fh;
            let lbls_without_name = self.temp_lset_nhcb.drop_reserved(|n| n == "__name__");
            self.bytes_nhcb.clear();
            self.bytes_nhcb.extend_from_slice(self.temp_lset_nhcb.get("__name__").as_bytes());
            if !lbls_without_name.is_empty() {
                self.bytes_nhcb.extend_from_slice(lbls_without_name.string_no_space().as_bytes());
            }
            self.lset_nhcb = self.temp_lset_nhcb.clone();
            self.swap_exemplars();
            self.st_nhcb = self.temp_st;
            self.state = CollectionState::Emitting;
        } else {
            self.state = CollectionState::Start;
        }
        self.temp_nhcb.reset();
        self.temp_exemplar_count = 0;
        self.temp_st = 0;
        ok
    }
}

impl Parser for NHCBParser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        let (b, _, _) = self.parser.series();
        (b, self.ts, self.value)
    }

    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        if self.state == CollectionState::Emitting {
            return (&self.bytes_nhcb, self.ts, self.h_nhcb.as_ref(), self.fh_nhcb.as_ref());
        }
        let ts = self.ts;
        let (b, _, h, fh) = self.parser.histogram();
        (b, ts, h, fh)
    }

    fn help(&mut self) -> (&[u8], &[u8]) {
        self.parser.help()
    }

    fn typ(&self) -> (&[u8], MetricType) {
        (&self.b_name, self.typ)
    }

    fn unit(&self) -> (&[u8], &[u8]) {
        self.parser.unit()
    }

    fn comment(&self) -> &[u8] {
        self.parser.comment()
    }

    fn labels(&mut self, l: &mut Labels) {
        let emitting = self.state == CollectionState::Emitting;
        self.copy_labels(emitting, l);
    }

    fn exemplar(&mut self, ex: &mut Exemplar) -> bool {
        if self.state == CollectionState::Emitting {
            if self.ex_pos >= self.ex_end {
                return false;
            }
            ex.clone_from(&self.temp_exemplars[self.ex_pos]);
            self.ex_pos += 1;
            return true;
        }
        self.parser.exemplar(ex)
    }

    fn start_timestamp(&mut self) -> i64 {
        match self.state {
            CollectionState::Start | CollectionState::Inhibiting => {
                if self.entry == Entry::Series || self.entry == Entry::Histogram {
                    return self.parser.start_timestamp();
                }
            }
            CollectionState::Collecting => return self.temp_st,
            CollectionState::Emitting => return self.st_nhcb,
        }
        0
    }

    fn next(&mut self) -> Result<Entry, Error> {
        loop {
            if self.state == CollectionState::Emitting {
                self.state = CollectionState::Start;
                if self.entry == Entry::Series {
                    let is_nhcb = self.handle_classic_histogram_series();
                    if is_nhcb && !self.keep_classic_histograms {
                        continue;
                    }
                }
                if let Some(e) = &self.err {
                    return Err(e.clone());
                }
                return Ok(self.entry);
            }

            match self.parser.next() {
                Ok(e) => {
                    self.entry = e;
                    self.err = None;
                }
                Err(err) => {
                    self.entry = Entry::Invalid;
                    self.err = Some(err.clone());
                    if err.is_eof() && self.process_nhcb() {
                        return Ok(Entry::Histogram);
                    }
                    return Err(err);
                }
            }
            match self.entry {
                Entry::Series => {
                    let (_, ts, v) = self.parser.series();
                    self.ts = ts;
                    self.value = v;
                    self.parser.labels(&mut self.lset);
                    let is_nhcb = match self.state {
                        CollectionState::Collecting => {
                            if self.different_metric() && self.process_nhcb() {
                                return Ok(Entry::Histogram);
                            }
                            self.handle_classic_histogram_series()
                        }
                        CollectionState::Inhibiting => {
                            if self.different_metric() {
                                self.state = CollectionState::Start;
                                self.handle_classic_histogram_series()
                            } else {
                                false
                            }
                        }
                        CollectionState::Start => self.handle_classic_histogram_series(),
                        CollectionState::Emitting => {
                            return Err(Error::msg("unexpected state in NHCBParser"));
                        }
                    };
                    if is_nhcb && !self.keep_classic_histograms {
                        continue;
                    }
                    return Ok(self.entry);
                }
                Entry::Histogram => {
                    self.state = CollectionState::Inhibiting;
                    let (_, ts, _, _) = self.parser.histogram();
                    self.ts = ts;
                    self.parser.labels(&mut self.lset);
                    self.store_exponential_labels();
                }
                Entry::Type => {
                    let (n, t) = self.parser.typ();
                    self.b_name.clear();
                    self.b_name.extend_from_slice(n);
                    self.typ = t;
                }
                _ => {}
            }
            if self.process_nhcb() {
                return Ok(Entry::Histogram);
            }
            return Ok(self.entry);
        }
    }

    fn kind(&self) -> ParserKind {
        ParserKind::Nhcb
    }
}
