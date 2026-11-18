//! Test helpers (port of the `parsedEntry`, `testParse` and `requireEntries` helpers of `interface_test.go`).
//! Comparison follows the Go options: NaNs are equal (`cmpopts.EquateNaNs`), nil and empty slices are equal
//! (`cmpopts.EquateEmpty`), labels compare with `labels.Equal`.

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram, Span};
use model::labels::Labels;
use model::metadata::MetricType;

use crate::{Entry, Parser};

/// The data parsed for one entry (Go `parsedEntry`).
#[derive(Clone, Debug, Default)]
pub struct ParsedEntry {
    /// In all but comment entries.
    pub m: Vec<u8>,
    /// In histogram entries.
    pub shs: Option<Histogram>,
    pub fhs: Option<FloatHistogram>,
    /// In series entries.
    pub v: f64,
    /// In series and histogram entries.
    pub lset: Labels,
    pub t: Option<i64>,
    pub es: Vec<Exemplar>,
    pub st: i64,
    /// In type entries.
    pub typ: MetricType,
    /// In help entries.
    pub help: String,
    /// In unit entries.
    pub unit: String,
    /// In comment entries.
    pub comment: String,
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Go `testParse`: reads all entries until EOF; panics on any other error.
pub fn test_parse(p: &mut dyn Parser) -> Vec<ParsedEntry> {
    let mut ret = Vec::new();
    loop {
        let et = match p.next() {
            Ok(et) => et,
            Err(e) if e.is_eof() => break,
            Err(e) => panic!("unexpected error: {e}"),
        };
        let mut got = ParsedEntry::default();
        match et {
            Entry::Invalid => panic!("entry invalid not expected"),
            Entry::Series | Entry::Histogram => {
                if et == Entry::Series {
                    let (m, ts, v) = p.series();
                    got.m = m.to_vec();
                    got.t = ts;
                    got.v = v;
                } else {
                    let (m, ts, h, fh) = p.histogram();
                    got.m = m.to_vec();
                    got.t = ts;
                    got.shs = h.cloned();
                    got.fhs = fh.cloned();
                }
                p.labels(&mut got.lset);
                got.st = p.start_timestamp();
                let mut e = Exemplar::default();
                while p.exemplar(&mut e) {
                    got.es.push(e.clone());
                }
            }
            Entry::Type => {
                let (m, t) = p.typ();
                got.m = m.to_vec();
                got.typ = t;
            }
            Entry::Help => {
                let (m, h) = p.help();
                got.m = m.to_vec();
                got.help = s(h);
            }
            Entry::Unit => {
                let (m, u) = p.unit();
                got.m = m.to_vec();
                got.unit = s(u);
            }
            Entry::Comment => {
                got.comment = s(p.comment());
            }
        }
        ret.push(got);
    }
    ret
}

/// Float equality with NaN == NaN.
pub fn feq(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

fn fslice_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| feq(*x, *y))
}

fn spans_eq(a: &[Span], b: &[Span]) -> bool {
    a == b
}

/// Deep equality of integer histograms as Go's `cmp` with EquateNaNs and EquateEmpty.
pub fn histogram_eq(a: &Histogram, b: &Histogram) -> bool {
    a.counter_reset_hint == b.counter_reset_hint
        && a.schema == b.schema
        && feq(a.zero_threshold, b.zero_threshold)
        && a.zero_count == b.zero_count
        && a.count == b.count
        && feq(a.sum, b.sum)
        && spans_eq(&a.positive_spans, &b.positive_spans)
        && spans_eq(&a.negative_spans, &b.negative_spans)
        && a.positive_buckets == b.positive_buckets
        && a.negative_buckets == b.negative_buckets
        && fslice_eq(a.custom_values.as_deref().unwrap_or(&[]), b.custom_values.as_deref().unwrap_or(&[]))
}

/// Deep equality of float histograms as Go's `cmp` with EquateNaNs and EquateEmpty.
pub fn float_histogram_eq(a: &FloatHistogram, b: &FloatHistogram) -> bool {
    a.counter_reset_hint == b.counter_reset_hint
        && a.schema == b.schema
        && feq(a.zero_threshold, b.zero_threshold)
        && feq(a.zero_count, b.zero_count)
        && feq(a.count, b.count)
        && feq(a.sum, b.sum)
        && spans_eq(&a.positive_spans, &b.positive_spans)
        && spans_eq(&a.negative_spans, &b.negative_spans)
        && fslice_eq(&a.positive_buckets, &b.positive_buckets)
        && fslice_eq(&a.negative_buckets, &b.negative_buckets)
        && fslice_eq(a.custom_values.as_deref().unwrap_or(&[]), b.custom_values.as_deref().unwrap_or(&[]))
}

/// Exemplar equality with NaN-equal values.
pub fn exemplar_eq(a: &Exemplar, b: &Exemplar) -> bool {
    a.labels == b.labels && feq(a.value, b.value) && a.ts == b.ts && a.has_ts == b.has_ts
}

/// Deep equality of two parsed entries.
pub fn entry_eq(a: &ParsedEntry, b: &ParsedEntry) -> bool {
    let h = match (&a.shs, &b.shs) {
        (None, None) => true,
        (Some(x), Some(y)) => histogram_eq(x, y),
        _ => false,
    };
    let fh = match (&a.fhs, &b.fhs) {
        (None, None) => true,
        (Some(x), Some(y)) => float_histogram_eq(x, y),
        _ => false,
    };
    a.m == b.m
        && h
        && fh
        && feq(a.v, b.v)
        && a.lset == b.lset
        && a.t == b.t
        && a.es.len() == b.es.len()
        && a.es.iter().zip(&b.es).all(|(x, y)| exemplar_eq(x, y))
        && a.st == b.st
        && a.typ == b.typ
        && a.help == b.help
        && a.unit == b.unit
        && a.comment == b.comment
}

/// Go `requireEntries`.
#[track_caller]
pub fn require_entries(exp: &[ParsedEntry], got: &[ParsedEntry]) {
    let n = exp.len().max(got.len());
    let mut diffs = String::new();
    for i in 0..n {
        match (exp.get(i), got.get(i)) {
            (Some(e), Some(g)) if entry_eq(e, g) => {}
            (e, g) => {
                diffs.push_str(&format!("entry {i}:\n  exp: {e:?}\n  got: {g:?}\n"));
            }
        }
    }
    if !diffs.is_empty() {
        panic!("entries differ (exp {} entries, got {}):\n{diffs}", exp.len(), got.len());
    }
}
