//! Port of `promql/value.go`: the values an expression evaluates to.
//!
//! Histograms are shared with [`Arc`] (Go shares `*FloatHistogram` pointers between samples and steps); code that
//! changes a histogram copies it first, as Go does with `Copy()`.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use chunkenc::{AnyIterator, ChunkIterator, ValueType as ChunkValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use storage::{Annotations, Error, Series as StorageSeriesTrait};

use crate::parser::ValueType;

/// A shared float histogram (Go `*histogram.FloatHistogram`).
pub type HistogramRef = Arc<FloatHistogram>;

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub fn format_float(f: f64) -> String {
    let mut s = String::new();
    write_float(&mut s, f);
    s
}

/// Appends Go `strconv.FormatFloat(f, 'f', -1, 64)` to `out`.
pub fn write_float(out: &mut String, f: f64) {
    use std::fmt::Write;
    if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
    } else if f.is_nan() {
        out.push_str("NaN");
    } else {
        let _ = write!(out, "{}", f);
    }
}

/// A string value (Go `promql.String`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StringValue {
    pub t: i64,
    pub v: String,
}

impl fmt::Display for StringValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.v)
    }
}

/// A scalar value (Go `promql.Scalar`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Scalar {
    pub t: i64,
    pub v: f64,
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scalar: {} @[{}]", format_float(self.v), self.t)
    }
}

/// A float point (Go `FPoint`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FPoint {
    pub t: i64,
    pub f: f64,
}

impl fmt::Display for FPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @[{}]", format_float(self.f), self.t)
    }
}

/// A histogram point (Go `HPoint`).
#[derive(Clone, Debug, PartialEq)]
pub struct HPoint {
    pub t: i64,
    pub h: HistogramRef,
}

impl HPoint {
    /// The size of the point in float samples, as the max samples limit counts it (Go `HPoint.size`).
    #[inline]
    pub fn size(&self) -> usize {
        histogram_point_size(&self.h)
    }
}

/// The size of a histogram in float samples (Go `HPoint.size`).
#[inline]
pub fn histogram_point_size(h: &FloatHistogram) -> usize {
    (h.size() + 8) / 16
}

impl fmt::Display for HPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @[{}]", self.h.string(), self.t)
    }
}

/// The total size of the histogram points (Go `totalHPointSize`).
pub fn total_hpoint_size(histograms: &[HPoint]) -> usize {
    histograms.iter().map(|h| h.size()).sum()
}

/// The number of samples after `cutoff` (Go `countSamplesAfter`).
pub fn count_samples_after(floats: &[FPoint], histograms: &[HPoint], cutoff: i64) -> i64 {
    let mut n = 0i64;
    for p in floats.iter().rev() {
        if p.t <= cutoff {
            break;
        }
        n += 1;
    }
    for p in histograms.iter().rev() {
        if p.t <= cutoff {
            break;
        }
        n += p.size() as i64;
    }
    n
}

/// A stream of data points of one metric (Go `promql.Series`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Series {
    pub metric: Labels,
    pub floats: Vec<FPoint>,
    pub histograms: Vec<HPoint>,
    /// Whether the metric name is to be dropped at the end of the query (delayed name removal).
    pub drop_name: bool,
}

impl Series {
    pub fn new(metric: Labels) -> Series {
        Series { metric, ..Default::default() }
    }
}

impl fmt::Display for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} =>\n", self.metric)?;
        let mut first = true;
        for p in &self.floats {
            if !first {
                f.write_str("\n")?;
            }
            first = false;
            write!(f, "{}", p)?;
        }
        for p in &self.histograms {
            if !first {
                f.write_str("\n")?;
            }
            first = false;
            write!(f, "{}", p)?;
        }
        Ok(())
    }
}

/// A single sample of a vector (Go `promql.Sample`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sample {
    pub t: i64,
    pub f: f64,
    pub h: Option<HistogramRef>,
    pub metric: Labels,
    /// Whether the metric name is to be dropped at the end of the query (delayed name removal).
    pub drop_name: bool,
}

impl Sample {
    /// A float sample without labels.
    #[inline]
    pub fn float(f: f64) -> Sample {
        Sample { f, ..Default::default() }
    }
}

impl fmt::Display for Sample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.h {
            None => write!(f, "{} => {}", self.metric, FPoint { t: self.t, f: self.f }),
            Some(h) => write!(f, "{} => {}", self.metric, HPoint { t: self.t, h: h.clone() }),
        }
    }
}

/// A set of samples that share a timestamp (Go `promql.Vector`).
pub type Vector = Vec<Sample>;
/// A set of series (Go `promql.Matrix`).
pub type Matrix = Vec<Series>;

/// Go `Vector.String()`.
pub fn vector_string(vec: &[Sample]) -> String {
    let mut out = String::new();
    for (i, s) in vec.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&s.to_string());
    }
    out
}

/// Go `Matrix.String()`.
pub fn matrix_string(m: &[Series]) -> String {
    let mut out = String::new();
    for (i, s) in m.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(&s.to_string());
    }
    out
}

/// Go `Vector.TotalSamples`.
pub fn vector_total_samples(vec: &[Sample]) -> usize {
    let mut n = 0;
    for s in vec {
        n += 1;
        if let Some(h) = &s.h {
            n += h.size() / 16;
        }
    }
    n
}

/// Go `Matrix.TotalSamples`.
pub fn matrix_total_samples(m: &[Series]) -> usize {
    m.iter().map(|s| s.floats.len() + total_hpoint_size(&s.histograms)).sum()
}

/// Whether two of the label sets are equal (Go `ContainsSameLabelset`, which compares hashes).
pub fn contains_same_labelset<'a>(mut metrics: impl ExactSizeIterator<Item = &'a Labels>) -> bool {
    match metrics.len() {
        0 | 1 => false,
        2 => {
            let a = metrics.next().unwrap();
            let b = metrics.next().unwrap();
            a.hash() == b.hash()
        }
        n => {
            let mut seen = HashSet::with_capacity(n);
            for m in metrics {
                if !seen.insert(m.hash()) {
                    return true;
                }
            }
            false
        }
    }
}

/// Go `Vector.ContainsSameLabelset`.
pub fn vector_contains_same_labelset(vec: &[Sample]) -> bool {
    contains_same_labelset(vec.iter().map(|s| &s.metric))
}

/// Go `Matrix.ContainsSameLabelset`.
pub fn matrix_contains_same_labelset(m: &[Series]) -> bool {
    contains_same_labelset(m.iter().map(|s| &s.metric))
}

/// Sorts a matrix by labels (Go `sort.Sort(Matrix)`).
pub fn sort_matrix(m: &mut [Series]) {
    m.sort_by(|a, b| a.metric.cmp(&b.metric));
}

/// The value of an expression (Go `parser.Value`).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Matrix(Matrix),
    Vector(Vector),
    Scalar(Scalar),
    String(StringValue),
}

impl Value {
    /// Go `Value.Type()`.
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Matrix(_) => ValueType::Matrix,
            Value::Vector(_) => ValueType::Vector,
            Value::Scalar(_) => ValueType::Scalar,
            Value::String(_) => ValueType::String,
        }
    }

    pub fn as_matrix(&self) -> Option<&Matrix> {
        match self {
            Value::Matrix(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_vector(&self) -> Option<&Vector> {
        match self {
            Value::Vector(v) => Some(v),
            _ => None,
        }
    }

    pub fn into_matrix(self) -> Option<Matrix> {
        match self {
            Value::Matrix(m) => Some(m),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Matrix(m) => f.write_str(&matrix_string(m)),
            Value::Vector(v) => f.write_str(&vector_string(v)),
            Value::Scalar(s) => fmt::Display::fmt(s, f),
            Value::String(s) => fmt::Display::fmt(s, f),
        }
    }
}

/// The result of a query (Go `promql.Result`).
#[derive(Clone, Debug, Default)]
pub struct QueryResult {
    pub err: Option<Error>,
    pub value: Option<Value>,
    pub warnings: Annotations,
}

impl QueryResult {
    /// Go `Result.Vector()`.
    pub fn vector(&self) -> Result<&Vector, Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        match &self.value {
            Some(Value::Vector(v)) => Ok(v),
            _ => Err(Error::msg("query result is not a Vector")),
        }
    }

    /// Go `Result.Matrix()`.
    pub fn matrix(&self) -> Result<&Matrix, Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        match &self.value {
            Some(Value::Matrix(m)) => Ok(m),
            _ => Err(Error::msg("query result is not a range Vector")),
        }
    }

    /// Go `Result.Scalar()`.
    pub fn scalar(&self) -> Result<Scalar, Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        match &self.value {
            Some(Value::Scalar(s)) => Ok(*s),
            _ => Err(Error::msg("query result is not a Scalar")),
        }
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(e) = &self.err {
            return fmt::Display::fmt(e, f);
        }
        match &self.value {
            None => Ok(()),
            Some(v) => fmt::Display::fmt(v, f),
        }
    }
}

/// Sample start timestamps aligned with the points of a matrix (Go `StartTimestamps`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StartTimestamps {
    pub floats: Vec<i64>,
    pub histograms: Vec<i64>,
}

impl StartTimestamps {
    pub fn reset(&mut self) {
        self.floats.clear();
        self.histograms.clear();
    }
}

/// A [`Series`] as a storage series (Go `StorageSeries`), for example the result of a subquery.
#[derive(Clone, Debug)]
pub struct StorageSeries {
    pub series: Series,
}

impl StorageSeries {
    pub fn new(series: Series) -> StorageSeries {
        StorageSeries { series }
    }
}

impl StorageSeriesTrait for StorageSeries {
    fn labels(&self) -> &Labels {
        &self.series.metric
    }

    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(StorageSeriesIterator::new(&self.series)))
    }
}

/// Iterates over the points of a [`Series`] (Go `storageSeriesIterator`).
pub struct StorageSeriesIterator<'a> {
    floats: &'a [FPoint],
    histograms: &'a [HPoint],
    i_floats: isize,
    i_histograms: usize,
    curr_t: i64,
    curr_f: f64,
    curr_h: Option<&'a HistogramRef>,
}

impl<'a> StorageSeriesIterator<'a> {
    pub fn new(series: &'a Series) -> StorageSeriesIterator<'a> {
        StorageSeriesIterator {
            floats: &series.floats,
            histograms: &series.histograms,
            i_floats: -1,
            i_histograms: 0,
            curr_t: i64::MIN,
            curr_f: 0.0,
            curr_h: None,
        }
    }
}

impl ChunkIterator for StorageSeriesIterator<'_> {
    fn seek(&mut self, t: i64) -> ChunkValueType {
        if self.i_floats >= self.floats.len() as isize && self.i_histograms >= self.histograms.len() {
            return ChunkValueType::None;
        }
        while self.curr_t < t {
            if self.next() == ChunkValueType::None {
                return ChunkValueType::None;
            }
        }
        if self.curr_h.is_some() { ChunkValueType::FloatHistogram } else { ChunkValueType::Float }
    }

    fn at(&self) -> (i64, f64) {
        (self.curr_t, self.curr_f)
    }

    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("storageSeriesIterator: AtHistogram not supported")
    }

    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let h = self.curr_h.expect("no histogram at the iterator position");
        match fh {
            None => (self.curr_t, (**h).clone()),
            Some(mut fh) => {
                h.copy_to(&mut fh);
                (self.curr_t, fh)
            }
        }
    }

    fn at_t(&self) -> i64 {
        self.curr_t
    }

    fn at_st(&self) -> i64 {
        0
    }

    fn next(&mut self) -> ChunkValueType {
        if self.curr_h.is_some() {
            self.i_histograms += 1;
        } else {
            self.i_floats += 1;
        }
        let floats_exhausted = self.i_floats >= self.floats.len() as isize;
        let histograms_exhausted = self.i_histograms >= self.histograms.len();
        let pick_h = if floats_exhausted {
            if histograms_exhausted {
                return ChunkValueType::None;
            }
            true
        } else if histograms_exhausted {
            false
        } else {
            self.histograms[self.i_histograms].t < self.floats[self.i_floats as usize].t
        };
        if pick_h {
            let p = &self.histograms[self.i_histograms];
            self.curr_t = p.t;
            self.curr_f = 0.0;
            self.curr_h = Some(&p.h);
            ChunkValueType::FloatHistogram
        } else {
            let p = self.floats[self.i_floats as usize];
            self.curr_t = p.t;
            self.curr_f = p.f;
            self.curr_h = None;
            ChunkValueType::Float
        }
    }

    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}
