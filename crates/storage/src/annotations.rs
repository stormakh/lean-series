//! Port of `util/annotations` (and of `promql/parser/posrange`, which it needs): warnings and infos that a query
//! returns with its result. Every annotation is an [`Error`]; annotations are deduplicated by their text.
//!
//! PromQL creates annotations with the `new_*` functions. Custom annotation kinds implement [`AnnoError`].

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use model::histogram::gofmt::g;
use model::unicode::quote;

use crate::error::{Error, Sentinel};

// ---------------------------------------------------------------------------------------------------------------
// posrange.

/// A position in a query string (Go `posrange.Pos`). Negative numbers are undefined positions.
pub type Pos = i64;

/// A position range in the input string of the parser (Go `posrange.PositionRange`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PositionRange {
    /// Start of the range, zero-indexed.
    pub start: Pos,
    /// End of the range, zero-indexed.
    pub end: Pos,
}

impl PositionRange {
    pub const fn new(start: Pos, end: Pos) -> PositionRange {
        PositionRange { start, end }
    }

    /// Go `StartPosInput`: the start position as "line:col" in `query`, "unknown position" for an empty query
    /// and "invalid position" when the position is outside the query. `line_offset` is added to the line.
    pub fn start_pos_input(&self, query: &str, line_offset: i64) -> String {
        if query.is_empty() {
            return "unknown position".to_string();
        }
        let pos = self.start;
        if pos < 0 || pos as usize > query.len() {
            return "invalid position".to_string();
        }
        let pos = pos as usize;
        let mut last_line_break: i64 = -1;
        let mut line = line_offset + 1;
        // Go ranges over the runes of query[:pos]; a cut inside a rune yields U+FFFD, never '\n'.
        for (i, &c) in query.as_bytes()[..pos].iter().enumerate() {
            if c == b'\n' {
                last_line_break = i as i64;
                line += 1;
            }
        }
        let col = pos as i64 - last_line_break;
        format!("{}:{}", line, col)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The annotation errors.

/// Go `annotations.PromQLInfo`.
pub static PROMQL_INFO: Sentinel = Sentinel::new("PromQL info");
/// Go `annotations.PromQLWarning`.
pub static PROMQL_WARNING: Sentinel = Sentinel::new("PromQL warning");

macro_rules! sentinels {
    ($($name:ident = $parent:ident, $msg:expr;)*) => {
        $(pub static $name: Sentinel = Sentinel::wrapping($msg, &$parent);)*
    };
}

sentinels! {
    INVALID_INTEGRAL_STRATEGY = PROMQL_WARNING, "PromQL warning: strategy should be 0, 1, or 2 (default), using 2";
    INVALID_RATIO_WARNING = PROMQL_WARNING, "PromQL warning: ratio value should be between -1 and 1";
    INVALID_QUANTILE_WARNING = PROMQL_WARNING, "PromQL warning: quantile value should be between 0 and 1";
    BAD_BUCKET_LABEL_WARNING = PROMQL_WARNING, "PromQL warning: bucket label \"le\" is missing or has a malformed value";
    MIXED_FLOATS_HISTOGRAMS_WARNING = PROMQL_WARNING, "PromQL warning: encountered a mix of histograms and floats for";
    MIXED_CLASSIC_NATIVE_HISTOGRAMS_WARNING = PROMQL_WARNING, "PromQL warning: vector contains a mix of classic and native histograms";
    NATIVE_HISTOGRAM_NOT_COUNTER_WARNING = PROMQL_WARNING, "PromQL warning: this native histogram metric is not a counter:";
    NATIVE_HISTOGRAM_NOT_GAUGE_WARNING = PROMQL_WARNING, "PromQL warning: this native histogram metric is not a gauge:";
    MIXED_EXPONENTIAL_CUSTOM_HISTOGRAMS_WARNING = PROMQL_WARNING, "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name";
    INCOMPATIBLE_BUCKET_LAYOUT_IN_BIN_OP_WARNING = PROMQL_WARNING, "PromQL warning: incompatible bucket layout encountered for binary operator";
    SORT_IN_RANGE_QUERY_WARNING = PROMQL_WARNING, "PromQL warning: sort is ineffective for range queries since results are always ordered by labels";

    POSSIBLE_NON_COUNTER_INFO = PROMQL_INFO, "PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket:";
    POSSIBLE_NON_COUNTER_LABEL_INFO = PROMQL_INFO, "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\"";
    HISTOGRAM_QUANTILE_FORCED_MONOTONICITY_INFO = PROMQL_INFO, "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)";
    INCOMPATIBLE_TYPES_IN_BIN_OP_INFO = PROMQL_INFO, "PromQL info: incompatible sample types encountered for binary operator";
    HISTOGRAM_IGNORED_IN_AGGREGATION_INFO = PROMQL_INFO, "PromQL info: ignored histogram in";
    HISTOGRAM_IGNORED_IN_MIXED_RANGE_INFO = PROMQL_INFO, "PromQL info: ignored histograms in a range containing both floats and histograms for metric name";
    NATIVE_HISTOGRAM_QUANTILE_NAN_RESULT_INFO = PROMQL_INFO, "PromQL info: input to histogram_quantile has NaN observations, result is NaN";
    NATIVE_HISTOGRAM_QUANTILE_NAN_SKEW_INFO = PROMQL_INFO, "PromQL info: input to histogram_quantile has NaN observations, result is skewed higher";
    NATIVE_HISTOGRAM_FRACTION_NANS_INFO = PROMQL_INFO, "PromQL info: input to histogram_fraction has NaN observations, which are excluded from all fractions";
    HISTOGRAM_COUNTER_RESET_COLLISION_WARNING = PROMQL_WARNING, "PromQL warning: conflicting counter resets during histogram";
    MISMATCHED_CUSTOM_BUCKETS_HISTOGRAMS_INFO = PROMQL_INFO, "PromQL info: mismatched custom buckets were reconciled during";
    START_TIME_OVERLAP_WARNING = PROMQL_WARNING, "PromQL warning: sample has start time that overlaps with previous sample timestamp";
}

/// An annotation error that can be merged with an equal one and that shows its position once the query is
/// known (Go `annoError`). Implementations are stored as `Error::Annotation(Arc<dyn AnnoError>)`.
pub trait AnnoError: fmt::Debug + Send + Sync + Any {
    /// The message (Go `Error()`). `query` is the query given to `SetQuery`, or "" when it is not set; with a
    /// query the message usually ends with the position, " (line:col)".
    fn error(&self, query: &str) -> String;
    /// The wrapped error (Go `Unwrap`); it tells warnings from infos.
    fn unwrap(&self) -> Option<&Error>;
    /// Go `Merge(prev)`: `self` is the new annotation and `prev` the stored one with the same text. Returns the
    /// annotation to store.
    fn merge(self: Arc<Self>, prev: &Error) -> Error;
}

/// Go `annoErr`: an annotation with a position.
#[derive(Clone, Debug)]
pub struct AnnoErr {
    pub position_range: PositionRange,
    pub err: Error,
}

impl AnnoError for AnnoErr {
    fn error(&self, query: &str) -> String {
        if query.is_empty() {
            return self.err.to_string();
        }
        format!(
            "{} ({})",
            self.err,
            self.position_range.start_pos_input(query, 0)
        )
    }
    fn unwrap(&self) -> Option<&Error> {
        Some(&self.err)
    }
    /// Generic annotations are not merged: the new one is kept.
    fn merge(self: Arc<Self>, _prev: &Error) -> Error {
        Error::Annotation(self)
    }
}

fn anno(pos: PositionRange, err: Error) -> Error {
    Error::Annotation(Arc::new(AnnoErr {
        position_range: pos,
        err,
    }))
}

/// `fmt.Errorf("%w<rest>", base)`.
fn wrapf(base: &'static Sentinel, rest: &str) -> Error {
    Error::wrapped(format!("{}{}", base.msg, rest), Error::Sentinel(base))
}

/// Go `maybeAddMetricName`.
fn maybe_add_metric_name(anno: Error, metric_name: &str) -> Error {
    if metric_name.is_empty() {
        return anno;
    }
    let msg = format!("{} for metric name {}", anno, quote(metric_name));
    Error::wrapped(msg, anno)
}

/// An invalid quantile value: outside [0, 1] or NaN.
pub fn new_invalid_quantile_warning(q: f64, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(&INVALID_QUANTILE_WARNING, &format!(", got {}", g(q))),
    )
}

/// An invalid ratio value: outside [-1, 1] or NaN.
pub fn new_invalid_ratio_warning(q: f64, to: f64, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &INVALID_RATIO_WARNING,
            &format!(", got {}, capping to {}", g(q), g(to)),
        ),
    )
}

/// The bucket label of a classic histogram could not be parsed.
pub fn new_bad_bucket_label_warning(metric_name: &str, label: &str, pos: PositionRange) -> Error {
    let a = wrapf(&BAD_BUCKET_LABEL_WARNING, &format!(" of {}", quote(label)));
    anno(pos, maybe_add_metric_name(a, metric_name))
}

/// The queried series has float and histogram samples, for functions that do not support mixed samples.
pub fn new_mixed_floats_histograms_warning(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &MIXED_FLOATS_HISTOGRAMS_WARNING,
            &format!(" metric name {}", quote(metric_name)),
        ),
    )
}

/// The series of an aggregation have float and histogram samples.
pub fn new_mixed_floats_histograms_agg_warning(pos: PositionRange) -> Error {
    anno(pos, wrapf(&MIXED_FLOATS_HISTOGRAMS_WARNING, " aggregation"))
}

/// The queried series include classic and native histograms.
pub fn new_mixed_classic_native_histograms_warning(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        maybe_add_metric_name(
            Error::Sentinel(&MIXED_CLASSIC_NATIVE_HISTOGRAMS_WARNING),
            metric_name,
        ),
    )
}

/// `histogramRate` with isCounter set on a gauge histogram.
pub fn new_native_histogram_not_counter_warning(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &NATIVE_HISTOGRAM_NOT_COUNTER_WARNING,
            &format!(" {}", quote(metric_name)),
        ),
    )
}

/// `histogramRate` with isCounter unset on a counter histogram.
pub fn new_native_histogram_not_gauge_warning(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &NATIVE_HISTOGRAM_NOT_GAUGE_WARNING,
            &format!(" {}", quote(metric_name)),
        ),
    )
}

/// The queried series include histograms with exponential and custom bucket schemas.
pub fn new_mixed_exponential_custom_histograms_warning(
    metric_name: &str,
    pos: PositionRange,
) -> Error {
    anno(
        pos,
        wrapf(
            &MIXED_EXPONENTIAL_CUSTOM_HISTOGRAMS_WARNING,
            &format!(" {}", quote(metric_name)),
        ),
    )
}

/// A counter metric with only float samples whose name does not end in _total, _sum, _count or _bucket.
pub fn new_possible_non_counter_info(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &POSSIBLE_NON_COUNTER_INFO,
            &format!(" {}", quote(metric_name)),
        ),
    )
}

/// A counter metric with only float samples whose __type__ label is not "counter".
pub fn new_possible_non_counter_label_info(
    metric_name: &str,
    type_label: &str,
    pos: PositionRange,
) -> Error {
    anno(
        pos,
        wrapf(
            &POSSIBLE_NON_COUNTER_LABEL_INFO,
            &format!(", got {}: {}", quote(type_label), quote(metric_name)),
        ),
    )
}

/// Go `histogramQuantileForcedMonotonicityErr`.
#[derive(Clone, Debug)]
pub struct HistogramQuantileForcedMonotonicityErr {
    pub position_range: PositionRange,
    pub err: Error,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_bucket: f64,
    pub max_bucket: f64,
    pub max_diff: f64,
    pub count: i64,
}

impl AnnoError for HistogramQuantileForcedMonotonicityErr {
    fn error(&self, query: &str) -> String {
        if query.is_empty() {
            return self.err.to_string();
        }
        format!(
            "{}, from buckets {} to {}, with a max diff of {}, over {} samples from {} to {} ({})",
            self.err,
            g(self.min_bucket),
            g(self.max_bucket),
            g_prec(self.max_diff, 2),
            self.count + 1,
            rfc3339_seconds(self.min_ts / 1000),
            rfc3339_seconds(self.max_ts / 1000),
            self.position_range.start_pos_input(query, 0)
        )
    }
    fn unwrap(&self) -> Option<&Error> {
        Some(&self.err)
    }
    fn merge(self: Arc<Self>, prev: &Error) -> Error {
        let Some(o) = prev.annotation_as::<HistogramQuantileForcedMonotonicityErr>() else {
            return Error::Annotation(self);
        };
        if self.err.to_string() != o.err.to_string() {
            return Error::Annotation(self);
        }
        let mut o = o.clone();
        if self.min_ts < o.min_ts {
            o.min_ts = self.min_ts;
        }
        if self.max_ts > o.max_ts {
            o.max_ts = self.max_ts;
        }
        if self.min_bucket < o.min_bucket {
            o.min_bucket = self.min_bucket;
        }
        if self.max_bucket > o.max_bucket {
            o.max_bucket = self.max_bucket;
        }
        if self.max_diff > o.max_diff {
            o.max_diff = self.max_diff;
        }
        o.count += self.count + 1;
        Error::Annotation(Arc::new(o))
    }
}

/// The input (classic histograms) to histogram_quantile had to be made monotonic.
pub fn new_histogram_quantile_forced_monotonicity_info(
    metric_name: &str,
    pos: PositionRange,
    ts: i64,
    min_bucket: f64,
    max_bucket: f64,
    max_diff: f64,
) -> Error {
    Error::Annotation(Arc::new(HistogramQuantileForcedMonotonicityErr {
        position_range: pos,
        err: maybe_add_metric_name(
            Error::Sentinel(&HISTOGRAM_QUANTILE_FORCED_MONOTONICITY_INFO),
            metric_name,
        ),
        min_ts: ts,
        max_ts: ts,
        min_bucket,
        max_bucket,
        max_diff,
        count: 0,
    }))
}

/// A binary operator on a combination of types that does not work.
pub fn new_incompatible_types_in_bin_op_info(
    lhs_type: &str,
    operator: &str,
    rhs_type: &str,
    pos: PositionRange,
) -> Error {
    anno(
        pos,
        wrapf(
            &INCOMPATIBLE_TYPES_IN_BIN_OP_INFO,
            &format!(
                " {}: {} {} {}",
                quote(operator),
                lhs_type,
                operator,
                rhs_type
            ),
        ),
    )
}

/// A histogram ignored by an aggregation operator that cannot handle histograms.
pub fn new_histogram_ignored_in_aggregation_info(aggregation: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &HISTOGRAM_IGNORED_IN_AGGREGATION_INFO,
            &format!(" {} aggregation", aggregation),
        ),
    )
}

/// A histogram ignored in a range vector with floats and histograms.
pub fn new_histogram_ignored_in_mixed_range_info(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &HISTOGRAM_IGNORED_IN_MIXED_RANGE_INFO,
            &format!(" {}", quote(metric_name)),
        ),
    )
}

/// A binary operator on two incompatible histograms.
pub fn new_incompatible_bucket_layout_in_bin_op_warning(
    operator: &str,
    pos: PositionRange,
) -> Error {
    anno(
        pos,
        wrapf(
            &INCOMPATIBLE_BUCKET_LAYOUT_IN_BIN_OP_WARNING,
            &format!(" {}", operator),
        ),
    )
}

/// sort or sort_desc in a range query.
pub fn new_sort_in_range_query_warning(pos: PositionRange) -> Error {
    anno(pos, Error::Sentinel(&SORT_IN_RANGE_QUERY_WARNING))
}

pub fn new_native_histogram_quantile_nan_result_info(
    metric_name: &str,
    pos: PositionRange,
) -> Error {
    anno(
        pos,
        maybe_add_metric_name(
            Error::Sentinel(&NATIVE_HISTOGRAM_QUANTILE_NAN_RESULT_INFO),
            metric_name,
        ),
    )
}

pub fn new_native_histogram_quantile_nan_skew_info(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        maybe_add_metric_name(
            Error::Sentinel(&NATIVE_HISTOGRAM_QUANTILE_NAN_SKEW_INFO),
            metric_name,
        ),
    )
}

pub fn new_native_histogram_fraction_nans_info(metric_name: &str, pos: PositionRange) -> Error {
    anno(
        pos,
        maybe_add_metric_name(
            Error::Sentinel(&NATIVE_HISTOGRAM_FRACTION_NANS_INFO),
            metric_name,
        ),
    )
}

/// Go `HistogramOperation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistogramOperation {
    Add,
    Sub,
    Agg,
}

impl HistogramOperation {
    /// Go `String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            HistogramOperation::Add => "addition",
            HistogramOperation::Sub => "subtraction",
            HistogramOperation::Agg => "aggregation",
        }
    }
}

impl fmt::Display for HistogramOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Two counter histograms are added or subtracted where one has a CounterReset hint and the other
/// NotCounterReset.
pub fn new_histogram_counter_reset_collision_warning(
    pos: PositionRange,
    operation: HistogramOperation,
) -> Error {
    anno(
        pos,
        wrapf(
            &HISTOGRAM_COUNTER_RESET_COLLISION_WARNING,
            &format!(" {}", operation.as_str()),
        ),
    )
}

/// Custom bucket histograms with mismatched bounds were reconciled.
pub fn new_mismatched_custom_buckets_histograms_info(
    pos: PositionRange,
    operation: HistogramOperation,
) -> Error {
    anno(
        pos,
        wrapf(
            &MISMATCHED_CUSTOM_BUCKETS_HISTOGRAMS_INFO,
            &format!(" {}", operation.as_str()),
        ),
    )
}

/// Go `startTimeOverlapErr`.
#[derive(Clone, Debug)]
pub struct StartTimeOverlapErr {
    pub position_range: PositionRange,
    pub err: Error,
    pub metric_name: String,
    pub count: i64,
}

impl AnnoError for StartTimeOverlapErr {
    fn error(&self, query: &str) -> String {
        if query.is_empty() {
            // Without the count, so that equal warnings deduplicate.
            return format!("{} for metric {}", self.err, quote(&self.metric_name));
        }
        if self.count > 1 {
            return format!(
                "{} for metric {} ({} occurrences) ({})",
                self.err,
                quote(&self.metric_name),
                self.count,
                self.position_range.start_pos_input(query, 0)
            );
        }
        format!(
            "{} for metric {} ({})",
            self.err,
            quote(&self.metric_name),
            self.position_range.start_pos_input(query, 0)
        )
    }
    fn unwrap(&self) -> Option<&Error> {
        Some(&self.err)
    }
    fn merge(self: Arc<Self>, prev: &Error) -> Error {
        let Some(o) = prev.annotation_as::<StartTimeOverlapErr>() else {
            return Error::Annotation(self);
        };
        if self.err.to_string() != o.err.to_string() || self.metric_name != o.metric_name {
            return Error::Annotation(self);
        }
        let mut o = o.clone();
        o.count += self.count;
        Error::Annotation(Arc::new(o))
    }
}

/// A sample's start time overlaps with the previous sample's timestamp.
pub fn new_start_time_overlap_warning(metric_name: &str, pos: PositionRange) -> Error {
    Error::Annotation(Arc::new(StartTimeOverlapErr {
        position_range: pos,
        err: Error::Sentinel(&START_TIME_OVERLAP_WARNING),
        metric_name: metric_name.to_string(),
        count: 1,
    }))
}

/// An invalid integral strategy.
pub fn new_invalid_integral_strategy_warning(strategy: f64, pos: PositionRange) -> Error {
    anno(
        pos,
        wrapf(
            &INVALID_INTEGRAL_STRATEGY,
            &format!(", got {}", g(strategy)),
        ),
    )
}

// ---------------------------------------------------------------------------------------------------------------
// Annotations.

/// Warnings and infos of a query, deduplicated by their text (Go `annotations.Annotations`, a map). The empty
/// value does not allocate. Iteration is in insertion order (Go's map order is random).
#[derive(Clone, Default)]
pub struct Annotations {
    errs: Vec<Error>,
    index: HashMap<String, usize>,
}

/// The annotation error inside `err` (Go `errors.AsType[annoError]`).
fn find_anno(err: &Error) -> Option<&Arc<dyn AnnoError>> {
    match err {
        Error::Annotation(a) => Some(a),
        Error::Wrapped(w) => find_anno(&w.1),
        _ => None,
    }
}

impl Annotations {
    /// Go `New()`.
    pub fn new() -> Annotations {
        Annotations::default()
    }

    /// Go `Add`: adds an annotation in place. An annotation with the same text is merged with it.
    pub fn add(&mut self, err: Error) -> &mut Annotations {
        let key = err.to_string();
        match self.index.get(&key) {
            Some(&i) => {
                let err = match find_anno(&err) {
                    Some(a) => a.clone().merge(&self.errs[i]),
                    None => err,
                };
                // Go stores it under err.Error(), which merging does not change (the text without a query).
                self.errs[i] = err;
            }
            None => {
                self.index.insert(key, self.errs.len());
                self.errs.push(err);
            }
        }
        self
    }

    /// Go `Add` as an expression: adds `err` and returns the annotations.
    pub fn with(mut self, err: Error) -> Annotations {
        self.add(err);
        self
    }

    /// Go `Merge`: adds every annotation of `aa`.
    pub fn merge(&mut self, aa: Annotations) -> &mut Annotations {
        for (key, i) in aa.index {
            let val = aa.errs[i].clone();
            match self.index.get(&key) {
                Some(&j) => {
                    let val = match find_anno(&val) {
                        Some(a) => a.clone().merge(&self.errs[j]),
                        None => val,
                    };
                    self.errs[j] = val;
                }
                None => {
                    self.index.insert(key, self.errs.len());
                    self.errs.push(val);
                }
            }
        }
        self
    }

    /// Like [`Annotations::merge`], without consuming `aa`.
    pub fn merge_ref(&mut self, aa: &Annotations) -> &mut Annotations {
        if !aa.is_empty() {
            self.merge(aa.clone());
        }
        self
    }

    pub fn len(&self) -> usize {
        self.errs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.errs.is_empty()
    }

    /// The annotations in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &Error> {
        self.errs.iter()
    }

    /// Whether an annotation with this text exists (Go map lookup).
    pub fn contains_key(&self, key: &str) -> bool {
        self.index.contains_key(key)
    }

    /// The annotation stored under `key`.
    pub fn get(&self, key: &str) -> Option<&Error> {
        self.index.get(key).map(|&i| &self.errs[i])
    }

    /// Go `AsErrors`.
    pub fn as_errors(&self) -> Vec<Error> {
        self.errs.clone()
    }

    /// Go `AsStrings`: the warnings and the infos as strings, with positions in `query`, at most `max_warnings`
    /// and `max_infos` of each (0 for no limit), plus a note on how many were left out.
    pub fn as_strings(
        &self,
        query: &str,
        max_warnings: usize,
        max_infos: usize,
    ) -> (Vec<String>, Vec<String>) {
        let mut warnings = Vec::with_capacity(max_warnings + 1);
        let mut infos = Vec::with_capacity(max_infos + 1);
        let mut warn_skipped = 0;
        let mut info_skipped = 0;
        for err in &self.errs {
            let text = match err {
                Error::Annotation(a) => a.error(query),
                _ => err.to_string(),
            };
            if err.is(&Error::Sentinel(&PROMQL_INFO)) {
                if max_infos == 0 || infos.len() < max_infos {
                    infos.push(text);
                } else {
                    info_skipped += 1;
                }
            } else if max_warnings == 0 || warnings.len() < max_warnings {
                warnings.push(text);
            } else {
                warn_skipped += 1;
            }
        }
        if warn_skipped > 0 {
            warnings.push(format!("{} more warning annotations omitted", warn_skipped));
        }
        if info_skipped > 0 {
            infos.push(format!("{} more info annotations omitted", info_skipped));
        }
        (warnings, infos)
    }

    /// Go `CountWarningsAndInfo`.
    pub fn count_warnings_and_info(&self) -> (usize, usize) {
        let (mut w, mut i) = (0, 0);
        for err in &self.errs {
            if err.is(&Error::Sentinel(&PROMQL_WARNING)) {
                w += 1;
            }
            if err.is(&Error::Sentinel(&PROMQL_INFO)) {
                i += 1;
            }
        }
        (w, i)
    }
}

impl fmt::Debug for Annotations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.errs.iter().map(|e| e.to_string()))
            .finish()
    }
}

/// Equal when both have the same keys with equal errors (Go `reflect.DeepEqual` of the maps).
impl PartialEq for Annotations {
    fn eq(&self, other: &Annotations) -> bool {
        self.len() == other.len()
            && self
                .index
                .iter()
                .all(|(k, &i)| other.get(k) == Some(&self.errs[i]))
    }
}

impl FromIterator<Error> for Annotations {
    fn from_iter<I: IntoIterator<Item = Error>>(iter: I) -> Annotations {
        let mut a = Annotations::new();
        for e in iter {
            a.add(e);
        }
        a
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Formatting helpers.

/// Go `%.<prec>g`.
pub fn g_prec(f: f64, prec: usize) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    let prec = prec.max(1);
    let s = format!("{:.*e}", prec - 1, f);
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let neg = mant.starts_with('-');
    let mut digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let exp: i64 = exp.parse().unwrap_or(0);
    // Go's decimal drops trailing zeros after rounding.
    while digits.len() > 1 && digits.ends_with('0') {
        digits.pop();
    }
    if f == 0.0 {
        digits = "0".to_string();
    }
    let nd = digits.len() as i64;
    let dp = if f == 0.0 { 0 } else { exp + 1 };
    let mut eprec = prec as i64;
    if eprec > nd && nd >= dp {
        eprec = nd;
    }
    let x = dp - 1;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    if x < -4 || x >= eprec {
        let p = (prec as i64).min(nd);
        out.push_str(&digits[..1]);
        if p > 1 {
            out.push('.');
            out.push_str(&digits[1..p as usize]);
        }
        out.push('e');
        out.push(if x < 0 { '-' } else { '+' });
        let ax = x.unsigned_abs();
        if ax < 10 {
            out.push('0');
        }
        out.push_str(&ax.to_string());
        return out;
    }
    let p = if prec as i64 > dp { nd } else { prec as i64 };
    let decimals = (p - dp).max(0);
    // Fixed notation with `decimals` digits after the point.
    if dp <= 0 {
        out.push('0');
        if decimals > 0 {
            out.push('.');
            for i in 0..decimals {
                let k = i + dp; // Index into digits (negative means a leading zero).
                if k < 0 || k >= nd {
                    out.push('0');
                } else {
                    out.push(digits.as_bytes()[k as usize] as char);
                }
            }
        }
    } else {
        for i in 0..dp {
            out.push(if i < nd {
                digits.as_bytes()[i as usize] as char
            } else {
                '0'
            });
        }
        if decimals > 0 {
            out.push('.');
            for i in dp..dp + decimals {
                out.push(if i < nd {
                    digits.as_bytes()[i as usize] as char
                } else {
                    '0'
                });
            }
        }
    }
    out
}

/// Go `time.Unix(secs, 0).UTC().Format(time.RFC3339)`.
pub fn rfc3339_seconds(secs: i64) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (h, m, s) = (rem / 3600, rem % 3600 / 60, rem % 60);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, m, s)
}
