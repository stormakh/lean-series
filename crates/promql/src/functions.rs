//! Port of `promql/functions.go`: the PromQL functions.
//!
//! STUB: being ported. The contract with the engine:
//! - A [`FunctionCall`] gets the instant vector arguments (`vals`; scalars are one-sample vectors, string
//!   arguments are empty vectors), the range vector argument (`matrix`, one series, empty for functions without
//!   one), the argument expressions, and the node helper. It appends its output to `enh.out` (cleared by the
//!   caller) and returns the annotations, or an error where Go panics.
//! - For range vector functions, the range, offset and the anchored/smoothed flags of the matrix argument are in
//!   `enh.range`, `enh.offset` (nanoseconds), `enh.anchored`, `enh.smoothed` (Go reads them from `args[0]`,
//!   which may be a subquery here).

use storage::{Annotations, Error};

use crate::engine::EvalNodeHelper;
use crate::parser::{Expr, FunctionId};
use crate::value::{Series, Vector};

/// A function implementation (Go `FunctionCall`).
pub type FunctionCall =
    fn(vals: &[Vector], matrix: &[Series], args: &[Expr], enh: &mut EvalNodeHelper) -> Result<Annotations, Error>;

/// The implementation of a built-in function, by id; `None` for functions the engine evaluates itself
/// (label_replace, label_join, info) or folds (start, end, range, step).
pub fn function_call(_id: FunctionId) -> Option<FunctionCall> {
    None
}

/// Go `AtModifierUnsafeFunctions`.
pub fn is_at_modifier_unsafe(name: &str) -> bool {
    matches!(
        name,
        "days_in_month"
            | "day_of_month"
            | "day_of_week"
            | "day_of_year"
            | "end"
            | "hour"
            | "minute"
            | "month"
            | "year"
            | "predict_linear"
            | "range"
            | "start"
            | "step"
            | "time"
            | "timestamp"
    )
}

/// Go `AnchoredSafeFunctions`, sorted.
pub const ANCHORED_SAFE_FUNCTIONS: [&str; 5] = ["changes", "delta", "increase", "rate", "resets"];
/// Go `SmoothedSafeFunctions`, sorted.
pub const SMOOTHED_SAFE_FUNCTIONS: [&str; 3] = ["delta", "increase", "rate"];

// ---------------------------------------------------------------------------------------------------------------
// Helpers the engine calls (signatures fixed; bodies being ported).

use model::histogram::FloatHistogram;
use model::labels::Labels;
use storage::annotations::PositionRange as AnnoPos;

use crate::engine::Evaluator;
use crate::value::{FPoint, Value};

/// Go `interpolate`.
pub fn interpolate(_p1: FPoint, _p2: FPoint, _t: i64, _is_counter: bool) -> f64 {
    unimplemented!("interpolate")
}

/// Go `interpolateHistograms`.
#[allow(clippy::too_many_arguments)]
pub fn interpolate_histograms(
    _h1: &FloatHistogram,
    _t1: i64,
    _h2: &FloatHistogram,
    _t2: i64,
    _t: i64,
    _is_counter: bool,
    _annos: &mut Annotations,
    _pos: AnnoPos,
) -> Result<FloatHistogram, Error> {
    unimplemented!("interpolate_histograms")
}

/// Go `annosFromInterpolationError`; errors that Go panics on are returned.
pub fn annos_from_interpolation_error(
    _annos: &mut Annotations,
    err: Error,
    _metric_name: &str,
    _pos: AnnoPos,
) -> Result<(), Error> {
    Err(err)
}

/// Go `extendFloats` (in engine.go): extends the floats of an anchored or smoothed selector to `mint` and
/// `maxt`.
pub fn extend_floats(
    floats: &[FPoint],
    _start_timestamps: &[i64],
    _mint: i64,
    _maxt: i64,
    _lookback_start: i64,
    _smoothed: bool,
) -> Vec<FPoint> {
    floats.to_vec()
}

/// Go `createLabelsForAbsentFunction`.
pub fn create_labels_for_absent_function(_expr: &Expr) -> Labels {
    Labels::empty()
}

/// Go `evalLabelReplace`.
pub(crate) fn eval_label_replace(_ev: &mut Evaluator<'_>, _args: &[Expr]) -> Result<(Value, Annotations), Error> {
    Err(Error::msg("label_replace is not implemented yet"))
}

/// Go `evalLabelJoin`.
pub(crate) fn eval_label_join(_ev: &mut Evaluator<'_>, _args: &[Expr]) -> Result<(Value, Annotations), Error> {
    Err(Error::msg("label_join is not implemented yet"))
}
