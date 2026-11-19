//! Port of `annotations.go`: categorized translation warnings.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use storage::annotations::PROMQL_INFO;
use storage::{Annotations, Error};

/// Go `WarningCategory`: a bounded label value for the translation warnings metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WarningCategory {
    /// An OTLP metric has no data points and is dropped.
    EmptyDataPoints,
    /// Two or more attribute names map to the same label name; their values are concatenated.
    LabelNameCollision,
    /// A histogram data point has a zero count but a non-zero sum.
    HistogramZeroCountNonZeroSum,
    /// A classic histogram data point defines +Inf as an explicit bound.
    HistogramPlusInfBound,
    /// Any other warning.
    Other,
}

impl WarningCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            WarningCategory::EmptyDataPoints => "empty_data_points",
            WarningCategory::LabelNameCollision => "label_name_collision",
            WarningCategory::HistogramZeroCountNonZeroSum => "histogram_zero_count_non_zero_sum",
            WarningCategory::HistogramPlusInfBound => "histogram_plus_inf_bound",
            WarningCategory::Other => "other",
        }
    }
}

impl fmt::Display for WarningCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Go `categorizedWarning`.
#[derive(Debug)]
pub struct CategorizedWarning {
    pub category: WarningCategory,
    pub msg: String,
}

impl fmt::Display for CategorizedWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for CategorizedWarning {}

/// Go `newCategorizedWarningf` (the message is already formatted).
pub fn new_categorized_warning(category: WarningCategory, msg: String) -> Error {
    Error::Other(Arc::new(CategorizedWarning { category, msg }))
}

/// Go `WarningCategoryOf`: the category of a translation warning, `Other` for uncategorized errors.
pub fn warning_category_of(err: &Error) -> WarningCategory {
    match err.downcast_ref::<CategorizedWarning>() {
        Some(cw) => cw.category,
        None => WarningCategory::Other,
    }
}

/// Go `CountWarningsByCategory`: the number of warnings per category, skipping infos. Empty for no annotations
/// (Go returns a nil map).
pub fn count_warnings_by_category(annots: &Annotations) -> HashMap<WarningCategory, usize> {
    let mut counts = HashMap::new();
    for err in annots.iter() {
        if err.is(&Error::Sentinel(&PROMQL_INFO)) {
            continue;
        }
        *counts.entry(warning_category_of(err)).or_insert(0) += 1;
    }
    counts
}
