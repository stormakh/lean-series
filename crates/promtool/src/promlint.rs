//! Port of client_golang (v1.25.0) `prometheus/testutil/promlint`: a linter for metric names, types and
//! metadata in the Prometheus text format.

use crate::expfmt::{self, MetricFamily, MetricType, ValidationScheme};

/// Go `Problem`: an issue detected by a linter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problem {
    /// The name of the metric indicated by this Problem.
    pub metric: String,
    /// A description of the issue for this Problem.
    pub text: String,
}

/// Go `Validation`.
pub type Validation = fn(&MetricFamily) -> Vec<String>;

/// Go `defaultValidations`.
pub const DEFAULT_VALIDATIONS: &[Validation] = &[
    lint_help,
    lint_metric_units,
    lint_counter,
    lint_histogram_summary_reserved,
    lint_metric_type_in_name,
    lint_reserved_chars,
    lint_camel_case,
    lint_unit_abbreviations,
    lint_duplicate_metric,
];

/// Go `Linter`.
pub struct Linter<'a> {
    r: Option<&'a [u8]>,
    mfs: Vec<MetricFamily>,
    custom_validations: Vec<Validation>,
}

impl<'a> Linter<'a> {
    /// Go `New`: lints the Prometheus text format in `r`.
    pub fn new(r: &'a [u8]) -> Linter<'a> {
        Linter { r: Some(r), mfs: Vec::new(), custom_validations: Vec::new() }
    }

    /// Go `NewWithMetricFamilies`.
    pub fn new_with_metric_families(mfs: Vec<MetricFamily>) -> Linter<'static> {
        Linter { r: None, mfs, custom_validations: Vec::new() }
    }

    /// Go `AddCustomValidations`.
    pub fn add_custom_validations(&mut self, vs: &[Validation]) {
        self.custom_validations.extend_from_slice(vs);
    }

    /// Go `Lint`: the problems sorted by metric name and text.
    pub fn lint(&self) -> Result<Vec<Problem>, expfmt::TextError> {
        let mut problems = Vec::new();
        if let Some(r) = self.r {
            // expfmt.NewDecoder(r, FmtText) parses the whole input with the legacy name validation, hands out
            // the families, then the error.
            let (fams, err) = expfmt::text_to_metric_families(ValidationScheme::Legacy, r);
            if let Some(e) = err {
                return Err(e);
            }
            for mf in fams.values() {
                problems.extend(self.lint_one(mf));
            }
        }
        for mf in &self.mfs {
            problems.extend(self.lint_one(mf));
        }
        // Ensure deterministic output (stable sort).
        problems.sort_by(|a, b| a.metric.cmp(&b.metric).then_with(|| a.text.cmp(&b.text)));
        Ok(problems)
    }

    fn lint_one(&self, mf: &MetricFamily) -> Vec<Problem> {
        let mut problems = Vec::new();
        for f in DEFAULT_VALIDATIONS.iter().chain(self.custom_validations.iter()) {
            for text in f(mf) {
                problems.push(Problem { metric: mf.name.clone(), text });
            }
        }
        problems
    }
}

/// Go `LintHelp`.
pub fn lint_help(mf: &MetricFamily) -> Vec<String> {
    if mf.help.is_none() { vec!["no help text".to_string()] } else { Vec::new() }
}

/// Go `LintMetricUnits`.
pub fn lint_metric_units(mf: &MetricFamily) -> Vec<String> {
    match metric_units(&mf.name) {
        Some((unit, base)) if unit != base => vec![format!("use base unit {} instead of {}", go_q(base), go_q(unit))],
        _ => Vec::new(),
    }
}

/// Go `LintCounter`.
pub fn lint_counter(mf: &MetricFamily) -> Vec<String> {
    let is_counter = mf.get_type() == MetricType::COUNTER;
    let is_untyped = mf.get_type() == MetricType::UNTYPED;
    let has_total_suffix = mf.name.ends_with("_total");
    if is_counter && !has_total_suffix {
        vec![r#"counter metrics should have "_total" suffix"#.to_string()]
    } else if !is_untyped && !is_counter && has_total_suffix {
        vec![r#"non-counter metrics should not have "_total" suffix"#.to_string()]
    } else {
        Vec::new()
    }
}

/// Go `LintHistogramSummaryReserved`.
pub fn lint_histogram_summary_reserved(mf: &MetricFamily) -> Vec<String> {
    let t = mf.get_type();
    if t == MetricType::UNTYPED {
        return Vec::new();
    }
    let mut problems = Vec::new();
    let is_histogram = t == MetricType::HISTOGRAM;
    let is_summary = t == MetricType::SUMMARY;
    let n = &mf.name;
    if !is_histogram && n.ends_with("_bucket") {
        problems.push(r#"non-histogram metrics should not have "_bucket" suffix"#.to_string());
    }
    if !is_histogram && !is_summary && n.ends_with("_count") {
        problems.push(r#"non-histogram and non-summary metrics should not have "_count" suffix"#.to_string());
    }
    if !is_histogram && !is_summary && n.ends_with("_sum") {
        problems.push(r#"non-histogram and non-summary metrics should not have "_sum" suffix"#.to_string());
    }
    for m in &mf.metric {
        for l in &m.label {
            if !is_histogram && l.name == "le" {
                problems.push(r#"non-histogram metrics should not have "le" label"#.to_string());
            }
            if !is_summary && l.name == "quantile" {
                problems.push(r#"non-summary metrics should not have "quantile" label"#.to_string());
            }
        }
    }
    problems
}

/// Go `LintMetricTypeInName`.
pub fn lint_metric_type_in_name(mf: &MetricFamily) -> Vec<String> {
    if mf.get_type() == MetricType::UNTYPED {
        return Vec::new();
    }
    let n = mf.name.to_lowercase();
    let typename = mf.get_type().name().to_lowercase();
    if n.contains(&format!("_{typename}_")) || n.ends_with(&format!("_{typename}")) {
        vec![format!("metric name should not include type '{typename}'")]
    } else {
        Vec::new()
    }
}

/// Go `LintReservedChars`.
pub fn lint_reserved_chars(mf: &MetricFamily) -> Vec<String> {
    if mf.name.contains(':') { vec!["metric names should not contain ':'".to_string()] } else { Vec::new() }
}

/// Go regexp `[a-z][A-Z]` matches somewhere in `s`.
fn has_camel_case(s: &str) -> bool {
    s.as_bytes().windows(2).any(|w| w[0].is_ascii_lowercase() && w[1].is_ascii_uppercase())
}

/// Go `LintCamelCase`.
pub fn lint_camel_case(mf: &MetricFamily) -> Vec<String> {
    let mut problems = Vec::new();
    if has_camel_case(&mf.name) {
        problems.push("metric names should be written in 'snake_case' not 'camelCase'".to_string());
    }
    for m in &mf.metric {
        for l in &m.label {
            if has_camel_case(&l.name) {
                problems.push("label names should be written in 'snake_case' not 'camelCase'".to_string());
            }
        }
    }
    problems
}

/// Go `LintUnitAbbreviations`.
pub fn lint_unit_abbreviations(mf: &MetricFamily) -> Vec<String> {
    let mut problems = Vec::new();
    let n = mf.name.to_lowercase();
    for s in UNIT_ABBREVIATIONS {
        if n.contains(&format!("_{s}_")) || n.ends_with(&format!("_{s}")) {
            problems.push("metric names should not contain abbreviated units".to_string());
        }
    }
    problems
}

/// Go `LintDuplicateMetric`.
pub fn lint_duplicate_metric(mf: &MetricFamily) -> Vec<String> {
    let mut problems = Vec::new();
    for (i, m) in mf.metric.iter().enumerate() {
        if mf.metric[i + 1..].iter().any(|k| k.label == m.label) {
            problems.push("metric not unique".to_string());
        }
    }
    problems
}

fn go_q(s: &str) -> String {
    textparse::strconv::quote_bytes(s.as_bytes())
}

/// Go `units`: a unit and its base unit.
const UNITS: &[(&str, &str)] = &[
    // Base units.
    ("amperes", "amperes"),
    ("bytes", "bytes"),
    ("celsius", "celsius"),
    ("grams", "grams"),
    ("joules", "joules"),
    ("kelvin", "kelvin"),
    ("meters", "meters"),
    ("metres", "metres"),
    ("seconds", "seconds"),
    ("volts", "volts"),
    // Non base units.
    ("minutes", "seconds"),
    ("hours", "seconds"),
    ("days", "seconds"),
    ("weeks", "seconds"),
    ("kelvins", "kelvin"),
    ("fahrenheit", "celsius"),
    ("rankine", "celsius"),
    ("inches", "meters"),
    ("yards", "meters"),
    ("miles", "meters"),
    ("bits", "bytes"),
    ("calories", "joules"),
    ("pounds", "grams"),
    ("ounces", "grams"),
];

const UNIT_PREFIXES: &[&str] = &[
    "pico", "nano", "micro", "milli", "centi", "deci", "deca", "hecto", "kilo", "kibi", "mega", "mebi", "giga",
    "gibi", "tera", "tebi", "peta", "pebi",
];

const UNIT_ABBREVIATIONS: &[&str] = &["s", "ms", "us", "ns", "sec", "b", "kb", "mb", "gb", "tb", "pb", "m", "h", "d"];

fn unit_base(s: &str) -> Option<&'static str> {
    UNITS.iter().find(|(u, _)| *u == s).map(|(_, b)| *b)
}

/// Go `metricUnits`: the first known unit (with an optional prefix) in the name and its base unit.
fn metric_units(m: &str) -> Option<(&str, &'static str)> {
    for s in m.split('_') {
        if let Some(base) = unit_base(s) {
            return Some((s, base));
        }
        for p in UNIT_PREFIXES {
            if let Some(rest) = s.strip_prefix(p)
                && let Some(base) = unit_base(rest)
            {
                return Some((s, base));
            }
        }
    }
    None
}
