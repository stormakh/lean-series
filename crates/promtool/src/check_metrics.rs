//! `promtool check metrics` (main.go `CheckMetrics`, `checkMetricsExtended`).

use std::io::Read;

use crate::expfmt::{self, MetricType, ValidationScheme};
use crate::promlint;
use crate::{Error, FAILURE_EXIT_CODE, Io, LINT_ERR_EXIT_CODE, SUCCESS_EXIT_CODE};

pub const LINT_OPTION_ALL: &str = "all";
pub const LINT_OPTION_NONE: &str = "none";

/// Go `metricStat`.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricStat {
    pub name: String,
    pub cardinality: usize,
    pub percentage: f64,
}

/// Go `CheckMetrics(extended, lint)`: lints the metrics on stdin (`--lint`, default "all") and prints the
/// cardinality table (`--extended`).
pub fn check_metrics(extended: bool, lint: &str, io: &mut Io) -> i32 {
    if !extended && lint == LINT_OPTION_NONE {
        let _ = writeln!(io.stderr, "error: at least one of --extended or linting must be enabled");
        let _ = writeln!(io.stderr, "Use --extended for cardinality analysis, or remove --lint=none to enable linting");
        return FAILURE_EXIT_CODE;
    }

    // Go tees stdin into a buffer while linting, or hands stdin to the extended check; reading it all first
    // is the same.
    let mut input = Vec::new();
    if let Err(e) = io.stdin.read_to_end(&mut input) {
        // Go reports a read error of the linter or of the text parser.
        let msg = net::types::go_io_error(&e);
        if lint != LINT_OPTION_NONE {
            let _ = writeln!(io.stderr, "error while linting: {msg}");
        } else {
            let _ = writeln!(io.stderr, "error while parsing text to metric families: {msg}");
        }
        return FAILURE_EXIT_CODE;
    }

    let mut problems = Vec::new();
    if lint != LINT_OPTION_NONE {
        let l = promlint::Linter::new(&input);
        match l.lint() {
            Ok(p) => problems = p,
            Err(e) => {
                let _ = writeln!(io.stderr, "error while linting: {e}");
                return FAILURE_EXIT_CODE;
            }
        }
        for p in &problems {
            let _ = writeln!(io.stderr, "{} {}", p.metric, p.text);
        }
    }

    let has_lint_problems = !problems.is_empty();

    if extended {
        let (stats, total) = match check_metrics_extended(&mut &input[..]) {
            Ok(r) => r,
            Err(e) => {
                let _ = writeln!(io.stderr, "{e}");
                return FAILURE_EXIT_CODE;
            }
        };
        let mut rows: Vec<[String; 3]> = Vec::with_capacity(stats.len() + 2);
        rows.push(["Metric".to_string(), "Cardinality".to_string(), "Percentage".to_string()]);
        for stat in &stats {
            rows.push([stat.name.clone(), stat.cardinality.to_string(), format!("{:.2}%", stat.percentage * 100.0)]);
        }
        rows.push(["Total".to_string(), total.to_string(), "100%".to_string()]);
        let _ = io.stdout.write_all(tabwriter(&rows, 4, 4).as_bytes());
    }

    if has_lint_problems {
        return LINT_ERR_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

/// Go `text/tabwriter` with `padchar ' '`, no AlignRight/Debug flags, for lines that all have the same number of
/// tab-terminated cells and an empty last cell (the only shape `CheckMetrics` writes). Each column is as wide as
/// its widest cell (in runes) plus `padding`, at least `minwidth`.
pub fn tabwriter(rows: &[[String; 3]], minwidth: usize, padding: usize) -> String {
    let mut widths = [minwidth; 3];
    for r in rows {
        for (j, c) in r.iter().enumerate() {
            let w = c.chars().count() + padding;
            if w > widths[j] {
                widths[j] = w;
            }
        }
    }
    let mut out = String::new();
    for r in rows {
        for (j, c) in r.iter().enumerate() {
            out.push_str(c);
            for _ in c.chars().count()..widths[j] {
                out.push(' ');
            }
        }
        out.push('\n');
    }
    out
}

/// Go `checkMetricsExtended`: the cardinality of each metric family, sorted by cardinality (descending, stable),
/// and the total. Families of equal cardinality are in name order (Go: map order, random).
pub fn check_metrics_extended(r: &mut dyn Read) -> Result<(Vec<MetricStat>, usize), Error> {
    let mut input = Vec::new();
    if let Err(e) = r.read_to_end(&mut input) {
        return Err(format!("error while parsing text to metric families: {}", net::types::go_io_error(&e)).into());
    }
    let r = &input[..];
    // promtool keeps model.NameValidationScheme at its default, UTF8Validation.
    let (metric_families, err) = expfmt::text_to_metric_families(ValidationScheme::Utf8, r);
    if let Some(e) = err {
        return Err(format!("error while parsing text to metric families: {e}").into());
    }

    let mut total = 0usize;
    let mut stats = Vec::with_capacity(metric_families.len());
    for mf in metric_families.values() {
        let cardinality = match mf.get_type() {
            MetricType::COUNTER | MetricType::GAUGE | MetricType::UNTYPED => mf.metric.len(),
            MetricType::HISTOGRAM => {
                // Histogram metrics includes sum, count, buckets.
                let buckets = mf.metric[0].histogram.as_ref().map_or(0, |h| h.bucket.len());
                mf.metric.len() * (2 + buckets)
            }
            MetricType::SUMMARY => {
                // Summary metrics includes sum, count, quantiles.
                let quantiles = mf.metric[0].summary.as_ref().map_or(0, |s| s.quantile.len());
                mf.metric.len() * (2 + quantiles)
            }
            _ => mf.metric.len(),
        };
        stats.push(MetricStat { name: mf.name.clone(), cardinality, percentage: 0.0 });
        total += cardinality;
    }

    for s in &mut stats {
        s.percentage = s.cardinality as f64 / total as f64;
    }

    stats.sort_by_key(|s| std::cmp::Reverse(s.cardinality));

    Ok((stats, total))
}
