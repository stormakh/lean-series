//! Port of `util/annotations/annotations_test.go`.

use std::sync::Arc;

use model::histogram::gofmt::g;
use storage::Error;
use util::annotations::*;

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[test]
fn test_annotations_as_strings() {
    let mut annos = Annotations::new();
    let pos = PositionRange { start: 3, end: 8 };

    annos.add(Error::msg("this is a non-annotation error"));

    annos.add(new_invalid_ratio_warning(1.1, 100.0, pos));
    annos.add(new_invalid_ratio_warning(1.2, 123.0, pos));

    annos.add(new_test_custom_warning(1.5, pos, 12.0, 14.0));
    annos.add(new_test_custom_warning(1.5, pos, 10.0, 20.0));
    annos.add(new_test_custom_warning(1.5, pos, 5.0, 15.0));
    annos.add(new_test_custom_warning(1.5, pos, 12.0, 14.0));

    annos.add(new_histogram_ignored_in_aggregation_info("sum", pos));

    annos.add(new_histogram_quantile_forced_monotonicity_info(
        "series_1",
        pos,
        1735084800000,
        5.0,
        50.0,
        5.5,
    ));
    annos.add(new_histogram_quantile_forced_monotonicity_info(
        "series_1",
        pos,
        1703462400000,
        10.0,
        100.0,
        10.0,
    ));
    annos.add(new_histogram_quantile_forced_monotonicity_info(
        "series_1",
        pos,
        1733011200000,
        2.5,
        75.0,
        7.5,
    ));

    let (warnings, infos) = annos.as_strings("lorem ipsum dolor sit amet", 0, 0);
    // Go require.ElementsMatch: same elements in any order.
    assert_eq!(
        sorted(warnings),
        sorted(vec![
            "this is a non-annotation error".to_string(),
            "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 100 (1:4)".to_string(),
            "PromQL warning: ratio value should be between -1 and 1, got 1.2, capping to 123 (1:4)".to_string(),
            "PromQL warning: custom value set to 1.5, 4 instances with smallest 5 and biggest 20 (1:4)".to_string(),
        ])
    );
    assert_eq!(
        sorted(infos),
        sorted(vec![
            "PromQL info: ignored histogram in sum aggregation (1:4)".to_string(),
            r#"PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name "series_1", from buckets 2.5 to 100, with a max diff of 10, over 3 samples from 2023-12-25T00:00:00Z to 2024-12-25T00:00:00Z (1:4)"#.to_string(),
        ])
    );
}

/// Go `testCustomError`.
#[derive(Clone, Debug)]
struct TestCustomError {
    position_range: PositionRange,
    err: Error,
    min: Vec<f64>,
    max: Vec<f64>,
    count: i64,
}

impl AnnoError for TestCustomError {
    fn error(&self, query: &str) -> String {
        if query.is_empty() {
            return self.err.to_string();
        }
        format!(
            "{}, {} instances with smallest {} and biggest {} ({})",
            self.err,
            self.count + 1,
            g(self.min[0]),
            g(self.max[0]),
            self.position_range.start_pos_input(query, 0)
        )
    }

    fn unwrap(&self) -> Option<&Error> {
        Some(&self.err)
    }

    fn merge(self: Arc<Self>, other: &Error) -> Error {
        let Some(o) = other.annotation_as::<TestCustomError>() else {
            return Error::Annotation(self);
        };
        if self.err.to_string() != o.err.to_string()
            || self.min.len() != o.min.len()
            || self.max.len() != o.max.len()
        {
            return Error::Annotation(self);
        }
        // Go mutates the stored error in place; the merged copy replaces it.
        let mut o = o.clone();
        for (i, &a_min) in self.min.iter().enumerate() {
            if a_min < o.min[i] {
                o.min[i] = a_min;
            }
        }
        for (i, &a_max) in self.max.iter().enumerate() {
            if a_max > o.max[i] {
                o.max[i] = a_max;
            }
        }
        o.count += self.count + 1;
        Error::Annotation(Arc::new(o))
    }
}

fn new_test_custom_warning(q: f64, pos: PositionRange, smallest: f64, largest: f64) -> Error {
    // fmt.Errorf("%w: custom value set to", PromQLWarning), then fmt.Errorf("%w %g", testCustomWarning, q).
    let test_custom_warning = Error::wrapped(
        format!("{}: custom value set to", PROMQL_WARNING.msg),
        Error::Sentinel(&PROMQL_WARNING),
    );
    let err = Error::wrapped(
        format!("{} {}", test_custom_warning, g(q)),
        test_custom_warning,
    );
    Error::Annotation(Arc::new(TestCustomError {
        position_range: pos,
        err,
        min: vec![smallest],
        max: vec![largest],
        count: 0,
    }))
}

#[test]
fn test_start_time_overlap_warning() {
    let pos = PositionRange { start: 5, end: 10 };

    // Test single warning.
    let warning = new_start_time_overlap_warning("test_metric", pos);
    assert!(warning.is(&Error::Sentinel(&START_TIME_OVERLAP_WARNING)));
    assert!(warning.is(&Error::Sentinel(&PROMQL_WARNING)));

    // Test merging of warnings.
    let mut annos = Annotations::new();
    annos.add(new_start_time_overlap_warning("test_metric", pos));
    annos.add(new_start_time_overlap_warning("test_metric", pos));
    annos.add(new_start_time_overlap_warning("test_metric", pos));

    let (warnings, _) = annos.as_strings("test query", 0, 0);
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("sample has start time that overlaps with previous sample timestamp")
    );
    assert!(warnings[0].contains("test_metric"));
    // Each warning starts with count=1. After merging 3 warnings: 1+1+1=3 occurrences.
    assert!(warnings[0].contains("3 occurrences"), "{}", warnings[0]);
}

#[test]
fn test_start_time_overlap_warning_different_metrics() {
    let pos = PositionRange { start: 5, end: 10 };

    let mut annos = Annotations::new();
    annos.add(new_start_time_overlap_warning("metric_a", pos));
    annos.add(new_start_time_overlap_warning("metric_b", pos));
    annos.add(new_start_time_overlap_warning("metric_a", pos));

    let (warnings, _) = annos.as_strings("test query", 0, 0);
    assert_eq!(warnings.len(), 2);

    // Check that we have warnings for both metrics.
    let mut has_metric_a = false;
    let mut has_metric_b = false;
    for w in &warnings {
        if w.contains("metric_a") {
            has_metric_a = true;
            assert!(w.contains("2 occurrences"), "{}", w);
        }
        if w.contains("metric_b") {
            has_metric_b = true;
            assert!(!w.contains("occurrences"), "{}", w);
        }
    }
    assert!(has_metric_a);
    assert!(has_metric_b);
}
