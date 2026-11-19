//! Port of `web/api/testhelpers/fixtures.go`: test fixture data.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use model::histogram::{FloatHistogram, Span};
use model::labels::{Labels, from_strings};
use promql::{FPoint, HPoint};
use storage::SeriesArc;
use web::api::{AlertingRuleView, RuleGroupView, RuleRef, RuleView};
use web::gotime::{MINUTE, Time};

use super::mocks::{FakeHistogramSeries, FakeSeries};
use crate::common::doubles::{MockGroup, MockRule};

/// Go `time.Now().UnixMilli()`.
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn fake(labels: &[&str], samples: Vec<FPoint>) -> SeriesArc {
    Arc::new(FakeSeries {
        labels: from_strings(labels),
        samples,
    })
}

/// Go `FixtureSeries`: a simple series with the "up" metric.
pub fn fixture_series() -> Vec<SeriesArc> {
    // Use timestamps relative to "now" so queries work.
    let now = now_millis();
    vec![fake(
        &[
            "__name__",
            "up",
            "job",
            "prometheus",
            "instance",
            "localhost:9090",
        ],
        vec![
            FPoint {
                t: now - 120000,
                f: 1.0,
            },
            FPoint {
                t: now - 60000,
                f: 1.0,
            },
            FPoint { t: now, f: 1.0 },
        ],
    )]
}

/// Go `FixtureMultipleSeries`.
pub fn fixture_multiple_series() -> Vec<SeriesArc> {
    // Use timestamps relative to "now" so queries work.
    let now = now_millis();
    vec![
        fake(
            &[
                "__name__",
                "up",
                "job",
                "prometheus",
                "instance",
                "localhost:9090",
            ],
            vec![
                FPoint {
                    t: now - 60000,
                    f: 1.0,
                },
                FPoint { t: now, f: 1.0 },
            ],
        ),
        fake(
            &[
                "__name__",
                "up",
                "job",
                "node",
                "instance",
                "localhost:9100",
            ],
            vec![
                FPoint {
                    t: now - 60000,
                    f: 1.0,
                },
                FPoint { t: now, f: 0.0 },
            ],
        ),
        fake(
            &[
                "__name__",
                "http_requests_total",
                "job",
                "api",
                "instance",
                "localhost:8080",
            ],
            vec![
                FPoint {
                    t: now - 60000,
                    f: 100.0,
                },
                FPoint { t: now, f: 150.0 },
            ],
        ),
    ]
}

/// Go `FixtureRuleGroups`: one group "example" with a recording rule and an alerting rule.
///
/// adapted: Go builds real `rules.RecordingRule`, `rules.AlertingRule` and `rules.Group` objects (the rules crate
/// is not available here). The doubles expose what those never-evaluated objects expose: health "unknown", no
/// error, zero evaluation time and timestamp, no active alerts (state "inactive"), limit 0.
pub fn fixture_rule_groups() -> Vec<Arc<dyn RuleGroupView>> {
    let unevaluated = |name: &str, query: &str, labels: Labels, hold| MockRule {
        name: name.to_string(),
        query: query.to_string(),
        labels,
        annotations: Labels::empty(),
        hold,
        health: "unknown".to_string(),
        eval_duration: 0,
        eval_timestamp: Time::ZERO,
        alerts: Vec::new(),
    };
    // A simple recording rule.
    let recording_rule: Arc<dyn RuleView> =
        Arc::new(unevaluated("job:up:sum", "up == 1", Labels::empty(), 0));
    // A simple alerting rule.
    let alerting_rule: Arc<dyn AlertingRuleView> = Arc::new(unevaluated(
        "InstanceDown",
        "up == 0",
        from_strings(&["severity", "critical"]),
        MINUTE,
    ));
    // A rule group.
    let group = MockGroup {
        name: "example".to_string(),
        file: "example.rules".to_string(),
        interval: MINUTE,
        eval_time: 0,
        last_eval: Time::ZERO,
        rules: vec![
            RuleRef::Recording(recording_rule),
            RuleRef::Alerting(alerting_rule),
        ],
    };
    vec![Arc::new(group)]
}

/// Go `FixtureEmptyRuleGroups`.
pub fn fixture_empty_rule_groups() -> Vec<Arc<dyn RuleGroupView>> {
    Vec::new()
}

/// Go `FixtureSingleSeries`.
pub fn fixture_single_series(metric_name: &str, value: f64) -> Vec<SeriesArc> {
    vec![fake(
        &["__name__", metric_name],
        vec![FPoint { t: 0, f: value }],
    )]
}

/// Go `FixtureHistogramSeries`: a series with native histogram data.
pub fn fixture_histogram_series() -> Vec<SeriesArc> {
    // Use timestamps relative to "now" so queries work.
    let now = now_millis();
    let h = |zero_count: f64,
             count: f64,
             sum: f64,
             positive_buckets: Vec<f64>,
             negative_buckets: Vec<f64>| {
        Arc::new(FloatHistogram {
            schema: 2,
            zero_threshold: 0.001,
            zero_count,
            count,
            sum,
            positive_spans: vec![
                Span {
                    offset: 0,
                    length: 2,
                },
                Span {
                    offset: 1,
                    length: 2,
                },
            ],
            negative_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            positive_buckets,
            negative_buckets,
            ..Default::default()
        })
    };
    vec![Arc::new(FakeHistogramSeries {
        labels: from_strings(&[
            "__name__",
            "test_histogram",
            "job",
            "prometheus",
            "instance",
            "localhost:9090",
        ]),
        histograms: vec![
            HPoint {
                t: now - 60000,
                h: h(5.0, 50.0, 100.0, vec![5.0, 10.0, 8.0, 7.0], vec![3.0]),
            },
            HPoint {
                t: now,
                h: h(8.0, 60.0, 120.0, vec![6.0, 12.0, 10.0, 9.0], vec![4.0]),
            },
        ],
    })]
}
