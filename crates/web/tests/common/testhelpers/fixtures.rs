//! Port of `web/api/testhelpers/fixtures.go`: test fixture data.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use model::histogram::{FloatHistogram, Span};
use model::labels::{Labels, from_strings};
use promql::{FPoint, HPoint};
use storage::SeriesArc;

use web::api::RuleGroupView;
use web::api::adapters::GroupView;

use super::mocks::{FakeHistogramSeries, FakeSeries};

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
pub fn fixture_rule_groups() -> Vec<Arc<dyn RuleGroupView>> {
    let parse = crate::common::doubles::parse_rule_expr;
    // Create a simple recording rule.
    let recording_rule: rules::RuleRef =
        Arc::new(rules::RecordingRule::new("job:up:sum", parse("up == 1"), Labels::empty()));

    // Create a simple alerting rule.
    let alerting_rule: rules::RuleRef = Arc::new(rules::AlertingRule::new(
        "InstanceDown",
        parse("up == 0"),
        Duration::from_secs(60),
        Duration::ZERO,
        from_strings(&["severity", "critical"]),
        Labels::empty(),
        Labels::empty(),
        "Instance {{ $labels.instance }} is down",
        true,
        rules::log::Logger::nop(),
    ));

    // Create a rule group.
    let group = rules::Group::new(rules::GroupOptions {
        name: "example".to_string(),
        file: "example.rules".to_string(),
        interval: Duration::from_secs(60),
        rules: vec![recording_rule, alerting_rule],
        ..Default::default()
    });

    vec![Arc::new(GroupView::new(Arc::new(group)))]
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
