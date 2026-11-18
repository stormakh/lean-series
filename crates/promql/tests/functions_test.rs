//! Port of `promql/functions_test.go`.
//!
//! adapted: Go runs these tests against `teststorage` (a real TSDB). The test storage here is a minimal in-memory
//! queryable over fixed samples (with start timestamps), which is what the queries read from the TSDB.

use std::sync::Arc;
use std::time::Duration;

use model::labels::{self, Labels, Matcher, METRIC_NAME};
use promql::functions::FUNCTION_CALLS;
use promql::parser::{self, Options, Parser};
use promql::{Engine, EngineOpts};
use storage::tsdbutil::generate_test_float_histogram;
use storage::{
    new_list_series, Annotations, BoxQuerier, BoxSeriesSet, Context, Error, MockQuerier, Queryable, Sample, SeriesArc,
    SeriesSet,
};

/// A minimal in-memory storage: the series are sorted by labels.
struct MemStorage {
    series: Vec<SeriesArc>,
}

impl MemStorage {
    fn new(mut series: Vec<(Labels, Vec<Sample>)>) -> Arc<MemStorage> {
        series.sort_by(|a, b| a.0.cmp(&b.0));
        Arc::new(MemStorage { series: series.into_iter().map(|(l, s)| new_list_series(l, s)).collect() })
    }
}

struct VecSeriesSet {
    series: Vec<SeriesArc>,
    i: isize,
}

impl SeriesSet for VecSeriesSet {
    fn next(&mut self) -> bool {
        self.i += 1;
        (self.i as usize) < self.series.len()
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.i as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

impl Queryable for MemStorage {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        let series = self.series.clone();
        Ok(Box::new(MockQuerier {
            select_mock_function: Box::new(move |_, _, matchers: &[Matcher]| -> BoxSeriesSet {
                let selected = series
                    .iter()
                    .filter(|s| matchers.iter().all(|m| m.matches(s.labels().get(&m.name))))
                    .cloned()
                    .collect();
                Box::new(VecSeriesSet { series: selected, i: -1 })
            }),
        }))
    }
}

/// Go `promqltest.TestParserOpts`.
fn test_parser() -> Parser {
    Parser::new(Options {
        enable_experimental_functions: true,
        enable_binop_fill_modifiers: true,
        enable_unquoted_utf8_names: true,
        ..Default::default()
    })
}

/// Go `promqltest.NewTestEngine(t, enablePerStepStats, lookbackDelta, maxSamples)`.
fn new_test_engine(enable_per_step_stats: bool, lookback_delta: Duration, max_samples: usize) -> Engine {
    Engine::new(EngineOpts {
        max_samples,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats,
        lookback_delta,
        enable_delayed_name_removal: true,
        use_start_timestamps: true,
        parser: Some(test_parser()),
        ..Default::default()
    })
}

#[test]
fn test_deriv() {
    // https://github.com/prometheus/prometheus/issues/2674#issuecomment-315439393
    // This requires more precision than the usual test system offers, so we test it by hand.
    let engine = Engine::new(EngineOpts { max_samples: 10000, timeout: Duration::from_secs(10), ..Default::default() });

    let metric = labels::from_strings(&["__name__", "foo"]);
    let start: i64 = 1493712816939;
    let interval: i64 = 30 * 1000;
    // Introduce some timestamp jitter to test 0 slope case.
    // https://github.com/prometheus/prometheus/issues/7180
    let mut samples = Vec::new();
    for i in 0..15i64 {
        let jitter = 12 * i % 2;
        samples.push(Sample::float(0, start + interval * i + jitter, 1.0));
    }
    let storage = MemStorage::new(vec![(metric, samples)]);

    let ctx = Context::background();
    let mut query = engine.new_instant_query(&ctx, storage, None, "deriv(foo[30m])", 1493712846939).unwrap();
    let result = query.exec(&ctx);
    assert!(result.err.is_none(), "{:?}", result.err);

    let vec = result.vector().unwrap();
    assert_eq!(1, vec.len(), "Expected 1 result, got {}", vec.len());
    assert_eq!(0.0, vec[0].f, "Expected 0.0 as value, got {}", vec[0].f);
}

#[test]
fn test_ts_of_last_over_time_before_epoch() {
    let ctx = Context::background();
    let storage = MemStorage::new(vec![
        (labels::from_strings(&["__name__", "float_metric"]), vec![Sample::float(0, -1000, 1.0)]),
        (
            labels::from_strings(&["__name__", "histogram_metric"]),
            vec![Sample::float_histogram(0, -1000, generate_test_float_histogram(0))],
        ),
    ]);

    let engine = new_test_engine(false, Duration::ZERO, 10000);
    for metric in ["float_metric", "histogram_metric"] {
        let q = format!("ts_of_last_over_time({}[1s])", metric);
        let mut query = engine.new_instant_query(&ctx, storage.clone(), None, &q, -1000).unwrap();
        let result = query.exec(&ctx);
        assert!(result.err.is_none(), "{:?}", result.err);
        let vector = result.vector().unwrap();
        assert_eq!(1, vector.len(), "{}", metric);
        assert_eq!(-1.0, vector[0].f, "{}", metric);
    }
}

#[test]
fn test_function_list() {
    // Test that Functions and parser.Functions list the same functions.
    for (name, _) in FUNCTION_CALLS.iter() {
        assert!(
            parser::functions().contains_key(*name),
            "function {} exists in promql package, but not in parser package",
            name
        );
    }
    for name in parser::functions().keys() {
        assert!(
            FUNCTION_CALLS.iter().any(|(n, _)| n == name),
            "function {} exists in parser package, but not in promql package",
            name
        );
    }
}

#[test]
fn test_start_timestamp_output_when_use_start_timestamp_is_disabled() {
    let mut series = Vec::new();
    for i in 0..5i64 {
        let input_label = labels::from_strings(&[METRIC_NAME, "some_series", "case", &i.to_string()]);
        let ts = i * 1000;
        let st = ts - i * 100;
        series.push((input_label, vec![Sample::float(st, ts, 0.0)]));
    }
    let storage = MemStorage::new(series);

    let tests: [(&str, bool, Vec<f64>); 2] = [
        ("use-start-timestamps enabled", true, vec![0.0, 0.9, 1.8, 2.7, 3.6]),
        ("use-start-timestamps disabled", false, vec![]),
    ];

    for (name, use_start_timestamps, expected) in tests {
        let engine = Engine::new(EngineOpts {
            max_samples: 10000,
            timeout: Duration::from_secs(10),
            use_start_timestamps,
            parser: Some(test_parser()),
            ..Default::default()
        });
        let ctx = Context::background();
        let mut query = engine.new_instant_query(&ctx, storage.clone(), None, "start_timestamp(some_series)", 5000).unwrap();
        let result = query.exec(&ctx);
        assert!(result.err.is_none(), "{}: {:?}", name, result.err);

        let vec = result.vector().unwrap();
        assert_eq!(expected.len(), vec.len(), "{}: Unexpected number of results, got {}", name, vec.len());
        for i in 0..expected.len() {
            assert_eq!(expected[i], vec[i].f, "{}: At index {}", name, i);
        }
    }
}
