//! Port of `promql/estimate_test.go`.
//!
//! Units: times are milliseconds, durations nanoseconds (see `promql::estimate`). Go's type assertion
//! `q.(storage.CostSamplingQueryable)` is the explicit `sampling` argument: a store passes itself, a wrapper
//! without that method passes `None`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::matcher::Matcher;
use promql::estimate::{CostEstimate, estimate_cost};
use promql::parser::{self, Parser};
use promql::promqltest::{self, TestStorage};
use promql::{Engine, EngineOpts, QueryOpts, preprocess_expr};
use storage::{
    Annotations, BoxChunkQuerier, BoxChunkSeriesSet, BoxMetaIterator, BoxQuerier, BoxSeriesSet, ChunkQuerier,
    ChunkQueryable, ChunkSeries, ChunkSeriesArc, ChunkSeriesSet, Context, CostSamplingQueryable, ERR_CANCELED,
    Error, LabelHints, LabelQuerier, LabelResult, Meta, MetaIterator, Querier, Queryable, QueryableFunc,
    SampleAndChunkQueryable, SelectHints, SeriesArc, SeriesEntry, SeriesSet,
};

const MS: i64 = 1_000_000;
const SECOND: i64 = 1000 * MS;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;

/// Go `time.Unix(s, 0)` in milliseconds.
fn unix(s: i64) -> i64 {
    s * 1000
}

fn estimate_test_parser() -> Parser {
    Parser::new(parser::Options::default())
}

fn loaded(input: &str) -> TestStorage {
    promqltest::loaded_storage(input).unwrap()
}

/// Go `newQueryCostEngine` (engine_test.go).
fn new_query_cost_engine() -> Engine {
    promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 1_000_000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_delayed_name_removal: true,
        enable_query_cost: true,
        enable_per_step_stats: true,
        parser: Some(Parser::new(promqltest::test_parser_opts())),
        ..Default::default()
    })
}

/// EstimateCost on a store that is its own cost sampling provider (Go passes the TSDB-backed store).
#[allow(clippy::too_many_arguments)]
fn est_store(
    store: &TestStorage,
    expr: &str,
    start: i64,
    end: i64,
    step: i64,
    lookback: i64,
    subq: i64,
    scrape: i64,
) -> (CostEstimate, Annotations, Option<Error>) {
    estimate_cost(
        &Context::background(),
        store,
        Some(store),
        &estimate_test_parser(),
        expr,
        start,
        end,
        step,
        lookback,
        subq,
        scrape,
    )
}

/// Runs a query and returns the engine's SamplesRead.
fn actual_samples_read(
    engine: &Engine,
    store: &TestStorage,
    opts: Option<&QueryOpts>,
    qs: &str,
    start: i64,
    end: i64,
    step: i64,
) -> i64 {
    let ctx = Context::background();
    let q: Arc<dyn Queryable> = Arc::new(store.clone());
    let mut qry = if step == 0 {
        engine.new_instant_query(&ctx, q, opts, qs, start).unwrap()
    } else {
        engine.new_range_query(&ctx, q, opts, qs, start, end, step / MS).unwrap()
    };
    let res = qry.exec(&ctx);
    assert!(res.err.is_none(), "{qs}: {:?}", res.err);
    let n = qry.stats().samples.unwrap().samples_read;
    qry.close();
    n
}

fn assert_in_delta(expected: i64, actual: i64, delta: f64, msg: &str) {
    let d = (expected as f64 - actual as f64).abs();
    assert!(d <= delta, "max difference between {expected} and {actual} allowed is {delta}, but difference was {d}: {msg}");
}

/// Go `queryableOnly`: a plain Queryable without chunk access or cost sampling.
struct QueryableOnly(TestStorage);

impl Queryable for QueryableOnly {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.0.querier(mint, maxt)
    }
}

#[test]
fn test_estimate_cost_instant_vector_selector() {
    let store = loaded(
        r#"
load 10s
  http_requests_total{job="a",instance="i1"} 0+1x5
  http_requests_total{job="a",instance="i2"} 0+1x5
  http_requests_total{job="b",instance="i1"} 0+1x5
  node_cpu{cpu="0"}                          0+2x5
  node_cpu{cpu="1"}                          0+2x5
"#,
    );
    let ts = unix(50);
    let (est, _, err) = est_store(&store, "http_requests_total", ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    // Three http_requests_total series match.
    assert_eq!(3, est.series_touched);
    // Samples are positive (at least one per series).
    assert!(est.samples_read >= est.series_touched);
}

#[test]
fn test_estimate_cost_range_selector_scales_with_window() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x100
  metric{a="2"} 0+1x100
"#,
    );
    let ts = unix(600);
    let scrape = 10 * SECOND;

    let (est5m, _, err) = est_store(&store, "rate(metric[5m])", ts, ts, 0, 5 * MINUTE, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est5m.series_touched);
    assert_in_delta(2 * (5 * 60 / 10), est5m.samples_read, 2.0, "");

    let (est10m, _, err) = est_store(&store, "rate(metric[10m])", ts, ts, 0, 5 * MINUTE, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est10m.series_touched);
    assert!(est10m.samples_read > est5m.samples_read);
    assert_in_delta(2 * (10 * 60 / 10), est10m.samples_read, 2.0, "");
}

#[test]
fn test_estimate_cost_range_query_matches_actual() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x2000
  metric{a="2"} 0+1x2000
"#,
    );
    let scrape = 10 * SECOND;
    let lookback = 5 * MINUTE;
    let engine = new_query_cost_engine();
    let opts = QueryOpts::new(true, Duration::from_nanos(lookback as u64), None);

    struct Case {
        name: String,
        query: String,
        start: i64,
        end: i64,
        step: i64,
        delta: f64,
    }
    let c = |name: &str, query: &str, start: i64, end: i64, step: i64, delta: f64| Case {
        name: name.to_string(),
        query: query.to_string(),
        start,
        end,
        step,
        delta,
    };
    let mut cases = vec![
        c("range-selector instant", "rate(metric[5m])", unix(5000), unix(5000), 0, 4.0),
        c("range-selector range query", "rate(metric[5m])", unix(4000), unix(4000 + 3600), MINUTE, 4.0),
        c("step wider than range", "rate(metric[5m])", unix(4000), unix(4000 + 3600), HOUR, 4.0),
        c("step shorter than scrape interval", "rate(metric[5m])", unix(4000), unix(7600), SECOND, 4.0),
        c("fractional scrapes per step", "rate(metric[5m])", unix(4000), unix(7600), 15 * SECOND, 4.0),
        c("instant selector range query", "metric", unix(4000), unix(4000 + 3600), MINUTE, 2.0),
    ];
    for query in [
        "metric smoothed",
        "rate(metric[100s] anchored)",
        "rate(metric[100s] smoothed)",
        "rate(metric[100s] smoothed @ 5000)",
        "rate(metric[100s] anchored offset 1m)",
        "sum_over_time((metric smoothed)[5m:1m])",
    ] {
        for (step, step_name) in [(0, "0s"), (MINUTE, "1m0s"), (15 * MINUTE, "15m0s")] {
            let (start, mut end) = (unix(4000), unix(7600));
            if step == 0 {
                end = start;
            }
            // Allow one inclusive-boundary sample per window and series.
            cases.push(c(&format!("{query}/{step_name}"), query, start, end, step, 12.0));
        }
    }

    for c in &cases {
        let actual = actual_samples_read(&engine, &store, Some(&opts), &c.query, c.start, c.end, c.step);
        let only = QueryableOnly(store.clone());
        let targets: [(&dyn Queryable, Option<&dyn CostSamplingQueryable>, &str); 2] =
            [(&store, Some(&store), "TestStorage"), (&only, None, "queryableOnly")];
        for (q, sampling, kind) in targets {
            let (est, _, err) = estimate_cost(
                &Context::background(),
                q,
                sampling,
                &estimate_test_parser(),
                &c.query,
                c.start,
                c.end,
                c.step,
                lookback,
                MINUTE,
                scrape,
            );
            assert!(err.is_none(), "{}: {err:?}", c.name);
            assert_in_delta(
                actual,
                est.samples_read,
                c.delta,
                &format!("{}: estimate {} vs actual {} with {}", c.name, est.samples_read, actual, kind),
            );
        }
    }
}

#[test]
fn test_estimate_cost_subquery_matches_actual() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x2000
  metric{a="2"} 0+1x2000
"#,
    );
    let scrape = 10 * SECOND;
    let lookback = 5 * MINUTE;
    let engine = promqltest::new_test_engine(
        true,
        Duration::from_nanos(lookback as u64),
        promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
    );

    let query = "sum_over_time(rate(metric[5m])[1h:1m])";
    let start = unix(4000);
    let end = unix(4000 + 1800);
    let step = MINUTE;

    let (est, _, err) = est_store(&store, query, start, end, step, lookback, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);

    let opts = QueryOpts::new(true, Duration::from_nanos(lookback as u64), None);
    let actual = actual_samples_read(&engine, &store, Some(&opts), query, start, end, step);
    assert_in_delta(
        actual,
        est.samples_read,
        16.0,
        &format!("subquery estimate {} vs actual {}", est.samples_read, actual),
    );

    let (plain, _, err) = est_store(&store, "rate(metric[5m])", start, end, step, lookback, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    assert!(est.samples_read > plain.samples_read);
}

#[test]
fn test_estimate_cost_saturates_samples_read() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x10
  metric{a="2"} 0+1x10
"#,
    );
    let start = unix(0);
    let end = unix(100000 * 86400); // 100000 days.
    let step = MS;
    let (est, _, err) = est_store(&store, "rate(metric[100000d])", start, end, step, 5 * MINUTE, MINUTE, MS);
    assert!(err.is_none(), "{err:?}");
    assert!(est.samples_read > 0);
    assert!(est.samples_read <= i64::MAX);
}

#[test]
fn test_estimate_cost_offset_and_at_modifier() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x100
  metric{a="2"} 0+1x100
"#,
    );
    let ts = unix(600);
    for q in ["metric offset 5m", "metric @ 300", "rate(metric[5m] offset 2m @ 400)"] {
        let (est, _, err) = est_store(&store, q, ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
        assert!(err.is_none(), "query {q:?} should not error: {err:?}");
        assert_eq!(2, est.series_touched, "query {q:?} should count both series");
    }
}

#[test]
fn test_estimate_cost_multi_selector_sums_series() {
    let store = loaded(
        r#"
load 10s
  a{x="1"} 0+1x10
  a{x="2"} 0+1x10
  a{x="3"} 0+1x10
  b{y="1"} 0+1x10
  b{y="2"} 0+1x10
"#,
    );
    let ts = unix(50);
    let (est, _, err) = est_store(&store, "a + b", ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    // SeriesTouched is the per-selector sum: 3 (a) + 2 (b) = 5.
    assert_eq!(5, est.series_touched);

    // a + a double-counts the same selector's series, by documented design.
    let (est_dup, _, err) = est_store(&store, "a + a", ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(6, est_dup.series_touched);
}

#[test]
fn test_estimate_cost_samples_per_series_fallback() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x10
  metric{a="2"} 0+1x10
"#,
    );
    let ts = unix(50);
    let only = QueryableOnly(store);
    let (est, _, err) = estimate_cost(
        &Context::background(),
        &only,
        None,
        &estimate_test_parser(),
        "rate(metric[5m])",
        ts,
        ts,
        0,
        5 * MINUTE,
        MINUTE,
        0,
    );
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);
    assert_eq!(est.series_touched, est.samples_read);
}

/// Go `s.CompactHead(tsdb.NewRangeHead(s.Head(), mint, maxt))`.
fn compact_head(s: &TestStorage, mint: i64, maxt: i64) {
    let rh = tsdb::head::RangeHead::new(s.head().clone(), mint, maxt);
    s.db().compact_head(&rh).expect("compact head");
}

#[test]
fn test_estimate_cost_sparse_series() {
    struct Case {
        name: &'static str,
        series: usize,
        gaps: bool,
        blocks: i64,
    }
    for tc in [
        Case { name: "single series", series: 1, gaps: false, blocks: 0 },
        Case { name: "more series than the sampling budget", series: 100, gaps: false, blocks: 0 },
        Case { name: "gaps across half the population", series: 100, gaps: true, blocks: 1 },
        Case { name: "gaps across multiple blocks", series: 100, gaps: true, blocks: 4 },
    ] {
        let mut input = String::from("load 15s\n");
        for i in 0..tc.series {
            input.push_str(&format!("metric{{instance=\"{i:03}\"}}"));
            let mut seconds = 0;
            while seconds < 14400 {
                if seconds % 300 != 0 || (tc.gaps && i % 2 == 0 && (7200..10800).contains(&seconds)) {
                    input.push_str(" _");
                } else {
                    input.push_str(&format!(" {seconds}"));
                }
                seconds += 15;
            }
            input.push('\n');
        }
        let s = loaded(&input);
        // Persist heterogeneous series in label order so the sampled mix is reproducible.
        for block in 0..tc.blocks {
            let width = 14400000 / tc.blocks;
            let mint = block * width;
            compact_head(&s, mint, mint + width - 1);
        }
        let engine = new_query_cost_engine();
        let (start, end) = (unix(7200), unix(14385));
        let expr = "sum_over_time(metric[10m])";
        let actual = actual_samples_read(&engine, &s, None, expr, start, end, MINUTE);
        let (est, annos, err) = est_store(&s, expr, start, end, MINUTE, 5 * MINUTE, MINUTE, 15 * SECOND);
        assert!(err.is_none(), "{}: {err:?}", tc.name);
        assert!(actual > 0, "{}", tc.name);
        if tc.blocks > 1 {
            assert_eq!(tc.series as i64 * 517, est.samples_read, "{}", tc.name);
            assert_eq!(1, annos.as_errors().len(), "{}", tc.name);
            assert!(annos.as_errors()[0].to_string().contains("bounded storage sampling is unavailable"));
        } else {
            assert!(annos.as_errors().is_empty(), "{}", tc.name);
            assert_in_delta(actual, est.samples_read, actual as f64 * 0.05, tc.name);
        }
    }
}

#[test]
fn test_estimate_cost_partial_density_sample() {
    for (name, series, points) in [("no complete series", 1usize, 12000i64), ("one complete and one partial series", 2, 4000)] {
        let mut input = String::from("load 10s\n");
        for i in 0..series {
            input.push_str(&format!("metric{{instance=\"{i}\"}} 0+1x{points}\n"));
        }
        let s = loaded(&input);
        let counter = ChunkMetaCounting::new(&s, &s, Some(&s));
        let ts = unix(points * 10);
        let expr = format!("metric[{}s]", points * 10);
        let (est, _, err) = estimate_cost(
            &Context::background(),
            &counter,
            Some(&counter),
            &estimate_test_parser(),
            &expr,
            ts,
            ts,
            0,
            5 * MINUTE,
            MINUTE,
            MS,
        );
        assert!(err.is_none(), "{name}: {err:?}");
        let metas = counter.metas();
        assert_eq!(1, metas.len(), "{name}");
        assert_eq!(50, metas[0], "{name}");
        let actual = actual_samples_read(&new_query_cost_engine(), &s, None, &expr, ts, ts, 0);
        // A partial series must not depress the average count of completed series. With none completed,
        // measured gaps supply the interval.
        assert_in_delta(actual, est.samples_read, series as f64, name);
    }
}

#[test]
fn test_estimate_cost_measures_interval_from_chunks() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x100
  metric{a="2"} 0+1x100
"#,
    );
    let ts = unix(600);
    let (est, _, err) = est_store(&store, "rate(metric[5m])", ts, ts, 0, 5 * MINUTE, MINUTE, 0);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);
    assert_in_delta(
        2 * (5 * 60 / 10),
        est.samples_read,
        2.0,
        &format!("measured-interval estimate {}", est.samples_read),
    );
}

#[test]
fn test_estimate_cost_samples_from_real_window_exact() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x5
  metric{a="2"} 0+1x5
"#,
    );
    let ts = unix(600);
    let (est, _, err) = est_store(&store, "metric[10m]", ts, ts, 0, 5 * MINUTE, MINUTE, MS);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);

    // The left boundary is excluded, so each series contributes five samples at 10,...,50s.
    assert_eq!(10, est.samples_read);
    let actual = actual_samples_read(&new_query_cost_engine(), &store, None, "metric[10m]", ts, ts, 0);
    assert_eq!(actual, est.samples_read);
}

#[test]
fn test_estimate_cost_defaults_lookback_delta() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x100
  metric{a="2"} 0+1x100
"#,
    );
    let ts = unix(600);
    let (est, _, err) = est_store(&store, "metric", ts, ts, 0, 0, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);
    assert_eq!(est.series_touched, est.samples_read);
}

#[test]
fn test_estimate_cost_histogram_matches_actual() {
    let store = loaded(
        r#"
load 10s
  nh{a="1"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x2000
  nh{a="2"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x2000
"#,
    );
    let scrape = 10 * SECOND;
    let lookback = 5 * MINUTE;
    let engine = promqltest::new_test_engine(
        true,
        Duration::from_nanos(lookback as u64),
        promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
    );
    let opts = QueryOpts::new(true, Duration::from_nanos(lookback as u64), None);

    for (name, query, start, end, step, delta) in [
        ("histogram range-selector instant", "rate(nh[5m])", unix(5000), unix(5000), 0, 28.0),
        ("histogram range query", "rate(nh[5m])", unix(4000), unix(4000 + 1800), MINUTE, 28.0),
        ("histogram count without buckets", "histogram_count(nh)", unix(5000), unix(5000), 0, 0.0),
        (
            "histogram sum of a range function without buckets",
            "histogram_sum(rate(nh[5m]))",
            unix(4000),
            unix(5800),
            MINUTE,
            24.0,
        ),
    ] {
        let (est, _, err) = est_store(&store, query, start, end, step, lookback, MINUTE, scrape);
        assert!(err.is_none(), "{name}: {err:?}");
        let actual = actual_samples_read(&engine, &store, Some(&opts), query, start, end, step);
        assert_in_delta(
            actual,
            est.samples_read,
            delta,
            &format!("{name}: estimate {} vs actual {}", est.samples_read, actual),
        );
        assert!(
            est.samples_read > actual / 2,
            "{name}: histogram estimate {} should reflect per-bucket cost",
            est.samples_read
        );
    }
}

#[test]
fn test_estimate_cost_float_still_accurate() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x2000
  metric{a="2"} 0+1x2000
"#,
    );
    let scrape = 10 * SECOND;
    let lookback = 5 * MINUTE;
    let engine = promqltest::new_test_engine(
        true,
        Duration::from_nanos(lookback as u64),
        promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
    );
    let query = "rate(metric[5m])";
    let start = unix(4000);
    let end = unix(4000 + 3600);
    let step = MINUTE;

    let (est, _, err) = est_store(&store, query, start, end, step, lookback, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    let opts = QueryOpts::new(true, Duration::from_nanos(lookback as u64), None);
    let actual = actual_samples_read(&engine, &store, Some(&opts), query, start, end, step);
    assert_in_delta(actual, est.samples_read, 4.0, &format!("float estimate {} vs actual {}", est.samples_read, actual));
}

#[test]
fn test_estimate_cost_histogram_fallback() {
    // 51 exceeds both histogramSampleLimit and chunkSampleLimit (50).
    const NUM_SERIES: i64 = 51;
    let mut sb = String::from("load 10s\n");
    for i in 0..NUM_SERIES {
        sb.push_str(&format!("  metric{{a=\"{i}\"}} 0+1x5\n"));
    }
    let store = loaded(&sb);
    let scrape = 10 * SECOND;
    let ts = unix(1000);
    let (est, _, err) = est_store(&store, "last_over_time(metric[30m])", ts, ts, 0, 5 * MINUTE, MINUTE, scrape);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(NUM_SERIES, est.series_touched);
    assert!(est.samples_read > 0);
}

// ---------------------------------------------------------------------------------------------------------------
// selectRecordingQueryable.

/// Go `selectRecordingQueryable`: records the hints of every select; no chunk access, no cost sampling.
struct SelectRecordingQueryable {
    q: TestStorage,
    hints: Arc<Mutex<Vec<SelectHints>>>,
}

impl Queryable for SelectRecordingQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let qr = self.q.querier(mint, maxt)?;
        Ok(Box::new(SelectRecordingQuerier { querier: qr, hints: self.hints.clone() }))
    }
}

struct SelectRecordingQuerier {
    querier: BoxQuerier,
    hints: Arc<Mutex<Vec<SelectHints>>>,
}

impl LabelQuerier for SelectRecordingQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.querier.close()
    }
}

impl Querier for SelectRecordingQuerier {
    fn select(&self, ctx: &Context, sorted: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        self.hints.lock().unwrap().push(hints.cloned().unwrap_or_default());
        self.querier.select(ctx, sorted, hints, m)
    }
}

#[test]
fn test_estimate_cost_plain_queryable_stays_index_only() {
    let store = loaded(
        r#"
load 10s
  nh{a="1"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x100
  nh{a="2"} {{schema:0 sum:5 count:4 buckets:[1 2 1]}}+{{schema:0 sum:5 count:4 buckets:[1 2 1]}}x100
"#,
    );
    let ts = unix(600);
    let recorder = SelectRecordingQueryable { q: store, hints: Arc::default() };
    let (est, _, err) = estimate_cost(
        &Context::background(),
        &recorder,
        None,
        &estimate_test_parser(),
        "nh[5m]",
        ts,
        ts,
        0,
        5 * MINUTE,
        MINUTE,
        10 * SECOND,
    );
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, est.series_touched);

    let hints = recorder.hints.lock().unwrap();
    assert_eq!(1, hints.len());
    assert_eq!("series", hints[0].func);

    assert_eq!(62, est.samples_read);
}

// ---------------------------------------------------------------------------------------------------------------
// chunkMetaCountingQueryable.

/// Go `chunkMetaCountingQueryable`: counts, per chunk querier it hands out, how many chunk metas its callers
/// walk. Its cost sampling source is a counting wrapper of the wrapped store's source that records into the
/// same owner (Go `owner`). The source is made once, because the Rust capability returns a reference.
struct ChunkMetaCounting<'a> {
    q: &'a dyn Queryable,
    cq: &'a dyn ChunkQueryable,
    metas: Arc<Mutex<Vec<Arc<AtomicUsize>>>>,
    source: Option<Box<ChunkMetaCounting<'a>>>,
    supported: bool,
}

impl<'a> ChunkMetaCounting<'a> {
    fn new(
        q: &'a dyn Queryable,
        cq: &'a dyn ChunkQueryable,
        provider: Option<&'a dyn CostSamplingQueryable>,
    ) -> ChunkMetaCounting<'a> {
        let metas: Arc<Mutex<Vec<Arc<AtomicUsize>>>> = Arc::default();
        let (source, supported) = match provider {
            Some(p) => p.cost_sampling_queryable(),
            None => (None, false),
        };
        let source = source.map(|s| {
            Box::new(ChunkMetaCounting { q: s, cq: s, metas: metas.clone(), source: None, supported: false })
        });
        ChunkMetaCounting { q, cq, metas, source, supported }
    }

    fn metas(&self) -> Vec<usize> {
        self.metas.lock().unwrap().iter().map(|n| n.load(Ordering::SeqCst)).collect()
    }
}

impl CostSamplingQueryable for ChunkMetaCounting<'_> {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        match &self.source {
            Some(s) => (Some(&**s), true),
            None => (None, self.supported),
        }
    }
}

impl Queryable for ChunkMetaCounting<'_> {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.q.querier(mint, maxt)
    }
}

impl ChunkQueryable for ChunkMetaCounting<'_> {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let qr = self.cq.chunk_querier(mint, maxt)?;
        let n = Arc::new(AtomicUsize::new(0));
        self.metas.lock().unwrap().push(n.clone());
        Ok(Box::new(ChunkMetaCountingQuerier { querier: qr, metas: n }))
    }
}

struct ChunkMetaCountingQuerier {
    querier: BoxChunkQuerier,
    metas: Arc<AtomicUsize>,
}

impl LabelQuerier for ChunkMetaCountingQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.querier.close()
    }
}

impl ChunkQuerier for ChunkMetaCountingQuerier {
    fn select(&self, ctx: &Context, sorted: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxChunkSeriesSet {
        Box::new(ChunkMetaCountingSeriesSet {
            set: self.querier.select(ctx, sorted, hints, m),
            metas: self.metas.clone(),
            cur: None,
        })
    }
}

struct ChunkMetaCountingSeriesSet {
    set: BoxChunkSeriesSet,
    metas: Arc<AtomicUsize>,
    cur: Option<ChunkSeriesArc>,
}

impl ChunkSeriesSet for ChunkMetaCountingSeriesSet {
    fn next(&mut self) -> bool {
        if !self.set.next() {
            return false;
        }
        self.cur = Some(Arc::new(ChunkMetaCountingSeries { series: self.set.at().clone(), metas: self.metas.clone() }));
        true
    }
    fn at(&self) -> &ChunkSeriesArc {
        self.cur.as_ref().unwrap()
    }
    fn err(&self) -> Option<Error> {
        self.set.err()
    }
    fn warnings(&self) -> Annotations {
        self.set.warnings()
    }
}

struct ChunkMetaCountingSeries {
    series: ChunkSeriesArc,
    metas: Arc<AtomicUsize>,
}

impl ChunkSeries for ChunkMetaCountingSeries {
    fn labels(&self) -> &Labels {
        self.series.labels()
    }
    // Ignores the iterator offered for reuse: it belongs to this wrapper, not to the wrapped series.
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        Box::new(ChunkMetaCountingIterator { it: self.series.iterator(None), metas: &self.metas })
    }
}

struct ChunkMetaCountingIterator<'a> {
    it: BoxMetaIterator<'a>,
    metas: &'a AtomicUsize,
}

impl MetaIterator for ChunkMetaCountingIterator<'_> {
    // Counts the chunks walked.
    fn next(&mut self) -> bool {
        if !self.it.next() {
            return false;
        }
        self.metas.fetch_add(1, Ordering::SeqCst);
        true
    }
    fn at(&self) -> &Meta {
        self.it.at()
    }
    fn err(&self) -> Option<Error> {
        self.it.err()
    }
}

#[test]
fn test_estimate_cost_chunk_sample_budget_bounds_chunks_examined() {
    const NUM_SERIES: i64 = 200;
    let mut sb = String::from("load 10s\n");
    for i in 0..NUM_SERIES {
        sb.push_str(&format!("  metric{{a=\"{i}\"}} 42\n"));
    }
    let store = loaded(&sb);
    let ts = unix(0);

    let counter = ChunkMetaCounting::new(&store, &store, Some(&store));
    let (est, _, err) = estimate_cost(
        &Context::background(),
        &counter,
        Some(&counter),
        &estimate_test_parser(),
        "metric[10m]",
        ts,
        ts,
        0,
        5 * MINUTE,
        MINUTE,
        10 * SECOND,
    );
    assert!(err.is_none(), "{err:?}");
    assert_eq!(NUM_SERIES, est.series_touched);

    // Only the density sampler reads chunk metadata, stopping at its budget.
    let metas = counter.metas();
    assert_eq!(1, metas.len());
    assert_eq!(50, metas[0]);
    assert_eq!(NUM_SERIES, est.samples_read);
}

#[test]
fn test_estimate_cost_sampling_uses_selector_window() {
    const NUM_SERIES: i64 = 51;
    let mut sb = String::from("load 10s\n");
    for i in 0..NUM_SERIES {
        // Sparse: a sample every 60s for t in [0s,540s]. Then dense: a sample every 10s for t in [600s,1200s].
        sb.push_str(&format!("  metric{{a=\"{i}\"}} {} 1+0x60\n", "1 _x5 ".repeat(10).trim()));
    }
    let store = loaded(&sb);
    let ts = unix(1200);
    for q in ["metric[5m] offset 10m", "metric[5m] @ 600"] {
        let (est, _, err) = est_store(&store, q, ts, ts, 0, 5 * MINUTE, MINUTE, MS);
        assert!(err.is_none(), "{q}: {err:?}");
        assert_eq!(NUM_SERIES, est.series_touched, "{q}");
        // The selector excludes 300s, leaving five points per series.
        assert_eq!(NUM_SERIES * 5, est.samples_read, "{q}");
    }
}

#[test]
fn test_estimate_cost_step_invariant_selector_read_once() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x2000
  metric{a="2"} 0+1x2000
"#,
    );
    let (start, end) = (unix(4000), unix(4000 + 3600));
    let step = MINUTE;

    let (invariant, _, err) = est_store(&store, "metric @ 5000", start, end, step, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(2, invariant.samples_read);

    let (per_step, _, err) = est_store(&store, "metric", start, end, step, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(122, per_step.samples_read);
}

#[test]
fn test_estimate_cost_resolves_duration_expressions() {
    let store = loaded(
        r#"
load 10s
  metric{a="1"} 0+1x2000
  metric{a="2"} 0+1x2000
"#,
    );
    let ts = unix(5000);
    let (expr, _, err) = est_store(&store, "rate(metric[2m*2])", ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    let (literal, _, err) = est_store(&store, "rate(metric[4m])", ts, ts, 0, 5 * MINUTE, MINUTE, 10 * SECOND);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(literal.samples_read, expr.samples_read);
    assert!(expr.samples_read > 0);
}

#[test]
fn test_estimate_cost_info_incomplete() {
    let s = loaded(
        r#"
load 10s
  metric{instance="a",job="1"} 1+0x100
  target_info{instance="a",job="1",data="one"} 1+0x100
  unrelated{data="one"} 1+0x100
"#,
    );
    let p = Parser::new(promqltest::test_parser_opts());
    for query in ["info(metric)", r#"info(metric, {data="one"})"#, "info(info(metric))"] {
        let ts = unix(1000);
        let (est, warnings, err) = estimate_cost(
            &Context::background(),
            &s,
            Some(&s),
            &p,
            query,
            ts,
            ts,
            0,
            5 * MINUTE,
            MINUTE,
            10 * SECOND,
        );
        assert!(err.is_none(), "{query}: {err:?}");
        assert_eq!(1, est.series_touched, "{query}: info label selectors must not be counted as standalone reads");
        assert_eq!(1, warnings.len(), "{query}");
        assert!(warnings.as_errors()[0].to_string().contains("estimate is incomplete: info()"), "{query}");
    }
}

#[test]
fn test_estimate_cost_evaluation_grids() {
    let s = loaded(
        r#"
load 10s
  metric{instance="a"} 0+1x2000
  metric{instance="b"} 0+1x2000
"#,
    );
    let engine = new_query_cost_engine();
    for query in [
        "sum_over_time(metric[5m:1m])",
        "quantile_over_time(time() / 10000, metric[20s:10s] @ 200)",
        "quantile_over_time(time() / 10000, metric[20s:10s] @ 200 offset 1m)",
        "quantile_over_time(time() / 10000, metric[20s:10s] @ 200 offset -1m)",
        "quantile_over_time(time() / 10000, metric[20s:10s] @ 10000)",
        "quantile_over_time(time() / 10000, metric[20s:] @ start())",
        "quantile_over_time(time() / 10000, metric[20s:10s] @ end())",
        "sum_over_time(quantile_over_time(time() / 10000, metric[20s:10s] @ 200)[20s:10s])",
        "sum_over_time(metric[1s:1m])",
        "sum_over_time(sum_over_time(metric[20m:1m] @ 3000)[5m:1m])",
        "sum_over_time(sum_over_time(metric[20m:1m] offset 25s)[5m:1m] @ 3000)",
        "sum_over_time(metric[5m:1m] @ 1000)",
        "sum_over_time((metric @ 1000)[5m:1m])",
        "sum_over_time(sum_over_time(metric[20m:1m])[5m:1m])",
        "sum_over_time(metric[5m:1m] offset 25s)",
        "sum_over_time(metric[5m:1m] offset -25s)",
        "sum_over_time(metric[5m:] @ start())",
        "sum_over_time(metric[5m:1m] @ end())",
        "sum_over_time(rate(metric[2m])[5m:1m])",
    ] {
        for step in [0, 30 * SECOND] {
            let (start, mut end) = (unix(4000), unix(4000));
            if step > 0 {
                end = unix(7607);
            }
            let (est, _, err) = est_store(&s, query, start, end, step, 5 * MINUTE, MINUTE, 10 * SECOND);
            assert!(err.is_none(), "{query}/{step}: {err:?}");
            let actual = actual_samples_read(&engine, &s, None, query, start, end, step);
            assert_in_delta(actual, est.samples_read, 4.0, &format!("{query}/{step}"));
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// sampleErrorQueryable: injects an iterator error only into the point-size sampling path.

struct SampleErrorQueryable<'a> {
    inner: &'a dyn SampleAndChunkQueryable,
    err: Error,
    source: Option<Box<SampleErrorQueryable<'a>>>,
    supported: bool,
}

impl<'a> SampleErrorQueryable<'a> {
    fn new(
        inner: &'a dyn SampleAndChunkQueryable,
        provider: &'a dyn CostSamplingQueryable,
        err: Error,
    ) -> SampleErrorQueryable<'a> {
        let (source, supported) = provider.cost_sampling_queryable();
        let source = source
            .map(|s| Box::new(SampleErrorQueryable { inner: s, err: err.clone(), source: None, supported: false }));
        SampleErrorQueryable { inner, err, source, supported }
    }
}

impl CostSamplingQueryable for SampleErrorQueryable<'_> {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        match &self.source {
            Some(s) => (Some(&**s), true),
            None => (None, self.supported),
        }
    }
}

impl Queryable for SampleErrorQueryable<'_> {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let inner = self.inner.querier(mint, maxt)?;
        Ok(Box::new(SampleErrorQuerier { querier: inner, err: self.err.clone() }))
    }
}

impl ChunkQueryable for SampleErrorQueryable<'_> {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        self.inner.chunk_querier(mint, maxt)
    }
}

struct SampleErrorQuerier {
    querier: BoxQuerier,
    err: Error,
}

impl LabelQuerier for SampleErrorQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_values(ctx, name, hints, m)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.querier.label_names(ctx, hints, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.querier.close()
    }
}

impl Querier for SampleErrorQuerier {
    fn select(&self, ctx: &Context, sorted: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        let set = self.querier.select(ctx, sorted, hints, m);
        if hints.is_some_and(|h| h.func == "series") {
            return set;
        }
        Box::new(SampleErrorSeriesSet { set, err: storage::to_chunk_error(&self.err), cur: None })
    }
}

struct SampleErrorSeriesSet {
    set: BoxSeriesSet,
    err: chunkenc::Error,
    cur: Option<SeriesArc>,
}

impl SeriesSet for SampleErrorSeriesSet {
    fn next(&mut self) -> bool {
        if !self.set.next() {
            return false;
        }
        self.cur = Some(Arc::new(SeriesEntry {
            lset: self.set.at().labels().clone(),
            data: self.err.clone(),
            sample_iterator_fn: |err, _| AnyIterator::Other(Box::new(SampleErrorIterator(err.clone()))),
        }));
        true
    }
    fn at(&self) -> &SeriesArc {
        self.cur.as_ref().unwrap()
    }
    fn err(&self) -> Option<Error> {
        self.set.err()
    }
    fn warnings(&self) -> Annotations {
        self.set.warnings()
    }
}

/// Go `sampleErrorIterator`: a nop iterator with an error.
struct SampleErrorIterator(chunkenc::Error);

impl ChunkIterator for SampleErrorIterator {
    fn next(&mut self) -> ValueType {
        ValueType::None
    }
    fn seek(&mut self, _t: i64) -> ValueType {
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        (i64::MIN, 0.0)
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        (i64::MIN, Histogram::default())
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (i64::MIN, FloatHistogram::default())
    }
    fn at_t(&self) -> i64 {
        i64::MIN
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        Some(&self.0)
    }
}

#[test]
fn test_estimate_cost_sampling_errors() {
    let s = loaded("load 10s\n  metric 1+1x10\n");
    for want in [Error::msg("corrupt sample data"), ERR_CANCELED] {
        let q = SampleErrorQueryable::new(&s, &s, want.clone());
        let ts = unix(100);
        let (_, _, err) = estimate_cost(
            &Context::background(),
            &q,
            Some(&q),
            &estimate_test_parser(),
            "metric",
            ts,
            ts,
            0,
            5 * MINUTE,
            MINUTE,
            10 * SECOND,
        );
        let err = err.expect("an error");
        if want.is(&ERR_CANCELED) {
            assert!(err.is(&want), "{want}: got {err}");
        } else {
            // adapted: sample iterators carry `chunkenc::Error`, which cannot hold a run-time `errors.New` value,
            // so its identity ends at that boundary; the same error text must come back.
            assert_eq!(want.to_string(), err.to_string());
        }
    }
}

#[test]
fn test_estimate_cost_invalid_query() {
    struct Case {
        name: &'static str,
        expr: &'static str,
        want_error: &'static str,
        step: i64,
        parse_error: bool,
        preprocess_error: bool,
    }
    for tc in [
        Case { name: "syntax", expr: "broken(", want_error: "parse error", step: 0, parse_error: true, preprocess_error: false },
        Case {
            name: "preprocessing",
            expr: "rate(metric[1s-2s])",
            want_error: "duration must be greater than 0",
            step: 0,
            parse_error: false,
            preprocess_error: true,
        },
        Case {
            name: "range result type",
            expr: "metric[5m]",
            want_error: "invalid expression type",
            step: SECOND,
            parse_error: false,
            preprocess_error: false,
        },
    ] {
        let (start, mut end) = (unix(100), unix(100));
        if tc.step > 0 {
            end += 60_000;
        }
        let parsed = estimate_test_parser().parse_expr(tc.expr);
        if tc.parse_error {
            assert!(parsed.is_err(), "{}", tc.name);
        } else {
            assert!(parsed.is_ok(), "{}", tc.name);
        }
        if tc.preprocess_error {
            let err = preprocess_expr(parsed.unwrap(), start, end, tc.step / MS).unwrap_err();
            assert!(err.contains(tc.want_error), "{}: {err}", tc.name);
        }

        let opens = Arc::new(AtomicUsize::new(0));
        let o = opens.clone();
        let q = QueryableFunc(move |_: i64, _: i64| -> Result<BoxQuerier, Error> {
            o.fetch_add(1, Ordering::SeqCst);
            Ok(storage::noop_querier())
        });
        let (estimate, _, err) = estimate_cost(
            &Context::background(),
            &q,
            None,
            &estimate_test_parser(),
            tc.expr,
            start,
            end,
            tc.step,
            MINUTE,
            MINUTE,
            SECOND,
        );
        let err = err.expect("an error");
        assert!(err.to_string().contains(tc.want_error), "{}: {err}", tc.name);
        if tc.parse_error {
            match err.downcast_ref::<parser::Error>() {
                Some(parser::Error::Parse(errs)) => assert!(!errs.0.is_empty(), "{}", tc.name),
                other => panic!("{}: not a parse error: {other:?}", tc.name),
            }
        }
        assert_eq!(CostEstimate::default(), estimate, "{}", tc.name);
        assert_eq!(0, opens.load(Ordering::SeqCst), "{}: invalid queries must not open storage", tc.name);
    }
}

