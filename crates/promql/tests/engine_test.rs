//! Port of `promql/engine_test.go`.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use model::labels::Matcher;
use promql::query_logger::ActiveQueryTracker;
use promql::{Engine, EngineOpts, ErrStorage, PromqlError, QueryTracker};
use storage::{
    Annotations, BoxQuerier, BoxSeriesSet, Context, ERR_CANCELED, ERR_DEADLINE_EXCEEDED, Error, LabelHints,
    LabelQuerier, LabelResult, Querier, QueryableFunc, SelectHints,
};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("promql-engine-test-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Go test helper `contextDone`.
fn context_done(ctx: &Context, env: &str) -> Result<(), Error> {
    match ctx.err() {
        None => Ok(()),
        Some(e) if e.is(&ERR_CANCELED) => Err(PromqlError::QueryCanceled(env.to_string()).err()),
        Some(e) if e.is(&ERR_DEADLINE_EXCEEDED) => Err(PromqlError::QueryTimeout(env.to_string()).err()),
        Some(e) => Err(e),
    }
}

fn engine_with(opts: EngineOpts) -> Engine {
    Engine::new(opts)
}

#[test]
fn test_query_concurrency() {
    let max_concurrency = 10;
    let dir = temp_dir("concurrency");
    let tracker: Arc<dyn QueryTracker> = Arc::new(ActiveQueryTracker::new(&dir, max_concurrency).unwrap());
    let engine = engine_with(EngineOpts {
        max_samples: 10,
        timeout: Duration::from_secs(100),
        active_query_tracker: Some(tracker),
        ..Default::default()
    });
    let (processing_tx, processing_rx) = mpsc::channel::<()>();
    let (block_tx, block_rx) = mpsc::channel::<()>();
    let block_rx = Arc::new(Mutex::new(block_rx));
    let processing_tx = Arc::new(Mutex::new(processing_tx));
    let make_query = || {
        let ptx = processing_tx.clone();
        let brx = block_rx.clone();
        engine.new_test_query(move |_| {
            let _ = ptx.lock().unwrap().send(());
            let _ = brx.lock().unwrap().recv();
            Ok(())
        })
    };
    let mut handles = Vec::new();
    for _ in 0..max_concurrency {
        let mut q = make_query();
        handles.push(thread::spawn(move || {
            q.exec(&Context::background());
        }));
        processing_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("Query within concurrency threshold not being executed");
    }
    let mut q = make_query();
    handles.push(thread::spawn(move || {
        q.exec(&Context::background());
    }));
    assert!(
        processing_rx.recv_timeout(Duration::from_millis(20)).is_err(),
        "Query above concurrency threshold being executed"
    );
    // Terminate a running query.
    block_tx.send(()).unwrap();
    processing_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("Query within concurrency threshold not being executed");
    // Terminate remaining queries.
    for _ in 0..max_concurrency {
        block_tx.send(()).unwrap();
    }
    for h in handles {
        h.join().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_query_timeout() {
    let engine = engine_with(EngineOpts { max_samples: 10, timeout: Duration::from_millis(5), ..Default::default() });
    let mut query = engine.new_test_query(|ctx| {
        thread::sleep(Duration::from_millis(100));
        context_done(ctx, "test statement execution")
    });
    let res = query.exec(&Context::background());
    let err = res.err.expect("expected timeout error but got none");
    assert!(
        matches!(PromqlError::find(&err), Some(PromqlError::QueryTimeout(_))),
        "expected timeout error but got: {}",
        err
    );
}

#[test]
fn test_query_cancel() {
    let engine = engine_with(EngineOpts { max_samples: 10, timeout: Duration::from_secs(10), ..Default::default() });
    let (processing_tx, processing_rx) = mpsc::channel::<()>();
    let (block_tx, block_rx) = mpsc::channel::<()>();
    let ptx = processing_tx.clone();
    let brx = Mutex::new(block_rx);
    let mut query1 = engine.new_test_query(move |ctx| {
        ptx.send(()).unwrap();
        brx.lock().unwrap().recv().unwrap();
        context_done(ctx, "test statement execution")
    });
    let canceler = query1.canceler();
    let h = thread::spawn(move || {
        let res = query1.exec(&Context::background());
        processing_tx.send(()).unwrap();
        res
    });
    processing_rx.recv().unwrap();
    canceler.cancel();
    block_tx.send(()).unwrap();
    processing_rx.recv().unwrap();
    let res = h.join().unwrap();
    let err = res.err.expect("expected cancellation error for query1 but got none");
    assert_eq!(
        Some(&PromqlError::QueryCanceled("test statement execution".to_string())),
        PromqlError::find(&err)
    );
    assert_eq!("query was canceled in test statement execution", err.to_string());

    // Canceling a query before starting it must have no effect.
    let mut query2 = engine.new_test_query(|ctx| context_done(ctx, "test statement execution"));
    query2.cancel();
    let res = query2.exec(&Context::background());
    assert!(res.err.is_none());
}

struct ErrQuerier {
    err: Error,
}

impl LabelQuerier for ErrQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for ErrQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        storage::err_series_set(self.err.clone())
    }
}

#[test]
fn test_query_error() {
    let engine = engine_with(EngineOpts { max_samples: 10, timeout: Duration::from_secs(10), ..Default::default() });
    let err_storage = Error::other(ErrStorage(Error::msg("storage error")));
    let es = err_storage.clone();
    let queryable = Arc::new(QueryableFunc(move |_, _| -> Result<BoxQuerier, Error> {
        Ok(Box::new(ErrQuerier { err: es.clone() }))
    }));
    let ctx = Context::background();
    let mut vq = engine.new_instant_query(&ctx, queryable.clone(), None, "foo", 1000).unwrap();
    let res = vq.exec(&ctx);
    let err = res.err.expect("expected error on failed select but got none");
    assert!(err.is(&err_storage), "expected error doesn't match: {}", err);

    let mut mq = engine.new_instant_query(&ctx, queryable, None, "foo[1m]", 1000).unwrap();
    let res = mq.exec(&ctx);
    let err = res.err.expect("expected error on failed select but got none");
    assert!(err.is(&err_storage), "expected error doesn't match: {}", err);
}

fn noop_queryable() -> Arc<dyn storage::Queryable> {
    Arc::new(QueryableFunc(|_, _| -> Result<BoxQuerier, Error> { Ok(storage::noop_querier()) }))
}

#[test]
fn test_engine_shutdown() {
    let engine = engine_with(EngineOpts { max_samples: 10, timeout: Duration::from_secs(10), ..Default::default() });
    let (ctx, cancel_ctx) = Context::background().with_cancel();
    let (processing_tx, processing_rx) = mpsc::channel::<()>();
    let (block_tx, block_rx) = mpsc::channel::<()>();
    let ptx = processing_tx.clone();
    let brx = Mutex::new(block_rx);
    let mut query1 = engine.new_test_query(move |ctx| {
        ptx.send(()).unwrap();
        brx.lock().unwrap().recv().unwrap();
        context_done(ctx, "test statement execution")
    });
    let ctx2 = ctx.clone();
    let h = thread::spawn(move || {
        let res = query1.exec(&ctx2);
        processing_tx.send(()).unwrap();
        res
    });
    processing_rx.recv().unwrap();
    cancel_ctx.cancel();
    block_tx.send(()).unwrap();
    processing_rx.recv().unwrap();
    let res = h.join().unwrap();
    let err = res.err.expect("expected error on shutdown during query but got none");
    assert_eq!(Some(&PromqlError::QueryCanceled("test statement execution".to_string())), PromqlError::find(&err));

    let mut query2 = engine.new_test_query(|_| panic!("reached query execution unexpectedly"));
    let res2 = query2.exec(&ctx);
    let err = res2.err.expect("expected error on querying with canceled context but got none");
    assert!(
        matches!(PromqlError::find(&err), Some(PromqlError::QueryCanceled(_))),
        "expected cancellation error but got: {}",
        err
    );
}

#[test]
fn test_find_min_max_time_for_info() {
    let p = promql::parser::Parser::new(promql::parser::Options {
        enable_experimental_functions: true,
        ..Default::default()
    });
    let cases: [(&str, &str, i64, i64, i64, i64); 9] = [
        ("mixed fixed references", "info(metric @ 120 or other_metric @ 480)", 480, 540, 115001, 540000),
        ("mixed offsets", "info(metric offset 2m or other_metric offset 6m)", 480, 540, 115001, 540000),
        (
            "selector-free input",
            r#"info(label_replace(label_replace(vector(1), "instance", "a", "__name__", ".*"), "job", "1", "__name__", ".*"))"#,
            480,
            540,
            475001,
            540000,
        ),
        ("offset subquery", "last_over_time((info(metric @ 120 or other_metric @ 480))[5m:1m] offset 6m)", 1200, 1260, 115001, 900000),
        (
            "anchored offset subquery",
            "last_over_time((info(metric @ 120 or other_metric @ 480))[5m:1m] @ 1200 offset 6m)",
            1200,
            1260,
            115001,
            840000,
        ),
        ("uniform fixed reference remains narrow", "info(metric @ 120 or other_metric @ 120)", 480, 540, 115001, 120000),
        ("uniform short range includes metadata lookback", "info(last_over_time(metric[1s]))", 480, 540, 475001, 540000),
        ("uniform fixed short range includes metadata lookback", "info(last_over_time(metric[1s] @ 120))", 480, 540, 115001, 120000),
        ("uniform offset short range includes metadata lookback", "info(last_over_time(metric[1s] offset 2m))", 480, 540, 355001, 420000),
    ];
    for (name, q, start, end, want_min, want_max) in cases {
        let expr = p.parse_expr(q).unwrap();
        let expr = promql::preprocess_expr(expr, start * 1000, end * 1000, 60_000).unwrap();
        let stmt = promql::EvalStmt {
            expr,
            start: start * 1000,
            end: end * 1000,
            interval: 60_000,
            lookback_delta: Duration::from_secs(5),
        };
        let (min, max) = promql::find_min_max_time(&stmt);
        assert_eq!(want_min, min, "{}", name);
        assert_eq!(want_max, max, "{}", name);
    }
}

#[test]
fn test_detect_histogram_stats_decoding() {
    let cases: [(&str, bool); 22] = [
        ("metric", false),
        ("histogram_count(metric)", true),
        ("histogram_sum(metric)", true),
        ("histogram_avg(metric)", true),
        ("histogram_count(rate(metric[1m]))", true),
        ("histogram_count(sum(rate(metric[1m])))", true),
        ("histogram_quantile(0.9, metric)", false),
        ("histogram_fraction(0, 0.5, metric)", false),
        ("timestamp(metric)", true),
        ("start_timestamp(metric)", true),
        ("absent(metric)", true),
        ("histogram_stddev(metric)", false),
        ("histogram_stdvar(metric)", false),
        ("timestamp(histogram_stddev(metric))", false),
        ("timestamp(histogram_stdvar(metric))", false),
        ("histogram_count(last_over_time(metric[5m:1m]))", false),
        ("histogram_count(metric == other)", false),
        ("timestamp(metric != other)", false),
        ("absent(metric == other)", false),
        ("histogram_count(metric + other)", true),
        ("timestamp(changes(metric[5m]))", false),
        (r#"timestamp(count_values("value", metric))"#, false),
    ];
    let p = promql::parser::Parser::new(promql::parser::Options {
        enable_experimental_functions: true,
        ..Default::default()
    });
    for (q, expect_skip) in cases {
        let expr = p.parse_expr(q).unwrap();
        let expr = promql::preprocess_expr(expr, 1_000_000, 9_999_000, 0).unwrap();
        let mut selectors = Vec::new();
        let _ = promql::parser::inspect(&expr, |n, _| -> Result<(), ()> {
            if let promql::parser::Expr::VectorSelector(vs) = n {
                selectors.push(vs.skip_histogram_buckets);
            }
            Ok(())
        });
        assert!(!selectors.is_empty());
        for s in selectors {
            assert_eq!(expect_skip, s, "error on input '{}'", q);
        }
    }
}

#[test]
fn test_engine_opts_validation() {
    use promql::{ERR_VALIDATION_AT_MODIFIER_DISABLED as AT, ERR_VALIDATION_NEGATIVE_OFFSET_DISABLED as NEG};
    let at = |b: bool| EngineOpts { enable_at_modifier: b, ..Default::default() };
    let neg = |b: bool| EngineOpts { enable_negative_offset: b, ..Default::default() };
    let both = || EngineOpts { enable_at_modifier: true, enable_negative_offset: true, ..Default::default() };
    let cases: Vec<(EngineOpts, &str, Option<Error>)> = vec![
        (at(false), "metric @ 100", Some(AT)),
        (at(false), "rate(metric[1m] @ 100)", Some(AT)),
        (at(false), "rate(metric[1h:1m] @ 100)", Some(AT)),
        (at(false), "metric @ start()", Some(AT)),
        (at(false), "rate(metric[1m] @ start())", Some(AT)),
        (at(false), "rate(metric[1h:1m] @ start())", Some(AT)),
        (at(false), "metric @ end()", Some(AT)),
        (at(false), "rate(metric[1m] @ end())", Some(AT)),
        (at(false), "rate(metric[1h:1m] @ end())", Some(AT)),
        (at(true), "metric @ 100", None),
        (at(true), "rate(metric[1m] @ start())", None),
        (at(true), "rate(metric[1h:1m] @ end())", None),
        (neg(false), "metric offset -1s", Some(NEG)),
        (neg(true), "metric offset -1s", None),
        (both(), "metric @ 100 offset -2m", None),
        (both(), "metric offset -2m @ 100", None),
    ];
    for (opts, q, exp) in cases {
        let eng = engine_with(opts);
        let ctx = Context::background();
        let r1 = eng.new_instant_query(&ctx, noop_queryable(), None, q, 10_000);
        let r2 = eng.new_range_query(&ctx, noop_queryable(), None, q, 0, 10_000, 1000);
        match exp {
            Some(e) => {
                assert!(r1.as_ref().err().is_some_and(|x| x.is(&e)), "{}", q);
                assert!(r2.as_ref().err().is_some_and(|x| x.is(&e)), "{}", q);
            }
            None => {
                assert!(r1.is_ok(), "{}: {:?}", q, r1.err());
                assert!(r2.is_ok(), "{}: {:?}", q, r2.err());
            }
        }
    }
}

#[test]
fn test_engine_close() {
    // adapted: Go also closes a nil *Engine; Rust has no nil engine.
    let ng = Engine::new(EngineOpts {
        timeout: Duration::from_secs(100),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_delayed_name_removal: true,
        ..Default::default()
    });
    assert!(ng.close().is_ok());
}

fn get_log_lines(name: &std::path::Path) -> Vec<String> {
    let content = std::fs::read_to_string(name).unwrap();
    content.split('\n').filter(|l| !l.is_empty()).map(|s| s.to_string()).collect()
}

fn logger_engine() -> Engine {
    engine_with(EngineOpts { max_samples: 10, timeout: Duration::from_secs(10), ..Default::default() })
}

#[test]
fn test_query_logger_basic() {
    use promql::QueryLogger;
    use promql::query_logger::JsonFileLogger;
    let engine = logger_engine();
    let query_exec = || {
        let (ctx, cancel) = Context::background().with_cancel();
        let mut q = engine.new_test_query(|ctx| context_done(ctx, "test statement execution"));
        let res = q.exec(&ctx);
        cancel.cancel();
        assert!(res.err.is_none());
    };
    // promql.Query works without query log initialized.
    query_exec();
    let dir = temp_dir("qlbasic");
    let ql1 = dir.join("query1.log");
    let f1 = Arc::new(JsonFileLogger::new(ql1.to_str().unwrap()).unwrap());
    engine.set_query_logger(Some(f1.clone()));
    query_exec();
    let lines = get_log_lines(&ql1);
    assert!(lines[0].contains("\"params\":{\"query\":\"test statement\"}"), "{}", lines[0]);
    assert_eq!(1, lines.len());
    let l = lines.len();
    query_exec();
    assert_eq!(2 * l, get_log_lines(&ql1).len());

    // Unsetting the logger closes it.
    engine.set_query_logger(None);
    let err = f1.close().unwrap_err();
    assert!(err.to_string().contains("file already closed"), "expected f1 to be closed, got open");
    query_exec();

    // Swapping closes the old logger.
    let f2 = Arc::new(JsonFileLogger::new(dir.join("query2.log").to_str().unwrap()).unwrap());
    let f3 = Arc::new(JsonFileLogger::new(dir.join("query3.log").to_str().unwrap()).unwrap());
    engine.set_query_logger(Some(f2.clone()));
    query_exec();
    engine.set_query_logger(Some(f3.clone()));
    let err = f2.close().unwrap_err();
    assert!(err.to_string().contains("file already closed"), "expected f2 to be closed, got open");
    query_exec();
    assert!(f3.close().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_query_logger_fields() {
    use promql::query_logger::JsonFileLogger;
    use promql::{LogValue, QueryLogger};
    let engine = logger_engine();
    let dir = temp_dir("qlfields");
    let ql1 = dir.join("query1.log");
    let f1 = Arc::new(JsonFileLogger::new(ql1.to_str().unwrap()).unwrap());
    engine.set_query_logger(Some(f1.clone()));
    let (ctx, cancel) = Context::background().with_cancel();
    let mut q = engine.new_test_query(|ctx| context_done(ctx, "test statement execution"));
    q.set_origin(vec![("foo".to_string(), LogValue::Str("bar".to_string()))]);
    let res = q.exec(&ctx);
    cancel.cancel();
    assert!(res.err.is_none());
    let lines = get_log_lines(&ql1);
    assert!(lines[0].contains("\"foo\":\"bar\""), "{}", lines[0]);
    assert!(f1.close().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_query_logger_error() {
    use promql::query_logger::JsonFileLogger;
    use promql::{LogValue, QueryLogger};
    let engine = logger_engine();
    let dir = temp_dir("qlerror");
    let ql1 = dir.join("query1.log");
    let f1 = Arc::new(JsonFileLogger::new(ql1.to_str().unwrap()).unwrap());
    engine.set_query_logger(Some(f1.clone()));
    let mut q = engine.new_test_query(|_| Err(Error::msg("failure")));
    q.set_origin(vec![("foo".to_string(), LogValue::Str("bar".to_string()))]);
    let res = q.exec(&Context::background());
    assert!(res.err.is_some(), "query should have failed");
    let lines = get_log_lines(&ql1);
    assert!(lines[0].contains("\"error\":\"failure\""), "{}", lines[0]);
    assert!(lines[0].contains("\"params\":{\"query\":\"test statement\"}"), "{}", lines[0]);
    assert!(f1.close().is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

// -----------------------------------------------------------------------------------------------------------------
// Query statistics and max samples.

use promql::promqltest::{self, TestStorage};

struct StatsCase {
    query: &'static str,
    skip_max_check: bool,
    total_samples: i64,
    total_samples_per_step: Vec<(i64, i64)>,
    samples_read: i64,
    samples_read_per_step: Vec<(i64, i64)>,
    peak_samples: usize,
    start: i64,
    end: i64,
    interval: i64,
}

fn run_stats_query(
    engine: &Engine,
    st: &TestStorage,
    opts: &promql::QueryOpts,
    query: &str,
    start: i64,
    end: i64,
    interval: i64,
) -> (Option<Error>, util::stats::Statistics) {
    let ctx = Context::background();
    let q: Arc<dyn storage::Queryable> = Arc::new(st.clone());
    let mut qry = if interval == 0 {
        engine.new_instant_query(&ctx, q, Some(opts), query, start).unwrap()
    } else {
        engine.new_range_query(&ctx, q, Some(opts), query, start, end, interval).unwrap()
    };
    let res = qry.exec(&ctx);
    (res.err, qry.stats())
}

fn is_too_many_samples(e: &Option<Error>) -> bool {
    matches!(e.as_ref().and_then(PromqlError::find), Some(PromqlError::TooManySamples(env)) if env == "query execution")
}

fn to_map(v: &[(i64, i64)]) -> std::collections::HashMap<i64, i64> {
    v.iter().copied().collect()
}

#[test]
fn test_query_statistics() {
    let st = promqltest::loaded_storage(
        r#"
load 10s
  metricWith1SampleEvery10Seconds 1+1x100
  metricWith3SampleEvery10Seconds{a="1",b="1"} 1+1x100
  metricWith3SampleEvery10Seconds{a="2",b="2"} 1+1x100
  metricWith3SampleEvery10Seconds{a="3",b="2"} 1+1x100
  metricWith1HistogramEvery10Seconds {{schema:1 count:5 sum:20 buckets:[1 2 1 1]}}+{{schema:1 count:10 sum:5 buckets:[1 2 3 4]}}x100
"#,
    )
    .unwrap();
    let mut failures = Vec::new();
    for c in stats_cases() {
        // Per-step stats enabled.
        let opts = promql::QueryOpts::new(true, Duration::ZERO, None);
        let engine = promqltest::new_test_engine(true, Duration::ZERO, promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY);
        let (err, stats) = run_stats_query(&engine, &st, &opts, c.query, c.start, c.end, c.interval);
        let s = stats.samples.unwrap();
        let mut check = |what: &str, ok: bool, detail: String| {
            if !ok {
                failures.push(format!("{}: {}: {}", c.query, what, detail));
            }
        };
        check("error", err.is_none(), format!("{:?}", err));
        check("Total samples mismatch", c.total_samples == s.total_samples, format!("{} != {}", c.total_samples, s.total_samples));
        check(
            "Total samples per time mismatch",
            Some(to_map(&c.total_samples_per_step)) == s.total_samples_per_step_map(),
            format!("{:?} != {:?}", c.total_samples_per_step, s.total_samples_per_step_map()),
        );
        check("Samples read mismatch", c.samples_read == s.samples_read, format!("{} != {}", c.samples_read, s.samples_read));
        check(
            "Samples read per step mismatch",
            Some(to_map(&c.samples_read_per_step)) == s.samples_read_per_step_map(),
            format!("{:?} != {:?}", c.samples_read_per_step, s.samples_read_per_step_map()),
        );
        check("Peak samples mismatch", c.peak_samples == s.peak_samples, format!("{} != {}", c.peak_samples, s.peak_samples));
        if c.interval != 0 {
            for &(ts, total) in &c.total_samples_per_step {
                let (_, is) = run_stats_query(&engine, &st, &opts, c.query, ts, 0, 0);
                let got = is.samples.unwrap().total_samples;
                check("Instant query total", total == got, format!("at {}: {} != {}", ts, total, got));
            }
        }
        if !c.skip_max_check {
            let engine = promqltest::new_test_engine(true, Duration::ZERO, s.peak_samples.saturating_sub(1));
            let (err, _) = run_stats_query(&engine, &st, &opts, c.query, c.start, c.end, c.interval);
            check("max samples", is_too_many_samples(&err), format!("{:?}", err));
        }

        // Per-step stats disabled.
        let opts = promql::QueryOpts::new(false, Duration::ZERO, None);
        let engine = promqltest::new_test_engine(false, Duration::ZERO, promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY);
        let (err, stats) = run_stats_query(&engine, &st, &opts, c.query, c.start, c.end, c.interval);
        let s = stats.samples.unwrap();
        check("error (per step disabled)", err.is_none(), format!("{:?}", err));
        check("TotalSamples (per step disabled)", c.total_samples == s.total_samples, format!("{} != {}", c.total_samples, s.total_samples));
        check("SamplesRead (per step disabled)", c.samples_read == s.samples_read, format!("{} != {}", c.samples_read, s.samples_read));
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn test_max_query_samples() {
    let st = promqltest::loaded_storage(
        r#"
load 10s
  metric 1+1x100
  bigmetric{a="1"} 1+1x100
  bigmetric{a="2"} 1+1x100
  metricWith1HistogramEvery10Seconds {{schema:1 count:5 sum:20 buckets:[1 2 1 1]}}+{{schema:1 count:10 sum:5 buckets:[1 2 3 4]}}x100
"#,
    )
    .unwrap();
    let mut failures = Vec::new();
    for (query, max_samples, start, end, interval) in max_samples_cases() {
        let opts = promql::QueryOpts::default();
        let engine = promqltest::new_test_engine(false, Duration::ZERO, max_samples);
        let (err, stats) = run_stats_query(&engine, &st, &opts, query, start, end, interval);
        if err.is_some() {
            failures.push(format!("{}: unexpected error {:?}", query, err));
        } else if stats.samples.as_ref().unwrap().peak_samples != max_samples {
            failures.push(format!(
                "{}: peak samples mismatch: want {} got {}",
                query,
                max_samples,
                stats.samples.as_ref().unwrap().peak_samples
            ));
        }
        let engine = promqltest::new_test_engine(false, Duration::ZERO, max_samples - 1);
        let (err, _) = run_stats_query(&engine, &st, &opts, query, start, end, interval);
        if !is_too_many_samples(&err) {
            failures.push(format!("{}: expected too many samples, got {:?}", query, err));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

// Generated from upstream TestQueryStatistics.
fn stats_cases() -> Vec<StatsCase> {
    vec![
        StatsCase { query: r#""literal string""#, skip_max_check: true, total_samples: 0, total_samples_per_step: vec![(21000, 0)], samples_read: 0, samples_read_per_step: vec![(21000, 0)], peak_samples: 0, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "1", skip_max_check: false, total_samples: 0, total_samples_per_step: vec![(21000, 0)], samples_read: 0, samples_read_per_step: vec![(21000, 0)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1HistogramEvery10Seconds", skip_max_check: false, total_samples: 13, total_samples_per_step: vec![(21000, 13)], samples_read: 13, samples_read_per_step: vec![(21000, 13)], peak_samples: 13, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "timestamp(metricWith1SampleEvery10Seconds)", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 2, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "timestamp(metricWith1HistogramEvery10Seconds)", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 2, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(22000, 1)], samples_read: 1, samples_read_per_step: vec![(22000, 1)], peak_samples: 1, start: 22000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds offset 10s", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds @ 15", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"}"#, skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"} @ 19"#, skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(21000, 1)], samples_read: 1, samples_read_per_step: vec![(21000, 1)], peak_samples: 1, start: 21000, end: 0, interval: 0 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"}[20s] @ 19"#, skip_max_check: false, total_samples: 2, total_samples_per_step: vec![(21000, 2)], samples_read: 2, samples_read_per_step: vec![(21000, 2)], peak_samples: 2, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith3SampleEvery10Seconds", skip_max_check: false, total_samples: 3, total_samples_per_step: vec![(21000, 3)], samples_read: 3, samples_read_per_step: vec![(21000, 3)], peak_samples: 3, start: 21000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds[60s]", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 6)], samples_read: 6, samples_read_per_step: vec![(201000, 6)], peak_samples: 6, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1HistogramEvery10Seconds[60s]", skip_max_check: false, total_samples: 78, total_samples_per_step: vec![(201000, 78)], samples_read: 78, samples_read_per_step: vec![(201000, 78)], peak_samples: 78, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[60s])[20s:5s]", skip_max_check: false, total_samples: 24, total_samples_per_step: vec![(201000, 24)], samples_read: 8, samples_read_per_step: vec![(201000, 8)], peak_samples: 10, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[61s])[20s:5s]", skip_max_check: false, total_samples: 26, total_samples_per_step: vec![(201000, 26)], samples_read: 8, samples_read_per_step: vec![(201000, 8)], peak_samples: 11, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1HistogramEvery10Seconds[60s])[20s:5s]", skip_max_check: false, total_samples: 312, total_samples_per_step: vec![(201000, 312)], samples_read: 104, samples_read_per_step: vec![(201000, 104)], peak_samples: 78, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds[60s] @ 30", skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(201000, 4)], samples_read: 4, samples_read_per_step: vec![(201000, 4)], peak_samples: 4, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1HistogramEvery10Seconds[60s] @ 30", skip_max_check: false, total_samples: 52, total_samples_per_step: vec![(201000, 52)], samples_read: 52, samples_read_per_step: vec![(201000, 52)], peak_samples: 52, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "sum(max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30))", skip_max_check: false, total_samples: 12, total_samples_per_step: vec![(201000, 12)], samples_read: 12, samples_read_per_step: vec![(201000, 12)], peak_samples: 7, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "sum by (b) (max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30))", skip_max_check: false, total_samples: 12, total_samples_per_step: vec![(201000, 12)], samples_read: 12, samples_read_per_step: vec![(201000, 12)], peak_samples: 7, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds[60s] offset 10s", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 6)], samples_read: 6, samples_read_per_step: vec![(201000, 6)], peak_samples: 6, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith3SampleEvery10Seconds[60s]", skip_max_check: false, total_samples: 18, total_samples_per_step: vec![(201000, 18)], samples_read: 18, samples_read_per_step: vec![(201000, 18)], peak_samples: 18, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[60s])", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 6)], samples_read: 6, samples_read_per_step: vec![(201000, 6)], peak_samples: 7, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "absent_over_time(metricWith1SampleEvery10Seconds[60s])", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 6)], samples_read: 6, samples_read_per_step: vec![(201000, 6)], peak_samples: 7, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s])", skip_max_check: false, total_samples: 18, total_samples_per_step: vec![(201000, 18)], samples_read: 18, samples_read_per_step: vec![(201000, 18)], peak_samples: 9, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds[60s:5s]", skip_max_check: false, total_samples: 12, total_samples_per_step: vec![(201000, 12)], samples_read: 12, samples_read_per_step: vec![(201000, 12)], peak_samples: 12, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "metricWith1SampleEvery10Seconds[60s:5s] offset 10s", skip_max_check: false, total_samples: 12, total_samples_per_step: vec![(201000, 12)], samples_read: 12, samples_read_per_step: vec![(201000, 12)], peak_samples: 12, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s:5s])", skip_max_check: false, total_samples: 36, total_samples_per_step: vec![(201000, 36)], samples_read: 36, samples_read_per_step: vec![(201000, 36)], peak_samples: 51, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s]))", skip_max_check: false, total_samples: 72, total_samples_per_step: vec![(201000, 72)], samples_read: 72, samples_read_per_step: vec![(201000, 72)], peak_samples: 52, start: 201000, end: 0, interval: 0 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"}"#, skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], samples_read: 4, samples_read_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 4, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"}"#, skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(204000, 1), (209000, 1), (214000, 1), (219000, 1)], samples_read: 4, samples_read_per_step: vec![(204000, 1), (209000, 1), (214000, 1), (219000, 1)], peak_samples: 4, start: 204000, end: 223000, interval: 5000 },
        StatsCase { query: r#"metricWith1HistogramEvery10Seconds"#, skip_max_check: false, total_samples: 52, total_samples_per_step: vec![(204000, 13), (209000, 13), (214000, 13), (219000, 13)], samples_read: 52, samples_read_per_step: vec![(204000, 13), (209000, 13), (214000, 13), (219000, 13)], peak_samples: 52, start: 204000, end: 223000, interval: 5000 },
        StatsCase { query: "timestamp(metricWith1SampleEvery10Seconds)", skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], samples_read: 4, samples_read_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 5, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "timestamp(metricWith1HistogramEvery10Seconds)", skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], samples_read: 4, samples_read_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 5, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: r#"max_over_time(metricWith3SampleEvery10Seconds{a="1"}[10s])"#, skip_max_check: false, total_samples: 2, total_samples_per_step: vec![(991000, 1), (1001000, 1), (1011000, 0), (1021000, 0)], samples_read: 2, samples_read_per_step: vec![(991000, 1), (1001000, 1), (1011000, 0), (1021000, 0)], peak_samples: 2, start: 991000, end: 1021000, interval: 10000 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds{a="1"} offset 10s"#, skip_max_check: false, total_samples: 4, total_samples_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], samples_read: 4, samples_read_per_step: vec![(201000, 1), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 4, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s] @ 30)", skip_max_check: false, total_samples: 48, total_samples_per_step: vec![(201000, 12), (206000, 12), (211000, 12), (216000, 12)], samples_read: 12, samples_read_per_step: vec![(201000, 12), (206000, 0), (211000, 0), (216000, 0)], peak_samples: 12, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: r#"metricWith3SampleEvery10Seconds"#, skip_max_check: false, total_samples: 12, total_samples_per_step: vec![(201000, 3), (206000, 3), (211000, 3), (216000, 3)], samples_read: 12, samples_read_per_step: vec![(201000, 3), (206000, 3), (211000, 3), (216000, 3)], peak_samples: 12, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: r#"max_over_time(metricWith3SampleEvery10Seconds[60s])"#, skip_max_check: false, total_samples: 72, total_samples_per_step: vec![(201000, 18), (206000, 18), (211000, 18), (216000, 18)], samples_read: 21, samples_read_per_step: vec![(201000, 18), (206000, 0), (211000, 3), (216000, 0)], peak_samples: 18, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "sum_over_time(metricWith1SampleEvery10Seconds[60s])", skip_max_check: false, total_samples: 24, total_samples_per_step: vec![(201000, 6), (211000, 6), (221000, 6), (231000, 6)], samples_read: 9, samples_read_per_step: vec![(201000, 6), (211000, 1), (221000, 1), (231000, 1)], peak_samples: 10, start: 201000, end: 231000, interval: 10000 },
        StatsCase { query: "last_over_time(sum_over_time(metricWith1SampleEvery10Seconds[60s])[10s:10s])", skip_max_check: false, total_samples: 24, total_samples_per_step: vec![(201000, 6), (211000, 6), (221000, 6), (231000, 6)], samples_read: 9, samples_read_per_step: vec![(201000, 6), (211000, 1), (221000, 1), (231000, 1)], peak_samples: 10, start: 201000, end: 231000, interval: 10000 },
        StatsCase { query: "last_over_time(last_over_time(sum_over_time(metricWith1SampleEvery10Seconds[60s])[10s:10s])[10s:10s])", skip_max_check: false, total_samples: 24, total_samples_per_step: vec![(201000, 6), (211000, 6), (221000, 6), (231000, 6)], samples_read: 9, samples_read_per_step: vec![(201000, 6), (211000, 1), (221000, 1), (231000, 1)], peak_samples: 10, start: 201000, end: 231000, interval: 10000 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s:5s])", skip_max_check: false, total_samples: 144, total_samples_per_step: vec![(201000, 36), (206000, 36), (211000, 36), (216000, 36)], samples_read: 45, samples_read_per_step: vec![(201000, 36), (206000, 3), (211000, 3), (216000, 3)], peak_samples: 69, start: 201000, end: 216000, interval: 5000 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s:5s])", skip_max_check: false, total_samples: 144, total_samples_per_step: vec![(201000, 36), (206000, 36), (211000, 36), (216000, 36)], samples_read: 45, samples_read_per_step: vec![(201000, 36), (206000, 3), (211000, 3), (216000, 3)], peak_samples: 69, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[60s:5s])", skip_max_check: false, total_samples: 48, total_samples_per_step: vec![(201000, 12), (206000, 12), (211000, 12), (216000, 12)], samples_read: 15, samples_read_per_step: vec![(201000, 12), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 31, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "sum by (b) (max_over_time(metricWith1SampleEvery10Seconds[60s:5s]))", skip_max_check: false, total_samples: 48, total_samples_per_step: vec![(201000, 12), (206000, 12), (211000, 12), (216000, 12)], samples_read: 15, samples_read_per_step: vec![(201000, 12), (206000, 1), (211000, 1), (216000, 1)], peak_samples: 31, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s]))", skip_max_check: false, total_samples: 288, total_samples_per_step: vec![(201000, 72), (206000, 72), (211000, 72), (216000, 72)], samples_read: 90, samples_read_per_step: vec![(201000, 72), (206000, 6), (211000, 6), (216000, 6)], peak_samples: 73, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith1SampleEvery10Seconds[60s:5s]))", skip_max_check: false, total_samples: 192, total_samples_per_step: vec![(201000, 48), (206000, 48), (211000, 48), (216000, 48)], samples_read: 60, samples_read_per_step: vec![(201000, 48), (206000, 4), (211000, 4), (216000, 4)], peak_samples: 69, start: 201000, end: 216000, interval: 5000 },
        StatsCase { query: "sum(max_over_time(metricWith3SampleEvery10Seconds[60s:5s])) + sum(max_over_time(metricWith1SampleEvery10Seconds[60s:5s]))", skip_max_check: false, total_samples: 192, total_samples_per_step: vec![(201000, 48), (206000, 48), (211000, 48), (216000, 48)], samples_read: 60, samples_read_per_step: vec![(201000, 48), (206000, 4), (211000, 4), (216000, 4)], peak_samples: 69, start: 201000, end: 220000, interval: 5000 },
        StatsCase { query: "quantile_over_time(time() / 1000, metricWith1SampleEvery10Seconds[20s:10s] @ 200)", skip_max_check: false, total_samples: 8, total_samples_per_step: vec![(250000, 2), (260000, 2), (270000, 2), (280000, 2)], samples_read: 2, samples_read_per_step: vec![(250000, 2), (260000, 0), (270000, 0), (280000, 0)], peak_samples: 16, start: 250000, end: 280000, interval: 10000 },
        StatsCase { query: "quantile_over_time(time() / 1000, metricWith1SampleEvery10Seconds[20s:10s] @ 200)", skip_max_check: false, total_samples: 2, total_samples_per_step: vec![(250000, 2)], samples_read: 2, samples_read_per_step: vec![(250000, 2)], peak_samples: 7, start: 250000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[20s:10s])", skip_max_check: false, total_samples: 2, total_samples_per_step: vec![(201000, 2)], samples_read: 2, samples_read_per_step: vec![(201000, 2)], peak_samples: 5, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "sum_over_time(metricWith1SampleEvery10Seconds[30s:30s])", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(90000, 1)], samples_read: 1, samples_read_per_step: vec![(90000, 1)], peak_samples: 3, start: 90000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[30s:2m])", skip_max_check: false, total_samples: 1, total_samples_per_step: vec![(240000, 1)], samples_read: 1, samples_read_per_step: vec![(240000, 1)], peak_samples: 3, start: 240000, end: 0, interval: 0 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[30s:10s])", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 3), (231000, 3)], samples_read: 6, samples_read_per_step: vec![(201000, 3), (231000, 3)], peak_samples: 11, start: 201000, end: 231000, interval: 30000 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[20s:10s])", skip_max_check: false, total_samples: 14, total_samples_per_step: vec![(201000, 2), (211000, 2), (221000, 2), (231000, 2), (241000, 2), (251000, 2), (261000, 2)], samples_read: 8, samples_read_per_step: vec![(201000, 2), (211000, 1), (221000, 1), (231000, 1), (241000, 1), (251000, 1), (261000, 1)], peak_samples: 17, start: 201000, end: 261000, interval: 10000 },
        StatsCase { query: "max_over_time(metricWith1SampleEvery10Seconds[30s:10s])", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(201000, 3), (261000, 3)], samples_read: 6, samples_read_per_step: vec![(201000, 3), (261000, 3)], peak_samples: 14, start: 201000, end: 261000, interval: 60000 },
        StatsCase { query: "histogram_count(max_over_time(metricWith1HistogramEvery10Seconds[20s:10s]))", skip_max_check: false, total_samples: 26, total_samples_per_step: vec![(201000, 26)], samples_read: 26, samples_read_per_step: vec![(201000, 26)], peak_samples: 52, start: 201000, end: 0, interval: 0 },
        StatsCase { query: "avg_over_time(metricWith1HistogramEvery10Seconds[2m:1m])", skip_max_check: false, total_samples: 78, total_samples_per_step: vec![(120000, 26), (180000, 26), (240000, 26)], samples_read: 52, samples_read_per_step: vec![(120000, 26), (180000, 13), (240000, 13)], peak_samples: 117, start: 120000, end: 240000, interval: 60000 },
        StatsCase { query: "max_over_time(metricWith3SampleEvery10Seconds[60s:10s])", skip_max_check: false, total_samples: 126, total_samples_per_step: vec![(200000, 18), (230000, 18), (260000, 18), (290000, 18), (320000, 18), (350000, 18), (380000, 18)], samples_read: 72, samples_read_per_step: vec![(200000, 18), (230000, 9), (260000, 9), (290000, 9), (320000, 9), (350000, 9), (380000, 9)], peak_samples: 99, start: 200000, end: 400000, interval: 30000 },
        StatsCase { query: "predict_linear(metricWith1SampleEvery10Seconds[60s] @ 100, 60)", skip_max_check: false, total_samples: 18, total_samples_per_step: vec![(100000, 6), (200000, 6), (300000, 6)], samples_read: 6, samples_read_per_step: vec![(100000, 6), (200000, 0), (300000, 0)], peak_samples: 12, start: 100000, end: 300000, interval: 100000 },
        StatsCase { query: "sum_over_time(metricWith3SampleEvery10Seconds[20s:10s] @ 200)", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(250000, 6)], samples_read: 6, samples_read_per_step: vec![(250000, 6)], peak_samples: 11, start: 250000, end: 0, interval: 0 },
        StatsCase { query: "sum_over_time(metricWith1SampleEvery10Seconds[20s:10s] offset 1m)", skip_max_check: false, total_samples: 2, total_samples_per_step: vec![(240000, 2)], samples_read: 2, samples_read_per_step: vec![(240000, 2)], peak_samples: 5, start: 240000, end: 0, interval: 0 },
        StatsCase { query: "sum_over_time(metricWith1SampleEvery10Seconds[20s:10s] offset 1m)", skip_max_check: false, total_samples: 8, total_samples_per_step: vec![(240000, 2), (250000, 2), (260000, 2), (270000, 2)], samples_read: 5, samples_read_per_step: vec![(240000, 2), (250000, 1), (260000, 1), (270000, 1)], peak_samples: 11, start: 240000, end: 270000, interval: 10000 },
        StatsCase { query: "sum_over_time(metricWith3SampleEvery10Seconds[1m:10s] @ 200 offset 1m)", skip_max_check: false, total_samples: 18, total_samples_per_step: vec![(300000, 18)], samples_read: 18, samples_read_per_step: vec![(300000, 18)], peak_samples: 27, start: 300000, end: 0, interval: 0 },
        StatsCase { query: "sum_over_time(max_over_time(metricWith3SampleEvery10Seconds[60s] @ 300)[5m:1m] @ 600)[10m:2m]", skip_max_check: false, total_samples: 450, total_samples_per_step: vec![(800000, 450)], samples_read: 18, samples_read_per_step: vec![(800000, 18)], peak_samples: 23, start: 800000, end: 0, interval: 0 },
        StatsCase { query: "rate(sum_over_time(metricWith1SampleEvery10Seconds[30s])[1m:30s])", skip_max_check: false, total_samples: 6, total_samples_per_step: vec![(240000, 6)], samples_read: 6, samples_read_per_step: vec![(240000, 6)], peak_samples: 5, start: 240000, end: 0, interval: 0 },
    ]
}

// Generated from upstream TestMaxQuerySamples: (query, max samples, start, end, interval).
fn max_samples_cases() -> Vec<(&'static str, usize, i64, i64, i64)> {
    vec![
        ("1", 1, 1000, 0, 0),
        ("metric", 1, 1000, 0, 0),
        ("metric[20s]", 2, 10000, 0, 0),
        ("rate(metric[20s])", 3, 10000, 0, 0),
        ("metric[20s:5s]", 3, 10000, 0, 0),
        ("metric[20s] @ 10", 2, 0, 0, 0),
        ("1", 3, 0, 2000, 1000),
        ("1", 3, 0, 2000, 1000),
        ("metric", 3, 0, 2000, 1000),
        ("metric", 3, 0, 10000, 5000),
        ("rate(bigmetric[1s])", 1, 0, 10000, 5000),
        ("metric @ 10", 3, 0, 10000, 5000),
        (r#"rate(bigmetric[10s:1s] @ 10)"#, 32, 0, 10000, 5000),
        (r#"rate(bigmetric[10s:1s] @ 10) + rate(bigmetric[10s:1s] @ 30)"#, 34, 0, 10000, 5000),
        (r#"rate(bigmetric[10s:1s]) + rate(bigmetric[10s:1s] @ 30)"#, 56, 10000, 20000, 5000),
        (r#"rate(rate(bigmetric[10:1s] @ 10)[100s:25s] @ 1000)[100s:20s] @ 2000"#, 32, 10000, 0, 0),
        (r#"rate(rate(bigmetric[10s:1s] @ 10)[100s:25s] @ 1000)[17s:1s] @ 2000"#, 34, 10000, 0, 0),
        ("max_over_time(metricWith1HistogramEvery10Seconds[61s])[20s:5s]", 91, 201000, 0, 0),
    ]
}

// -----------------------------------------------------------------------------------------------------------------
// Select hints.

struct HintRecordingQuerier {
    hints: Arc<Mutex<Vec<SelectHints>>>,
}

impl LabelQuerier for HintRecordingQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for HintRecordingQuerier {
    fn select(&self, _: &Context, _: bool, hints: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        self.hints.lock().unwrap().push(hints.cloned().unwrap_or_default());
        storage::empty_series_set()
    }
}

#[test]
fn test_select_hints_set_correctly() {
    let opts = EngineOpts {
        max_samples: 10,
        timeout: Duration::from_secs(10),
        lookback_delta: Duration::from_secs(5),
        enable_at_modifier: true,
        ..Default::default()
    };
    let mut failures = Vec::new();
    for (query, start, end, expected) in select_hints_cases() {
        let engine = Engine::new(opts.clone());
        let hints = Arc::new(Mutex::new(Vec::new()));
        let h = hints.clone();
        let q = Arc::new(QueryableFunc(move |_, _| -> Result<BoxQuerier, Error> {
            Ok(Box::new(HintRecordingQuerier { hints: h.clone() }))
        }));
        let ctx = Context::background();
        let mut qry = if end == 0 {
            engine.new_instant_query(&ctx, q, None, query, start).unwrap()
        } else {
            engine.new_range_query(&ctx, q, None, query, start, end, 1000).unwrap()
        };
        let res = qry.exec(&ctx);
        if let Some(e) = res.err {
            failures.push(format!("{}: error {}", query, e));
            continue;
        }
        let got = hints.lock().unwrap().clone();
        if got != expected {
            failures.push(format!("{}:\n  want {:?}\n  got  {:?}", query, expected, got));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn select_hints_cases() -> Vec<(&'static str, i64, i64, Vec<SelectHints>)> {
    vec![
        ("foo", 10000, 0, vec![SelectHints { start: 5001, end: 10000, ..Default::default() }]),
        ("foo @ 15", 10000, 0, vec![SelectHints { start: 10001, end: 15000, ..Default::default() }]),
        ("foo @ 1", 10000, 0, vec![SelectHints { start: -3999, end: 1000, ..Default::default() }]),
        ("foo[2m]", 200000, 0, vec![SelectHints { start: 80001, end: 200000, range: 120000, ..Default::default() }]),
        ("foo[2m] @ 180", 200000, 0, vec![SelectHints { start: 60001, end: 180000, range: 120000, ..Default::default() }]),
        ("foo[2m] @ 300", 200000, 0, vec![SelectHints { start: 180001, end: 300000, range: 120000, ..Default::default() }]),
        ("foo[2m] @ 60", 200000, 0, vec![SelectHints { start: -59999, end: 60000, range: 120000, ..Default::default() }]),
        ("foo[2m] offset 2m", 300000, 0, vec![SelectHints { start: 60001, end: 180000, range: 120000, ..Default::default() }]),
        ("foo[2m] @ 200 offset 2m", 300000, 0, vec![SelectHints { start: -39999, end: 80000, range: 120000, ..Default::default() }]),
        ("foo[2m:1s]", 300000, 0, vec![SelectHints { start: 175001, end: 300000, step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s])", 300000, 0, vec![SelectHints { start: 175001, end: 300000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 300)", 200000, 0, vec![SelectHints { start: 175001, end: 300000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 200)", 200000, 0, vec![SelectHints { start: 75001, end: 200000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 100)", 200000, 0, vec![SelectHints { start: -24999, end: 100000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] offset 10s)", 300000, 0, vec![SelectHints { start: 165001, end: 290000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo offset 10s)[2m:1s] offset 10s)", 300000, 0, vec![SelectHints { start: 155001, end: 280000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", 300000, 0, vec![SelectHints { start: 185001, end: 190000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", 300000, 0, vec![SelectHints { start: 185001, end: 190000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", 300000, 0, vec![SelectHints { start: -44999, end: 80000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("foo", 10000, 20000, vec![SelectHints { start: 5001, end: 20000, step: 1000, ..Default::default() }]),
        ("foo @ 15", 10000, 20000, vec![SelectHints { start: 10001, end: 15000, step: 1000, ..Default::default() }]),
        ("foo @ 1", 10000, 20000, vec![SelectHints { start: -3999, end: 1000, step: 1000, ..Default::default() }]),
        ("rate(foo[2m] @ 180)", 200000, 500000, vec![SelectHints { start: 60001, end: 180000, range: 120000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("rate(foo[2m] @ 300)", 200000, 500000, vec![SelectHints { start: 180001, end: 300000, range: 120000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("rate(foo[2m] @ 60)", 200000, 500000, vec![SelectHints { start: -59999, end: 60000, range: 120000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("rate(foo[2m])", 200000, 500000, vec![SelectHints { start: 80001, end: 500000, range: 120000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("rate(foo[2m] offset 2m)", 300000, 500000, vec![SelectHints { start: 60001, end: 380000, range: 120000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("rate(foo[2m:1s])", 300000, 500000, vec![SelectHints { start: 175001, end: 500000, func: "rate".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s])", 300000, 500000, vec![SelectHints { start: 175001, end: 500000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] offset 10s)", 300000, 500000, vec![SelectHints { start: 165001, end: 490000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 300)", 200000, 500000, vec![SelectHints { start: 175001, end: 300000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 200)", 200000, 500000, vec![SelectHints { start: 75001, end: 200000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time(foo[2m:1s] @ 100)", 200000, 500000, vec![SelectHints { start: -24999, end: 100000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo offset 10s)[2m:1s] offset 10s)", 300000, 500000, vec![SelectHints { start: 155001, end: 480000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo @ 200 offset 10s)[2m:1s] offset 10s)", 300000, 500000, vec![SelectHints { start: 185001, end: 190000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo @ 200 offset 10s)[2m:1s] @ 100 offset 10s)", 300000, 500000, vec![SelectHints { start: 185001, end: 190000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("count_over_time((foo offset 10s)[2m:1s] @ 100 offset 10s)", 300000, 500000, vec![SelectHints { start: -44999, end: 80000, func: "count_over_time".to_string(), step: 1000, ..Default::default() }]),
        ("sum by (dim1) (foo)", 10000, 0, vec![SelectHints { start: 5001, end: 10000, func: "sum".to_string(), by: true, grouping: vec!["dim1".to_string()], ..Default::default() }]),
        ("sum without (dim1) (foo)", 10000, 0, vec![SelectHints { start: 5001, end: 10000, func: "sum".to_string(), grouping: vec!["dim1".to_string()], ..Default::default() }]),
        ("sum by (dim1) (avg_over_time(foo[1s]))", 10000, 0, vec![SelectHints { start: 9001, end: 10000, func: "avg_over_time".to_string(), range: 1000, ..Default::default() }]),
        ("sum by (dim1) (max by (dim2) (foo))", 10000, 0, vec![SelectHints { start: 5001, end: 10000, func: "max".to_string(), by: true, grouping: vec!["dim2".to_string()], ..Default::default() }]),
        ("(max by (dim1) (foo))[5s:1s]", 10000, 0, vec![SelectHints { start: 1, end: 10000, func: "max".to_string(), by: true, grouping: vec!["dim1".to_string()], step: 1000, ..Default::default() }]),
        ("(sum(http_requests{group=~\"p.*\"})+max(http_requests{group=~\"c.*\"}))[20s:5s]", 120000, 0, vec![SelectHints { start: 95001, end: 120000, func: "sum".to_string(), by: true, step: 5000, ..Default::default() }, SelectHints { start: 95001, end: 120000, func: "max".to_string(), by: true, step: 5000, ..Default::default() }]),
        ("foo @ 50 + bar @ 250 + baz @ 900", 100000, 500000, vec![SelectHints { start: 45001, end: 50000, step: 1000, ..Default::default() }, SelectHints { start: 245001, end: 250000, step: 1000, ..Default::default() }, SelectHints { start: 895001, end: 900000, step: 1000, ..Default::default() }]),
        ("foo @ 50 + bar + baz @ 900", 100000, 500000, vec![SelectHints { start: 45001, end: 50000, step: 1000, ..Default::default() }, SelectHints { start: 95001, end: 500000, step: 1000, ..Default::default() }, SelectHints { start: 895001, end: 900000, step: 1000, ..Default::default() }]),
        ("rate(foo[2s] @ 50) + bar @ 250 + baz @ 900", 100000, 500000, vec![SelectHints { start: 48001, end: 50000, step: 1000, func: "rate".to_string(), range: 2000, ..Default::default() }, SelectHints { start: 245001, end: 250000, step: 1000, ..Default::default() }, SelectHints { start: 895001, end: 900000, step: 1000, ..Default::default() }]),
        ("rate(foo[2s:1s] @ 50) + bar + baz", 100000, 500000, vec![SelectHints { start: 43001, end: 50000, step: 1000, func: "rate".to_string(), ..Default::default() }, SelectHints { start: 95001, end: 500000, step: 1000, ..Default::default() }, SelectHints { start: 95001, end: 500000, step: 1000, ..Default::default() }]),
        ("rate(foo[2s:1s] @ 50) + bar + rate(baz[2m:1s] @ 900 offset 2m) ", 100000, 500000, vec![SelectHints { start: 43001, end: 50000, step: 1000, func: "rate".to_string(), ..Default::default() }, SelectHints { start: 95001, end: 500000, step: 1000, ..Default::default() }, SelectHints { start: 655001, end: 780000, step: 1000, func: "rate".to_string(), ..Default::default() }]),
        (r#"sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 50)[3s:1s] @ 3000"#, 100000, 0, vec![SelectHints { start: -149999, end: 50000, range: 100000, func: "sum_over_time".to_string(), step: 25000, ..Default::default() }]),
        (r#"sum_over_time(sum_over_time(metric{job="1"}[100s])[100s:25s] @ 3000)[3s:1s] @ 50"#, 0, 0, vec![SelectHints { start: 2800001, end: 3000000, range: 100000, func: "sum_over_time".to_string(), step: 25000, ..Default::default() }]),
    ]
}

// -----------------------------------------------------------------------------------------------------------------
// PreprocessExpr.

#[path = "common/mod.rs"]
mod common;

#[test]
fn test_preprocess_expr() {
    let test_parser = promql::parser::Parser::default();
    // Preserve subquery duration expressions.
    for input in ["foo[1h / 2:1h / 2]", "foo[1h / 2:30m]", "foo[1h / 2:]"] {
        let expr = test_parser.parse_expr(input).unwrap();
        let expr = promql::preprocess_expr(expr, 1_000_000, 9_999_000, 60_000).unwrap();
        assert_eq!(input, expr.to_string());
    }

    // Wrap step-invariant expressions: the expected ASTs are dumped from the upstream table
    // (scripts/gen/promql_engine_fixtures.py).
    let path = format!("{}/../../tests-data/promql/engine/preprocess_expr.json", env!("CARGO_MANIFEST_DIR"));
    let fixture = json::Json::parse(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let mut failures = Vec::new();
    for tc in fixture.as_arr().unwrap() {
        let input = tc.str_of("input");
        let expr = test_parser.parse_expr(input).unwrap();
        let expr = promql::preprocess_expr(expr, 1_000_000, 9_999_000, 0).unwrap();
        if tc.bool_of("outputTest") && input != expr.to_string() {
            failures.push(format!("error on input '{}': printed {}", input, expr));
        }
        let want = common::lossy(tc.get("expected").unwrap());
        let got = common::dump_expr(&expr);
        if let Some(d) = common::json_diff(&want, &got, "") {
            failures.push(format!("error on input '{}': {}", input, d));
        }
    }

    // Info step invariance.
    let p = promql::parser::Parser::new(promql::parser::Options {
        enable_experimental_functions: true,
        ..Default::default()
    });
    for (input, want_call, want_input) in info_step_invariance_cases() {
        let expr = p.parse_expr(input).unwrap();
        let mut expr = promql::preprocess_expr(expr, 1_000_000, 9_999_000, 0).unwrap();
        let got_call = matches!(expr, promql::parser::Expr::StepInvariant(_));
        if got_call != want_call {
            failures.push(format!("{}: call step invariant {} != {}", input, got_call, want_call));
        }
        if let promql::parser::Expr::StepInvariant(si) = expr {
            expr = si.expr;
        }
        let promql::parser::Expr::Call(call) = &expr else { panic!("{}: not a call", input) };
        let got_input = matches!(call.args[0], promql::parser::Expr::StepInvariant(_));
        if got_input != want_input {
            failures.push(format!("{}: input step invariant {} != {}", input, got_input, want_input));
        }
        if call.args.len() > 1 {
            assert!(matches!(call.args[1], promql::parser::Expr::VectorSelector(_)), "{}", input);
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn info_step_invariance_cases() -> Vec<(&'static str, bool, bool)> {
    vec![
        ("info(metric @ 120 or other_metric @ 480)", false, true),
        (r#"info(metric @ 120 or other_metric @ 480, {foo="bar"} @ 120)"#, false, true),
        ("info(metric @ 120 or other_metric @ 120)", true, false),
        (r#"info(metric @ 120 or other_metric @ 120, {foo="bar"})"#, true, false),
        ("info(metric @ 180 offset 1m or other_metric @ 120)", true, false),
        (r#"info(label_replace(label_replace(vector(1), "instance", "a", "__name__", ".*"), "job", "1", "__name__", ".*"), {version=~".*"})"#, false, true),
        (r#"info(label_replace(label_replace(vector(1), "instance", "a", "__name__", ".*"), "job", "1", "__name__", ".*") or other_metric @ 120, {version=~".*"})"#, false, true),
        (r#"info(other_metric @ 120 or label_replace(label_replace(vector(1), "instance", "a", "__name__", ".*"), "job", "1", "__name__", ".*"), {version=~".*"})"#, false, true),
    ]
}
