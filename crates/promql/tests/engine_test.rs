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

#[test]
fn test_query_lookback_delta() {
    let load = "load 5m\nmetric 0 1 2\n";
    let last = 600_000i64;
    let m = 60_000i64;
    // (name, ts, engine lookback, query lookback, expect samples)
    // adapted: Go's negative engine lookback (-10m) cannot be a Rust Duration; zero is used, which the engine
    // treats like Go treats a non-positive value here: the query lookback (20m) overrides it.
    let cases: [(&str, i64, u64, u64, bool); 7] = [
        ("default lookback delta", last + 5 * m - 1, 0, 0, true),
        ("outside default lookback delta", last + 5 * m, 0, 0, false),
        ("custom engine lookback delta", last + 10 * m - 1, 10, 0, true),
        ("outside custom engine lookback delta", last + 10 * m, 10, 0, false),
        ("custom query lookback delta", last + 20 * m - 1, 10, 20, true),
        ("outside custom query lookback delta", last + 20 * m, 10, 20, false),
        ("negative custom query lookback delta", last + 20 * m - 1, 0, 20, true),
    ];
    for (name, ts, engine_lb, query_lb, expect) in cases {
        let engine = promqltest::new_test_engine(
            false,
            Duration::from_secs(engine_lb * 60),
            promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
        );
        let st = promqltest::loaded_storage(load).unwrap();
        let opts = promql::QueryOpts::new(false, Duration::from_secs(query_lb * 60), None);
        let ctx = Context::background();
        let mut qry = engine.new_instant_query(&ctx, Arc::new(st), Some(&opts), "metric", ts).unwrap();
        let res = qry.exec(&ctx);
        assert!(res.err.is_none(), "{}", name);
        let v = res.vector().expect(name);
        assert_eq!(expect, !v.is_empty(), "{}", name);
    }
}

// -----------------------------------------------------------------------------------------------------------------
// Query cost.

fn new_query_cost_engine() -> Engine {
    Engine::new(EngineOpts {
        max_samples: 1_000_000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_delayed_name_removal: true,
        enable_query_cost: true,
        enable_per_step_stats: true,
        parser: Some(promql::parser::Parser::new(promqltest::test_parser_opts())),
        ..Default::default()
    })
}

fn exec_instant(engine: &Engine, st: &TestStorage, opts: Option<&promql::QueryOpts>, q: &str, ts: i64) -> Option<Error> {
    let ctx = Context::background();
    let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), opts, q, ts).unwrap();
    qry.exec(&ctx).err
}

fn err_is(e: &Option<Error>, want: &PromqlError) -> bool {
    e.as_ref().and_then(PromqlError::find) == Some(want)
}

fn env() -> String {
    "query execution".to_string()
}

#[test]
fn test_query_cost_max_series() {
    let st = promqltest::loaded_storage(
        "load 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n  bigmetric{a=\"3\"} 1+1x100\n",
    )
    .unwrap();
    let engine = new_query_cost_engine();
    engine.set_query_limits(3, 0, Duration::ZERO);
    assert!(exec_instant(&engine, &st, None, "bigmetric", 0).is_none());
    engine.set_query_limits(2, 0, Duration::ZERO);
    assert!(err_is(&exec_instant(&engine, &st, None, "bigmetric", 0), &PromqlError::TooManySeries(env())));
    engine.set_query_limits(10, 0, Duration::ZERO);
    let opts = promql::QueryOpts::with_limits(false, Duration::ZERO, 2, 0, Duration::ZERO, None);
    assert!(err_is(&exec_instant(&engine, &st, Some(&opts), "bigmetric", 0), &PromqlError::TooManySeries(env())));
    engine.set_query_limits(2, 0, Duration::ZERO);
    let opts = promql::QueryOpts::with_limits(false, Duration::ZERO, 100, 0, Duration::ZERO, None);
    let err = engine
        .new_instant_query(&Context::background(), Arc::new(st.clone()), Some(&opts), "bigmetric", 0)
        .err()
        .unwrap();
    match PromqlError::find(&err) {
        Some(PromqlError::LimitAboveCeiling { limit, .. }) => assert_eq!("max_series", limit),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_query_cost_max_samples_read() {
    let st = promqltest::loaded_storage("load 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n").unwrap();
    let engine = new_query_cost_engine();
    let q = "sum_over_time(bigmetric[100s])";
    engine.set_query_limits(0, 1_000_000, Duration::ZERO);
    assert!(exec_instant(&engine, &st, None, q, 1_000_000).is_none());
    engine.set_query_limits(0, 1, Duration::ZERO);
    assert!(err_is(&exec_instant(&engine, &st, None, q, 1_000_000), &PromqlError::TooManySamplesRead(env())));
}

#[test]
fn test_query_cost_disabled_feature() {
    let st = promqltest::loaded_storage("load 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n").unwrap();
    let engine = promqltest::new_test_engine(false, Duration::ZERO, 1_000_000);
    engine.set_query_limits(1, 1, Duration::ZERO);
    let opts = promql::QueryOpts::with_limits(false, Duration::ZERO, 1, 1, Duration::ZERO, None);
    assert!(exec_instant(&engine, &st, Some(&opts), "bigmetric", 0).is_none());
}

#[test]
fn test_query_cost_samples_accounting() {
    let st = promqltest::loaded_storage("load 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n").unwrap();
    for (name, engine) in [
        ("feature disabled", promqltest::new_test_engine(false, Duration::ZERO, 1_000_000)),
        ("feature enabled", new_query_cost_engine()),
    ] {
        let ctx = Context::background();
        let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, "bigmetric", 0).unwrap();
        assert!(qry.exec(&ctx).err.is_none(), "{}", name);
        let s = qry.stats().samples.unwrap();
        assert_eq!(2, s.total_series, "{}: series touched must be accounted", name);
        assert_eq!(2, s.samples_read, "{}: samples read must be accounted", name);
        assert!(s.peak_samples > 0, "{}: peak samples must be accounted", name);
    }
}

#[test]
fn test_query_cost_limit_resolution() {
    let st = promqltest::loaded_storage(
        "load 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n  bigmetric{a=\"3\"} 1+1x100\n",
    )
    .unwrap();
    let ts = 1_000_000;
    // (name, query, tight, loose, rejection)
    let limits: [(&str, &str, u64, u64, PromqlError); 2] = [
        ("max_series", "bigmetric", 2, 1000, PromqlError::TooManySeries(env())),
        ("max_samples_read", "sum_over_time(bigmetric[100s])", 1, 1_000_000, PromqlError::TooManySamplesRead(env())),
    ];
    // (name, ceiling, requested, want construct error, want rejected); 0 unset, 1 tight, 2 loose.
    let cases: [(&str, u8, u8, bool, bool); 7] = [
        ("request below the ceiling tightens it", 2, 1, false, true),
        ("request equal to the ceiling is accepted", 1, 1, false, true),
        ("request above the ceiling is rejected", 1, 2, true, false),
        ("no request falls back to the ceiling", 1, 0, false, true),
        ("no request and a generous ceiling allows the query", 2, 0, false, false),
        ("unlimited ceiling applies the request", 0, 1, false, true),
        ("unlimited ceiling applies a large request", 0, 2, false, false),
    ];
    for (lname, query, tight, loose, rejected) in &limits {
        let value = |w: u8| match w {
            1 => *tight,
            2 => *loose,
            _ => 0,
        };
        for (cname, ceiling, requested, want_construct, want_rejected) in cases {
            let name = format!("{}/{}", lname, cname);
            let engine = new_query_cost_engine();
            if *lname == "max_series" {
                engine.set_query_limits(value(ceiling), 0, Duration::ZERO);
            } else {
                engine.set_query_limits(0, value(ceiling), Duration::ZERO);
            }
            let opts = (value(requested) > 0).then(|| {
                if *lname == "max_series" {
                    promql::QueryOpts::with_limits(false, Duration::ZERO, value(requested), 0, Duration::ZERO, None)
                } else {
                    promql::QueryOpts::with_limits(false, Duration::ZERO, 0, value(requested), Duration::ZERO, None)
                }
            });
            let ctx = Context::background();
            let r = engine.new_instant_query(&ctx, Arc::new(st.clone()), opts.as_ref(), query, ts);
            if want_construct {
                let err = r.err().expect(&name);
                match PromqlError::find(&err) {
                    Some(PromqlError::LimitAboveCeiling { limit, requested: rq, ceiling: cl }) => {
                        assert_eq!(lname, limit, "{}", name);
                        assert_eq!(&value(requested).to_string(), rq, "{}", name);
                        assert_eq!(&value(ceiling).to_string(), cl, "{}", name);
                    }
                    other => panic!("{}: {:?}", name, other),
                }
                continue;
            }
            let mut qry = r.expect(&name);
            let err = qry.exec(&ctx).err;
            if want_rejected {
                assert!(err_is(&err, rejected), "{}: {:?}", name, err);
            } else {
                assert!(err.is_none(), "{}: {:?}", name, err);
            }
        }
    }
}

// -----------------------------------------------------------------------------------------------------------------
// @ modifier.

use model::labels::{Labels, from_strings};
use storage::{AppenderTransaction, AppenderV2};
use promql::{FPoint, Sample, Scalar, Series, Value};

fn new_test_engine() -> Engine {
    promqltest::new_test_engine(false, Duration::ZERO, promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY)
}

fn sort_value(v: &mut Value) {
    if let Value::Matrix(m) = v {
        promql::sort_matrix(m);
    }
}

#[test]
fn test_at_modifier() {
    let engine = new_test_engine();
    let store = promqltest::loaded_storage(
        r#"
load 10s
  metric{job="1"} 0+1x1000
  metric{job="2"} 0+2x1000
  metric_topk{instance="1"} 0+1x1000
  metric_topk{instance="2"} 0+2x1000
  metric_topk{instance="3"} 1000-1x1000

load 1ms
  metric_ms 0+1x10000
"#,
    )
    .unwrap();
    let lbls1 = from_strings(&["__name__", "metric", "job", "1"]);
    let lbls2 = from_strings(&["__name__", "metric", "job", "2"]);
    let lblstopk2 = from_strings(&["__name__", "metric_topk", "instance", "2"]);
    let lblstopk3 = from_strings(&["__name__", "metric_topk", "instance", "3"]);
    let lblsms = from_strings(&["__name__", "metric_ms"]);
    let lblsneg = from_strings(&["__name__", "metric_neg"]);

    // Add some samples with negative timestamp.
    let opts = storage::AppendV2Options::default();
    let mut app = store.appender_v2();
    let r = app.append(0, &lblsneg, 0, -1_000_000, 1000.0, None, None, &opts).unwrap();
    let mut ts = -1_000_000 + 1000;
    while ts <= 0 {
        app.append(r, &lblsneg, 0, ts, -((ts / 1000) as f64) + 1.0, None, None, &opts).unwrap();
        ts += 1000;
    }
    // To test the fix for https://github.com/prometheus/prometheus/issues/8433.
    app.append(0, &from_strings(&["__name__", "metric_timestamp"]), 0, 3600 * 1000, 1000.0, None, None, &opts).unwrap();
    assert!(app.commit().is_none());

    let mut failures = Vec::new();
    for (query, start, end, interval, mut want) in
        at_modifier_cases(&lbls1, &lbls2, &lblstopk2, &lblstopk3, &lblsms, &lblsneg)
    {
        let interval = if interval == 0 { 1 } else { interval };
        let ctx = Context::background();
        let q: Arc<dyn storage::Queryable> = Arc::new(store.clone());
        let mut qry = if end == 0 {
            engine.new_instant_query(&ctx, q, None, query, start * 1000).unwrap()
        } else {
            engine.new_range_query(&ctx, q, None, query, start * 1000, end * 1000, interval * 1000).unwrap()
        };
        let res = qry.exec(&ctx);
        if let Some(e) = res.err {
            failures.push(format!("{}: {}", query, e));
            continue;
        }
        let mut got = res.value.unwrap();
        sort_value(&mut want);
        sort_value(&mut got);
        if got != want {
            failures.push(format!("query {:?} failed:\n  want {:?}\n  got  {:?}", query, want, got));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn at_modifier_cases(lbls1: &Labels, lbls2: &Labels, lblstopk2: &Labels, lblstopk3: &Labels, lblsms: &Labels, lblsneg: &Labels) -> Vec<(&'static str, i64, i64, i64, Value)> {
    let (lbls1, lbls2, lblstopk2, lblstopk3, lblsms, lblsneg) = (lbls1.clone(), lbls2.clone(), lblstopk2.clone(), lblstopk3.clone(), lblsms.clone(), lblsneg.clone());
    vec![
        (r#"metric_neg @ 0"#, 100, 0, 0, Value::Vector(vec![Sample { t: 100000, f: 1.0, metric: lblsneg.clone(), ..Default::default() }])),
        (r#"metric_neg @ -200"#, 100, 0, 0, Value::Vector(vec![Sample { t: 100000, f: 201.0, metric: lblsneg.clone(), ..Default::default() }])),
        (r#"metric{job="2"} @ 50"#, - 2, 2, 1, Value::Matrix(vec![Series { floats: vec![FPoint { f: 10.0, t: - 2000 }, FPoint { f: 10.0, t: - 1000 }, FPoint { f: 10.0, t: 0 }, FPoint { f: 10.0, t: 1000 }, FPoint { f: 10.0, t: 2000 }], metric: lbls2.clone(), ..Default::default() }])),
        ("metric[20s] @ 300", 10, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 29.0, t: 290000 }, FPoint { f: 30.0, t: 300000 }], metric: lbls1.clone(), ..Default::default() }, Series { floats: vec![FPoint { f: 58.0, t: 290000 }, FPoint { f: 60.0, t: 300000 }], metric: lbls2.clone(), ..Default::default() }])),
        (r#"metric_neg[2s] @ 0"#, 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2.0, t: - 1000 }, FPoint { f: 1.0, t: 0 }], metric: lblsneg.clone(), ..Default::default() }])),
        (r#"metric_neg[3s] @ -500"#, 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 503.0, t: - 502000 }, FPoint { f: 502.0, t: - 501000 }, FPoint { f: 501.0, t: - 500000 }], metric: lblsneg.clone(), ..Default::default() }])),
        (r#"metric_ms[3ms] @ 2.345"#, 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2343.0, t: 2343 }, FPoint { f: 2344.0, t: 2344 }, FPoint { f: 2345.0, t: 2345 }], metric: lblsms.clone(), ..Default::default() }])),
        ("metric[100s:25s] @ 300", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 22.0, t: 225000 }, FPoint { f: 25.0, t: 250000 }, FPoint { f: 27.0, t: 275000 }, FPoint { f: 30.0, t: 300000 }], metric: lbls1.clone(), ..Default::default() }, Series { floats: vec![FPoint { f: 44.0, t: 225000 }, FPoint { f: 50.0, t: 250000 }, FPoint { f: 54.0, t: 275000 }, FPoint { f: 60.0, t: 300000 }], metric: lbls2.clone(), ..Default::default() }])),
        ("metric[100s1ms:25s] @ 300", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 20.0, t: 200000 }, FPoint { f: 22.0, t: 225000 }, FPoint { f: 25.0, t: 250000 }, FPoint { f: 27.0, t: 275000 }, FPoint { f: 30.0, t: 300000 }], metric: lbls1.clone(), ..Default::default() }, Series { floats: vec![FPoint { f: 40.0, t: 200000 }, FPoint { f: 44.0, t: 225000 }, FPoint { f: 50.0, t: 250000 }, FPoint { f: 54.0, t: 275000 }, FPoint { f: 60.0, t: 300000 }], metric: lbls2.clone(), ..Default::default() }])),
        ("metric_neg[50s:25s] @ 0", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 26.0, t: - 25000 }, FPoint { f: 1.0, t: 0 }], metric: lblsneg.clone(), ..Default::default() }])),
        ("metric_neg[50s1ms:25s] @ 0", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 51.0, t: - 50000 }, FPoint { f: 26.0, t: - 25000 }, FPoint { f: 1.0, t: 0 }], metric: lblsneg.clone(), ..Default::default() }])),
        ("metric_neg[50s:25s] @ -100", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 126.0, t: - 125000 }, FPoint { f: 101.0, t: - 100000 }], metric: lblsneg.clone(), ..Default::default() }])),
        ("metric_neg[50s1ms:25s] @ -100", 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 151.0, t: - 150000 }, FPoint { f: 126.0, t: - 125000 }, FPoint { f: 101.0, t: - 100000 }], metric: lblsneg.clone(), ..Default::default() }])),
        (r#"metric_ms[101ms:25ms] @ 2.345"#, 100, 0, 0, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2250.0, t: 2250 }, FPoint { f: 2275.0, t: 2275 }, FPoint { f: 2300.0, t: 2300 }, FPoint { f: 2325.0, t: 2325 }], metric: lblsms.clone(), ..Default::default() }])),
        (r#"metric_topk and topk(1, sum_over_time(metric_topk[50s] @ 100))"#, 50, 80, 10, Value::Matrix(vec![Series { floats: vec![FPoint { f: 995.0, t: 50000 }, FPoint { f: 994.0, t: 60000 }, FPoint { f: 993.0, t: 70000 }, FPoint { f: 992.0, t: 80000 }], metric: lblstopk3.clone(), ..Default::default() }])),
        (r#"metric_topk and topk(1, sum_over_time(metric_topk[50s] @ 5000))"#, 50, 80, 10, Value::Matrix(vec![Series { floats: vec![FPoint { f: 10.0, t: 50000 }, FPoint { f: 12.0, t: 60000 }, FPoint { f: 14.0, t: 70000 }, FPoint { f: 16.0, t: 80000 }], metric: lblstopk2.clone(), ..Default::default() }])),
        (r#"metric_topk and topk(1, sum_over_time(metric_topk[50s] @ end()))"#, 70, 100, 10, Value::Matrix(vec![Series { floats: vec![FPoint { f: 993.0, t: 70000 }, FPoint { f: 992.0, t: 80000 }, FPoint { f: 991.0, t: 90000 }, FPoint { f: 990.0, t: 100000 }], metric: lblstopk3.clone(), ..Default::default() }])),
        (r#"metric_topk and topk(1, sum_over_time(metric_topk[50s] @ start()))"#, 100, 130, 10, Value::Matrix(vec![Series { floats: vec![FPoint { f: 990.0, t: 100000 }, FPoint { f: 989.0, t: 110000 }, FPoint { f: 988.0, t: 120000 }, FPoint { f: 987.0, t: 130000 }], metric: lblstopk3.clone(), ..Default::default() }])),
        (r#"timestamp(metric_timestamp @ 3600)"#, 0, 7 * 60, 60, Value::Matrix(vec![Series { floats: vec![FPoint { f: 3600.0, t: 0 }, FPoint { f: 3600.0, t: 60 * 1000 }, FPoint { f: 3600.0, t: 2 * 60 * 1000 }, FPoint { f: 3600.0, t: 3 * 60 * 1000 }, FPoint { f: 3600.0, t: 4 * 60 * 1000 }, FPoint { f: 3600.0, t: 5 * 60 * 1000 }, FPoint { f: 3600.0, t: 6 * 60 * 1000 }, FPoint { f: 3600.0, t: 7 * 60 * 1000 }], metric: Labels::empty(), drop_name: true, ..Default::default() }])),
    ]
}

#[test]
fn test_subquery_selector() {
    let mut failures = Vec::new();
    for (load, cases) in subquery_selector_cases() {
        let engine = new_test_engine();
        let store = promqltest::loaded_storage(load).unwrap();
        for (query, start, want) in cases {
            let ctx = Context::background();
            let mut qry = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, query, start).unwrap();
            let res = qry.exec(&ctx);
            if let Some(e) = res.err {
                failures.push(format!("{}: {}", query, e));
                continue;
            }
            let mut got = res.value.unwrap();
            sort_value(&mut got);
            if got != want {
                failures.push(format!("{}:\n  want {:?}\n  got  {:?}", query, want, got));
            }
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn subquery_selector_cases() -> Vec<(&'static str, Vec<(&'static str, i64, Value)>)> {
    vec![
        (r#"load 10s
							metric 1 2"#, vec![
            ("metric[20s:10s]", 10000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s]", 10000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 5000 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 2s", 12000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 5000 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 6s", 20000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 5000 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 4s", 35000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2.0, t: 15000 }, FPoint { f: 2.0, t: 20000 }, FPoint { f: 2.0, t: 25000 }, FPoint { f: 2.0, t: 30000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 5s", 35000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2.0, t: 15000 }, FPoint { f: 2.0, t: 20000 }, FPoint { f: 2.0, t: 25000 }, FPoint { f: 2.0, t: 30000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 6s", 35000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2.0, t: 10000 }, FPoint { f: 2.0, t: 15000 }, FPoint { f: 2.0, t: 20000 }, FPoint { f: 2.0, t: 25000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
            ("metric[20s:5s] offset 7s", 35000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 2.0, t: 10000 }, FPoint { f: 2.0, t: 15000 }, FPoint { f: 2.0, t: 20000 }, FPoint { f: 2.0, t: 25000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }])),
        ]),
        (r#"load 10s
							http_requests{job="api-server", instance="0", group="production"}	0+10x1000 100+30x1000
							http_requests{job="api-server", instance="1", group="production"}	0+20x1000 200+30x1000
							http_requests{job="api-server", instance="0", group="canary"}		0+30x1000 300+80x1000
							http_requests{job="api-server", instance="1", group="canary"}		0+40x2000"#, vec![
            (r#"http_requests{group=~"pro.*",instance="0"}[30s:10s]"#, 10020000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 10000.0, t: 10000000 }, FPoint { f: 100.0, t: 10010000 }, FPoint { f: 130.0, t: 10020000 }], metric: from_strings(&["__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"]), ..Default::default() }])),
            (r#"http_requests{group=~"pro.*",instance="0"}[30s1ms:10s]"#, 10020000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 9990.0, t: 9990000 }, FPoint { f: 10000.0, t: 10000000 }, FPoint { f: 100.0, t: 10010000 }, FPoint { f: 130.0, t: 10020000 }], metric: from_strings(&["__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"]), ..Default::default() }])),
            (r#"http_requests{group=~"pro.*",instance="0"}[5m:]"#, 10100000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 9840.0, t: 9840000 }, FPoint { f: 9900.0, t: 9900000 }, FPoint { f: 9960.0, t: 9960000 }, FPoint { f: 130.0, t: 10020000 }, FPoint { f: 310.0, t: 10080000 }], metric: from_strings(&["__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"]), ..Default::default() }])),
            (r#"http_requests{group=~"pro.*",instance="0"}[5m:] offset 20m"#, 10100000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 8640.0, t: 8640000 }, FPoint { f: 8700.0, t: 8700000 }, FPoint { f: 8760.0, t: 8760000 }, FPoint { f: 8820.0, t: 8820000 }, FPoint { f: 8880.0, t: 8880000 }], metric: from_strings(&["__name__", "http_requests", "job", "api-server", "instance", "0", "group", "production"]), ..Default::default() }])),
            (r#"rate(http_requests[1m])[15s:5s]"#, 8000000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 3.0, t: 7990000 }, FPoint { f: 3.0, t: 7995000 }, FPoint { f: 3.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "0", "group", "canary"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 4.0, t: 7990000 }, FPoint { f: 4.0, t: 7995000 }, FPoint { f: 4.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "1", "group", "canary"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 1.0, t: 7990000 }, FPoint { f: 1.0, t: 7995000 }, FPoint { f: 1.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "0", "group", "production"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 2.0, t: 7990000 }, FPoint { f: 2.0, t: 7995000 }, FPoint { f: 2.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "1", "group", "production"]), drop_name: true, ..Default::default() }])),
            (r#"rate(http_requests[1m])[15s1ms:5s]"#, 8000000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 3.0, t: 7985000 }, FPoint { f: 3.0, t: 7990000 }, FPoint { f: 3.0, t: 7995000 }, FPoint { f: 3.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "0", "group", "canary"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 4.0, t: 7985000 }, FPoint { f: 4.0, t: 7990000 }, FPoint { f: 4.0, t: 7995000 }, FPoint { f: 4.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "1", "group", "canary"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 1.0, t: 7985000 }, FPoint { f: 1.0, t: 7990000 }, FPoint { f: 1.0, t: 7995000 }, FPoint { f: 1.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "0", "group", "production"]), drop_name: true, ..Default::default() }, Series { floats: vec![FPoint { f: 2.0, t: 7985000 }, FPoint { f: 2.0, t: 7990000 }, FPoint { f: 2.0, t: 7995000 }, FPoint { f: 2.0, t: 8000000 }], metric: from_strings(&["job", "api-server", "instance", "1", "group", "production"]), drop_name: true, ..Default::default() }])),
            (r#"sum(http_requests{group=~"pro.*"})[30s:10s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 300.0, t: 100000 }, FPoint { f: 330.0, t: 110000 }, FPoint { f: 360.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"sum(http_requests{group=~"pro.*"})[30s:10s]"#, 121000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 300.0, t: 100000 }, FPoint { f: 330.0, t: 110000 }, FPoint { f: 360.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"sum(http_requests{group=~"pro.*"})[30s1ms:10s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 270.0, t: 90000 }, FPoint { f: 300.0, t: 100000 }, FPoint { f: 330.0, t: 110000 }, FPoint { f: 360.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"sum(http_requests)[40s:10s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 900.0, t: 90000 }, FPoint { f: 1000.0, t: 100000 }, FPoint { f: 1100.0, t: 110000 }, FPoint { f: 1200.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"sum(http_requests)[40s1ms:10s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 800.0, t: 80000 }, FPoint { f: 900.0, t: 90000 }, FPoint { f: 1000.0, t: 100000 }, FPoint { f: 1100.0, t: 110000 }, FPoint { f: 1200.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"(sum(http_requests{group=~"p.*"})+sum(http_requests{group=~"c.*"}))[20s:5s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1000.0, t: 105000 }, FPoint { f: 1100.0, t: 110000 }, FPoint { f: 1100.0, t: 115000 }, FPoint { f: 1200.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
            (r#"(sum(http_requests{group=~"p.*"})+sum(http_requests{group=~"c.*"}))[20s1ms:5s]"#, 120000, Value::Matrix(vec![Series { floats: vec![FPoint { f: 1000.0, t: 100000 }, FPoint { f: 1000.0, t: 105000 }, FPoint { f: 1100.0, t: 110000 }, FPoint { f: 1100.0, t: 115000 }, FPoint { f: 1200.0, t: 120000 }], metric: Labels::empty(), ..Default::default() }])),
        ]),
    ]
}

#[test]
fn test_engine_eval_stmt_timestamps() {
    let store = promqltest::loaded_storage("\nload 10s\n  metric 1 2\n").unwrap();
    let mut failures = Vec::new();
    for (i, (query, want, start, end, interval, should_error)) in eval_stmt_timestamps_cases().into_iter().enumerate() {
        let engine = new_test_engine();
        let ctx = Context::background();
        let q: Arc<dyn storage::Queryable> = Arc::new(store.clone());
        let mut qry = if interval == 0 {
            engine.new_instant_query(&ctx, q, None, query, start).unwrap()
        } else {
            engine.new_range_query(&ctx, q, None, query, start, end, interval).unwrap()
        };
        let res = qry.exec(&ctx);
        if should_error {
            if res.err.is_none() {
                failures.push(format!("{} query={}: expected error for the query", i, query));
            }
            continue;
        }
        match (res.err, res.value) {
            (Some(e), _) => failures.push(format!("{} query={}: {}", i, query, e)),
            (None, Some(v)) if v == want => {}
            (None, v) => failures.push(format!("{} query={} failed:\n  want {:?}\n  got  {:?}", i, query, want, v)),
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

fn eval_stmt_timestamps_cases() -> Vec<(&'static str, Value, i64, i64, i64, bool)> {
    vec![
        ("1", Value::Scalar(Scalar { v: 1.0, t: 1000 }), 1000, 0, 0, false),
        ("metric", Value::Vector(vec![Sample { t: 1000, f: 1.0, metric: from_strings(&["__name__", "metric"]), ..Default::default() }]), 1000, 0, 0, false),
        ("metric[20s]", Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }]), 10000, 0, 0, false),
        ("1", Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 1000 }, FPoint { f: 1.0, t: 2000 }], metric: Labels::empty(), ..Default::default() }]), 0, 2000, 1000, false),
        ("metric", Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 1000 }, FPoint { f: 1.0, t: 2000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }]), 0, 2000, 1000, false),
        ("metric", Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 0 }, FPoint { f: 1.0, t: 5000 }, FPoint { f: 2.0, t: 10000 }], metric: from_strings(&["__name__", "metric"]), ..Default::default() }]), 0, 10000, 5000, false),
        (r#"count_values("wrong label!\xff", metric)"#, Value::Scalar(Scalar::default()), 0, 0, 0, true),
    ]
}

#[test]
fn test_parser_config_isolation() {
    let store = promqltest::loaded_storage("\n\t\tload 10s\n\t\t\tmetric 1+1x10\n\t").unwrap();
    let query = "mad_over_time(metric[10s])";
    let ctx = Context::background();
    let mk = |experimental: bool| {
        Engine::new(EngineOpts {
            max_samples: 1000,
            timeout: Duration::from_secs(10),
            parser: Some(promql::parser::Parser::new(promql::parser::Options {
                enable_experimental_functions: experimental,
                ..Default::default()
            })),
            ..Default::default()
        })
    };
    // Engine with the feature disabled rejects.
    let engine = mk(false);
    let err = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, query, 10_000).err().unwrap();
    assert!(err.to_string().contains("parse"), "{}", err);
    // Engine with the feature enabled accepts.
    let engine = mk(true);
    let mut q = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, query, 10_000).unwrap();
    assert!(q.exec(&ctx).err.is_none());
}

#[test]
fn test_unquoted_utf8_names_evaluation() {
    let store = promqltest::loaded_storage(UTF8_LOAD).unwrap();
    let engine = Engine::new(EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_secs(60),
        parser: Some(promql::parser::Parser::new(promql::parser::Options {
            enable_unquoted_utf8_names: true,
            ..Default::default()
        })),
        ..Default::default()
    });
    let ctx = Context::background();
    for (unquoted, quoted) in UTF8_CASES {
        let mut u = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, unquoted, 120_000).unwrap();
        let mut q = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, quoted, 120_000).unwrap();
        let actual = u.exec(&ctx);
        let expected = q.exec(&ctx);
        assert!(actual.err.is_none(), "{}: {:?}", unquoted, actual.err);
        assert!(expected.err.is_none(), "{}: {:?}", quoted, expected.err);
        let av = actual.value.clone().unwrap();
        match &av {
            Value::Vector(v) => assert!(!v.is_empty(), "{}", unquoted),
            Value::Matrix(m) => assert!(!m.is_empty(), "{}", unquoted),
            _ => {}
        }
        assert_eq!(expected.value, actual.value, "{}", unquoted);
        if unquoted == "a or on (__name__) а" || unquoted == r#"ö or on (__name__) {"ö"}"# {
            let Value::Vector(v) = av else { panic!() };
            assert_eq!(2, v.len());
            assert_ne!(v[0].f, v[1].f);
        }
    }
}

struct HintsClampingQueryable(TestStorage);

struct HintsClampingQuerier {
    inner: BoxQuerier,
    mint: i64,
    maxt: i64,
}

impl LabelQuerier for HintsClampingQuerier {
    fn label_values(&self, ctx: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.inner.label_values(ctx, n, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.inner.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }
}

impl Querier for HintsClampingQuerier {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        let Some(h) = hints else { return self.inner.select(ctx, sort, None, m) };
        let mut c = h.clone();
        c.start = c.start.max(self.mint);
        c.end = c.end.min(self.maxt);
        if c.start > c.end {
            return storage::empty_series_set();
        }
        self.inner.select(ctx, sort, Some(&c), m)
    }
}

impl storage::Queryable for HintsClampingQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let inner = self.0.querier(mint, maxt)?;
        Ok(Box::new(HintsClampingQuerier { inner, mint, maxt }))
    }
}

#[test]
fn test_info_includes_metadata_lookback_in_querier_bounds() {
    let st = promqltest::loaded_storage(
        r#"
		load 1m
			metric{instance="a", job="1"} _ _ _ _ _ _ _ _ _ _ 1
			target_info{instance="a", job="1", version="v1"} _ _ _ _ _ _ 1
	"#,
    )
    .unwrap();
    let engine = promqltest::new_test_engine(false, Duration::from_secs(300), promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY);
    let ctx = Context::background();
    let mut q = engine
        .new_instant_query(&ctx, Arc::new(HintsClampingQueryable(st)), None, "info(last_over_time(metric[1m]))", 600_000)
        .unwrap();
    let res = q.exec(&ctx);
    assert!(res.err.is_none(), "{:?}", res.err);
    let v = res.vector().unwrap();
    assert_eq!(1, v.len());
    assert_eq!(from_strings(&["__name__", "metric", "instance", "a", "job", "1", "version", "v1"]), v[0].metric);
}

const UTF8_LOAD: &str = r#"
load 1m
    {"http.server.requests", "service.name"="api", "resource.k8s.namespace"="system"} 0 60 120
    {"http.server.requests", "service.name"="web", "resource.k8s.namespace"="system"} 0 120 240
    service_info{"service.name"="api", "équipe"="infra"} 2 2 2
    service_info{"service.name"="web", "équipe"="frontend"} 3 3 3
    {"温度", "場所"="東京"} 10 20 30
    {"histogram.count"} 1 2 3
    {"target.info"} 4 5 6
    {"foo..bar."} 7 8 9
    a 1 1 1
    {"а"} 2 2 2
    {"ö"} 3 3 3
    {"ö"} 4 4 4
"#;
const UTF8_CASES: [(&str, &str); 12] = [
    (r#"http.server.requests{resource.k8s.namespace="system",service.name="api"}"#, r#"{"http.server.requests","resource.k8s.namespace"="system","service.name"="api"}"#),
    (r#"温度{場所="東京"}"#, r#"{"温度","場所"="東京"}"#),
    (r#"sum by (service.name) (http.server.requests)"#, r#"sum by ("service.name") ({"http.server.requests"})"#),
    (r#"sum without (service.name) (http.server.requests)"#, r#"sum without ("service.name") ({"http.server.requests"})"#),
    (r#"http.server.requests + on (service.name) group_left (équipe) service_info"#, r#"{"http.server.requests"} + on ("service.name") group_left ("équipe") service_info"#),
    (r#"service_info + on (service.name) group_right (équipe) http.server.requests"#, r#"service_info + on ("service.name") group_right ("équipe") {"http.server.requests"}"#),
    (r#"rate(http.server.requests[2m])"#, r#"rate({"http.server.requests"}[2m])"#),
    (r#"histogram.count"#, r#"{"histogram.count"}"#),
    (r#"target.info"#, r#"{"target.info"}"#),
    (r#"foo..bar."#, r#"{"foo..bar."}"#),
    (r#"a or on (__name__) а"#, r#"{"a"} or on (__name__) {"а"}"#),
    (r#"ö or on (__name__) {"ö"}"#, r#"{"ö"} or on (__name__) {"ö"}"#),
];

#[test]
fn test_query_cost_max_series_subquery() {
    let st = promqltest::loaded_storage(
        "\nload 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n  bigmetric{a=\"3\"} 1+1x100\n  other{b=\"1\"} 1+1x100\n  other{b=\"2\"} 1+1x100\n  other{b=\"3\"} 1+1x100\n",
    )
    .unwrap();
    let engine = new_query_cost_engine();
    let ts = 1_000_000;
    for query in [
        "other + sum_over_time(bigmetric[20s:10s])",
        "other + bigmetric @ 1000",
        "bigmetric @ 1000 + other",
        "other + sum_over_time(bigmetric[20s:10s] @ 1000)",
        "other + sum_over_time(bigmetric[20s] @ 1000)",
    ] {
        let ctx = Context::background();
        engine.set_query_limits(6, 0, Duration::ZERO);
        let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, query, ts).unwrap();
        assert!(qry.exec(&ctx).err.is_none(), "{}", query);
        assert_eq!(6, qry.stats().samples.unwrap().total_series, "{}", query);
        let samples = qry.stats().samples.unwrap().samples_read;

        engine.set_query_limits(4, 0, Duration::ZERO);
        let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, query, ts).unwrap();
        assert!(err_is(&qry.exec(&ctx).err, &PromqlError::TooManySeries(env())), "{}", query);
        assert_eq!(5, qry.stats().samples.unwrap().total_series, "{}", query);

        for limit in [samples, samples - 1] {
            engine.set_query_limits(0, limit as u64, Duration::ZERO);
            let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, query, ts).unwrap();
            let err = qry.exec(&ctx).err;
            if limit == samples {
                assert!(err.is_none(), "{}: {:?}", query, err);
            } else {
                assert!(err_is(&err, &PromqlError::TooManySamplesRead(env())), "{}: {:?}", query, err);
            }
        }
    }
}

#[test]
fn test_query_cost_max_series_info() {
    let st = promqltest::loaded_storage(
        "\nload 10s\n  metric{instance=\"a\", job=\"1\"} 1+1x100\n  metric{instance=\"a\", job=\"2\"} 1+1x100\n  metric{instance=\"a\", job=\"3\"} 1+1x100\n  target_info{instance=\"a\", job=\"1\", data=\"one\"} 1+0x100\n  target_info{instance=\"a\", job=\"2\", data=\"two\"} 1+0x100\n  target_info{instance=\"a\", job=\"3\", data=\"three\"} 1+0x100\n",
    )
    .unwrap();
    let engine = new_query_cost_engine();
    engine.set_query_limits(10, 0, Duration::ZERO);
    assert!(exec_instant(&engine, &st, None, "info(metric)", 0).is_none());
    engine.set_query_limits(4, 0, Duration::ZERO);
    assert!(err_is(&exec_instant(&engine, &st, None, "info(metric)", 0), &PromqlError::TooManySeries(env())));
}

struct CountingSeriesSet {
    total: usize,
    produced: usize,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    cur: Option<storage::SeriesArc>,
}

impl storage::SeriesSet for CountingSeriesSet {
    fn next(&mut self) -> bool {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.produced >= self.total {
            return false;
        }
        self.produced += 1;
        self.cur = Some(storage::new_list_series(
            from_strings(&["__name__", "bigmetric", "i", &self.produced.to_string()]),
            Vec::new(),
        ));
        true
    }
    fn at(&self) -> &storage::SeriesArc {
        self.cur.as_ref().unwrap()
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

struct CountingQuerier(Arc<std::sync::atomic::AtomicUsize>);

impl LabelQuerier for CountingQuerier {
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

impl Querier for CountingQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        Box::new(CountingSeriesSet { total: 1000, produced: 0, calls: self.0.clone(), cur: None })
    }
}

#[test]
fn test_query_cost_max_series_early_termination() {
    const MAX_SERIES: u64 = 5;
    const TOTAL_AVAIL: usize = 1000;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = calls.clone();
    let queryable = Arc::new(QueryableFunc(move |_, _| -> Result<BoxQuerier, Error> {
        Ok(Box::new(CountingQuerier(c.clone())))
    }));
    let engine = new_query_cost_engine();
    engine.set_query_limits(MAX_SERIES, 0, Duration::ZERO);
    let ctx = Context::background();
    let mut qry = engine.new_instant_query(&ctx, queryable, None, "bigmetric", 0).unwrap();
    assert!(err_is(&qry.exec(&ctx).err, &PromqlError::TooManySeries(env())));
    let n = calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(MAX_SERIES as usize + 1, n, "expected early termination after exceeding the ceiling");
    assert!(n < TOTAL_AVAIL, "the whole series set must not be materialized");
}


// Counts storage sample accesses before they enter the evaluator's buffer (Go `sampleReadQueryable`).
struct SampleReads {
    reads: std::sync::atomic::AtomicUsize,
    on_read: Option<Box<dyn Fn(usize) + Send + Sync>>,
}

struct SampleReadQueryable {
    inner: Arc<dyn storage::Queryable>,
    owner: Arc<SampleReads>,
}

impl storage::Queryable for SampleReadQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(SampleReadQuerier { inner: self.inner.querier(mint, maxt)?, owner: self.owner.clone() }))
    }
}

struct SampleReadQuerier {
    inner: BoxQuerier,
    owner: Arc<SampleReads>,
}

impl LabelQuerier for SampleReadQuerier {
    fn label_values(&self, ctx: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.inner.label_values(ctx, n, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.inner.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }
}

impl Querier for SampleReadQuerier {
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        Box::new(SampleReadSet { inner: self.inner.select(ctx, sort, hints, m), owner: self.owner.clone(), cur: None })
    }
}

struct SampleReadSet {
    inner: BoxSeriesSet,
    owner: Arc<SampleReads>,
    cur: Option<storage::SeriesArc>,
}

impl storage::SeriesSet for SampleReadSet {
    fn next(&mut self) -> bool {
        if !self.inner.next() {
            return false;
        }
        self.cur = Some(Arc::new(SampleReadSeries { inner: self.inner.at().clone(), owner: self.owner.clone() }));
        true
    }
    fn at(&self) -> &storage::SeriesArc {
        self.cur.as_ref().unwrap()
    }
    fn err(&self) -> Option<Error> {
        self.inner.err()
    }
    fn warnings(&self) -> Annotations {
        self.inner.warnings()
    }
}

struct SampleReadSeries {
    inner: storage::SeriesArc,
    owner: Arc<SampleReads>,
}

impl storage::Series for SampleReadSeries {
    fn labels(&self) -> &Labels {
        self.inner.labels()
    }
    fn iterator<'a>(&'a self, _reuse: Option<chunkenc::AnyIterator<'a>>) -> chunkenc::AnyIterator<'a> {
        chunkenc::AnyIterator::Other(Box::new(SampleReadIterator { inner: self.inner.iterator(None), owner: &self.owner }))
    }
}

struct SampleReadIterator<'a> {
    inner: chunkenc::AnyIterator<'a>,
    owner: &'a SampleReads,
}

impl SampleReadIterator<'_> {
    fn record_read(&self) {
        let n = self.owner.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        if let Some(f) = &self.owner.on_read {
            f(n);
        }
    }
}

impl chunkenc::ChunkIterator for SampleReadIterator<'_> {
    fn next(&mut self) -> chunkenc::ValueType {
        self.inner.next()
    }
    fn seek(&mut self, t: i64) -> chunkenc::ValueType {
        self.inner.seek(t)
    }
    fn at(&self) -> (i64, f64) {
        self.record_read();
        self.inner.at()
    }
    fn at_histogram(&self, h: Option<model::histogram::Histogram>) -> (i64, model::histogram::Histogram) {
        self.record_read();
        self.inner.at_histogram(h)
    }
    fn at_float_histogram(&self, h: Option<model::histogram::FloatHistogram>) -> (i64, model::histogram::FloatHistogram) {
        self.record_read();
        self.inner.at_float_histogram(h)
    }
    fn at_t(&self) -> i64 {
        self.inner.at_t()
    }
    fn at_st(&self) -> i64 {
        self.inner.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.inner.err()
    }
}

fn counted(st: Arc<dyn storage::Queryable>, on_read: Option<Box<dyn Fn(usize) + Send + Sync>>) -> (Arc<SampleReadQueryable>, Arc<SampleReads>) {
    let owner = Arc::new(SampleReads { reads: std::sync::atomic::AtomicUsize::new(0), on_read });
    (Arc::new(SampleReadQueryable { inner: st, owner: owner.clone() }), owner)
}

#[test]
fn test_query_cost_max_samples_read_matrix() {
    use model::histogram::{CUSTOM_BUCKETS_SCHEMA, Histogram, Span, custom_values};
    let mut failures: Vec<String> = Vec::new();
    // Limits: a large matrix window must stop consuming points as soon as its budget is exhausted.
    {
        let st = promqltest::loaded_storage("\nload 10s\n  bigmetric{a=\"1\"} 1+1x1000\n").unwrap();
        let engine = new_query_cost_engine();
        let ts = 10_000_000;
        for (query, reads) in [
            ("bigmetric[10000s]", 1000u64),
            ("sum_over_time(bigmetric[10000s])", 1000),
            ("sum_over_time(bigmetric[10000s] @ 10000)", 1000),
            ("rate(bigmetric[10000s] anchored)", 1001),
            ("rate(bigmetric[10000s] smoothed)", 1001),
        ] {
            let ctx = Context::background();
            engine.set_query_limits(0, reads, Duration::ZERO);
            let mut qry = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, query, ts).unwrap();
            let err = qry.exec(&ctx).err;
            if err.is_some() {
                failures.push(format!("{}: {:?}", query, err));
                continue;
            }
            let got = qry.stats().samples.unwrap().samples_read;
            if got != reads as i64 {
                failures.push(format!("{}: samples read {} != {}", query, got, reads));
            }
            engine.set_query_limits(0, 10, Duration::ZERO);
            let (cq, owner) = counted(Arc::new(st.clone()), None);
            let mut qry = engine.new_instant_query(&ctx, cq, None, query, ts).unwrap();
            let err = qry.exec(&ctx).err;
            if !err_is(&err, &PromqlError::TooManySamplesRead(env())) {
                failures.push(format!("{}: want too many samples read, got {:?}", query, err));
            }
            let got = qry.stats().samples.unwrap().samples_read;
            if got != 11 {
                failures.push(format!("{}: stop before consuming the full window: {} != 11", query, got));
            }
            let r = owner.reads.load(std::sync::atomic::Ordering::SeqCst);
            if r > 12 {
                failures.push(format!("{}: stop before buffering the full window: {} reads", query, r));
            }
        }
    }
    // Buffer accounting.
    for kind in ["float", "histogram", "float histogram", "custom histogram", "mixed", "stale"] {
        let st = TestStorage::new();
        let metric = from_strings(&["__name__", "metric"]);
        let mut app = st.appender_v1();
        let mut interrupted_cost: i64 = 0;
        for i in 0..1001i64 {
            let ts = i * 10000;
            let h = Histogram {
                count: (i + 1) as u64,
                sum: (i + 1) as f64,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![i + 1],
                ..Default::default()
            };
            let mut point_cost = 1i64;
            let r = if kind == "float" || (kind == "mixed" && i % 2 == 0) {
                storage::Appender::append(&mut app, 0, &metric, ts, i as f64)
            } else if kind == "stale" {
                point_cost = 0;
                storage::Appender::append(&mut app, 0, &metric, ts, model::value::stale_nan())
            } else if kind == "histogram" {
                point_cost = ((h.to_float().size() + 8) / 16) as i64;
                storage::Appender::append_histogram(&mut app, 0, &metric, ts, Some(&h), None)
            } else {
                let mut fh = h.to_float();
                if kind == "custom histogram" {
                    fh.schema = CUSTOM_BUCKETS_SCHEMA;
                    fh.custom_values = custom_values(&[1.0, 2.0]);
                }
                point_cost = ((fh.size() + 8) / 16) as i64;
                storage::Appender::append_histogram(&mut app, 0, &metric, ts, None, Some(&fh))
            };
            r.unwrap();
            if i > 0 && i <= 10 {
                interrupted_cost += point_cost;
            }
        }
        assert!(storage::AppenderTransaction::commit(&mut app).is_none());
        let queryable: Arc<dyn storage::Queryable> = Arc::new(st.clone());
        let engine = new_query_cost_engine();
        for expr in [
            "metric[10000s]",
            "metric smoothed",
            "sum_over_time(metric[10000s])",
            "last_over_time(metric[10000s])",
            "histogram_sum(sum_over_time(metric[10000s]))",
            "last_over_time(sum_over_time(metric[10000s])[1s:1s])",
        ] {
            let name = format!("{}/{}", kind, expr);
            let ctx = Context::background();
            engine.set_query_limits(0, 0, Duration::ZERO);
            let mut q = engine.new_instant_query(&ctx, queryable.clone(), None, expr, 10_000_000).unwrap();
            let result = q.exec(&ctx);
            if let Some(e) = result.err {
                failures.push(format!("{}: {}", name, e));
                continue;
            }
            let want = result.value.unwrap().to_string();
            let reads = q.stats().samples.unwrap().samples_read;
            engine.set_query_limits(0, reads as u64, Duration::ZERO);
            let mut q = engine.new_instant_query(&ctx, queryable.clone(), None, expr, 10_000_000).unwrap();
            let result = q.exec(&ctx);
            match (&result.err, &result.value) {
                (None, Some(v)) if v.to_string() == want => {}
                _ => failures.push(format!("{}: with the exact budget: {:?}", name, result.err)),
            }
            let got = q.stats().samples.unwrap().samples_read;
            if got != reads {
                failures.push(format!("{}: samples read {} != {}", name, got, reads));
            }
            if kind == "stale" {
                continue;
            }
            engine.set_query_limits(0, 25, Duration::ZERO);
            let (cq, owner) = counted(queryable.clone(), None);
            let mut q = engine.new_instant_query(&ctx, cq, None, expr, 10_000_000).unwrap();
            let err = q.exec(&ctx).err;
            if !err_is(&err, &PromqlError::TooManySamplesRead(env())) {
                failures.push(format!("{}: want too many samples read, got {:?}", name, err));
            }
            let sr = q.stats().samples.unwrap().samples_read;
            if !(sr > 25 && sr <= 38) {
                failures.push(format!("{}: samples read {} not in (25, 38]", name, sr));
            }
            let r = owner.reads.load(std::sync::atomic::Ordering::SeqCst);
            if r > 27 {
                failures.push(format!("{}: storage input must stop before the full window: {} reads", name, r));
            }
        }
        for quota in [0u64, 1_000_000] {
            let name = format!("{}/cancellation/quota={}", kind, quota);
            let (ctx, cancel) = Context::background().with_cancel();
            let (cq, owner) = counted(
                queryable.clone(),
                Some(Box::new(move |n| {
                    if n == 10 {
                        cancel.cancel();
                    }
                })),
            );
            engine.set_query_limits(0, quota, Duration::ZERO);
            let mut q = engine.new_instant_query(&ctx, cq, None, "metric[10000s]", 10_000_000).unwrap();
            let err = q.exec(&ctx).err;
            if !err_is(&err, &PromqlError::QueryCanceled(env())) {
                failures.push(format!("{}: want canceled, got {:?}", name, err));
            }
            let r = owner.reads.load(std::sync::atomic::Ordering::SeqCst);
            if r != 10 {
                failures.push(format!("{}: {} reads != 10", name, r));
            }
            let sr = q.stats().samples.unwrap().samples_read;
            if sr != interrupted_cost {
                failures.push(format!("{}: samples read {} != {}", name, sr, interrupted_cost));
            }
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}


// -----------------------------------------------------------------------------------------------------------------
// Query cost: duration limits.

/// Go `sleepyQuerier`: Select blocks for `sleep` or until the context is done.
struct SleepyQuerier {
    sleep: Duration,
}

impl LabelQuerier for SleepyQuerier {
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

impl Querier for SleepyQuerier {
    fn select(&self, ctx: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        let end = std::time::Instant::now() + self.sleep;
        while std::time::Instant::now() < end && !ctx.is_done() {
            thread::sleep(Duration::from_millis(1));
        }
        match ctx.err() {
            Some(e) => storage::err_series_set(e),
            None => storage::empty_series_set(),
        }
    }
}

fn sleepy(sleep: Duration) -> Arc<dyn storage::Queryable> {
    Arc::new(QueryableFunc(move |_, _| -> Result<BoxQuerier, Error> { Ok(Box::new(SleepyQuerier { sleep })) }))
}

fn cost_engine_with_timeout(timeout: Duration) -> Engine {
    Engine::new(EngineOpts {
        max_samples: 1_000_000,
        timeout,
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
        enable_query_cost: true,
        parser: Some(promql::parser::Parser::new(promqltest::test_parser_opts())),
        ..Default::default()
    })
}

fn is_timeout(e: &Option<Error>) -> bool {
    matches!(e.as_ref().and_then(PromqlError::find), Some(PromqlError::QueryTimeout(_)))
}

#[test]
fn test_query_cost_max_duration() {
    let engine = cost_engine_with_timeout(Duration::from_secs(100));
    let q = sleepy(Duration::from_secs(1));
    let ctx = Context::background();
    engine.set_query_limits(0, 0, Duration::from_millis(5));
    let mut qry = engine.new_instant_query(&ctx, q.clone(), None, "metric", 0).unwrap();
    assert!(is_timeout(&qry.exec(&ctx).err));
    engine.set_query_limits(0, 0, Duration::ZERO);
    let opts = promql::QueryOpts::with_limits(false, Duration::ZERO, 0, 0, Duration::from_millis(5), None);
    let mut qry = engine.new_instant_query(&ctx, q, Some(&opts), "metric", 0).unwrap();
    assert!(is_timeout(&qry.exec(&ctx).err));
}

#[test]
fn test_query_cost_max_duration_resolution() {
    let query_sleep = Duration::from_millis(500);
    let ms = Duration::from_millis;
    let s = Duration::from_secs;
    // (name, engine timeout, ceiling, requested, want construct error, want error ceiling, want timeout)
    let cases: Vec<(&str, Duration, Duration, Duration, bool, &str, bool)> = vec![
        ("request below the ceiling tightens it", s(100), s(10), ms(5), false, "", true),
        ("request equal to the ceiling is accepted", s(100), ms(5), ms(5), false, "", true),
        ("request above the ceiling is rejected", s(100), ms(5), s(10), true, "5ms", false),
        ("no request falls back to the ceiling", s(100), ms(5), Duration::ZERO, false, "", true),
        ("no request and a generous ceiling allows the query", s(100), s(10), Duration::ZERO, false, "", false),
        ("unset ceiling leaves the query timeout flag in force", ms(5), Duration::ZERO, Duration::ZERO, false, "", true),
        ("unset ceiling accepts a request below the query timeout flag", s(100), Duration::ZERO, ms(5), false, "", true),
        ("unset ceiling rejects a request above the query timeout flag", ms(50), Duration::ZERO, s(10), true, "50ms", false),
        ("ceiling above the query timeout flag is effective on its own", ms(50), s(10), Duration::ZERO, false, "", false),
        ("ceiling above the query timeout flag allows a request above it", ms(50), s(10), s(5), false, "", false),
    ];
    for (name, engine_timeout, ceiling, requested, want_construct, want_ceiling, want_timeout) in cases {
        let engine = cost_engine_with_timeout(engine_timeout);
        engine.set_query_limits(0, 0, ceiling);
        let opts = (!requested.is_zero())
            .then(|| promql::QueryOpts::with_limits(false, Duration::ZERO, 0, 0, requested, None));
        let ctx = Context::background();
        let r = engine.new_instant_query(&ctx, sleepy(query_sleep), opts.as_ref(), "metric", 0);
        if want_construct {
            let err = r.err().expect(name);
            match PromqlError::find(&err) {
                Some(PromqlError::LimitAboveCeiling { limit, requested: rq, ceiling: cl }) => {
                    assert_eq!("max_query_duration", limit, "{}", name);
                    assert_eq!(&util::stats::go_duration_string(requested), rq, "{}", name);
                    assert_eq!(want_ceiling, cl, "{}", name);
                }
                other => panic!("{}: {:?}", name, other),
            }
            continue;
        }
        let mut q = r.expect(name);
        let err = q.exec(&ctx).err;
        if want_timeout {
            assert!(is_timeout(&err), "{}: {:?}", name, err);
        } else {
            assert!(err.is_none(), "{}: {:?}", name, err);
        }
    }
}

#[test]
fn test_query_cost_max_duration_reject_attribution() {
    // adapted: Go runs this under synctest (a fake clock); here real time with the same durations is used.
    let ms = Duration::from_millis;
    // (name, ceiling, requested, caller, cancel, want reject)
    let cases: Vec<(&str, Duration, Duration, Duration, bool, u64)> = vec![
        ("configured duration", ms(100), Duration::ZERO, Duration::ZERO, false, 1),
        ("request duration", Duration::ZERO, ms(100), Duration::ZERO, false, 1),
        ("startup timeout", Duration::ZERO, Duration::ZERO, Duration::ZERO, false, 0),
        ("earlier caller deadline", Duration::from_secs(1), Duration::ZERO, ms(10), false, 0),
        ("caller cancellation", Duration::from_secs(1), Duration::ZERO, ms(10), true, 0),
    ];
    for (name, ceiling, requested, caller, cancel, want_reject) in cases {
        let engine = Engine::new(EngineOpts {
            max_samples: 1000,
            timeout: ms(100),
            enable_query_cost: true,
            ..Default::default()
        });
        engine.set_query_limits(0, 0, ceiling);
        let opts = promql::QueryOpts::with_limits(false, Duration::ZERO, 0, 0, requested, None);
        let mut ctx = Context::background();
        let mut _guard = None;
        if !caller.is_zero() {
            if cancel {
                let (c, token) = ctx.with_cancel();
                ctx = c;
                _guard = Some(thread::spawn(move || {
                    thread::sleep(caller);
                    token.cancel();
                }));
            } else {
                let (c, token) = ctx.with_timeout(caller);
                ctx = c;
                let _ = token;
            }
        }
        let mut q = engine.new_instant_query(&ctx, sleepy(Duration::from_secs(2)), Some(&opts), "metric", 0).unwrap();
        let err = q.exec(&ctx).err;
        if cancel {
            assert!(matches!(err.as_ref().and_then(PromqlError::find), Some(PromqlError::QueryCanceled(_))), "{}: {:?}", name, err);
        } else {
            assert!(is_timeout(&err), "{}: {:?}", name, err);
        }
        assert_eq!(want_reject, engine.query_rejected_total("max_duration"), "{}", name);
    }
}


#[test]
fn test_query_cost_reject_metric_reason() {
    let st = promqltest::loaded_storage(
        "\nload 10s\n  bigmetric{a=\"1\"} 1+1x100\n  bigmetric{a=\"2\"} 1+1x100\n  bigmetric{a=\"3\"} 1+1x100\n",
    )
    .unwrap();
    let cases: [(&str, &str, i64, u64, u64, PromqlError); 2] = [
        ("max_series", "bigmetric", 0, 2, 0, PromqlError::TooManySeries(env())),
        ("max_samples_read", "sum_over_time(bigmetric[100s])", 1000, 0, 1, PromqlError::TooManySamplesRead(env())),
    ];
    for (name, query, ts, max_series, max_samples_read, want) in cases {
        // adapted: Go reads the counter from a Prometheus registry; the engine exposes it directly.
        let engine = new_query_cost_engine();
        engine.set_query_limits(max_series, max_samples_read, Duration::ZERO);
        let err = exec_instant(&engine, &st, None, query, ts * 1000);
        assert!(err_is(&err, &want), "{}: {:?}", name, err);
        for reason in ["max_series", "max_samples_read", "max_duration"] {
            let want = if reason == name { 1 } else { 0 };
            assert_eq!(want, engine.query_rejected_total(reason), "rejection counter for {}", reason);
        }
    }
}

#[test]
fn test_query_cost_samples_accounting_range() {
    let s = promqltest::loaded_storage("\nload 10s\n  metric{instance=\"a\"} 1+1x100\n  metric{instance=\"b\"} 1+1x100\n").unwrap();
    let engine = new_query_cost_engine();
    let cases: Vec<(&str, i64, [i64; 4])> = vec![
        ("metric", 8, [2, 2, 2, 2]),
        ("rate(metric[20s])", 16, [4, 4, 4, 4]),
        ("metric + metric @ 100", 10, [4, 4, 4, 4]),
        ("sum_over_time(metric[10s:10s])", 20, [2, 2, 2, 2]),
        ("sum_over_time(metric[10s:10s] offset 10s)", 20, [2, 2, 2, 2]),
        ("sum_over_time(metric[10s:10s] offset -10s)", 20, [2, 2, 2, 2]),
        ("last_over_time(sum_over_time(metric[60s])[10s:10s])", 30, [12, 12, 12, 12]),
        ("last_over_time(last_over_time(sum_over_time(metric[60s])[10s:10s])[10s:10s])", 30, [12, 12, 12, 12]),
        ("sum_over_time(metric[60s:10s])", 30, [12, 12, 12, 12]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ 200)", 4, [4, 4, 4, 4]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ 50)", 4, [4, 4, 4, 4]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ 200 offset 1m)", 4, [4, 4, 4, 4]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ 200 offset -1m)", 4, [4, 4, 4, 4]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ start())", 4, [4, 4, 4, 4]),
        ("quantile_over_time(time() / 1000, metric[20s:10s] @ end())", 4, [4, 4, 4, 4]),
    ];
    let mut failures = Vec::new();
    for (query, reads, totals) in cases {
        for per_step in [false, true] {
            let name = format!("{}/perStep={}", query, per_step);
            let ctx = Context::background();
            engine.set_query_limits(0, reads as u64, Duration::ZERO);
            let opts = promql::QueryOpts::new(per_step, Duration::ZERO, None);
            let mut q = engine.new_range_query(&ctx, Arc::new(s.clone()), Some(&opts), query, 100_000, 200_000, 30_000).unwrap();
            let err = q.exec(&ctx).err;
            if err.is_some() {
                failures.push(format!("{}: {:?}", name, err));
                continue;
            }
            let st = q.stats().samples.unwrap();
            let series = if query == "metric + metric @ 100" { 4 } else { 2 };
            if st.total_series != series {
                failures.push(format!("{}: series {} != {}", name, st.total_series, series));
            }
            if st.samples_read != reads {
                failures.push(format!("{}: reads {} != {}", name, st.samples_read, reads));
            }
            let total: i64 = totals.iter().sum();
            if st.total_samples != total {
                failures.push(format!("{}: total {} != {}", name, st.total_samples, total));
            }
            if per_step {
                if st.total_samples_per_step != totals {
                    failures.push(format!("{}: totals {:?} != {:?}", name, st.total_samples_per_step, totals));
                }
                let r: i64 = st.samples_read_per_step.iter().sum();
                if r != st.samples_read {
                    failures.push(format!("{}: per-step reads {} != {}", name, r, st.samples_read));
                }
                if query.starts_with("quantile_over_time(") && st.samples_read_per_step != [reads, 0, 0, 0] {
                    failures.push(format!("{}: per-step reads {:?}", name, st.samples_read_per_step));
                }
            }
            engine.set_query_limits(0, (reads - 1) as u64, Duration::ZERO);
            let mut limited =
                engine.new_range_query(&ctx, Arc::new(s.clone()), Some(&opts), query, 100_000, 200_000, 30_000).unwrap();
            let err = limited.exec(&ctx).err;
            if !err_is(&err, &PromqlError::TooManySamplesRead(env())) {
                failures.push(format!("{}: limited: {:?}", name, err));
            }
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}


#[test]
fn test_query_start_timestamps_override() {
    let opts = promqltest::teststorage::Options { use_xor2: true, enable_st_storage: true, ..Default::default() };
    let st = promqltest::loaded_storage_with_options("load 10s\n  metric@st 0 -5s\n  metric 1 1\n", opts).unwrap();
    let cases: [(&str, bool, Option<bool>, &str); 4] = [
        ("Override to false (engine default: true)", true, Some(false), "{} => 0 @[10000]"),
        ("Override to true on false default (engine default: false)", false, Some(true), "{} => 2 @[10000]"),
        ("Default is used when nil (engine default: true)", true, None, "{} => 2 @[10000]"),
        ("Default is used when nil (engine default: false)", false, None, "{} => 0 @[10000]"),
    ];
    for (name, engine_st, query_st, expected) in cases {
        let engine = Engine::new(EngineOpts {
            use_start_timestamps: engine_st,
            timeout: Duration::from_secs(300),
            max_samples: promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
            ..Default::default()
        });
        let opts = promql::QueryOpts::new(false, Duration::ZERO, query_st);
        let ctx = Context::background();
        let mut q = engine.new_instant_query(&ctx, Arc::new(st.clone()), Some(&opts), "increase(metric[20s])", 10_000).unwrap();
        let res = q.exec(&ctx);
        assert!(res.err.is_none(), "{}: {:?}", name, res.err);
        assert_eq!(expected, res.value.unwrap().to_string(), "{}", name);
    }
}

#[test]
fn test_type_and_unit_metadata_labels_in_functions_and_matching() {
    let st = promqltest::loaded_storage(
        r#"load 1m
  hist_metric{__type__="histogram", __unit__="seconds", job="api"} {{schema:1 sum:10 count:5 buckets:[1 2 2]}}
  lhs_metric{__type__="counter", __unit__="requests", job="api", instance="0"} 10
  rhs_metric{__type__="gauge", __unit__="seconds", job="only-rhs", instance="1"} 20
  target_info{__type__="info", job="api", instance="0", env="prod"} 1
"#,
    )
    .unwrap();
    for delayed in [false, true] {
        let engine = Engine::new(EngineOpts {
            timeout: Duration::from_secs(300),
            max_samples: promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
            enable_delayed_name_removal: delayed,
            enable_type_and_unit_labels: true,
            parser: Some(promql::parser::Parser::new(promql::parser::Options {
                enable_binop_fill_modifiers: true,
                enable_experimental_functions: true,
                ..Default::default()
            })),
            ..Default::default()
        });
        for (expr, expected) in [
            ("histogram_count(hist_metric)", r#"{job="api"} => 5 @[0]"#),
            ("histogram_sum(hist_metric)", r#"{job="api"} => 10 @[0]"#),
            (r#"absent(nonexistent{__type__="counter", __unit__="seconds", job="api"})"#, r#"{job="api"} => 1 @[0]"#),
            ("sum without (instance) (lhs_metric)", r#"{job="api"} => 10 @[0]"#),
            ("lhs_metric > ignoring(instance) fill_left(100) rhs_metric", r#"{job="only-rhs"} => 100 @[0]"#),
            ("info(sum by (job, instance) (lhs_metric))", r#"{env="prod", instance="0", job="api"} => 10 @[0]"#),
        ] {
            let ctx = Context::background();
            let mut q = engine.new_instant_query(&ctx, Arc::new(st.clone()), None, expr, 0).unwrap();
            let res = q.exec(&ctx);
            assert!(res.err.is_none(), "delayed={} {}: {:?}", delayed, expr, res.err);
            assert_eq!(expected, res.value.unwrap().to_string(), "delayed={} query: {}", delayed, expr);
        }
    }
}


#[test]
fn test_rate_annotations() {
    let mut failures = Vec::new();
    for (name, data, expr, type_and_unit, want_warnings, want_infos) in rate_annotation_cases() {
        let store = promqltest::loaded_storage(&format!("load 1m\n{}", data.trim())).unwrap();
        let engine = Engine::new(EngineOpts {
            max_samples: promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
            timeout: Duration::from_secs(6000),
            no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
            enable_at_modifier: true,
            enable_negative_offset: true,
            enable_delayed_name_removal: true,
            enable_type_and_unit_labels: type_and_unit,
            ..Default::default()
        });
        let ctx = Context::background();
        let mut q = engine.new_instant_query(&ctx, Arc::new(store), None, expr, 60_000).unwrap();
        let res = q.exec(&ctx);
        if let Some(e) = res.err {
            failures.push(format!("{}: {}", name, e));
            continue;
        }
        let (warnings, infos) = res.warnings.as_strings(expr, 0, 0);
        if warnings != want_warnings || infos != want_infos {
            failures.push(format!("{}:\n  warnings {:?} want {:?}\n  infos {:?} want {:?}", name, warnings, want_warnings, infos, want_infos));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn test_histogram_copy_from_iterator_regression() {
    use model::histogram::{CounterResetHint, FloatHistogram, Span};
    let store = promqltest::loaded_storage(
        "load 1m\nhistogram {{sum:4 count:4 buckets:[2 2]}} {{sum:6 count:6 buckets:[3 3]}} {{sum:1 count:1 buckets:[1]}}\n",
    )
    .unwrap();
    let engine = promqltest::new_test_engine(false, Duration::ZERO, promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY);
    let verify = |q: &mut promql::Query, expected: Vec<FloatHistogram>| {
        let res = q.exec(&Context::background());
        assert!(res.err.is_none(), "{:?}", res.err);
        let Some(Value::Matrix(m)) = res.value else { panic!("not a matrix") };
        assert_eq!(1, m.len());
        let series = &m[0];
        assert!(series.floats.is_empty());
        assert_eq!(expected.len(), series.histograms.len());
        for (i, e) in expected.iter().enumerate() {
            let mut h = (*series.histograms[i].h).clone();
            h.counter_reset_hint = CounterResetHint::UnknownCounterReset; // Don't care.
            assert_eq!(e, &h);
        }
    };
    let ctx = Context::background();
    let mut q = engine.new_range_query(&ctx, Arc::new(store.clone()), None, "increase(histogram[90s])", 0, 60_000, 60_000).unwrap();
    verify(
        &mut q,
        vec![FloatHistogram {
            count: 3.0,
            sum: 3.0,
            positive_spans: vec![Span::new(0, 2)],
            positive_buckets: vec![1.5, 1.5],
            ..Default::default()
        }],
    );
    let mut q = engine.new_instant_query(&ctx, Arc::new(store.clone()), None, "histogram[61s]", 120_000).unwrap();
    verify(
        &mut q,
        vec![
            FloatHistogram {
                count: 6.0,
                sum: 6.0,
                positive_spans: vec![Span::new(0, 2)],
                positive_buckets: vec![3.0, 3.0],
                ..Default::default()
            },
            FloatHistogram {
                count: 1.0,
                sum: 1.0,
                positive_spans: vec![Span::new(0, 1)],
                positive_buckets: vec![1.0],
                ..Default::default()
            },
        ],
    );
}

fn rate_annotation_cases() -> Vec<(&'static str, &'static str, &'static str, bool, Vec<&'static str>, Vec<&'static str>)> {
    vec![
        ("info annotation when two samples are selected", r#"
				series 1 2
			"#, "rate(series[1m1s])", false, vec![], vec![r#"PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "series" (1:6)"#]),
        ("no info annotations when no samples", r#"
				series
			"#, "rate(series[1m1s])", false, vec![], vec![]),
        ("no info annotations when selecting one sample", r#"
				series 1 2
			"#, "rate(series[10s])", false, vec![], vec![]),
        ("no info annotations when no samples due to mixed data types", r#"
				series{label="a"} 1 {{schema:1 sum:15 count:10 buckets:[1 2 3]}}
			"#, "rate(series[1m1s])", false, vec![r#"PromQL warning: encountered a mix of histograms and floats for metric name "series" (1:6)"#], vec![]),
        ("no info annotations when selecting two native histograms", r#"
				series{label="a"} {{schema:1 sum:10 count:5 buckets:[1 2 3]}} {{schema:1 sum:15 count:10 buckets:[1 2 3]}}
			"#, "rate(series[1m1s])", false, vec![], vec![]),
        ("info annotation when rate() over series without _total suffix", r#"
				series{label="a"} 1 2 3
			"#, "rate(series[1m1s])", false, vec![], vec![r#"PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "series" (1:6)"#]),
        ("info annotation when increase() over series without _total suffix", r#"
				series{label="a"} 1 2 3
			"#, "increase(series[1m1s])", false, vec![], vec![r#"PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "series" (1:10)"#]),
        ("info annotation when rate() over series without __type__=counter label", r#"
				series{label="a"} 1 2 3
			"#, "rate(series[1m1s])", true, vec![], vec![r#"PromQL info: metric might not be a counter, __type__ label is not set to "counter" or "histogram", got "": "series" (1:6)"#]),
        ("info annotation when increase() over series without __type__=counter label", r#"
				series{label="a"} 1 2 3
			"#, "increase(series[1m1s])", true, vec![], vec![r#"PromQL info: metric might not be a counter, __type__ label is not set to "counter" or "histogram", got "": "series" (1:10)"#]),
        ("no info annotation when rate() over series with __type__=counter label", r#"
				series{label="a", __type__="counter"} 1 2 3
			"#, "rate(series[1m1s])", true, vec![], vec![]),
        ("no info annotation when rate() over series with __type__=histogram label", r#"
				series{label="a", __type__="histogram"} 1 2 3
			"#, "rate(series[1m1s])", true, vec![], vec![]),
        ("no info annotation when increase() over series with __type__=counter label", r#"
				series{label="a", __type__="counter"} 1 2 3
			"#, "increase(series[1m1s])", true, vec![], vec![]),
        ("no info annotation when increase() over series with __type__=histogram label", r#"
				series{label="a", __type__="histogram"} 1 2 3
			"#, "increase(series[1m1s])", true, vec![], vec![]),
        ("no info annotation when rate() over series with _total suffix", r#"
				series_total{label="a"} 1 2 3
			"#, "rate(series_total[1m1s])", false, vec![], vec![]),
        ("no info annotation when rate() over series with _sum suffix", r#"
				series_sum{label="a"} 1 2 3
			"#, "rate(series_sum[1m1s])", false, vec![], vec![]),
        ("no info annotation when rate() over series with _count suffix", r#"
				series_count{label="a"} 1 2 3
			"#, "rate(series_count[1m1s])", false, vec![], vec![]),
        ("no info annotation when rate() over series with _bucket suffix", r#"
				series_bucket{label="a"} 1 2 3
			"#, "rate(series_bucket[1m1s])", false, vec![], vec![]),
        ("no info annotation when increase() over series with _total suffix", r#"
				series_total{label="a"} 1 2 3
			"#, "increase(series_total[1m1s])", false, vec![], vec![]),
        ("no info annotation when increase() over series with _sum suffix", r#"
				series_sum{label="a"} 1 2 3
			"#, "increase(series_sum[1m1s])", false, vec![], vec![]),
        ("no info annotation when increase() over series with _count suffix", r#"
				series_count{label="a"} 1 2 3
			"#, "increase(series_count[1m1s])", false, vec![], vec![]),
        ("no info annotation when increase() over series with _bucket suffix", r#"
				series_bucket{label="a"} 1 2 3
			"#, "increase(series_bucket[1m1s])", false, vec![], vec![]),
    ]
}
