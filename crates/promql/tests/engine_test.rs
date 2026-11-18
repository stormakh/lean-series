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
