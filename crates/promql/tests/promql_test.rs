//! Port of `promql/promql_test.go` (with the data setup and cases of `bench_test.go` that it uses).

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use model::labels::{Labels, from_strings};
use promql::promqltest::{self, TestStorage};
use promql::{Engine, EngineOpts};
use storage::{AppendV2Options, AppenderTransaction, AppenderV2, Context};

fn new_test_engine() -> Engine {
    promqltest::new_test_engine(false, Duration::ZERO, promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY)
}

fn run_with_big_stack<F: FnOnce() + Send + 'static>(f: F) {
    thread::Builder::new().stack_size(256 << 20).spawn(f).unwrap().join().unwrap();
}

#[test]
fn test_evaluations() {
    run_with_big_stack(|| {
        let engine = new_test_engine();
        let mut failures = Vec::new();
        let errs = promqltest::run_builtin_tests(&engine, &mut |file, o| {
            if let Err(e) = &o.result {
                failures.push(format!("{}: {}", file, e));
            }
        });
        for (file, e) in errs {
            failures.push(format!("{}: {}", file, e));
        }
        assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
    });
}

/// Go `setupRangeQueryTestData` (bench_test.go).
fn setup_range_query_test_data(stor: &TestStorage, interval: i64, num_intervals: usize) {
    let mut metrics: Vec<Labels> = Vec::new();
    metrics.push(from_strings(&["__name__", "a_one"]));
    metrics.push(from_strings(&["__name__", "b_one"]));
    for j in 0..10 {
        metrics.push(from_strings(&["__name__", "h_one", "le", &j.to_string()]));
    }
    metrics.push(from_strings(&["__name__", "h_one", "le", "+Inf"]));
    for i in 0..10 {
        let l = i.to_string();
        metrics.push(from_strings(&["__name__", "a_ten", "l", &l]));
        metrics.push(from_strings(&["__name__", "b_ten", "l", &l]));
        for j in 0..10 {
            metrics.push(from_strings(&["__name__", "h_ten", "l", &l, "le", &j.to_string()]));
        }
        metrics.push(from_strings(&["__name__", "h_ten", "l", &l, "le", "+Inf"]));
    }
    for i in 0..100 {
        let l = i.to_string();
        metrics.push(from_strings(&["__name__", "a_hundred", "l", &l]));
        metrics.push(from_strings(&["__name__", "b_hundred", "l", &l]));
        for j in 0..10 {
            metrics.push(from_strings(&["__name__", "h_hundred", "l", &l, "le", &j.to_string()]));
        }
        metrics.push(from_strings(&["__name__", "h_hundred", "l", &l, "le", "+Inf"]));
    }
    let mut refs = vec![0u64; metrics.len()];
    let points_per_sparse_series = num_intervals / 50;
    let opts = AppendV2Options::default();
    for s in 0..num_intervals {
        let mut a = stor.appender_v2();
        let ts = s as i64 * interval;
        for (i, m) in metrics.iter().enumerate() {
            let v = s as f64 + i as f64 / metrics.len() as f64;
            if let Ok(r) = a.append(refs[i], m, 0, ts, v, None, None, &opts) {
                refs[i] = r;
            }
        }
        let metric = from_strings(&["__name__", "sparse", "l", &(s / points_per_sparse_series).to_string()]);
        a.append(0, &metric, 0, ts, s as f64 / metrics.len() as f64, None, None, &opts).unwrap();
        assert!(a.commit().is_none());
    }
}

/// Go `rangeQueryCases` (bench_test.go).
fn range_query_cases() -> Vec<(String, usize)> {
    let base: &[(&str, usize)] = &[
        ("a_X", 0),
        ("rate(a_X[1m])", 0),
        ("rate(a_X[1m])", 10000),
        ("rate(sparse[1m])", 10000),
        ("rate(a_X[1m] smoothed)", 0),
        ("rate(a_X[1m] smoothed)", 10000),
        ("rate(sparse[1m] smoothed)", 10000),
        ("double_exponential_smoothing(a_X[1d], 0.3, 0.3)", 0),
        ("changes(a_X[1d])", 0),
        ("rate(a_X[1d])", 0),
        ("absent_over_time(a_X[1d])", 0),
        ("-a_X", 0),
        ("a_X - b_X", 0),
        ("a_X - b_X", 10000),
        ("a_X and b_X{l=~'.*[0-4]$'}", 0),
        ("a_X or b_X{l=~'.*[0-4]$'}", 0),
        ("a_X unless b_X{l=~'.*[0-4]$'}", 0),
        ("a_X and b_X{l='notfound'}", 0),
        ("abs(a_X)", 0),
        ("label_replace(a_X, 'l2', '$1', 'l', '(.*)')", 0),
        ("label_join(a_X, 'l2', '-', 'l', 'l')", 0),
        ("sum(a_X)", 0),
        ("avg(a_X)", 0),
        ("sum without (l)(h_X)", 0),
        ("sum without (le)(h_X)", 0),
        ("sum by (l)(h_X)", 0),
        ("sum by (le)(h_X)", 0),
        ("count_values('value', h_X)", 100),
        ("topk(1, a_X)", 0),
        ("topk(5, a_X)", 0),
        ("limitk(1, a_X)", 0),
        ("limitk(5, a_X)", 0),
        ("limit_ratio(0.1, a_X)", 0),
        ("limit_ratio(0.5, a_X)", 0),
        ("limit_ratio(-0.5, a_X)", 0),
        ("rate(a_X[1m]) + rate(b_X[1m])", 0),
        ("sum without (l)(rate(a_X[1m]))", 0),
        ("sum without (l)(rate(a_X[1m])) / sum without (l)(rate(b_X[1m]))", 0),
        ("histogram_quantile(0.9, rate(h_X[5m]))", 0),
        ("a_X + on(l) group_right a_one", 0),
        ("count({__name__!=\"\"})", 1),
        ("count({__name__!=\"\",l=\"\"})", 1),
        ("quantile_over_time(time() % 100 / 100, a_X[1m:10s] @ 86400)", 0),
        ("timestamp(a_X)", 0),
    ];
    let mut tmp = Vec::new();
    for &(e, s) in base {
        if !e.contains('X') {
            tmp.push((e.to_string(), s));
        } else {
            tmp.push((e.replace('X', "one"), s));
            tmp.push((e.replace('X', "hundred"), s));
        }
    }
    let mut out = Vec::new();
    for (e, s) in tmp {
        if s != 0 {
            out.push((e, s));
        } else {
            out.push((e.clone(), 1));
            out.push((e, 1000));
        }
    }
    out
}

#[test]
fn test_concurrent_range_queries() {
    let stor = TestStorage::new();
    let engine = Engine::new(EngineOpts {
        max_samples: 50_000_000,
        timeout: Duration::from_secs(100),
        enable_at_modifier: true,
        parser: Some(promql::parser::Parser::new(promql::parser::Options {
            enable_experimental_functions: true,
            ..Default::default()
        })),
        ..Default::default()
    });
    let interval = 10_000;
    let num_intervals = 8640 + 10000;
    setup_range_query_test_data(&stor, interval, num_intervals);
    let cases: Vec<(String, usize)> = range_query_cases()
        .into_iter()
        .filter(|(e, s)| !(e.contains("count_values") && *s > 10) && !(e.contains("[1d]") && *s > 100))
        .collect();
    // Run 4 queries at the same time.
    let queue = Arc::new(Mutex::new(cases));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let queue = queue.clone();
        let errors = errors.clone();
        let engine = engine.clone();
        let stor = stor.clone();
        handles.push(thread::Builder::new().stack_size(64 << 20).spawn(move || loop {
            let Some((expr, steps)) = queue.lock().unwrap().pop() else { return };
            let ctx = Context::background();
            let start = ((num_intervals - steps) * 10 * 1000) as i64;
            let end = (num_intervals * 10 * 1000) as i64;
            let r = engine.new_range_query(&ctx, Arc::new(stor.clone()), None, &expr, start, end, 10_000);
            match r {
                Err(e) => errors.lock().unwrap().push(format!("{}: {}", expr, e)),
                Ok(mut q) => {
                    let res = q.exec(&ctx);
                    if let Some(e) = res.err {
                        errors.lock().unwrap().push(format!("Query: {:?}, steps: {}, result: {}", expr, steps, e));
                    }
                    q.close();
                }
            }
        }).unwrap());
    }
    for h in handles {
        h.join().unwrap();
    }
    let errors = errors.lock().unwrap();
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
