//! Port of upstream/rules/manager_test.go.
use std::collections::{BTreeMap, HashMap};
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chunkenc::{ChunkIterator as _, ValueType};
use discovery::metrics::Registry;
use model::labels::{self, Label, Labels, ValidationScheme};
use model::matcher::{MatchType, Matcher};
use model::value::{is_stale_nan, STALE_NAN};
use promql::parser::{Expr, Options, Parser};
use promql::promqltest;
use tsdb::db::DB;

mod common;
use promql::{Engine, EngineOpts, QueryResult, Sample, Value, Vector};
use rules::group::is_independent;
use rules::log::Logger;
use rules::rulefmt;
use rules::{
    build_dependency_map, default_eval_iteration_func, engine_query_func, from_maps, group_key, parse_files, same_rule,
    Alert, AlertState, AlertingRule, DependencyMap, EngineQuery, Group, GroupEvalIterationFunc, GroupOptions, Manager,
    ManagerOptions, NotifyFunc, QueryEngine, QueryFunc, RecordingRule, Rule, RuleHealth, RuleRef,
    SequentialRuleEvalController, Time,
};
use storage::{AppendV2Options, AppendableV2, BoxSeriesSet, Context, Error, Queryable};

// ---------------- helpers ----------------

fn fixture(f: &str) -> String {
    format!("{}/../../upstream/rules/fixtures/{f}", env!("CARGO_MANIFEST_DIR"))
}

/// A temporary directory under bench/tmp/w3-rules (Go `t.TempDir()`), removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let p = std::path::PathBuf::from(format!(
            "{}/../../bench/tmp/w3-rules/manager_test-{name}-{}",
            env!("CARGO_MANIFEST_DIR"),
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn path(&self, f: &str) -> String {
        self.0.join(f).to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Go `testParser` (recording_test.go): `parser.NewParser(parser.Options{})`.
fn test_parser() -> Parser {
    Parser::new(Options::default())
}

fn parse(q: &str) -> Expr {
    test_parser().parse_expr(q).unwrap()
}

/// Go `testEngine` (alerting_test.go).
fn test_engine() -> Arc<Engine> {
    Arc::new(promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60 * 1000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats: true,
        ..Default::default()
    }))
}

/// The engine options most tests here use: `MaxSamples: 10, Timeout: 10 * time.Second`.
fn small_engine() -> Arc<Engine> {
    Arc::new(promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10,
        timeout: Duration::from_secs(10),
        ..Default::default()
    }))
}

fn query_func(engine: &Arc<Engine>, st: &Arc<DB>) -> QueryFunc {
    let q: Arc<dyn Queryable> = st.clone();
    engine_query_func(engine.clone(), q)
}

fn nop() -> Logger {
    Logger::nop()
}

fn noop_notify() -> NotifyFunc {
    Arc::new(|_: &Context, _: &str, _: Vec<Alert>| {})
}

fn ls(ss: &[&str]) -> Labels {
    labels::from_strings(ss)
}

fn dur_ns(d: Duration) -> i64 {
    d.as_nanos() as i64
}

#[allow(clippy::too_many_arguments)]
fn new_alerting_rule(name: &str, expr: impl Into<Option<Expr>>, hold: Duration, keep: Duration, lbls: Labels, ann: Labels, restored: bool) -> Arc<AlertingRule> {
    Arc::new(AlertingRule::new(name, expr, hold, keep, lbls, ann, Labels::empty(), "", restored, nop()))
}

fn new_recording_rule(name: &str, expr: impl Into<Option<Expr>>, lbls: Labels) -> Arc<RecordingRule> {
    Arc::new(RecordingRule::new(name, expr, lbls))
}

/// Go's nil parser.Expr.
fn nil_expr() -> Option<Expr> {
    None
}

fn group_samples(g: &Group) -> f64 {
    g.metrics().group_samples.with_label_values(&[&group_key(g.file(), g.name())]).get()
}

/// Calls `Stop` on drop (Go `defer ruleManager.Stop()`).
struct StopOnDrop<'a>(&'a Manager);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

fn sort_by_labels(v: &mut [Sample]) {
    v.sort_by(|a, b| labels::compare(&a.metric, &b.metric).cmp(&0));
}

/// Go `sortAlerts`.
fn sort_alerts(items: &mut [Alert]) {
    items.sort_by(|a, b| labels::compare(&a.labels, &b.labels).cmp(&0));
}

/// Go `readSeriesSet`: converts a SeriesSet into a map from series labels to points.
fn read_series_set(mut ss: BoxSeriesSet) -> Result<HashMap<String, Vec<(i64, f64)>>, Error> {
    let mut result = HashMap::new();
    while ss.next() {
        let series = ss.at().clone();
        let mut points = Vec::new();
        let mut it = series.iterator(None);
        while it.next() == ValueType::Float {
            points.push(it.at());
        }
        result.insert(series.labels().to_string(), points);
    }
    match ss.err() {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

fn eq_matcher(n: &str, v: &str) -> Matcher {
    Matcher::new(MatchType::Equal, n, v).unwrap()
}

fn same_rules(a: &[RuleRef], b: &[RuleRef]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| same_rule(x, y))
}

/// Go `require.ElementsMatch` on rules (by identity).
fn elements_match(a: &[RuleRef], b: &[RuleRef]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut used = vec![false; b.len()];
    for x in a {
        match b.iter().enumerate().position(|(i, y)| !used[i] && same_rule(x, y)) {
            Some(i) => used[i] = true,
            None => return false,
        }
    }
    true
}

fn names(v: &[RuleRef]) -> Vec<String> {
    v.iter().map(|r| r.name().to_string()).collect()
}

/// Go `depMap.dependents(r)` on a possibly nil map.
fn dependents(m: &Option<DependencyMap>, r: &RuleRef) -> Vec<RuleRef> {
    m.as_ref().map(|m| m.dependents(r)).unwrap_or_default()
}

/// Go `depMap.dependencies(r)` on a possibly nil map.
fn dependencies(m: &Option<DependencyMap>, r: &RuleRef) -> Vec<RuleRef> {
    m.as_ref().map(|m| m.dependencies(r)).unwrap_or_default()
}

/// Go `depMap.isIndependent(r)`.
fn independent(m: &Option<DependencyMap>, r: &RuleRef) -> bool {
    is_independent(m.as_ref(), r)
}

/// Go `require.Empty(depMap)`: nil or no entries.
fn dep_map_empty(m: &Option<DependencyMap>) -> bool {
    m.as_ref().is_none_or(|m| m.is_empty())
}

/// Go `reflect.DeepEqual` of two dependency maps (a nil map differs from a non-nil one), over the rules of a group.
fn dep_maps_equal(a: Option<&DependencyMap>, b: Option<&DependencyMap>, rules: &[RuleRef]) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.len() == b.len() && rules.iter().all(|r| same_rules(&a.dependents(r), &b.dependents(r))),
        _ => false,
    }
}

// ---------------- tests ----------------

fn alerting_rule_results(name: &str) -> Vec<Sample> {
    let mk = |state: Option<&str>, instance: &str| {
        let mut v = vec!["__name__", name, "alertname", "HTTPRequestRateLow"];
        if let Some(s) = state {
            v.extend(["alertstate", s]);
        }
        v.extend(["group", "canary", "instance", instance, "job", "app-server", "severity", "critical"]);
        Sample { metric: ls(&v), f: 1.0, ..Default::default() }
    };
    if name == "ALERTS" {
        vec![mk(Some("pending"), "0"), mk(Some("pending"), "1"), mk(Some("firing"), "0"), mk(Some("firing"), "1")]
    } else {
        vec![mk(None, "0"), mk(None, "1"), mk(None, "0"), mk(None, "1")]
    }
}

#[test]
fn test_alerting_rule() {
    let storage_owner = common::loaded_storage(r#"
		load 5m
			http_requests{job="app-server", instance="0", group="canary", severity="overwrite-me"}	75 85  95 105 105  95  85
			http_requests{job="app-server", instance="1", group="canary", severity="overwrite-me"}	80 90 100 110 120 130 140
	"#);
    let storage = storage_owner.db.clone();

    let expr = parse(r#"http_requests{group="canary", job="app-server"} < 100"#);

    let rule = new_alerting_rule(
        "HTTPRequestRateLow",
        expr,
        Duration::from_secs(60),
        Duration::ZERO,
        ls(&["severity", "{{\"c\"}}ritical"]),
        Labels::empty(),
        true,
    );
    let result = alerting_rule_results("ALERTS");

    let base_time = Time::unix(0, 0);
    let m = |x: u64| Duration::from_secs(60 * x);
    let tests: Vec<(Duration, Vec<Sample>)> = vec![
        (Duration::ZERO, result[..2].to_vec()),
        (m(5), result[2..].to_vec()),
        (m(10), result[2..3].to_vec()),
        (m(15), vec![]),
        (m(20), vec![]),
        (m(25), result[..1].to_vec()),
        (m(30), result[2..3].to_vec()),
    ];

    let ng = test_engine();
    for (i, (time, mut expected)) in tests.into_iter().enumerate() {
        let eval_time = base_time.add(time);

        let res = rule.eval(&Context::background(), Duration::ZERO, eval_time, &query_func(&ng, &storage), None, 0).unwrap();

        let mut filtered_res = Vector::new(); // After removing 'ALERTS_FOR_STATE' samples.
        for smpl in &res {
            let smpl_name = smpl.metric.get("__name__");
            if smpl_name == "ALERTS" {
                filtered_res.push(smpl.clone());
            } else {
                // If not 'ALERTS', it has to be 'ALERTS_FOR_STATE'.
                assert_eq!("ALERTS_FOR_STATE", smpl_name);
            }
        }
        for r in expected.iter_mut() {
            r.t = eval_time.millis();
        }
        assert_eq!(
            filtered_res.len(),
            expected.len(),
            "{i}. Number of samples in expected and actual output don't match ({} vs. {})",
            expected.len(),
            res.len()
        );

        sort_by_labels(&mut filtered_res);
        assert_eq!(expected, filtered_res, "case {i}");

        for aa in rule.active_alerts() {
            assert!(aa.labels.get(labels::METRIC_NAME).is_empty(), "__name__ label set on active alert: {}", aa.labels);
        }
    }
}

#[test]
fn test_for_state_add_samples() {
    for query_offset in [Duration::ZERO, Duration::from_secs(60)] {
        let storage_owner = common::loaded_storage(r#"
		load 5m
			http_requests{job="app-server", instance="0", group="canary", severity="overwrite-me"}	75 85  95 105 105  95  85
			http_requests{job="app-server", instance="1", group="canary", severity="overwrite-me"}	80 90 100 110 120 130 140
	"#);
    let storage = storage_owner.db.clone();

        let expr = parse(r#"http_requests{group="canary", job="app-server"} < 100"#);

        let rule = new_alerting_rule(
            "HTTPRequestRateLow",
            expr,
            Duration::from_secs(60),
            Duration::ZERO,
            ls(&["severity", "{{\"c\"}}ritical"]),
            Labels::empty(),
            true,
        );
        let result = alerting_rule_results("ALERTS_FOR_STATE");

        let base_time = Time::unix(0, 0);
        let m = |x: u64| Duration::from_secs(60 * x);
        // (time, result, persistThisTime); `None` is Go's nil result.
        let tests: Vec<(Duration, Option<Vec<Sample>>, bool)> = vec![
            (Duration::ZERO, Some(result[..2].to_vec()), true),
            (m(5), Some(result[2..].to_vec()), false),
            (m(10), Some(result[2..3].to_vec()), false),
            (m(15), None, false),
            (m(20), None, false),
            (m(25), Some(result[..1].to_vec()), true),
            (m(30), Some(result[2..3].to_vec()), false),
        ];

        let ng = test_engine();
        let mut for_state = 0f64;
        for (i, (time, expected, persist_this_time)) in tests.into_iter().enumerate() {
            let eval_time = base_time.add(time).add(query_offset);

            if persist_this_time {
                for_state = eval_time.unix_secs() as f64;
            }
            if expected.is_none() {
                for_state = f64::from_bits(STALE_NAN);
            }

            let res = rule.eval(&Context::background(), query_offset, eval_time, &query_func(&ng, &storage), None, 0).unwrap();

            let mut filtered_res = Vector::new(); // After removing 'ALERTS' samples.
            for smpl in &res {
                let smpl_name = smpl.metric.get("__name__");
                if smpl_name == "ALERTS_FOR_STATE" {
                    filtered_res.push(smpl.clone());
                } else {
                    // If not 'ALERTS_FOR_STATE', it has to be 'ALERTS'.
                    assert_eq!("ALERTS", smpl_name);
                }
            }
            let mut expected = expected.unwrap_or_default();
            for r in expected.iter_mut() {
                r.t = eval_time.sub_dur(query_offset).millis();
                // Updating the expected 'for' state.
                if r.f >= 0.0 {
                    r.f = for_state;
                }
            }
            assert_eq!(
                filtered_res.len(),
                expected.len(),
                "{i}. Number of samples in expected and actual output don't match ({} vs. {})",
                expected.len(),
                res.len()
            );

            sort_by_labels(&mut filtered_res);
            assert_eq!(expected, filtered_res, "queryOffset {query_offset:?} case {i}");

            for aa in rule.active_alerts() {
                assert!(aa.labels.get(labels::METRIC_NAME).is_empty(), "__name__ label set on active alert: {}", aa.labels);
            }
        }
    }
}

#[test]
fn test_for_state_restore_hold_duration() {
    struct Case {
        name: &'static str,
        hold_duration: Duration,
        initial_evaluations: Vec<Duration>,
        restore_duration: Duration,
        prior_state: AlertState,
        active_at: Duration,
        next_state: AlertState,
    }
    let m = |x: u64| Duration::from_secs(60 * x);
    let cases = vec![
        Case {
            name: "firing below grace period",
            hold_duration: m(5),
            initial_evaluations: vec![m(0), m(4), m(6), m(8)],
            restore_duration: m(12),
            prior_state: AlertState::Firing,
            active_at: Duration::ZERO,
            next_state: AlertState::Firing,
        },
        Case {
            name: "firing at grace period",
            hold_duration: m(10),
            initial_evaluations: vec![m(0), m(9), m(11), m(13)],
            restore_duration: m(17),
            prior_state: AlertState::Firing,
            active_at: Duration::ZERO,
            next_state: AlertState::Firing,
        },
        Case {
            name: "firing above grace period",
            hold_duration: m(15),
            initial_evaluations: vec![m(0), m(14), m(16), m(18)],
            restore_duration: m(22),
            prior_state: AlertState::Firing,
            active_at: Duration::ZERO,
            next_state: AlertState::Firing,
        },
        Case {
            name: "pending below grace period restarts hold duration",
            hold_duration: m(5),
            initial_evaluations: vec![m(0), m(2)],
            restore_duration: m(6),
            prior_state: AlertState::Pending,
            active_at: m(5),
            next_state: AlertState::Pending,
        },
        Case {
            name: "pending above grace period excludes outage",
            hold_duration: m(25),
            initial_evaluations: vec![m(0), m(5)],
            restore_duration: m(15),
            prior_state: AlertState::Pending,
            active_at: m(10),
            next_state: AlertState::Pending,
        },
        Case {
            name: "pending near hold duration receives grace period",
            hold_duration: m(25),
            initial_evaluations: vec![m(0), m(20)],
            restore_duration: m(25),
            prior_state: AlertState::Pending,
            active_at: m(10),
            next_state: AlertState::Pending,
        },
        Case {
            name: "firing outside outage tolerance restarts hold duration",
            hold_duration: m(5),
            initial_evaluations: vec![m(0), m(4), m(6), m(8)],
            restore_duration: m(40),
            prior_state: AlertState::Firing,
            active_at: m(39),
            next_state: AlertState::Pending,
        },
        Case {
            name: "zero hold duration keeps evaluated firing timestamp",
            hold_duration: Duration::ZERO,
            initial_evaluations: vec![m(0), m(2)],
            restore_duration: m(6),
            prior_state: AlertState::Firing,
            active_at: Duration::ZERO,
            next_state: AlertState::Firing,
        },
    ];

    for test in cases {
        let name = test.name;
        let st_owner = common::new();
    let st = st_owner.db.clone();
        let expr = parse("vector(1)");
        let opts = Arc::new(ManagerOptions {
            query_func: Some(query_func(&test_engine(), &st)),
            appendable_v2: Some(st.clone()),
            queryable: Some(st.clone()),
            context: Context::background(),
            logger: nop(),
            notify_func: Some(noop_notify()),
            outage_tolerance: m(30),
            for_grace_period: m(10),
            ..Default::default()
        });
        const ALERT_NAME: &str = "AlwaysActive";
        let base_time = Time::unix(1_700_000_000, 0);
        let rule = new_alerting_rule(ALERT_NAME, expr.clone(), test.hold_duration, Duration::ZERO, Labels::empty(), Labels::empty(), true);
        let group = Group::new(GroupOptions {
            name: "default".into(),
            interval: Duration::from_secs(60),
            rules: vec![rule.clone()],
            opts: Some(opts.clone()),
            ..Default::default()
        });
        let ctx = Context::background();
        for evaluation in &test.initial_evaluations {
            group.eval(&ctx, base_time.add(*evaluation));
        }
        assert_eq!(test.prior_state, rule.state(), "{name}");
        if test.prior_state == AlertState::Firing && test.hold_duration > Duration::ZERO {
            // The firing transition occurs at an evaluation, not at ActiveAt + for.
            assert_eq!(base_time.add(test.hold_duration + m(1)), rule.active_alerts()[0].fired_at, "{name}");
        }

        let restored_rule = new_alerting_rule(ALERT_NAME, expr, test.hold_duration, Duration::ZERO, Labels::empty(), Labels::empty(), false);
        let restored_group = Group::new(GroupOptions {
            name: "default".into(),
            interval: Duration::from_secs(60),
            rules: vec![restored_rule.clone()],
            should_restore: true,
            opts: Some(opts.clone()),
            ..Default::default()
        });
        let restore_time = base_time.add(test.restore_duration);
        // Group.run evaluates twice before restoring state after a restart.
        restored_group.eval(&ctx, restore_time.sub_dur(m(1)));
        restored_group.eval(&ctx, restore_time);
        restored_group.restore_for_state(restore_time);
        assert!(restored_rule.restored(), "{name}");
        assert_eq!(restored_rule.active_alerts().len(), 1, "{name}");
        assert_eq!(base_time.add(test.active_at), restored_rule.active_alerts()[0].active_at, "{name}");

        // Eval sets FiredAt when the restored ActiveAt satisfies the hold duration.
        let next_evaluation = restore_time.add(m(1));
        restored_group.eval(&ctx, next_evaluation);
        assert_eq!(test.next_state, restored_rule.state(), "{name}");
        if test.next_state == AlertState::Firing {
            let mut expected_fired_at = next_evaluation;
            if test.hold_duration == Duration::ZERO {
                expected_fired_at = restore_time.sub_dur(m(1));
            }
            assert_eq!(expected_fired_at, restored_rule.active_alerts()[0].fired_at, "{name}");
        } else {
            assert!(restored_rule.active_alerts()[0].fired_at.is_zero(), "{name}");
            // Pending alerts must still wait for the entire remaining hold duration.
            let firing_time = base_time.add(test.active_at + test.hold_duration);
            restored_group.eval(&ctx, firing_time.sub_dur(Duration::from_secs(1)));
            assert_eq!(AlertState::Pending, restored_rule.state(), "{name}");
            restored_group.eval(&ctx, firing_time);
            assert_eq!(AlertState::Firing, restored_rule.state(), "{name}");
            assert_eq!(firing_time, restored_rule.active_alerts()[0].fired_at, "{name}");
        }
    }
}

#[test]
fn test_for_state_restore() {
    for query_offset in [Duration::ZERO, Duration::from_secs(60)] {
        let storage_owner = common::loaded_storage(r#"
		load 5m
		http_requests{job="app-server", instance="0", group="canary", severity="overwrite-me"}	75  85 50 0 0 25 0 0 40 0 120
		http_requests{job="app-server", instance="1", group="canary", severity="overwrite-me"}	125 90 60 0 0 25 0 0 40 0 130
	"#);
    let storage = storage_owner.db.clone();

        let expr = parse(r#"http_requests{group="canary", job="app-server"} < 100"#);

        let ng = test_engine();
        let m = |x: u64| Duration::from_secs(60 * x);
        let opts = Arc::new(ManagerOptions {
            query_func: Some(query_func(&ng, &storage)),
            appendable_v2: Some(storage.clone()),
            queryable: Some(storage.clone()),
            context: Context::background(),
            logger: nop(),
            notify_func: Some(noop_notify()),
            outage_tolerance: m(30),
            for_grace_period: m(10),
            ..Default::default()
        });

        let alert_for_duration = m(25);
        // Initial run before prometheus goes down.
        let rule = new_alerting_rule(
            "HTTPRequestRateLow",
            expr.clone(),
            alert_for_duration,
            Duration::ZERO,
            ls(&["severity", "critical"]),
            Labels::empty(),
            true,
        );

        let group = Group::new(GroupOptions {
            name: "default".into(),
            interval: Duration::from_secs(1),
            rules: vec![rule.clone()],
            should_restore: true,
            opts: Some(opts.clone()),
            ..Default::default()
        });

        let initial_runs = [Duration::ZERO, m(5)];

        let base_time = Time::unix(0, 0);
        for duration in initial_runs {
            group.eval(&Context::background(), base_time.add(duration));
        }

        // Prometheus goes down here. We create new rules and groups.
        struct TestInput {
            name: &'static str,
            restore_duration: Duration,
            expected_alerts: Vec<Alert>,
            num: usize,
            no_restore: bool,
            grace_period: bool,
            down_duration: Duration,
            /// Go `before`: evaluations of the old group at these offsets.
            before: Vec<Duration>,
        }

        let tests = vec![
            TestInput {
                name: "normal restore (alerts were not firing)",
                restore_duration: m(15),
                expected_alerts: rule.active_alerts(),
                down_duration: m(10),
                num: 0,
                no_restore: false,
                grace_period: false,
                before: vec![],
            },
            TestInput {
                name: "outage tolerance",
                restore_duration: m(40),
                no_restore: true,
                num: 2,
                expected_alerts: vec![],
                grace_period: false,
                down_duration: Duration::ZERO,
                before: vec![],
            },
            TestInput {
                name: "no active alerts",
                restore_duration: m(50),
                expected_alerts: vec![],
                num: 0,
                no_restore: false,
                grace_period: false,
                down_duration: Duration::ZERO,
                before: vec![],
            },
            TestInput {
                name: "test the grace period",
                restore_duration: m(25),
                expected_alerts: vec![],
                grace_period: true,
                before: vec![m(10), m(15), m(20)],
                num: 2,
                no_restore: false,
                down_duration: Duration::ZERO,
            },
        ];

        for tt in tests {
            let name = tt.name;
            for duration in &tt.before {
                group.eval(&Context::background(), base_time.add(*duration));
            }

            let new_rule = new_alerting_rule(
                "HTTPRequestRateLow",
                expr.clone(),
                alert_for_duration,
                Duration::ZERO,
                ls(&["severity", "critical"]),
                Labels::empty(),
                false,
            );
            let new_group = Group::new(GroupOptions {
                name: "default".into(),
                interval: Duration::from_secs(1),
                rules: vec![new_rule.clone()],
                should_restore: true,
                opts: Some(opts.clone()),
                query_offset: Some(query_offset),
                ..Default::default()
            });

            let restore_time = base_time.add(tt.restore_duration).add(query_offset);
            // First eval before restoration.
            new_group.eval(&Context::background(), restore_time);
            // Restore happens here.
            new_group.restore_for_state(restore_time);

            let mut got = new_rule.active_alerts();
            for aa in &got {
                assert!(aa.labels.get(labels::METRIC_NAME).is_empty(), "{name}: __name__ label set on active alert: {}", aa.labels);
            }
            got.sort_by(|a, b| labels::compare(&a.labels, &b.labels).cmp(&0));

            // In all cases, we expect the restoration process to have completed.
            assert!(new_rule.restored(), "{name}: expected the rule restoration process to have completed");

            // Checking if we have restored it correctly.
            if tt.no_restore {
                assert_eq!(got.len(), tt.num, "{name}");
                for e in &got {
                    assert_eq!(e.active_at, restore_time, "{name}");
                }
            } else if tt.grace_period {
                assert_eq!(got.len(), tt.num, "{name}");
                for e in &got {
                    assert_eq!(dur_ns(opts.for_grace_period), e.active_at.add(alert_for_duration).sub(restore_time), "{name}");
                }
            } else {
                let mut exp = tt.expected_alerts.clone();
                assert_eq!(got.len(), exp.len(), "{name}");
                sort_alerts(&mut exp);
                sort_alerts(&mut got);
                for (i, e) in exp.iter().enumerate() {
                    assert_eq!(e.labels, got[i].labels, "{name}");

                    // Difference in time should be within 1e6 ns, i.e. 1ms
                    // (due to conversion between ns & ms, float64 & int64).
                    let active_at_diff = query_offset.as_secs_f64()
                        + (e.active_at.unix_secs() + tt.down_duration.as_secs() as i64 - got[i].active_at.unix_secs()) as f64;
                    assert_eq!(0.0, active_at_diff.abs(), "{name}: 'for' state restored time is wrong");
                }
            }
        }
    }
}

#[test]
fn test_staleness() {
    for query_offset in [Duration::ZERO, Duration::from_secs(60)] {
        let st_owner = common::new();
    let st = st_owner.db.clone();

        let engine = small_engine();
        let opts = Arc::new(ManagerOptions {
            query_func: Some(query_func(&engine, &st)),
            appendable_v2: Some(st.clone()),
            queryable: Some(st.clone()),
            context: Context::background(),
            logger: nop(),
            ..Default::default()
        });

        let expr = parse("a + 1");
        let rule = new_recording_rule("a_plus_one", expr, Labels::empty());
        let group = Group::new(GroupOptions {
            name: "default".into(),
            interval: Duration::from_secs(1),
            rules: vec![rule],
            should_restore: true,
            opts: Some(opts),
            query_offset: Some(query_offset),
            ..Default::default()
        });

        // A time series that has two samples and then goes stale.
        let mut app = st.appender_v2(&Context::background());
        let a = ls(&["__name__", "a"]);
        let o = AppendV2Options::default();
        app.append(0, &a, 0, 0, 1.0, None, None, &o).unwrap();
        app.append(0, &a, 0, 1000, 2.0, None, None, &o).unwrap();
        app.append(0, &a, 0, 2000, f64::from_bits(STALE_NAN), None, None, &o).unwrap();
        assert!(app.commit().is_none());

        let ctx = Context::background();

        // Execute 3 times, 1 second apart.
        group.eval(&ctx, Time::unix(0, 0).add(query_offset));
        group.eval(&ctx, Time::unix(1, 0).add(query_offset));
        group.eval(&ctx, Time::unix(2, 0).add(query_offset));

        let mut querier = st.querier(0, 2000).unwrap();

        let matcher = eq_matcher("__name__", "a_plus_one");

        let set = querier.select(&ctx, false, None, &[matcher]);
        let mut samples = read_series_set(set).unwrap();

        let metric = ls(&["__name__", "a_plus_one"]).to_string();
        let metric_sample = samples.get_mut(&metric).unwrap_or_else(|| panic!("Series {metric} not returned."));
        assert!(
            is_stale_nan(metric_sample[2].1),
            "Appended second sample not as expected. Wanted: stale NaN Got: {:x}",
            metric_sample[2].1.to_bits()
        );
        metric_sample[2].1 = 42.0; // assert_eq cannot handle NaN.

        let want: HashMap<String, Vec<(i64, f64)>> = HashMap::from([(metric, vec![(0, 2.0), (1000, 3.0), (2000, 42.0)])]);
        assert_eq!(want, samples);
        querier.close();
    }
}

#[test]
fn test_group_query_offset() {
    let config = r#"
groups:
  - name: group1
    query_offset: 2m
  - name: group2
    query_offset: 0s
  - name: group3
"#;

    let dir = TempDir::new("query_offset");
    let fname = dir.path("rules.yaml");
    std::fs::write(&fname, config).unwrap();

    let m = Manager::new(ManagerOptions {
        logger: nop(),
        default_rule_query_offset: Some(Arc::new(|| Duration::from_secs(60))),
        ..Default::default()
    });
    m.start();
    m.update(Duration::from_secs(1), &[fname], &Labels::empty(), "", None).unwrap();

    let mut rgs = m.rule_groups();
    rgs.sort_by(|a, b| a.name().cmp(b.name()));

    // From config.
    assert_eq!(Duration::from_secs(120), rgs[0].query_offset());
    // Setting 0 in config is detected.
    assert_eq!(Duration::ZERO, rgs[1].query_offset());
    // Default when nothing is set.
    assert_eq!(Duration::from_secs(60), rgs[2].query_offset());

    m.stop();
}

fn series_map(entries: &[(&str, Labels)]) -> HashMap<Vec<u8>, Labels> {
    entries.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.clone())).collect()
}

#[test]
fn test_copy_state() {
    let old_rules: Vec<RuleRef> = vec![
        new_alerting_rule("alert", nil_expr(), Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), true),
        new_recording_rule("rule1", nil_expr(), Labels::empty()),
        new_recording_rule("rule2", nil_expr(), Labels::empty()),
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v1"])),
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v2"])),
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v3"])),
        new_alerting_rule("alert2", nil_expr(), Duration::ZERO, Duration::ZERO, ls(&["l2", "v1"]), Labels::empty(), true),
    ];
    let old_group = Group::new(GroupOptions { rules: old_rules.clone(), ..Default::default() });
    old_group.set_series_in_previous_eval(vec![
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        series_map(&[("r3a", ls(&["l1", "v1"]))]),
        series_map(&[("r3b", ls(&["l1", "v2"]))]),
        series_map(&[("r3c", ls(&["l1", "v3"]))]),
        series_map(&[("a2", ls(&["l2", "v1"]))]),
    ]);
    old_group.set_evaluation_time(Duration::from_secs(1));
    // adapted: Go stores a nil *Alert under key 42; Rust stores a default Alert.
    old_rules[0].as_alerting().unwrap().active_map().insert(42, Alert::default());

    let new_rules: Vec<RuleRef> = vec![
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v0"])),
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v1"])),
        new_recording_rule("rule3", nil_expr(), ls(&["l1", "v2"])),
        new_alerting_rule("alert", nil_expr(), Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), true),
        new_recording_rule("rule1", nil_expr(), Labels::empty()),
        new_alerting_rule("alert2", nil_expr(), Duration::ZERO, Duration::ZERO, ls(&["l2", "v0"]), Labels::empty(), true),
        new_alerting_rule("alert2", nil_expr(), Duration::ZERO, Duration::ZERO, ls(&["l2", "v1"]), Labels::empty(), true),
        new_recording_rule("rule4", nil_expr(), Labels::empty()),
    ];
    let new_group = Group::new(GroupOptions { rules: new_rules.clone(), ..Default::default() });
    new_group.copy_state(&old_group);

    // adapted: Go distinguishes nil maps (entries not copied) from empty ones; Rust has only empty maps.
    let want = vec![
        HashMap::new(),
        series_map(&[("r3a", ls(&["l1", "v1"]))]),
        series_map(&[("r3b", ls(&["l1", "v2"]))]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        series_map(&[("a2", ls(&["l2", "v1"]))]),
        HashMap::new(),
    ];
    assert_eq!(want, new_group.series_in_previous_eval());
    // Go `require.Equal(oldGroup.rules[0], newGroup.rules[3])`: same name, labels and active alerts.
    let (old_alert, new_alert) = (old_rules[0].as_alerting().unwrap(), new_rules[3].as_alerting().unwrap());
    assert_eq!(old_alert.name(), new_alert.name());
    assert_eq!(old_alert.labels(), new_alert.labels());
    assert_eq!(old_alert.restored(), new_alert.restored());
    assert_eq!(*old_alert.active_map(), *new_alert.active_map());
    assert_eq!(old_group.get_evaluation_time(), new_group.get_evaluation_time());
    assert_eq!(old_group.get_last_evaluation(), new_group.get_last_evaluation());
    assert_eq!(vec![ls(&["l1", "v3"])], new_group.stale_series());
}

#[test]
fn test_deleted_rule_marked_stale() {
    let st_owner = common::new();
    let st = st_owner.db.clone();

    let old_group = Group::new(GroupOptions {
        rules: vec![new_recording_rule("rule1", nil_expr(), ls(&["l1", "v1"]))],
        ..Default::default()
    });
    old_group.set_series_in_previous_eval(vec![series_map(&[("r1", ls(&["l1", "v1"]))])]);
    let new_group = Group::new(GroupOptions {
        rules: vec![],
        opts: Some(Arc::new(ManagerOptions {
            appendable_v2: Some(st.clone()),
            rule_concurrency_controller: Some(Arc::new(SequentialRuleEvalController)),
            ..Default::default()
        })),
        ..Default::default()
    });
    new_group.copy_state(&old_group);

    new_group.eval(&Context::background(), Time::unix(0, 0));

    let mut querier = st.querier(0, 2000).unwrap();

    let matcher = eq_matcher("l1", "v1");

    let set = querier.select(&Context::background(), false, None, &[matcher]);
    let samples = read_series_set(set).unwrap();

    let metric = ls(&["l1", "v1"]).to_string();
    let metric_sample = samples.get(&metric).unwrap_or_else(|| panic!("Series {metric} not returned."));
    assert!(
        is_stale_nan(metric_sample[0].1),
        "Appended sample not as expected. Wanted: stale NaN Got: {:x}",
        metric_sample[0].1.to_bits()
    );
    querier.close();
}

/// Go `formatRules` + `yaml.Marshal`: the rule groups as go-yaml v2 writes `ruleGroupsTest`.
fn format_rules(r: &rulefmt::RuleGroups) -> String {
    use yaml::Value as V;
    let s = |x: &str| V::Str(x.to_string());
    let map = |m: &BTreeMap<String, String>| V::Map(m.iter().map(|(k, v)| (s(k), s(v))).collect());
    let mut groups = Vec::new();
    for g in &r.groups {
        let mut rules = Vec::new();
        for r in &g.rules {
            // Go copies Record, Alert, Expr, For, Labels and Annotations into a new rulefmt.Rule.
            let mut m: Vec<(V, V)> = Vec::new();
            if !r.record.is_empty() {
                m.push((s("record"), s(&r.record)));
            }
            if !r.alert.is_empty() {
                m.push((s("alert"), s(&r.alert)));
            }
            m.push((s("expr"), s(&r.expr)));
            if r.r#for.0 != 0 {
                m.push((s("for"), s(&r.r#for.to_string())));
            }
            if !r.labels.is_empty() {
                m.push((s("labels"), map(&r.labels)));
            }
            if !r.annotations.is_empty() {
                m.push((s("annotations"), map(&r.annotations)));
            }
            rules.push(V::Map(m));
        }
        let mut gm: Vec<(V, V)> = vec![(s("name"), s(&g.name))];
        if g.interval.0 != 0 {
            gm.push((s("interval"), s(&g.interval.to_string())));
        }
        if g.limit != 0 {
            gm.push((s("limit"), V::Int(g.limit)));
        }
        gm.push((s("rules"), V::Seq(rules)));
        if !g.labels.is_empty() {
            gm.push((s("labels"), map(&g.labels)));
        }
        groups.push(V::Map(gm));
    }
    yaml::emit(&V::Map(vec![(s("groups"), V::Seq(groups))]))
}

/// Go `reloadAndValidate`.
fn reload_and_validate(
    rgs: &rulefmt::RuleGroups,
    tmp_file: &mut std::fs::File,
    tmp_name: &str,
    rule_manager: &Manager,
    ogs: &mut HashMap<String, Arc<Group>>,
) {
    let bs = format_rules(rgs);
    tmp_file.seek(SeekFrom::Start(0)).unwrap();
    tmp_file.write_all(bs.as_bytes()).unwrap();
    rule_manager.update(Duration::from_secs(10), &[tmp_name.to_string()], &Labels::empty(), "", None).unwrap();
    for (h, g) in rule_manager.groups() {
        if ogs.get(&h).is_some_and(|o| Arc::ptr_eq(o, &g)) {
            panic!("group {h} was not recreated");
        }
        ogs.insert(h, g);
    }
}

#[test]
fn test_update() {
    let files = vec![fixture("rules.yaml")];
    let expected = series_map(&[("test", ls(&["name", "value"]))]);
    let st_owner = common::new();
    let st = st_owner.db.clone();

    let engine = small_engine();
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(st.clone()),
        queryable: Some(st.clone()),
        query_func: Some(query_func(&engine, &st)),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });
    rule_manager.start();
    let _stop = StopOnDrop(&rule_manager);

    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();
    assert!(!rule_manager.groups().is_empty(), "expected non-empty rule groups");
    let mut ogs: HashMap<String, Arc<Group>> = HashMap::new();
    for (h, g) in rule_manager.groups() {
        g.set_series_in_previous_eval(vec![expected.clone()]);
        ogs.insert(h, g);
    }

    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();
    for (h, g) in rule_manager.groups() {
        for actual in g.series_in_previous_eval() {
            assert_eq!(expected, actual);
        }
        // Groups are the same because of no updates.
        assert!(Arc::ptr_eq(&ogs[&h], &g));
    }

    // Groups will be recreated if updated.
    let (rgs, errs) = rulefmt::parse_file(&fixture("rules.yaml"), false, ValidationScheme::Utf8, &test_parser(), &nop());
    assert!(errs.is_empty(), "file parsing failures");
    let mut rgs = rgs.unwrap();

    let dir = TempDir::new("update");
    let tmp_name = dir.path("rules.test.yaml");
    let mut tmp_file = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&tmp_name).unwrap();

    rule_manager.update(Duration::from_secs(10), &[tmp_name.clone()], &Labels::empty(), "", None).unwrap();

    ogs.extend(rule_manager.groups());

    // Update interval and reload.
    for g in rgs.groups.iter_mut() {
        if g.interval.0 != 0 {
            g.interval = yaml::Duration(g.interval.0 * 2);
        } else {
            g.interval = yaml::Duration(10);
        }
    }
    reload_and_validate(&rgs, &mut tmp_file, &tmp_name, &rule_manager, &mut ogs);

    // Update limit and reload.
    for g in rgs.groups.iter_mut() {
        g.limit = 1;
    }
    reload_and_validate(&rgs, &mut tmp_file, &tmp_name, &rule_manager, &mut ogs);

    // Change group rules and reload.
    for g in rgs.groups.iter_mut() {
        for r in g.rules.iter_mut() {
            r.expr = format!("{} * 0", r.expr);
        }
    }
    reload_and_validate(&rgs, &mut tmp_file, &tmp_name, &rule_manager, &mut ogs);
}

#[test]
fn test_notify() {
    let st_owner = common::new();
    let st = st_owner.db.clone();

    let engine = small_engine();
    let last_notified: Arc<Mutex<Vec<Alert>>> = Arc::new(Mutex::new(Vec::new()));
    let ln = last_notified.clone();
    let notify_func: NotifyFunc = Arc::new(move |_: &Context, _: &str, alerts: Vec<Alert>| {
        *ln.lock().unwrap() = alerts;
    });
    let opts = Arc::new(ManagerOptions {
        query_func: Some(query_func(&engine, &st)),
        appendable_v2: Some(st.clone()),
        queryable: Some(st.clone()),
        context: Context::background(),
        logger: nop(),
        notify_func: Some(notify_func),
        resend_delay: Duration::from_secs(2),
        ..Default::default()
    });

    let expr = parse("a > 1");
    let rule = new_alerting_rule("aTooHigh", expr, Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), true);
    let group = Group::new(GroupOptions {
        name: "alert".into(),
        interval: Duration::from_secs(1),
        rules: vec![rule],
        should_restore: true,
        opts: Some(opts),
        ..Default::default()
    });

    let mut app = st.appender_v2(&Context::background());
    let a = ls(&["__name__", "a"]);
    let o = AppendV2Options::default();
    app.append(0, &a, 0, 1000, 2.0, None, None, &o).unwrap();
    app.append(0, &a, 0, 2000, 3.0, None, None, &o).unwrap();
    app.append(0, &a, 0, 5000, 3.0, None, None, &o).unwrap();
    app.append(0, &a, 0, 6000, 0.0, None, None, &o).unwrap();
    assert!(app.commit().is_none());

    let ctx = Context::background();

    // Alert sent right away.
    group.eval(&ctx, Time::unix(1, 0));
    assert_eq!(last_notified.lock().unwrap().len(), 1);
    assert!(!last_notified.lock().unwrap()[0].valid_until.is_zero(), "ValidUntil should not be zero");

    // Alert is not sent 1s later.
    group.eval(&ctx, Time::unix(2, 0));
    assert!(last_notified.lock().unwrap().is_empty());

    // Alert is resent at t=5s.
    group.eval(&ctx, Time::unix(5, 0));
    assert_eq!(last_notified.lock().unwrap().len(), 1);

    // Resolution alert sent right away.
    group.eval(&ctx, Time::unix(6, 0));
    assert_eq!(last_notified.lock().unwrap().len(), 1);
}

#[test]
fn test_metrics_update() {
    let files = vec![fixture("rules.yaml"), fixture("rules2.yaml")];
    let metric_names = [
        "prometheus_rule_evaluations_total",
        "prometheus_rule_evaluation_failures_total",
        "prometheus_rule_group_interval_seconds",
        "prometheus_rule_group_last_duration_seconds",
        "prometheus_rule_group_last_rule_duration_sum_seconds",
        "prometheus_rule_group_last_evaluation_timestamp_seconds",
        "prometheus_rule_group_rules",
    ];

    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let registry = Registry::new();
    let engine = small_engine();
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(storage.clone()),
        queryable: Some(storage.clone()),
        query_func: Some(query_func(&engine, &storage)),
        context: Context::background(),
        logger: nop(),
        registerer: Some(registry.clone()),
        ..Default::default()
    });
    rule_manager.start();
    let _stop = StopOnDrop(&rule_manager);

    let count_metrics = || -> usize {
        let ms = registry.gather();
        let mut metrics = 0;
        for m in &ms {
            if metric_names.contains(&m.name.as_str()) {
                metrics += m.metrics.len();
            }
        }
        metrics
    };

    let cases: Vec<(&[String], usize)> = vec![(&files[..], 14), (&files[..1], 7), (&files[..0], 0), (&files[1..], 7)];

    for (i, (files, metrics)) in cases.into_iter().enumerate() {
        rule_manager.update(Duration::from_secs(1), files, &Labels::empty(), "", None).unwrap();
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(metrics, count_metrics(), "test {i}: invalid count of metrics");
    }
}

/// Go `countStaleNaN`.
fn count_stale_nan(st: &DB) -> usize {
    let mut c = 0;
    let mut querier = st.querier(0, Time::now().unix_secs() * 1000).unwrap();

    let matcher = eq_matcher("__name__", "test_2");

    let set = querier.select(&Context::background(), false, None, &[matcher]);
    let samples = read_series_set(set).unwrap();

    let metric = ls(&["__name__", "test_2"]).to_string();
    let metric_sample = samples.get(&metric).unwrap_or_else(|| panic!("Series {metric} not returned."));
    for s in metric_sample {
        if is_stale_nan(s.1) {
            c += 1;
        }
    }
    querier.close();
    c
}

#[test]
fn test_group_staleness_on_removal() {
    let files = vec![fixture("rules2.yaml")];
    let same_files = vec![fixture("rules2_copy.yaml")];

    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let engine = small_engine();
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(storage.clone()),
        queryable: Some(storage.clone()),
        query_func: Some(query_func(&engine, &storage)),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });
    rule_manager.start();

    let cases: Vec<(&[String], usize)> = vec![
        (&files[..], 0),
        // When we remove the files, it should produce a staleness marker.
        (&files[..0], 1),
        // Rules that produce the same metrics but in a different file should not produce staleness marker.
        (&same_files[..], 0),
        // Staleness marker should be present as we don't have any rules loaded anymore.
        (&files[..0], 1),
        // Add rules back so we have rules loaded when we stop the manager and check for the absence of
        // staleness markers.
        (&same_files[..], 0),
    ];

    let mut total_stale_nan = 0;
    for (i, (files, stale_nan)) in cases.into_iter().enumerate() {
        rule_manager.update(Duration::from_secs(1), files, &Labels::empty(), "", None).unwrap();
        std::thread::sleep(Duration::from_secs(3));
        total_stale_nan += stale_nan;
        assert_eq!(total_stale_nan, count_stale_nan(&storage), "test {i}/{files:?}: invalid count of staleness markers");
    }
    rule_manager.stop();
    assert_eq!(total_stale_nan, count_stale_nan(&storage), "invalid count of staleness markers after stopping the engine");
}

#[test]
fn test_metrics_staleness_on_manager_shutdown() {
    let files = vec![fixture("rules2.yaml")];

    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let engine = small_engine();
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(storage.clone()),
        queryable: Some(storage.clone()),
        query_func: Some(query_func(&engine, &storage)),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });
    rule_manager.start();

    let err = rule_manager.update(Duration::from_secs(2), &files, &Labels::empty(), "", None);
    std::thread::sleep(Duration::from_secs(4));
    err.unwrap();
    let start = Instant::now();
    rule_manager.update(Duration::from_secs(3), &files[..0], &Labels::empty(), "", None).unwrap();
    rule_manager.stop();
    assert!(start.elapsed() < Duration::from_secs(1), "rule manager does not stop early");
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(0, count_stale_nan(&storage), "invalid count of staleness markers after stopping the engine");
}

#[test]
fn test_rule_moved_between_groups() {
    let storage_owner = common::new_with(|opt| opt.out_of_order_time_window = 600000);
    let storage = storage_owner.db.clone();

    let engine = Arc::new(Engine::new(EngineOpts { max_samples: 10, timeout: Duration::from_secs(10), ..Default::default() }));
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(storage.clone()),
        queryable: Some(storage.clone()),
        query_func: Some(query_func(&engine, &storage)),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });
    rule_manager.start();
    let _stop = StopOnDrop(&rule_manager);

    let rule2 = fixture("rules2.yaml");
    let rule1 = fixture("rules1.yaml");

    // Load initial configuration of rules2.
    rule_manager.update(Duration::from_secs(1), &[rule2], &Labels::empty(), "", None).unwrap();

    // Wait for rule to be evaluated.
    std::thread::sleep(Duration::from_secs(3));

    // Reload configuration of rules1.
    rule_manager.update(Duration::from_secs(1), &[rule1], &Labels::empty(), "", None).unwrap();

    // Wait for rule to be evaluated in new location and potential staleness marker.
    std::thread::sleep(Duration::from_secs(3));

    assert_eq!(0, count_stale_nan(&storage)); // Not expecting any stale markers.
}

#[test]
fn test_group_has_alerting_rules() {
    let tests: Vec<(Group, bool)> = vec![
        (
            Group::new(GroupOptions {
                name: "HasAlertingRule".into(),
                rules: vec![
                    new_alerting_rule("alert", nil_expr(), Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), true),
                    new_recording_rule("record", nil_expr(), Labels::empty()),
                ],
                ..Default::default()
            }),
            true,
        ),
        (Group::new(GroupOptions { name: "HasNoRule".into(), rules: vec![], ..Default::default() }), false),
        (
            Group::new(GroupOptions {
                name: "HasOnlyRecordingRule".into(),
                rules: vec![new_recording_rule("record", nil_expr(), Labels::empty())],
                ..Default::default()
            }),
            false,
        ),
    ];

    for (i, (group, want)) in tests.iter().enumerate() {
        let got = group.has_alerting_rules();
        assert_eq!(*want, got, "test case {i} failed, expected:{want} got:{got}");
    }
}

#[test]
fn test_rule_health_updates() {
    let st_owner = common::new();
    let st = st_owner.db.clone();

    let engine = small_engine();
    let opts = Arc::new(ManagerOptions {
        query_func: Some(query_func(&engine, &st)),
        appendable_v2: Some(st.clone()),
        queryable: Some(st.clone()),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });

    let expr = parse("a + 1");
    let rule = new_recording_rule("a_plus_one", expr, Labels::empty());
    let group = Group::new(GroupOptions {
        name: "default".into(),
        interval: Duration::from_secs(1),
        rules: vec![rule],
        should_restore: true,
        opts: Some(opts),
        ..Default::default()
    });

    // A time series that has two samples.
    let mut app = st.appender_v2(&Context::background());
    let a = ls(&["__name__", "a"]);
    let o = AppendV2Options::default();
    app.append(0, &a, 0, 0, 1.0, None, None, &o).unwrap();
    app.append(0, &a, 0, 1000, 2.0, None, None, &o).unwrap();
    assert!(app.commit().is_none());

    let ctx = Context::background();

    let rules = group.rules(&[])[0].clone();
    assert!(rules.last_error().is_none());
    assert_eq!(RuleHealth::Unknown, rules.health());

    // Execute 2 times, it should be all green.
    group.eval(&ctx, Time::unix(0, 0));
    group.eval(&ctx, Time::unix(1, 0));

    let rules = group.rules(&[])[0].clone();
    assert!(rules.last_error().is_none(), "{:?}", rules.last_error().map(|e| e.to_string()));
    assert_eq!(RuleHealth::Good, rules.health());

    // Now execute the rule in the past again, this should cause append failures.
    group.eval(&ctx, Time::unix(0, 0));
    let rules = group.rules(&[])[0].clone();
    assert_eq!(rules.last_error().map(|e| e.to_string()), Some(storage::ERR_OUT_OF_ORDER_SAMPLE.to_string()));
    assert_eq!(RuleHealth::Bad, rules.health());
}

#[test]
fn test_rule_group_eval_iteration_func() {
    let storage_owner = common::loaded_storage(r#"
		load 5m
		http_requests{instance="0"}	75  85 50 0 0 25 0 0 40 0 120
	"#);
    let storage = storage_owner.db.clone();

    let expr = parse(r#"http_requests{group="canary", job="app-server"} < 100"#);

    let test_value = Arc::new(AtomicI32::new(1));

    let tv = test_value.clone();
    let eval_iteration_func: GroupEvalIterationFunc = Arc::new(move |ctx: &Context, g: &Group, eval_timestamp: Time| {
        tv.store(2, Ordering::SeqCst);
        default_eval_iteration_func(ctx, g, eval_timestamp);
        tv.store(3, Ordering::SeqCst);
    });

    let tv = test_value.clone();
    let skip_eval_iteration_func: GroupEvalIterationFunc = Arc::new(move |_: &Context, _: &Group, _: Time| {
        tv.store(4, Ordering::SeqCst);
    });

    struct TestInput {
        eval_iteration_func: Option<GroupEvalIterationFunc>,
        expected_value: i32,
        last_eval_timestamp_is_zero: bool,
    }

    let tests = vec![
        // testValue should still have value of 1 since the default iteration function will be called.
        TestInput { eval_iteration_func: None, expected_value: 1, last_eval_timestamp_is_zero: false },
        // testValue should be incremented to 3 since evalIterationFunc is called.
        TestInput { eval_iteration_func: Some(eval_iteration_func), expected_value: 3, last_eval_timestamp_is_zero: false },
        // testValue should be incremented to 4 since skipEvalIterationFunc is called.
        TestInput { eval_iteration_func: Some(skip_eval_iteration_func), expected_value: 4, last_eval_timestamp_is_zero: true },
    ];

    let ng = test_engine();
    for (i, tst) in tests.into_iter().enumerate() {
        let opts = Arc::new(ManagerOptions {
            query_func: Some(query_func(&ng, &storage)),
            appendable_v2: Some(storage.clone()),
            queryable: Some(storage.clone()),
            context: Context::background(),
            logger: nop(),
            notify_func: Some(noop_notify()),
            outage_tolerance: Duration::from_secs(30 * 60),
            for_grace_period: Duration::from_secs(10 * 60),
            ..Default::default()
        });

        let active_alert = Alert { state: AlertState::Firing, active_at: Time::now(), ..Default::default() };

        let rule = Arc::new(AlertingRule::new(
            "HTTPRequestRateLow",
            expr.clone(),
            Duration::from_secs(5 * 60),
            Duration::ZERO,
            ls(&["severity", "critical"]),
            Labels::empty(),
            Labels::empty(),
            "",
            true,
            nop(),
        ));
        rule.active_map().insert(1, active_alert);

        let group = Arc::new(Group::new(GroupOptions {
            name: "default".into(),
            interval: Duration::from_secs(1),
            rules: vec![rule],
            should_restore: true,
            opts: Some(opts.clone()),
            eval_iteration_func: tst.eval_iteration_func,
            ..Default::default()
        }));

        let g = group.clone();
        let ctx = opts.context.clone();
        let h = std::thread::spawn(move || g.run(&ctx));

        std::thread::sleep(Duration::from_secs(3));
        group.stop();
        h.join().unwrap();

        assert_eq!(tst.expected_value, test_value.load(Ordering::SeqCst), "case {i}");
        if tst.last_eval_timestamp_is_zero {
            assert!(group.get_last_eval_timestamp().is_zero(), "case {i}");
        } else {
            let one_minute = dur_ns(Duration::from_secs(60));
            let delta = Time::now().sub(group.get_last_eval_timestamp());
            assert!(delta.abs() <= one_minute, "case {i}: last eval timestamp not within 1m of now (delta {delta}ns)");
        }
    }
}

#[test]
fn test_native_histograms_in_recording_rules() {
    let st_owner = common::new();
    let st = st_owner.db.clone();

    // Add some histograms.
    let db = &st_owner.db;
    let hists = storage::tsdbutil::generate_test_histograms(5);
    let ts = Time::now();
    let mut app = db.appender_v2(&Context::background());
    for (i, h) in hists.iter().enumerate() {
        let l = ls(&["__name__", "histogram_metric", "idx", &i.to_string()]);
        app.append(0, &l, 0, ts.millis(), 0.0, Some(&h.clone()), None, &AppendV2Options::default()).unwrap();
    }
    assert!(app.commit().is_none());

    let ng = test_engine();
    let opts = Arc::new(ManagerOptions {
        query_func: Some(query_func(&ng, &st)),
        appendable_v2: Some(st.clone()),
        queryable: Some(st.clone()),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });

    let expr = parse("sum(histogram_metric)");
    let rule = new_recording_rule("sum:histogram_metric", expr, Labels::empty());

    let group = Group::new(GroupOptions {
        name: "default".into(),
        interval: Duration::from_secs(3600),
        rules: vec![rule],
        should_restore: true,
        opts: Some(opts),
        ..Default::default()
    });

    group.eval(&Context::background(), ts.add(Duration::from_secs(10)));

    let mut q = db.querier(ts.millis(), ts.add(Duration::from_secs(20)).millis()).unwrap();
    let mut ss = q.select(&Context::background(), false, None, &[eq_matcher("__name__", "sum:histogram_metric")]);
    assert!(ss.next());
    let s = ss.at().clone();
    assert!(!ss.next());

    assert_eq!(&ls(&["__name__", "sum:histogram_metric"]), s.labels());

    let mut exp_hist = hists[0].to_float();
    for h in &hists[1..] {
        exp_hist.add(&h.to_float()).unwrap();
    }

    let mut it = s.iterator(None);
    assert_eq!(ValueType::FloatHistogram, it.next());
    let (tsp, fh) = it.at_float_histogram(None);
    assert_eq!(ts.add(Duration::from_secs(10)).millis(), tsp);
    assert_eq!(exp_hist, fh);
    assert_eq!(ValueType::None, it.next());
    drop(it);
    q.close();
}

/// TestManager_LoadGroups_WithoutLogger covers the nil pointer dereference in the default GroupLoader.
#[test]
fn test_manager_load_groups_without_logger() {
    let rule_manager = Manager::new(ManagerOptions::default());

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rule_manager.load_groups(Duration::from_secs(1), &Labels::empty(), "", None, false, &[fixture("rules_multi_doc.yaml")])
    }));
    let groups = match r {
        Ok(Ok(g)) => g,
        Ok(Err(errs)) => panic!("unexpected errors: {:?}", errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()),
        Err(_) => panic!("load_groups panicked"),
    };
    assert_eq!(groups.len(), 1);
}

#[test]
fn test_manager_load_groups_should_check_whether_each_rule_has_dependents_and_dependencies() {
    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let rule_manager = Manager::new(ManagerOptions {
        context: Context::background(),
        logger: nop(),
        appendable_v2: Some(storage.clone()),
        query_func: Some(Arc::new(|_: &Context, _: &str, _: Time| Ok(Vector::new()))),
        ..Default::default()
    });

    // load a mix of dependent and independent rules
    {
        let groups = rule_manager
            .load_groups(Duration::from_secs(1), &Labels::empty(), "", None, false, &[fixture("rules_multiple.yaml")])
            .unwrap_or_else(|e| panic!("{:?}", e.iter().map(|e| e.to_string()).collect::<Vec<_>>()));
        assert_eq!(groups.len(), 1);

        let expected: HashMap<&str, (bool, bool)> = HashMap::from([
            ("job:http_requests:rate1m", (true, true)),
            ("job:http_requests:rate5m", (true, true)),
            ("job:http_requests:rate15m", (true, false)),
            ("TooManyRequests", (false, true)),
        ]);

        // Go ranges over ruleManager.Rules(), which LoadGroups does not change.
        for r in rule_manager.rules(&[]) {
            let exp = expected.get(r.name()).unwrap_or_else(|| panic!("rule: {}", r.string()));
            assert_eq!(exp.0, r.no_dependent_rules(), "rule: {}", r.string());
            assert_eq!(exp.1, r.no_dependency_rules(), "rule: {}", r.string());
        }
    }

    // load only independent rules
    {
        let groups = rule_manager
            .load_groups(Duration::from_secs(1), &Labels::empty(), "", None, false, &[fixture("rules_multiple_independent.yaml")])
            .unwrap_or_else(|e| panic!("{:?}", e.iter().map(|e| e.to_string()).collect::<Vec<_>>()));
        assert_eq!(groups.len(), 1);

        for r in rule_manager.rules(&[]) {
            assert!(r.no_dependent_rules(), "rule: {}", r.string());
            assert!(r.no_dependency_rules(), "rule: {}", r.string());
        }
    }
}

fn rule_group(rules: Vec<RuleRef>) -> Group {
    Group::new(GroupOptions {
        name: "rule_group".into(),
        interval: Duration::from_secs(1),
        rules,
        opts: Some(Arc::new(ManagerOptions { context: Context::background(), logger: nop(), ..Default::default() })),
        ..Default::default()
    })
}

fn rec(name: &str, q: &str) -> RuleRef {
    new_recording_rule(name, parse(q), Labels::empty())
}

fn alert(name: &str, q: &str) -> RuleRef {
    new_alerting_rule(name, parse(q), Duration::ZERO, Duration::ZERO, Labels::empty(), Labels::empty(), true)
}

#[test]
fn test_dependency_map() {
    let rule = rec("user:requests:rate1m", "sum by (user) (rate(requests[1m]))");
    let rule2 = alert("ZeroRequests", "user:requests:rate1m <= 0");
    let rule3 = rec("user:requests:rate5m", "sum by (user) (rate(requests[5m]))");
    let rule4 = rec("user:requests:increase1h", "increase(user:requests:rate1m[1h])");
    let rule5 = rec("user:requests:sum5m", r#"sum by (user) ({__name__=~"user:requests.+5m"})"#);

    let group = rule_group(vec![rule.clone(), rule2.clone(), rule3.clone(), rule4.clone(), rule5.clone()]);

    let dep_map = build_dependency_map(group.all_rules());

    assert!(dependencies(&dep_map, &rule).is_empty());
    assert!(same_rules(&[rule2.clone(), rule4.clone()], &dependents(&dep_map, &rule)), "{:?}", names(&dependents(&dep_map, &rule)));
    assert_eq!(dependents(&dep_map, &rule).len(), 2);
    assert!(!independent(&dep_map, &rule));

    assert!(dependents(&dep_map, &rule2).is_empty());
    assert!(same_rules(&[rule.clone()], &dependencies(&dep_map, &rule2)));
    assert!(!independent(&dep_map, &rule2));

    assert!(same_rules(&[rule5.clone()], &dependents(&dep_map, &rule3)));
    assert!(dependencies(&dep_map, &rule3).is_empty());
    assert!(!independent(&dep_map, &rule3));

    assert!(dependents(&dep_map, &rule4).is_empty());
    assert!(same_rules(&[rule.clone()], &dependencies(&dep_map, &rule4)));
    assert!(!independent(&dep_map, &rule4));

    assert!(dependents(&dep_map, &rule5).is_empty());
    assert!(same_rules(&[rule3.clone()], &dependencies(&dep_map, &rule5)));
    assert!(!independent(&dep_map, &rule5));
}

#[test]
fn test_no_dependency() {
    let rule = rec("user:requests:rate1m", "sum by (user) (rate(requests[1m]))");

    let group = rule_group(vec![rule]);

    let dep_map = build_dependency_map(group.all_rules());
    // A group with only one rule cannot have dependencies.
    assert!(dep_map_empty(&dep_map));
}

#[test]
fn test_dependencies_edge_cases() {
    // empty group
    {
        let group = rule_group(vec![]);

        let rule = rec("user:requests:rate1m", "sum by (user) (rate(requests[1m]))");

        let dep_map = build_dependency_map(group.all_rules());
        // A group with no rules has no dependency map, but doesn't panic if the map is queried.
        assert!(dep_map_empty(&dep_map));
        assert!(independent(&dep_map, &rule));
    }

    // rules which reference no series
    {
        let rule1 = rec("1", "one");
        let rule2 = rec("2", "two");

        let group = rule_group(vec![rule1.clone(), rule2.clone()]);

        let dep_map = build_dependency_map(group.all_rules());
        // A group with rules which reference no series will still produce a dependency map.
        assert!(independent(&dep_map, &rule1));
        assert!(independent(&dep_map, &rule2));
    }

    // rule with regexp matcher on metric name; not equal; not regexp
    for second in [r#"sum({__name__=~".+"})"#, r#"sum({__name__!="requests", service="app"})"#, r#"sum({__name__!~"requests.+", service="app"})"#] {
        let rule1 = rec("first", "sum(requests)");
        let rule2 = rec("second", second);

        let group = rule_group(vec![rule1.clone(), rule2.clone()]);

        let dep_map = build_dependency_map(group.all_rules());
        // Such a matcher on the metric name causes the whole group to be indeterminate.
        assert!(!independent(&dep_map, &rule1), "{second}");
        assert!(!independent(&dep_map, &rule2), "{second}");
    }

    for meta_metric in ["ALERTS", "ALERTS_FOR_STATE"] {
        // rule querying alerts meta-metric with alertname
        {
            let rule1 = alert("first", "sum(requests) > 0");
            let rule2 = alert("second", &format!(r#"sum({meta_metric}{{alertname="test"}}) > 0"#));
            let rule3 = alert("third", &format!(r#"sum({meta_metric}{{alertname=~"first.*"}}) > 0"#));
            let rule4 = alert("fourth", &format!(r#"sum({meta_metric}{{alertname!="first"}}) > 0"#));
            let rule5 = rec("fifth", "sum(failures)");
            let rule6 = alert("sixth", &format!(r#"fifth > 0 and sum({meta_metric}{{alertname="fourth"}}) > 0"#));

            let group = rule_group(vec![rule1.clone(), rule2.clone(), rule3.clone(), rule4.clone(), rule5.clone(), rule6.clone()]);

            let dm = build_dependency_map(group.all_rules());

            assert!(!independent(&dm, &rule1), "{meta_metric}");
            assert!(dependencies(&dm, &rule1).is_empty(), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule1), &[rule3.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule2), "{meta_metric}");
            assert!(dependencies(&dm, &rule2).is_empty(), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule2), &[rule4.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule3), "{meta_metric}");
            assert!(elements_match(&dependencies(&dm, &rule3), &[rule1.clone()]), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule3), &[rule4.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule4), "{meta_metric}");
            assert!(elements_match(&dependencies(&dm, &rule4), &[rule2.clone(), rule3.clone()]), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule4), &[rule6.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule5), "{meta_metric}");
            assert!(dependencies(&dm, &rule5).is_empty(), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule5), &[rule6.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule6), "{meta_metric}");
            assert!(elements_match(&dependencies(&dm, &rule6), &[rule4.clone(), rule5.clone()]), "{meta_metric}");
            assert!(dependents(&dm, &rule6).is_empty(), "{meta_metric}");
        }

        // rule querying alerts meta-metric without alertname
        {
            let rule1 = rec("first", "sum(requests)");
            let rule2 = alert("second", "sum(requests) > 0");
            let rule3 = alert("third", &format!("sum({meta_metric}) > 0"));
            let rule4 = rec("fourth", "sum(failures)");
            let rule5 = alert("fifth", &format!("fourth > 0 and sum({meta_metric}) > 0"));

            let group = rule_group(vec![rule1.clone(), rule2.clone(), rule3.clone(), rule4.clone(), rule5.clone()]);

            let dm = build_dependency_map(group.all_rules());

            assert!(independent(&dm, &rule1), "{meta_metric}");
            assert!(dependencies(&dm, &rule1).is_empty(), "{meta_metric}");
            assert!(dependents(&dm, &rule1).is_empty(), "{meta_metric}");

            assert!(!independent(&dm, &rule2), "{meta_metric}");
            assert!(dependencies(&dm, &rule2).is_empty(), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule2), &[rule3.clone(), rule5.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule3), "{meta_metric}");
            assert!(elements_match(&dependencies(&dm, &rule3), &[rule2.clone()]), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule3), &[rule5.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule4), "{meta_metric}");
            assert!(dependencies(&dm, &rule4).is_empty(), "{meta_metric}");
            assert!(elements_match(&dependents(&dm, &rule4), &[rule5.clone()]), "{meta_metric}");

            assert!(!independent(&dm, &rule5), "{meta_metric}");
            assert!(elements_match(&dependencies(&dm, &rule5), &[rule2.clone(), rule3.clone(), rule4.clone()]), "{meta_metric}");
            assert!(dependents(&dm, &rule5).is_empty(), "{meta_metric}");
        }
    }
}

#[test]
fn test_no_metric_selector() {
    let rule = rec("user:requests:rate1m", "sum by (user) (rate(requests[1m]))");
    let rule2 = rec("user:requests:rate1m", r#"count({user="bob"})"#);

    let group = rule_group(vec![rule.clone(), rule2.clone()]);

    let dep_map = build_dependency_map(group.all_rules());
    // A rule with no metric selector cannot be reliably determined to have no dependencies on other rules, and
    // therefore all rules are not considered independent.
    assert!(!independent(&dep_map, &rule));
    assert!(!independent(&dep_map, &rule2));
}

#[test]
fn test_dependent_rules_with_non_metric_expression() {
    let rule = rec("user:requests:rate1m", "sum by (user) (rate(requests[1m]))");
    let rule2 = alert("ZeroRequests", "user:requests:rate1m <= 0");
    let rule3 = rec("three", "3");

    let group = rule_group(vec![rule.clone(), rule2.clone(), rule3.clone()]);

    let dep_map = build_dependency_map(group.all_rules());
    assert!(!independent(&dep_map, &rule));
    assert!(!independent(&dep_map, &rule2));
    assert!(independent(&dep_map, &rule3));
}

#[test]
fn test_dependency_map_updates_on_group_update() {
    let storage_owner = common::new();
    let storage = storage_owner.db.clone();
    let engine = test_engine();

    let mut files = vec![fixture("rules.yaml")];
    let rule_manager = Manager::new(ManagerOptions {
        appendable_v2: Some(storage.clone()),
        queryable: Some(storage.clone()),
        query_func: Some(query_func(&engine, &storage)),
        context: Context::background(),
        logger: nop(),
        ..Default::default()
    });

    rule_manager.start();
    let _stop = StopOnDrop(&rule_manager);

    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();
    assert!(!rule_manager.groups().is_empty(), "expected non-empty rule groups");

    // Go keys `orig` by group name but looks it up by group key below, as here.
    let mut orig: HashMap<String, Option<DependencyMap>> = HashMap::new();
    for g in rule_manager.groups().values() {
        let dep_map = build_dependency_map(g.all_rules());
        // No dependency map is expected because there is only one rule in the group.
        assert!(dep_map_empty(&dep_map));
        orig.insert(g.name().to_string(), dep_map);
    }

    // Update once without changing groups.
    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();
    for (h, g) in rule_manager.groups() {
        let dep_map = build_dependency_map(g.all_rules());
        // Dependency maps are the same because of no updates.
        let o = orig.get(&h).and_then(|o| o.as_ref());
        if o.is_none() {
            assert!(dep_map_empty(&dep_map));
        } else {
            assert!(dep_maps_equal(o, dep_map.as_ref(), g.all_rules()));
        }
    }

    // Groups will be recreated when updated.
    files[0] = fixture("rules_dependencies.yaml");
    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();

    for (h, g) in rule_manager.groups() {
        const RULE_NAME: &str = "job:http_requests:rate5m";
        let mut rr: Option<RuleRef> = None;

        for r in g.all_rules() {
            if r.name() == RULE_NAME {
                assert!(r.as_recording().is_some());
                rr = Some(r.clone());
            }
        }

        let rr = rr.unwrap_or_else(|| panic!("expected to find {RULE_NAME:?} recording rule in fixture"));

        let dep_map = build_dependency_map(g.all_rules());
        // Dependency maps must change because the groups would've been updated.
        assert!(!dep_maps_equal(orig.get(&h).and_then(|o| o.as_ref()), dep_map.as_ref(), g.all_rules()));
        // We expect there to be some dependencies since the new rule group contains a dependency.
        assert!(!dep_map_empty(&dep_map));
        assert_eq!(dependents(&dep_map, &rr).len(), 1);
        assert_eq!("HighRequestRate", dependents(&dep_map, &rr)[0].name());
        assert!(dependencies(&dep_map, &rr).is_empty());
    }
}

const ARTIFICIAL_DELAY: Duration = Duration::from_millis(250);

/// Go `optsFactory`.
fn opts_factory(storage: &Arc<DB>, max_inflight: &Arc<AtomicI32>, inflight_queries: &Arc<AtomicI32>, max_concurrent: i64) -> ManagerOptions {
    let inflight_mu = Arc::new(Mutex::new(()));

    let concurrent = max_concurrent > 0;

    let (max_inflight, inflight_queries) = (max_inflight.clone(), inflight_queries.clone());
    ManagerOptions {
        context: Context::background(),
        logger: nop(),
        concurrent_evals_enabled: concurrent,
        max_concurrent_evals: max_concurrent,
        appendable_v2: Some(storage.clone() as Arc<dyn AppendableV2>),
        query_func: Some(Arc::new(move |_: &Context, _: &str, ts: Time| {
            let guard = inflight_mu.lock().unwrap();

            let current = inflight_queries.fetch_add(1, Ordering::SeqCst) + 1;
            struct Dec(Arc<AtomicI32>);
            impl Drop for Dec {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _dec = Dec(inflight_queries.clone());

            let high_watermark = max_inflight.load(Ordering::SeqCst);

            if current > high_watermark {
                max_inflight.store(current, Ordering::SeqCst);
            }
            drop(guard);

            // Artificially delay all query executions to highlight concurrent execution improvement.
            std::thread::sleep(ARTIFICIAL_DELAY);

            // Return a stub sample.
            Ok(vec![Sample { metric: ls(&["__name__", "test"]), t: ts.millis(), f: 12345.0, ..Default::default() }])
        })),
        notify_func: Some(noop_notify()),
        ..Default::default()
    }
}

fn load_one(rule_manager: &Manager, file: &str) -> HashMap<String, Arc<Group>> {
    let groups = rule_manager
        .load_groups(Duration::from_secs(1), &Labels::empty(), "", None, false, &[fixture(file)])
        .unwrap_or_else(|e| panic!("{:?}", e.iter().map(|e| e.to_string()).collect::<Vec<_>>()));
    assert_eq!(groups.len(), 1);
    groups
}

fn split(group: &Group, ctx: &Context) -> Vec<Vec<usize>> {
    group.opts().rule_concurrency_controller.as_ref().unwrap().split_group_into_batches(ctx, group)
}

// adapted: Go runs these subtests in a synctest bubble (fake clock); here they run on the real clock, so each
// evaluation really waits for the artificial query delay.
#[test]
fn test_async_rule_evaluation() {
    // synchronous evaluation with independent rules
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let rule_manager = Manager::new(opts_factory(&storage, &max_inflight, &inflight_queries, 0));
        let groups = load_one(&rule_manager, "rules_multiple.yaml");

        let expected_rule_count = 6;
        let expected_sample_count = 4.0;

        for group in groups.values() {
            assert_eq!(group.all_rules().len(), expected_rule_count);

            let start = Instant::now();
            default_eval_iteration_func(&ctx, group, Time::now());

            // Expected evaluation order.
            let order = split(group, &ctx);
            assert!(order.is_empty());

            // Never expect more than 1 inflight query at a time.
            assert_eq!(1, max_inflight.load(Ordering::SeqCst));
            // Each rule should take at least 1 second to execute sequentially.
            assert!(start.elapsed() >= ARTIFICIAL_DELAY * expected_rule_count as u32);
            // Each recording rule produces one vector.
            assert_eq!(expected_sample_count, group_samples(group));
            // Group duration is higher than the sum of rule durations (group overhead).
            assert!(group.get_evaluation_time() >= group.get_rule_evaluation_time_sum());
        }
    }

    // asynchronous evaluation with independent and dependent rules
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let expected_rule_count = 6;
        let expected_sample_count = 4.0;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = 2;
        opts.rule_concurrency_controller = None;
        let max_concurrent_evals = opts.max_concurrent_evals;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_multiple.yaml");

        for group in groups.values() {
            assert_eq!(group.all_rules().len(), expected_rule_count);

            let start = Instant::now();
            default_eval_iteration_func(&ctx, group, Time::now());

            // Max inflight can be 1 synchronous eval and up to MaxConcurrentEvals concurrent evals.
            assert_eq!(max_concurrent_evals + 1, max_inflight.load(Ordering::SeqCst) as i64);
            // Some rules should execute concurrently so should complete quicker.
            assert!(start.elapsed() < ARTIFICIAL_DELAY * expected_rule_count as u32);
            // Each recording rule produces one vector.
            assert_eq!(expected_sample_count, group_samples(group));
        }
    }

    // asynchronous evaluation of all independent rules, insufficient concurrency
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let expected_rule_count = 8;
        let expected_sample_count = expected_rule_count as f64;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = 2;
        opts.rule_concurrency_controller = None;
        let max_concurrent_evals = opts.max_concurrent_evals;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_multiple_independent.yaml");

        for group in groups.values() {
            assert_eq!(group.all_rules().len(), expected_rule_count);

            let start = Instant::now();
            default_eval_iteration_func(&ctx, group, Time::now());

            // Expected evaluation order (isn't affected by concurrency settings).
            let order = split(group, &ctx);
            assert_eq!(vec![vec![0, 1, 2, 3, 4, 5, 6, 7]], order);

            // Max inflight can be 1 synchronous eval and up to MaxConcurrentEvals concurrent evals.
            assert_eq!(max_concurrent_evals + 1, max_inflight.load(Ordering::SeqCst) as i64);
            // Some rules should execute concurrently so should complete quicker.
            assert!(start.elapsed() < ARTIFICIAL_DELAY * expected_rule_count as u32);
            // Each recording rule produces one vector.
            assert_eq!(expected_sample_count, group_samples(group));
        }
    }

    // asynchronous evaluation of all independent rules, sufficient concurrency
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let expected_rule_count = 8;
        let expected_sample_count = expected_rule_count as f64;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = expected_rule_count as i64 * 2;
        opts.rule_concurrency_controller = None;
        let max_concurrent_evals = opts.max_concurrent_evals;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_multiple_independent.yaml");

        for group in groups.values() {
            assert_eq!(group.all_rules().len(), expected_rule_count);

            let start = Instant::now();

            default_eval_iteration_func(&ctx, group, Time::now());

            // Expected evaluation order.
            let order = split(group, &ctx);
            assert_eq!(vec![vec![0, 1, 2, 3, 4, 5, 6, 7]], order);

            // Max inflight can be up to MaxConcurrentEvals concurrent evals, since there is sufficient concurrency
            // to run all rules at once.
            assert!(max_inflight.load(Ordering::SeqCst) as i64 <= max_concurrent_evals);
            // Some rules should execute concurrently so should complete quicker.
            assert!(start.elapsed() < ARTIFICIAL_DELAY * expected_rule_count as u32);
            // Each recording rule produces one vector.
            assert_eq!(expected_sample_count, group_samples(group));
            // Group duration is less than the sum of rule durations.
            assert!(group.get_evaluation_time() < group.get_rule_evaluation_time_sum());
        }
    }

    // asynchronous evaluation of independent rules, with indeterminate. Should be synchronous
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let rule_count = 7;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = rule_count as i64 * 2;
        opts.rule_concurrency_controller = None;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_indeterminates.yaml");

        for group in groups.values() {
            assert_eq!(group.all_rules().len(), rule_count);

            let start = Instant::now();

            group.eval(&ctx, Time::now());

            // Never expect more than 1 inflight query at a time.
            assert_eq!(1, max_inflight.load(Ordering::SeqCst));
            // Each rule should take at least 1 second to execute sequentially.
            assert!(start.elapsed() >= ARTIFICIAL_DELAY * rule_count as u32);
            // Each rule produces one vector.
            assert_eq!(rule_count as f64, group_samples(group));
        }
    }

    // asynchronous evaluation of rules that benefit from reordering
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let rule_count = 8;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = rule_count as i64 * 2;
        opts.rule_concurrency_controller = None;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_multiple_dependents_on_base.yaml");
        let group = groups.values().next().unwrap();

        let start = Instant::now();

        // Expected evaluation order.
        let order = split(group, &ctx);
        assert_eq!(vec![vec![0, 4], vec![1, 2, 3, 5, 6, 7]], order);

        group.eval(&ctx, Time::now());

        // Inflight queries should be equal to 6. This is the size of the second batch of rules that can be
        // executed concurrently.
        assert_eq!(6, max_inflight.load(Ordering::SeqCst));
        // Some rules should execute concurrently so should complete quicker.
        assert!(start.elapsed() < ARTIFICIAL_DELAY * rule_count as u32);
        // Each rule produces one vector.
        assert_eq!(rule_count as f64, group_samples(group));
    }

    // attempted asynchronous evaluation of chained rules
    {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();
        let inflight_queries = Arc::new(AtomicI32::new(0));
        let max_inflight = Arc::new(AtomicI32::new(0));
        let ctx = Context::background();

        let rule_count = 7;
        let mut opts = opts_factory(&storage, &max_inflight, &inflight_queries, 0);

        // Configure concurrency settings.
        opts.concurrent_evals_enabled = true;
        opts.max_concurrent_evals = rule_count as i64 * 2;
        opts.rule_concurrency_controller = None;
        let rule_manager = Manager::new(opts);

        let groups = load_one(&rule_manager, "rules_chain.yaml");
        let group = groups.values().next().unwrap();

        let start = Instant::now();

        // Expected evaluation order.
        let order = split(group, &ctx);
        assert_eq!(vec![vec![0, 1], vec![2], vec![3], vec![4, 5, 6]], order);

        group.eval(&ctx, Time::now());

        assert_eq!(3, max_inflight.load(Ordering::SeqCst));
        // Some rules should execute concurrently so should complete quicker.
        assert!(start.elapsed() < ARTIFICIAL_DELAY * rule_count as u32);
        // Each rule produces one vector.
        assert_eq!(rule_count as f64, group_samples(group));
    }
}

/// Go `waitForEvaluations`.
fn wait_for_evaluations(ch: &std::sync::mpsc::Receiver<i32>, target_count: i32) {
    loop {
        match ch.recv_timeout(Duration::from_secs(5)) {
            Ok(cnt) if cnt == target_count => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

fn new_rule_group_restoration(restore_new_rule_groups: bool) {
    let store_owner = common::new();
    let store = store_owner.db.clone();

    let inflight_queries = Arc::new(AtomicI32::new(0));
    let max_inflight = Arc::new(AtomicI32::new(0));
    let max_concurrency = 0i64;
    let interval = Duration::from_secs(60);

    let mut files = vec![fixture("alert_rule.yaml")];

    let mut option = opts_factory(&store, &max_inflight, &inflight_queries, max_concurrency);
    option.queryable = Some(store.clone());
    option.appendable_v2 = Some(store.clone());
    option.restore_new_rule_groups = restore_new_rule_groups;
    option.notify_func = Some(noop_notify());

    let eval_count = Arc::new(AtomicI32::new(0));
    // Go: an unbuffered channel.
    let (tx, rx) = std::sync::mpsc::sync_channel::<i32>(0);
    let tx = Mutex::new(tx);
    let ec = eval_count.clone();
    let noop_eval_iter_func: GroupEvalIterationFunc = Arc::new(move |_: &Context, _: &Group, _: Time| {
        ec.fetch_add(1, Ordering::SeqCst);
        // adapted: Go blocks forever once nobody receives; here the send fails once the receiver is dropped.
        let _ = tx.lock().unwrap().send(ec.load(Ordering::SeqCst));
    });

    let rule_manager = Arc::new(Manager::new(option));
    let rm = rule_manager.clone();
    let run = std::thread::spawn(move || rm.run());
    rule_manager.update(interval, &files, &Labels::empty(), "", Some(noop_eval_iter_func)).unwrap();

    wait_for_evaluations(&rx, 3);
    drop(rx);
    assert_eq!(3, eval_count.load(Ordering::SeqCst));
    let mut rule_groups = std::collections::HashSet::new();
    for group in rule_manager.groups().values() {
        rule_groups.insert(group.name().to_string());
        assert!(!group.should_restore());
        for rule in group.all_rules() {
            assert!(rule.as_alerting().unwrap().restored());
        }
    }

    files.push(fixture("alert_rule1.yaml"));
    rule_manager.update(interval, &files, &Labels::empty(), "", None).unwrap();
    // Stop eval.
    rule_manager.stop();
    run.join().unwrap();
    for group in rule_manager.groups().values() {
        if !restore_new_rule_groups {
            // New rule groups added to existing manager will not be restored.
            assert!(!group.should_restore());
            continue;
        }
        if rule_groups.contains(group.name()) {
            // Already restored.
            assert!(!group.should_restore());
            continue;
        }
        // New rule groups added to existing manager will be restored.
        assert!(group.should_restore());
    }
}

#[test]
fn test_new_rule_group_restoration() {
    new_rule_group_restoration(false);
}

#[test]
fn test_new_rule_group_restoration_with_restore_new_group_option() {
    new_rule_group_restoration(true);
}

#[test]
fn test_bounded_rule_eval_concurrency() {
    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let inflight_queries = Arc::new(AtomicI32::new(0));
    let max_inflight = Arc::new(AtomicI32::new(0));
    let max_concurrency = 3i64;
    let group_count = 2;

    let files = vec![fixture("rules_multiple_groups.yaml")];

    let rule_manager = Manager::new(opts_factory(&storage, &max_inflight, &inflight_queries, max_concurrency));

    let groups = rule_manager
        .load_groups(Duration::from_secs(1), &Labels::empty(), "", None, false, &files)
        .unwrap_or_else(|e| panic!("{:?}", e.iter().map(|e| e.to_string()).collect::<Vec<_>>()));
    assert_eq!(groups.len(), group_count);

    let (ctx, cancel) = Context::background().with_cancel();

    // Evaluate groups concurrently (like they normally do).
    std::thread::scope(|s| {
        for group in groups.values() {
            let ctx = &ctx;
            s.spawn(move || group.eval(ctx, Time::now()));
        }
    });
    cancel.cancel();

    // Synchronous queries also count towards inflight, so at most we can have maxConcurrency+$groupCount inflight
    // evaluations.
    assert_eq!(max_inflight.load(Ordering::SeqCst), max_concurrency as i32 + group_count as i32);
}

#[test]
fn test_update_when_stopped() {
    let files = vec![fixture("rules.yaml")];
    let rule_manager = Manager::new(ManagerOptions { context: Context::background(), logger: nop(), ..Default::default() });
    rule_manager.start();
    rule_manager.update(Duration::from_secs(10), &files, &Labels::empty(), "", None).unwrap();
    assert!(!rule_manager.groups().is_empty());

    rule_manager.stop();
    // Updates following a stop are no-op.
    rule_manager.update(Duration::from_secs(10), &[], &Labels::empty(), "", None).unwrap();
}

#[test]
fn test_group_eval_race_condition_on_stopping_group_evaluation_while_rules_are_evaluated_concurrently() {
    let storage_owner = common::new();
    let storage = storage_owner.db.clone();

    let inflight_queries = Arc::new(AtomicI32::new(0));
    let max_inflight = Arc::new(AtomicI32::new(0));
    let max_concurrency = 10i64;

    let files = vec![fixture("rules_multiple_groups.yaml")];
    let files2 = vec![fixture("rules.yaml")];

    let rule_manager = Arc::new(Manager::new(opts_factory(&storage, &max_inflight, &inflight_queries, max_concurrency)));
    let rm = rule_manager.clone();
    let run = std::thread::spawn(move || rm.run());
    rule_manager.block().wait();

    // Update the group a decent number of times to simulate start and stopping in the middle of an evaluation.
    for _ in 0..10 {
        rule_manager.update(Duration::from_secs(1), &files, &Labels::empty(), "", None).unwrap();

        // Wait half of the query execution duration and then change the rule groups loaded by the manager so that
        // the previous rule group will be interrupted while the query is executing.
        std::thread::sleep(ARTIFICIAL_DELAY / 2);

        rule_manager.update(Duration::from_secs(1), &files2, &Labels::empty(), "", None).unwrap();
    }

    rule_manager.stop();
    run.join().unwrap();
}

#[test]
fn test_labels_from_maps() {
    let m1: BTreeMap<String, String> = [("aaa", "101"), ("bbb", "222")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let m2: BTreeMap<String, String> = [("aaa", "111"), ("ccc", "333")].iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let m_labels = from_maps(&[&m1, &m2]);

    let expected = labels::new(&mut [Label::new("aaa", "111"), Label::new("bbb", "222"), Label::new("ccc", "333")]);

    assert_eq!(expected, m_labels, "unexpected labelset");
}

#[test]
fn test_parse_files() {
    // good files
    {
        let r = parse_files(&[fixture("rules.y*ml")], ValidationScheme::Utf8, &test_parser(), &nop());
        assert!(r.is_ok(), "{:?}", r.err().map(|e| e.to_string()));
    }
    // bad files
    {
        let err = parse_files(&[fixture("invalid_rules.y*ml")], ValidationScheme::Utf8, &test_parser(), &nop()).unwrap_err();
        assert!(err.to_string().contains("field unexpected_field not found in type rulefmt.Rule"), "{err}");
    }
}

#[test]
fn test_rule_dependency_controller_analyse_rules() {
    struct TestCase {
        name: &'static str,
        rule_file: &'static str,
        expected: HashMap<&'static str, (bool, bool)>,
    }

    let test_cases = vec![
        TestCase {
            name: "all independent rules",
            rule_file: "rules_multiple_independent.yaml",
            expected: HashMap::from([
                ("job:http_requests:rate1m", (true, true)),
                ("job:http_requests:rate5m", (true, true)),
                ("job:http_requests:rate15m", (true, true)),
                ("job:http_requests:rate30m", (true, true)),
                ("job:http_requests:rate1h", (true, true)),
                ("job:http_requests:rate2h", (true, true)),
                ("job:http_requests_unless_single_alert:rate2h", (true, true)),
                ("job:http_requests_unless_multiple_alerts:rate2h", (true, true)),
            ]),
        },
        TestCase {
            name: "some dependent rules",
            rule_file: "rules_multiple.yaml",
            expected: HashMap::from([
                ("job:http_requests:rate1m", (true, true)),
                ("job:http_requests:rate5m", (true, true)),
                ("job:http_requests:rate15m", (false, true)),
                ("TooManyRequests", (true, false)),
                ("TooManyFailures", (false, true)),
                ("TooManyFailuresAlerts", (true, false)),
            ]),
        },
        TestCase {
            name: "indeterminate rules",
            rule_file: "rules_indeterminates.yaml",
            expected: HashMap::from([
                ("job:http_requests:rate1m", (false, false)),
                ("job:http_requests:rate5m", (false, false)),
                ("job:http_requests:rate15m", (false, false)),
                ("job:http_requests:rate30m", (false, false)),
                ("job:http_requests:rate1h", (false, false)),
                ("job:http_requests:rate2h", (false, false)),
                ("matcher", (false, false)),
            ]),
        },
    ];

    for tc in test_cases {
        let storage_owner = common::new();
    let storage = storage_owner.db.clone();

        let rule_manager = Manager::new(ManagerOptions {
            context: Context::background(),
            logger: nop(),
            appendable_v2: Some(storage.clone()),
            query_func: Some(Arc::new(|_: &Context, _: &str, _: Time| Ok(Vector::new()))),
            ..Default::default()
        });

        let groups = load_one(&rule_manager, tc.rule_file);

        for g in groups.values() {
            rule_manager.opts().rule_dependency_controller.as_ref().unwrap().analyse_rules(g.all_rules());

            for r in g.all_rules() {
                let exp = tc.expected.get(r.name()).unwrap_or_else(|| panic!("{}: rule not found in expected: {}", tc.name, r.string()));
                assert_eq!(exp.0, r.no_dependent_rules(), "{}: rule: {}", tc.name, r.string());
                assert_eq!(exp.1, r.no_dependency_rules(), "{}: rule: {}", tc.name, r.string());
            }
        }
    }
}

struct CloseCountingEngine {
    close_calls: Arc<AtomicUsize>,
}

struct CloseCountingQuery {
    close_calls: Arc<AtomicUsize>,
}

impl QueryEngine for CloseCountingEngine {
    fn new_instant_query(&self, _: &Context, _: Arc<dyn Queryable>, _: &str, _: i64) -> Result<Box<dyn EngineQuery>, Error> {
        Ok(Box::new(CloseCountingQuery { close_calls: self.close_calls.clone() }))
    }
}

impl EngineQuery for CloseCountingQuery {
    fn exec(&mut self, _: &Context) -> QueryResult {
        QueryResult { err: None, value: Some(Value::Vector(Vector::new())), warnings: Default::default() }
    }
    fn close(&mut self) {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// TestEngineQueryFunc_ClosesQuery verifies EngineQueryFunc closes the query it creates after every evaluation.
#[test]
fn test_engine_query_func_closes_query() {
    let eng = Arc::new(CloseCountingEngine { close_calls: Arc::new(AtomicUsize::new(0)) });
    // adapted: Go passes a nil Queryable; the engine here never uses it.
    let st_owner = common::new();
    let q: Arc<dyn Queryable> = st_owner.db.clone();
    let qf = engine_query_func(eng.clone(), q);

    qf(&Context::background(), "vector(0)", Time::unix(0, 0)).unwrap();
    assert_eq!(1, eng.close_calls.load(Ordering::SeqCst), "Close must be called exactly once");
}
