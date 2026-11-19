//! Port of `util/teststorage/appender_test.go`.

use std::sync::Arc;

use model::exemplar::Exemplar;
use model::labels::{empty_labels, from_strings};
use model::metadata::{Metadata, MetricType};
use model::value::STALE_NAN;
use storage::tsdbutil::{generate_test_float_histogram, generate_test_histogram};
use storage::{AppendV2Options, Appendable as _, AppendableV2 as _, Context, Error, ERR_NOT_FOUND};
use util::teststorage::{reorder_expected_for_staleness, require_equal, require_not_equal, Appendable, Sample};

fn stale() -> f64 {
    f64::from_bits(STALE_NAN)
}

fn meta(typ: MetricType, unit: &str, help: &str) -> Metadata {
    Metadata { typ, unit: unit.to_string(), help: help.to_string() }
}

fn ls(s: &[&str]) -> model::labels::Labels {
    from_strings(s)
}

fn test_appendable_v1(app_test: &Appendable, a: &dyn storage::Appendable) {
    let ctx = Context::background();
    for commit in [true, false] {
        app_test.result_reset();

        let mut app = a.appender(&ctx);

        let ref1 = app.append(0, &ls(&["__name__", "test_metric1", "app", "v1"]), 1, 2.0).unwrap();

        let h = generate_test_histogram(0);
        app.append_histogram(0, &ls(&["__name__", "test_metric2", "app", "v1"]), 2, Some(&h), None).unwrap();

        let fh = generate_test_float_histogram(0);
        app.append_histogram(0, &ls(&["__name__", "test_metric3", "app", "v1"]), 3, None, Some(&fh)).unwrap();

        // Update meta of first series.
        let m1 = meta(MetricType::Gauge, "", "other help text");
        app.update_metadata(ref1, &ls(&["__name__", "test_metric1", "app", "v1"]), &m1).unwrap();

        // Add exemplars to the first series.
        let e1 = Exemplar { labels: ls(&["__name__", "yolo"]), has_ts: true, ts: 1, ..Default::default() };
        app.append_exemplar(ref1, &ls(&["__name__", "test_metric1", "app", "v1"]), &e1).unwrap();

        let exp = vec![
            Sample {
                l: ls(&["__name__", "test_metric1", "app", "v1"]),
                m: m1.clone(),
                t: 1,
                v: 2.0,
                es: vec![e1.clone()],
                ..Default::default()
            },
            Sample { l: ls(&["__name__", "test_metric2", "app", "v1"]), t: 2, h: Some(h.clone()), ..Default::default() },
            Sample { l: ls(&["__name__", "test_metric3", "app", "v1"]), t: 3, fh: Some(fh.clone()), ..Default::default() },
        ];
        require_equal(&exp, &app_test.pending_samples());
        assert!(app_test.result_samples().is_empty());
        assert!(app_test.rolledback_samples().is_empty());

        if commit {
            assert!(app.commit().is_none());
            assert!(app_test.pending_samples().is_empty());
            require_equal(&exp, &app_test.result_samples());
            assert!(app_test.rolledback_samples().is_empty());
            break;
        }

        assert!(app.rollback().is_none());
        assert!(app_test.pending_samples().is_empty());
        assert!(app_test.result_samples().is_empty());
        require_equal(&exp, &app_test.rolledback_samples());
    }
}

fn test_appendable_v2(app_test: &Appendable, a: &dyn storage::AppendableV2) {
    let ctx = Context::background();
    for commit in [true, false] {
        app_test.result_reset();

        let mut app = a.appender_v2(&ctx);

        let m1 = meta(MetricType::Gauge, "", "other help text");
        let e1 = Exemplar { labels: ls(&["__name__", "yolo"]), has_ts: true, ts: 1, ..Default::default() };
        let e1s = [e1.clone()];
        app.append(
            0,
            &ls(&["__name__", "test_metric1", "app", "v2"]),
            -1,
            1,
            2.0,
            None,
            None,
            &AppendV2Options {
                metric_family_name: "test_metric1",
                metadata: Some(&m1),
                exemplars: &e1s,
                ..Default::default()
            },
        )
        .unwrap();

        let h = generate_test_histogram(0);
        app.append(
            0,
            &ls(&["__name__", "test_metric2", "app", "v2"]),
            -2,
            2,
            0.0,
            Some(&h),
            None,
            &AppendV2Options::default(),
        )
        .unwrap();

        let fh = generate_test_float_histogram(0);
        let ref3 = app
            .append(
                0,
                &ls(&["__name__", "test_metric3", "app", "v2"]),
                -3,
                3,
                0.0,
                None,
                Some(&fh),
                &AppendV2Options::default(),
            )
            .unwrap();

        let ex_app = app.as_exemplar_appender_v2().expect("ExemplarAppenderV2");
        let err = ex_app.append_exemplars(0, &ls(&["__name__", "unknown"]), &[]).unwrap_err();
        assert!(err.is(&ERR_NOT_FOUND), "{err}");

        let e3 = Exemplar { labels: ls(&["__name__", "trace"]), has_ts: true, ts: 3, value: 5.0 };
        ex_app.append_exemplars(ref3, &empty_labels(), &[e3.clone()]).unwrap();

        let exp = vec![
            Sample {
                l: ls(&["__name__", "test_metric1", "app", "v2"]),
                mf: "test_metric1".to_string(),
                m: m1.clone(),
                st: -1,
                t: 1,
                v: 2.0,
                es: vec![e1.clone()],
                ..Default::default()
            },
            Sample {
                l: ls(&["__name__", "test_metric2", "app", "v2"]),
                st: -2,
                t: 2,
                h: Some(h.clone()),
                ..Default::default()
            },
            Sample {
                l: ls(&["__name__", "test_metric3", "app", "v2"]),
                st: -3,
                t: 3,
                fh: Some(fh.clone()),
                es: vec![e3.clone()],
                ..Default::default()
            },
        ];
        require_equal(&exp, &app_test.pending_samples());
        assert!(app_test.result_samples().is_empty());
        assert!(app_test.rolledback_samples().is_empty());

        if commit {
            assert!(app.commit().is_none());
            assert!(app_test.pending_samples().is_empty());
            require_equal(&exp, &app_test.result_samples());
            assert!(app_test.rolledback_samples().is_empty());
            break;
        }

        assert!(app.rollback().is_none());
        assert!(app_test.pending_samples().is_empty());
        assert!(app_test.result_samples().is_empty());
        require_equal(&exp, &app_test.rolledback_samples());
    }
}

#[test]
fn test_appendable() {
    let app_test = Appendable::new();
    test_appendable_v1(&app_test, &app_test);
    test_appendable_v2(&app_test, &app_test);
}

#[test]
fn test_appendable_then() {
    let next_app_test = Appendable::new();
    let app = Appendable::new().then(Arc::new(next_app_test.clone()));

    // Ensure next mock record all the appends when appending to app.
    test_appendable_v1(&next_app_test, &app);
    // Ensure next mock record all the appends when appending to app.
    test_appendable_v2(&next_app_test, &app);
}

fn s(l: &[&str], m: Metadata) -> Sample {
    Sample { l: ls(l), m, ..Default::default() }
}

fn es(name: &str) -> Sample {
    Sample { es: vec![Exemplar { labels: ls(&["__name__", name]), ..Default::default() }], ..Default::default() }
}

#[test]
fn test_sample_require_equal() {
    let counter = || meta(MetricType::Counter, "metric", "some help text");
    let gauge = || meta(MetricType::Gauge, "", "other help text");

    let a = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: 123.123, ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_equal(&a, &a);

    let b1 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        // test_metric2_diff is different.
        Sample { v: 123.123, ..s(&["__name__", "test_metric2_diff", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_not_equal(&a, &b1);

    let b2 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: 123.123, ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo2"), // exemplar is different.
    ];
    require_not_equal(&a, &b2);

    let b3 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        // Timestamp is different.
        Sample { v: 123.123, t: 123, ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_not_equal(&a, &b3);

    let b4 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        // Value is different.
        Sample { v: 456.456, ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_not_equal(&a, &b4);

    // adapted: Go uses the type string "counter2"; the Rust metric type is an enum that cannot hold an unknown
    // type, so a different valid type ("gauge") stands for "different type".
    let b5 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], meta(MetricType::Gauge, "metric", "some help text")), // Different type.
        Sample { v: 123.123, ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_not_equal(&a, &b5);

    // NaN comparison.
    let a = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: stale(), ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_equal(&a, &a);

    // NaN comparison with different order.
    let a = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: stale(), ..s(&["__name__", "test_metric10", "foo", "bar"], gauge()) },
        Sample { v: stale(), ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    let b6 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: stale(), ..s(&["__name__", "test_metric2", "foo", "bar"], gauge()) },
        Sample { v: stale(), ..s(&["__name__", "test_metric10", "foo", "bar"], gauge()) },
        es("yolo"),
    ];
    require_equal(&a, &b6);

    // Not equal with NaNs.
    // adapted: Go uses the type string "gauge2"; "counter" stands for a different type (see b5).
    let b7 = vec![
        Sample::default(),
        s(&["__name__", "test_metric_total"], counter()),
        Sample { v: stale(), ..s(&["__name__", "test_metric10", "foo", "bar"], gauge()) },
        // metadata different
        Sample {
            v: stale(),
            ..s(&["__name__", "test_metric2", "foo", "bar"], meta(MetricType::Counter, "", "other help text"))
        },
        es("yolo"),
    ];
    require_not_equal(&a, &b7);
}

#[test]
fn test_concurrent_appender_returns_err_appender() {
    let ctx = Context::background();
    let a = Appendable::new();

    // Non-concurrent multiple use if fine.
    let mut app = a.appender(&ctx);
    assert_eq!(1, a.open_appenders());
    assert!(app.commit().is_none());
    // Repeated commit fails.
    assert!(app.commit().is_some());

    let mut app = a.appender(&ctx);
    assert!(app.rollback().is_none());
    // Commit after rollback fails.
    assert!(app.commit().is_some());

    a.with_errs(None, None, Some(Error::msg("commit err")));
    let mut app = a.appender(&ctx);
    assert!(app.commit().is_some());

    a.with_errs(None, None, None);
    let mut app = a.appender(&ctx);
    assert!(app.commit().is_none());
    assert_eq!(0, a.open_appenders());

    // Concurrent use should return appender that errors.
    let _first = a.appender(&ctx);
    let mut app = a.appender(&ctx);
    assert!(app.append(0, &empty_labels(), 0, 0.0).is_err());
    assert!(app.append_histogram(0, &empty_labels(), 0, None, None).is_err());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());
}

#[test]
fn test_concurrent_appender_v2_returns_err_appender() {
    let ctx = Context::background();
    let a = Appendable::new();

    // Non-concurrent multiple use if fine.
    let mut app = a.appender_v2(&ctx);
    assert_eq!(1, a.open_appenders());
    assert!(app.commit().is_none());
    // Repeated commit fails.
    assert!(app.commit().is_some());

    let mut app = a.appender_v2(&ctx);
    assert!(app.rollback().is_none());
    // Commit after rollback fails.
    assert!(app.commit().is_some());

    a.with_errs(None, None, Some(Error::msg("commit err")));
    let mut app = a.appender_v2(&ctx);
    assert!(app.commit().is_some());

    a.with_errs(None, None, None);
    let mut app = a.appender_v2(&ctx);
    assert!(app.commit().is_none());
    assert_eq!(0, a.open_appenders());

    // Concurrent use should return appender that errors.
    let _first = a.appender_v2(&ctx);
    let mut app = a.appender_v2(&ctx);
    assert!(app.append(0, &empty_labels(), 0, 0, 0.0, None, None, &AppendV2Options::default()).is_err());
    assert!(app.commit().is_some());
    assert!(app.rollback().is_some());
}

#[test]
fn test_reorder_expected_for_staleness() {
    fn sm(a: &str, t: i64, v: f64) -> Sample {
        Sample { l: ls(&["a", a]), t, v, ..Default::default() }
    }
    let st = stale();
    struct Case {
        name: &'static str,
        in_expected: Vec<Sample>,
        in_got: Vec<Sample>,
        expected: Option<Vec<Sample>>,
    }
    let testcases = vec![
        Case {
            name: "no staleness markers",
            in_expected: vec![sm("1", 1, 1.0), sm("2", 1, 2.0)],
            in_got: vec![sm("2", 1, 2.0), sm("1", 1, 1.0)],
            expected: None,
        },
        Case {
            name: "with staleness markers",
            in_expected: vec![sm("1", 1, 1.0), sm("2", 2, 2.0), sm("3", 3, st), sm("4", 4, st)],
            in_got: vec![sm("1", 1, 1.0), sm("2", 2, 2.0), sm("3", 3, st), sm("4", 4, st)],
            expected: None,
        },
        Case {
            name: "with staleness markers wrong order",
            in_expected: vec![sm("1", 1, 1.0), sm("2", 2, 2.0), sm("3", 3, st), sm("4", 4, st)],
            in_got: vec![sm("2", 2, 2.0), sm("1", 1, 1.0), sm("4", 4, st), sm("3", 3, st)],
            expected: Some(vec![sm("1", 1, 1.0), sm("2", 2, 2.0), sm("4", 4, st), sm("3", 3, st)]),
        },
        Case {
            name: "with staleness markers wrong order but not consecutive",
            in_expected: vec![sm("1", 1, 1.0), sm("3", 3, st), sm("2", 2, 2.0), sm("4", 4, st)],
            in_got: vec![sm("2", 2, 2.0), sm("1", 1, 1.0), sm("4", 4, st), sm("3", 3, st)],
            expected: Some(vec![sm("1", 1, 1.0), sm("3", 3, st), sm("2", 2, 2.0), sm("4", 4, st)]),
        },
    ];
    for tc in testcases {
        let expected = tc.expected.unwrap_or_else(|| tc.in_expected.clone());
        let got = reorder_expected_for_staleness(&tc.in_expected, &tc.in_got);
        util::teststorage::require_equal_msg(&expected, &got, tc.name);
    }
}

#[test]
fn test_sample_is_stale() {
    let s1 = Sample { v: 1.0, ..Default::default() };
    assert!(!s1.is_stale());
    let s2 = Sample { v: stale(), ..Default::default() };
    assert!(s2.is_stale());
    let mut h = generate_test_histogram(0);
    let h1 = Sample { v: stale(), h: Some(h.clone()), ..Default::default() };
    assert!(!h1.is_stale()); // Histogram takes precedence over V.
    h.sum = stale();
    let h2 = Sample { v: 1.0, h: Some(h.clone()), ..Default::default() };
    assert!(h2.is_stale());
    let mut fh = generate_test_float_histogram(0);
    let fh1 = Sample { v: stale(), h: Some(h.clone()), fh: Some(fh.clone()), ..Default::default() };
    assert!(!fh1.is_stale()); // FloatHistogram takes precedence over all.
    fh.sum = stale();
    let fh2 = Sample { v: 1.0, h: Some(generate_test_histogram(1)), fh: Some(fh), ..Default::default() };
    assert!(fh2.is_stale());
}
