//! Port of `storage/fanout_test.go`.
//!
//! Waiting for TSDB (they use `teststorage.New`, a real TSDB, and are not ported yet): `TestFanout_CostSamplingQueryable`,
//! `TestFanout_SelectSorted`, `TestFanout_SelectSorted_AppenderV2`, `TestFanoutErrors`,
//! `TestFanout_Querier_SearcherCapability`. Benchmarks are not ported.

#[path = "common/teststorage.rs"]
mod teststorage;

use std::sync::Arc;

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use storage::tsdbutil::{generate_test_float_histogram, generate_test_histogram};
use storage::*;
use teststorage::{Sample, new_appendable, require_equal};

/// Go `mockStorage`: a storage whose appenders come from the given appendables; other methods are not used.
struct MockStorage {
    app: Option<Arc<dyn Appendable>>,
    app_v2: Option<Arc<dyn AppendableV2>>,
}

fn mock_app(a: &teststorage::Appendable) -> Arc<dyn Storage> {
    Arc::new(MockStorage {
        app: Some(Arc::new(a.clone())),
        app_v2: None,
    })
}

fn mock_app_v2(a: Arc<dyn AppendableV2>) -> Arc<dyn Storage> {
    Arc::new(MockStorage {
        app: None,
        app_v2: Some(a),
    })
}

impl Queryable for MockStorage {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        unimplemented!("mockStorage.Querier")
    }
}

impl ChunkQueryable for MockStorage {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        unimplemented!("mockStorage.ChunkQuerier")
    }
}

impl Appendable for MockStorage {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        self.app.as_ref().expect("appendable").appender(ctx)
    }
}

impl AppendableV2 for MockStorage {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        self.app_v2
            .as_ref()
            .expect("appendable v2")
            .appender_v2(ctx)
    }
}

impl Storage for MockStorage {
    fn start_time(&self) -> Result<i64, Error> {
        unimplemented!("mockStorage.StartTime")
    }
    fn close(&self) -> Option<Error> {
        unimplemented!("mockStorage.Close")
    }
}

fn without_exemplars(s: &[Sample]) -> Vec<Sample> {
    s.iter()
        .map(|x| Sample {
            es: Vec::new(),
            ..x.clone()
        })
        .collect()
}

struct FanoutAppenderTestCase {
    name: &'static str,
    primary: teststorage::Appendable,
    secondary: teststorage::Appendable,

    expect_append_err: bool,
    expect_exemplar_error: bool,
    expect_commit_error: bool,

    expect_primary_samples: Vec<Sample>,
    expect_secondary_samples: Vec<Sample>,
}

fn fanout_appender_test_cases(expected: &[Sample]) -> Vec<FanoutAppenderTestCase> {
    let app_err = Error::msg("append test error");
    let ex_err = Error::msg("exemplar test error");
    let commit_err = Error::msg("commit test error");
    let failing = |e: &Error| -> teststorage::AppendErrFn {
        let e = e.clone();
        Box::new(move |_: &Labels| Some(e.clone()))
    };
    let ok = || -> teststorage::AppendErrFn { Box::new(|_: &Labels| None) };
    vec![
        FanoutAppenderTestCase {
            name: "both works",
            primary: new_appendable(),
            secondary: new_appendable(),
            expect_append_err: false,
            expect_exemplar_error: false,
            expect_commit_error: false,
            expect_primary_samples: expected.to_vec(),
            expect_secondary_samples: expected.to_vec(),
        },
        FanoutAppenderTestCase {
            name: "primary errors",
            primary: new_appendable().with_errs(
                failing(&app_err),
                Some(ex_err.clone()),
                Some(commit_err.clone()),
            ),
            secondary: new_appendable(),
            expect_append_err: true,
            expect_exemplar_error: true,
            expect_commit_error: true,
            expect_primary_samples: Vec::new(),
            expect_secondary_samples: Vec::new(),
        },
        FanoutAppenderTestCase {
            name: "exemplar errors",
            primary: new_appendable().with_errs(ok(), Some(ex_err.clone()), None),
            secondary: new_appendable().with_errs(ok(), Some(ex_err.clone()), None),
            expect_append_err: false,
            expect_exemplar_error: true,
            expect_commit_error: false,
            expect_primary_samples: without_exemplars(expected),
            expect_secondary_samples: without_exemplars(expected),
        },
        FanoutAppenderTestCase {
            name: "secondary errors",
            primary: new_appendable(),
            secondary: new_appendable().with_errs(
                failing(&app_err),
                Some(ex_err),
                Some(commit_err),
            ),
            expect_append_err: true,
            expect_exemplar_error: true,
            expect_commit_error: true,
            expect_primary_samples: expected.to_vec(),
            expect_secondary_samples: Vec::new(),
        },
    ]
}

fn metric(name: &str) -> Labels {
    labels::from_strings(&[labels::METRIC_NAME, name])
}

fn check_err<T>(name: &str, what: &str, expect_err: bool, res: &Result<T, Error>) {
    assert_eq!(
        res.is_err(),
        expect_err,
        "{}: {}: {:?}",
        name,
        what,
        res.as_ref().err()
    );
}

/// The final checks of every appender test case: nothing pending or rolled back, the expected results.
fn check_results(tt: &FanoutAppenderTestCase) {
    assert!(tt.primary.pending_samples().is_none(), "{}", tt.name);
    require_equal(
        &tt.expect_primary_samples,
        &tt.primary.result_samples().unwrap_or_default(),
    );
    assert!(tt.primary.rolledback_samples().is_none(), "{}", tt.name);

    assert!(tt.secondary.pending_samples().is_none(), "{}", tt.name);
    require_equal(
        &tt.expect_secondary_samples,
        &tt.secondary.result_samples().unwrap_or_default(),
    );
    assert!(tt.secondary.rolledback_samples().is_none(), "{}", tt.name);
}

fn sample(
    l: Labels,
    st: i64,
    t: i64,
    v: f64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
    es: Vec<Exemplar>,
) -> Sample {
    Sample {
        l,
        st,
        t,
        v,
        h,
        fh,
        es,
        ..Default::default()
    }
}

#[test]
fn test_fanout_appender() {
    let h = generate_test_histogram(0);
    let fh = generate_test_float_histogram(0);
    let ex = Exemplar {
        value: 1.0,
        ..Default::default()
    };

    let expected = vec![
        sample(metric("metric1"), 0, 0, 1.0, None, None, vec![ex.clone()]),
        sample(metric("metric2"), 0, 1, 0.0, Some(h.clone()), None, vec![]),
        sample(metric("metric3"), 0, 2, 0.0, None, Some(fh.clone()), vec![]),
    ];
    let ctx = Context::background();
    for tt in fanout_appender_test_cases(&expected) {
        let f = new_fanout(None, mock_app(&tt.primary), vec![mock_app(&tt.secondary)]);
        let mut app = f.appender(&ctx);

        let res = app.append(0, &metric("metric1"), 0, 1.0);
        check_err(tt.name, "Append", tt.expect_append_err, &res);
        // Go keeps using the returned ref, which is 0 on errors.
        let r = res.unwrap_or(0);

        let res = app.append_exemplar(r, &metric("metric1"), &ex);
        check_err(tt.name, "AppendExemplar", tt.expect_exemplar_error, &res);

        let res = app.append_histogram(0, &metric("metric2"), 1, Some(&h), None);
        check_err(tt.name, "AppendHistogram", tt.expect_append_err, &res);

        let res = app.append_histogram(0, &metric("metric3"), 2, None, Some(&fh));
        check_err(tt.name, "AppendHistogram float", tt.expect_append_err, &res);

        let err = app.commit();
        assert_eq!(
            err.is_some(),
            tt.expect_commit_error,
            "{}: Commit: {:?}",
            tt.name,
            err
        );

        check_results(&tt);
    }
}

#[test]
fn test_fanout_appender_v2() {
    let h = generate_test_histogram(0);
    let fh = generate_test_float_histogram(0);
    let ex = Exemplar {
        value: 1.0,
        ..Default::default()
    };

    let expected = vec![
        sample(metric("metric1"), -1, 0, 1.0, None, None, vec![ex.clone()]),
        sample(metric("metric2"), -2, 1, 0.0, Some(h.clone()), None, vec![]),
        sample(
            metric("metric3"),
            -3,
            2,
            0.0,
            None,
            Some(fh.clone()),
            vec![],
        ),
    ];
    let ctx = Context::background();
    for tt in fanout_appender_test_cases(&expected) {
        let f = new_fanout(
            None,
            mock_app_v2(Arc::new(tt.primary.clone())),
            vec![mock_app_v2(Arc::new(tt.secondary.clone()))],
        );
        let mut app = f.appender_v2(&ctx);

        let exs = [ex.clone()];
        let res = app.append(
            0,
            &metric("metric1"),
            -1,
            0,
            1.0,
            None,
            None,
            &AOptions {
                exemplars: &exs,
                ..Default::default()
            },
        );
        if tt.expect_append_err {
            assert!(res.is_err(), "{}", tt.name);
        } else if tt.expect_exemplar_error {
            let err = res.expect_err("partial error");
            let p = err.as_append_partial().expect("an AppendPartialError");
            // One for primary, one for secondary: in the V2 flow the sample is appended even when the first
            // append partially failed with exemplars.
            assert_eq!(p.exemplar_errors.len(), 2, "{}", tt.name);
            // Both errors refer to the same, only exemplar.
            for e in &p.exemplar_errors {
                assert_eq!(e.index, 0);
            }
        } else {
            assert!(res.is_ok(), "{}: {:?}", tt.name, res);
        }

        let res = app.append(
            0,
            &metric("metric2"),
            -2,
            1,
            0.0,
            Some(&h),
            None,
            &AOptions::default(),
        );
        check_err(tt.name, "Append histogram", tt.expect_append_err, &res);

        let res = app.append(
            0,
            &metric("metric3"),
            -3,
            2,
            0.0,
            None,
            Some(&fh),
            &AOptions::default(),
        );
        check_err(
            tt.name,
            "Append float histogram",
            tt.expect_append_err,
            &res,
        );

        let err = app.commit();
        assert_eq!(
            err.is_some(),
            tt.expect_commit_error,
            "{}: Commit: {:?}",
            tt.name,
            err
        );

        check_results(&tt);
    }
}

/// Go `appendableV2WithoutExemplars`: hides the `ExemplarAppenderV2` capability of the wrapped appender.
struct AppendableV2WithoutExemplars(teststorage::Appendable);

struct AppenderV2WithoutExemplars(Box<dyn AppenderV2>);

impl AppendableV2 for AppendableV2WithoutExemplars {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(AppenderV2WithoutExemplars(self.0.appender_v2(ctx)))
    }
}

impl AppenderTransaction for AppenderV2WithoutExemplars {
    fn commit(&mut self) -> Option<Error> {
        self.0.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.0.rollback()
    }
}

impl AppenderV2 for AppenderV2WithoutExemplars {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        self.0.append(r, ls, st, t, v, h, fh, opts)
    }
}

#[test]
fn test_fanout_appender_v2_append_exemplars() {
    let h = generate_test_histogram(0);
    let fh = generate_test_float_histogram(0);
    let ex = Exemplar {
        value: 1.0,
        ..Default::default()
    };

    let expected = vec![
        sample(metric("metric1"), -1, 0, 1.0, None, None, vec![ex.clone()]),
        sample(metric("metric2"), -2, 1, 0.0, Some(h.clone()), None, vec![]),
        sample(
            metric("metric3"),
            -3,
            2,
            0.0,
            None,
            Some(fh.clone()),
            vec![],
        ),
    ];
    let ctx = Context::background();
    for tt in fanout_appender_test_cases(&expected) {
        let f = new_fanout(
            None,
            mock_app_v2(Arc::new(tt.primary.clone())),
            vec![mock_app_v2(Arc::new(tt.secondary.clone()))],
        );
        let mut app = f.appender_v2(&ctx);
        let ex_app = app.as_exemplar_appender_v2();
        assert!(ex_app.is_some(), "{}", tt.name);

        let res = app.append(
            0,
            &metric("metric1"),
            -1,
            0,
            1.0,
            None,
            None,
            &AOptions::default(),
        );
        check_err(tt.name, "Append", tt.expect_append_err, &res);
        let r = res.unwrap_or(0);

        let res = app
            .as_exemplar_appender_v2()
            .expect("exemplar appender")
            .append_exemplars(r, &metric("metric1"), &[ex.clone()]);
        if tt.expect_append_err {
            assert!(res.is_err(), "{}", tt.name);
        } else if tt.expect_exemplar_error {
            let err = res.expect_err("partial error");
            let p = err.as_append_partial().expect("an AppendPartialError");
            // One for primary, one for secondary, both for the only exemplar.
            assert_eq!(p.exemplar_errors.len(), 2, "{}", tt.name);
            for e in &p.exemplar_errors {
                assert_eq!(e.index, 0);
            }
        } else {
            assert!(res.is_ok(), "{}: {:?}", tt.name, res);
        }

        let res = app.append(
            0,
            &metric("metric2"),
            -2,
            1,
            0.0,
            Some(&h),
            None,
            &AOptions::default(),
        );
        check_err(tt.name, "Append histogram", tt.expect_append_err, &res);

        let res = app.append(
            0,
            &metric("metric3"),
            -3,
            2,
            0.0,
            None,
            Some(&fh),
            &AOptions::default(),
        );
        check_err(
            tt.name,
            "Append float histogram",
            tt.expect_append_err,
            &res,
        );

        let err = app.commit();
        assert_eq!(
            err.is_some(),
            tt.expect_commit_error,
            "{}: Commit: {:?}",
            tt.name,
            err
        );

        check_results(&tt);
    }

    // secondary without ExemplarAppenderV2 is skipped.
    {
        let primary = new_appendable();
        let secondary = new_appendable();
        let f = new_fanout(
            None,
            mock_app_v2(Arc::new(primary.clone())),
            vec![mock_app_v2(Arc::new(AppendableV2WithoutExemplars(
                secondary.clone(),
            )))],
        );
        let mut app = f.appender_v2(&ctx);
        let lbls = metric("metric1");
        let r = app
            .append(0, &lbls, -1, 0, 1.0, None, None, &AOptions::default())
            .expect("append");
        app.as_exemplar_appender_v2()
            .expect("exemplar appender")
            .append_exemplars(r, &lbls, &[ex.clone()])
            .expect("exemplars");
        assert!(app.commit().is_none());

        require_equal(
            &[sample(
                lbls.clone(),
                -1,
                0,
                1.0,
                None,
                None,
                vec![ex.clone()],
            )],
            &primary.result_samples().unwrap_or_default(),
        );
        require_equal(
            &[sample(lbls, -1, 0, 1.0, None, None, vec![])],
            &secondary.result_samples().unwrap_or_default(),
        );
    }

    // primary without ExemplarAppenderV2 returns error.
    {
        let primary = new_appendable();
        let secondary = new_appendable();
        let f = new_fanout(
            None,
            mock_app_v2(Arc::new(AppendableV2WithoutExemplars(primary))),
            vec![mock_app_v2(Arc::new(secondary))],
        );
        let mut app = f.appender_v2(&ctx);
        let lbls = metric("metric1");
        let r = app
            .append(0, &lbls, -1, 0, 1.0, None, None, &AOptions::default())
            .expect("append");
        let err = app
            .as_exemplar_appender_v2()
            .expect("exemplar appender")
            .append_exemplars(r, &lbls, &[ex.clone()])
            .expect_err("an error");
        assert!(
            err.to_string()
                .contains("does not implement ExemplarAppenderV2"),
            "{}",
            err
        );
        assert!(app.rollback().is_none());
    }
}
