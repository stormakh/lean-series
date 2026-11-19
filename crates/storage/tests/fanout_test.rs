//! Port of `storage/fanout_test.go`.
//!
//! The tests that use `teststorage.New` (a real TSDB) use `common/teststorage_db.rs`. Benchmarks are not ported.

#[path = "common/teststorage.rs"]
mod teststorage;
#[path = "common/teststorage_db.rs"]
mod teststorage_db;

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

// ---------------------------------------------------------------------------------------------------------------
// Tests with a real TSDB (Go `teststorage.New`).

/// Delegates the storage methods to a test storage (Go embedding `storage.Storage`).
macro_rules! delegate_storage {
    ($t:ty, $inner:tt) => {
        impl Appendable for $t {
            fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
                Appendable::appender(&*self.$inner, ctx)
            }
        }
        impl AppendableV2 for $t {
            fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
                AppendableV2::appender_v2(&*self.$inner, ctx)
            }
        }
    };
}

/// Go `costSamplingTestStorage`.
struct CostSamplingTestStorage {
    primary: Arc<tsdb::db::DB>,
    /// Whether `source` is the primary's native source (Go `source: native`); `None` otherwise.
    native_source: bool,
    supported: std::sync::atomic::AtomicBool,
    queries: std::sync::atomic::AtomicUsize,
}

impl CostSamplingTestStorage {
    fn new(primary: &Arc<tsdb::db::DB>, native_source: bool, supported: bool) -> Arc<CostSamplingTestStorage> {
        Arc::new(CostSamplingTestStorage {
            primary: primary.clone(),
            native_source,
            supported: std::sync::atomic::AtomicBool::new(supported),
            queries: std::sync::atomic::AtomicUsize::new(0),
        })
    }
}

impl CostSamplingQueryable for CostSamplingTestStorage {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        let source = if self.native_source { Some(self.primary.cost_sampling_queryable().0) } else { None };
        (source, self.supported.load(std::sync::atomic::Ordering::SeqCst))
    }
}

impl Queryable for CostSamplingTestStorage {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        self.queries.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Error::msg("unexpected secondary sampling"))
    }
}

impl ChunkQueryable for CostSamplingTestStorage {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        self.queries.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Error::msg("unexpected secondary sampling"))
    }
}

delegate_storage!(CostSamplingTestStorage, primary);

impl Storage for CostSamplingTestStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Storage::start_time(&*self.primary)
    }
    fn close(&self) -> Option<Error> {
        Storage::close(&*self.primary)
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn CostSamplingQueryable> {
        Some(self)
    }
}

/// Go `costSamplingUnknownStorage`: hides the optional sampling capability of the embedded concrete storage.
struct CostSamplingUnknownStorage(Arc<tsdb::db::DB>);

impl Queryable for CostSamplingUnknownStorage {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        Queryable::querier(&*self.0, mint, maxt)
    }
}

impl ChunkQueryable for CostSamplingUnknownStorage {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        ChunkQueryable::chunk_querier(&*self.0, mint, maxt)
    }
}

delegate_storage!(CostSamplingUnknownStorage, 0);

impl Storage for CostSamplingUnknownStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Storage::start_time(&*self.0)
    }
    fn close(&self) -> Option<Error> {
        Storage::close(&*self.0)
    }
}

fn same_source(a: &dyn SampleAndChunkQueryable, b: &dyn SampleAndChunkQueryable) -> bool {
    std::ptr::addr_eq(a as *const dyn SampleAndChunkQueryable, b as *const dyn SampleAndChunkQueryable)
}

#[test]
fn test_fanout_cost_sampling_queryable() {
    let primary_ts = teststorage_db::new(&[]);
    let primary: Arc<dyn Storage> = primary_ts.db.clone();
    let (native, supported) = primary_ts.cost_sampling_queryable();
    assert!(supported);
    let empty = CostSamplingTestStorage::new(&primary_ts.db, false, true);
    let unsafe_ = CostSamplingTestStorage::new(&primary_ts.db, false, false);
    let extra_native = CostSamplingTestStorage::new(&primary_ts.db, true, true);
    let unknown: Arc<dyn Storage> = Arc::new(CostSamplingUnknownStorage(primary_ts.db.clone()));
    let empty_s: Arc<dyn Storage> = empty.clone();
    struct Case {
        name: &'static str,
        primary: Arc<dyn Storage>,
        secondary: Vec<Arc<dyn Storage>>,
        supported: bool,
        empty: bool,
    }
    let cases = vec![
        Case { name: "native primary", primary: primary.clone(), secondary: vec![], supported: true, empty: false },
        Case {
            name: "empty secondary",
            primary: primary.clone(),
            secondary: vec![empty_s.clone()],
            supported: true,
            empty: false,
        },
        Case {
            name: "unsafe secondary",
            primary: primary.clone(),
            secondary: vec![unsafe_.clone()],
            supported: false,
            empty: false,
        },
        Case {
            name: "unknown secondary",
            primary: primary.clone(),
            secondary: vec![unknown.clone()],
            supported: false,
            empty: false,
        },
        Case {
            name: "multiple native sources",
            primary: primary.clone(),
            secondary: vec![extra_native.clone()],
            supported: false,
            empty: false,
        },
        Case { name: "unknown primary", primary: unknown.clone(), secondary: vec![], supported: false, empty: false },
        Case {
            name: "all empty",
            primary: empty_s.clone(),
            secondary: vec![empty_s.clone()],
            supported: true,
            empty: true,
        },
    ];
    for tc in cases {
        let fanout = new_fanout(None, tc.primary, tc.secondary);
        let (source, supported) = fanout.cost_sampling_queryable();
        assert_eq!(tc.supported, supported, "{}", tc.name);
        if !supported || tc.empty {
            assert!(source.is_none(), "{}", tc.name);
        } else {
            assert!(same_source(native, source.expect("a source")), "{}", tc.name);
        }
    }

    // Selected source survives secondary reload.
    {
        let fanout = new_fanout(None, primary.clone(), vec![empty_s.clone()]);
        let (source, supported) = fanout.cost_sampling_queryable();
        assert!(supported);
        empty.supported.store(false, std::sync::atomic::Ordering::SeqCst);
        let mut q = source.expect("a source").chunk_querier(0, 100).unwrap();
        assert!(q.close().is_none());
        assert_eq!(
            0,
            empty.queries.load(std::sync::atomic::Ordering::SeqCst),
            "the selected source must not re-enter the live fanout"
        );
        let (_, supported) = new_fanout(None, primary.clone(), vec![empty_s.clone()]).cost_sampling_queryable();
        assert!(!supported);
    }
}

fn check_select_sorted(fanout: &Fanout, input_total_size: usize) {
    use chunkenc::ChunkIterator;
    let ctx = Context::background();
    let output_label = labels::from_strings(&["__name__", "a"]);
    // Querier.
    {
        let mut querier = fanout.querier(0, 8000).unwrap();
        let matcher = model::matcher::Matcher::new(model::matcher::MatchType::Equal, "__name__", "a").unwrap();
        let mut series_set = querier.select(&ctx, true, None, &[matcher]);
        let mut result = std::collections::HashMap::new();
        let mut labels_result = Labels::default();
        while series_set.next() {
            let series = series_set.at().clone();
            labels_result = series.labels().clone();
            let mut iterator = series.iterator(None);
            while iterator.next() == chunkenc::ValueType::Float {
                let (timestamp, value) = iterator.at();
                result.insert(timestamp, value);
            }
        }
        assert_eq!(labels_result, output_label);
        assert_eq!(input_total_size, result.len());
        drop(series_set);
        querier.close();
    }
    // Chunk querier.
    {
        let mut querier = fanout.chunk_querier(0, 8000).unwrap();
        let matcher = model::matcher::Matcher::new(model::matcher::MatchType::Equal, "__name__", "a").unwrap();
        let mut series_set = new_series_set_from_chunk_series_set(querier.select(&ctx, true, None, &[matcher]));
        let mut result = std::collections::HashMap::new();
        let mut labels_result = Labels::default();
        while series_set.next() {
            let series = series_set.at().clone();
            labels_result = series.labels().clone();
            let mut iterator = series.iterator(None);
            while iterator.next() == chunkenc::ValueType::Float {
                let (timestamp, value) = iterator.at();
                result.insert(timestamp, value);
            }
        }
        assert!(series_set.err().is_none());
        assert_eq!(labels_result, output_label);
        assert_eq!(input_total_size, result.len());
        drop(series_set);
        querier.close();
    }
}

#[test]
fn test_fanout_select_sorted() {
    let input_label = labels::from_strings(&["__name__", "a"]);
    let mut input_total_size = 0;
    let ctx = Context::background();

    let pri_storage = teststorage_db::new(&[]);
    let mut app1 = pri_storage.appender(&ctx);
    for (t, v) in [(0, 0.0), (1000, 1.0), (2000, 2.0)] {
        let _ = app1.append(0, &input_label, t, v);
        input_total_size += 1;
    }
    assert!(app1.commit().is_none());

    let remote_storage1 = teststorage_db::new(&[]);
    let mut app2 = remote_storage1.appender(&ctx);
    for (t, v) in [(3000, 3.0), (4000, 4.0), (5000, 5.0)] {
        let _ = app2.append(0, &input_label, t, v);
        input_total_size += 1;
    }
    assert!(app2.commit().is_none());

    let remote_storage2 = teststorage_db::new(&[]);
    let mut app3 = remote_storage2.appender(&ctx);
    for (t, v) in [(6000, 6.0), (7000, 7.0), (8000, 8.0)] {
        let _ = app3.append(0, &input_label, t, v);
        input_total_size += 1;
    }
    assert!(app3.commit().is_none());

    let fanout = new_fanout(
        None,
        pri_storage.db.clone(),
        vec![remote_storage1.db.clone(), remote_storage2.db.clone()],
    );
    check_select_sorted(&fanout, input_total_size);
}

#[test]
fn test_fanout_select_sorted_appender_v2() {
    let input_label = labels::from_strings(&["__name__", "a"]);
    let mut input_total_size = 0;
    let ctx = Context::background();
    let o = AOptions::default();

    let pri_storage = teststorage_db::new(&[]);
    let mut app1 = pri_storage.appender_v2(&ctx);
    for (t, v) in [(0, 0.0), (1000, 1.0), (2000, 2.0)] {
        app1.append(0, &input_label, 0, t, v, None, None, &o).unwrap();
        input_total_size += 1;
    }
    assert!(app1.commit().is_none());

    let remote_storage1 = teststorage_db::new(&[]);
    let mut app2 = remote_storage1.appender_v2(&ctx);
    for (t, v) in [(3000, 3.0), (4000, 4.0), (5000, 5.0)] {
        app2.append(0, &input_label, 0, t, v, None, None, &o).unwrap();
        input_total_size += 1;
    }
    assert!(app2.commit().is_none());

    let remote_storage2 = teststorage_db::new(&[]);
    let mut app3 = remote_storage2.appender_v2(&ctx);
    for (t, v) in [(6000, 6.0), (7000, 7.0), (8000, 8.0)] {
        app3.append(0, &input_label, 0, t, v, None, None, &o).unwrap();
        input_total_size += 1;
    }
    assert!(app3.commit().is_none());

    let fanout = new_fanout(
        None,
        pri_storage.db.clone(),
        vec![remote_storage1.db.clone(), remote_storage2.db.clone()],
    );
    check_select_sorted(&fanout, input_total_size);
}

/// Go `errSelect`.
fn err_select_text() -> &'static str {
    "select error"
}

/// Go `errStorage`: queriers whose selects fail with `errSelect`.
struct ErrStorage;

/// Go `errQuerier`.
struct ErrQuerier;

impl LabelQuerier for ErrQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[model::matcher::Matcher]) -> LabelResult {
        (Vec::new(), Annotations::default(), Some(Error::msg("label values error")))
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[model::matcher::Matcher]) -> LabelResult {
        (Vec::new(), Annotations::default(), Some(Error::msg("label names error")))
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for ErrQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[model::matcher::Matcher]) -> BoxSeriesSet {
        err_series_set(Error::msg(err_select_text()))
    }
}

impl ChunkQuerier for ErrQuerier {
    fn select(
        &self,
        _: &Context,
        _: bool,
        _: Option<&SelectHints>,
        _: &[model::matcher::Matcher],
    ) -> BoxChunkSeriesSet {
        err_chunk_series_set(Error::msg(err_select_text()))
    }
}

impl Queryable for ErrStorage {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(ErrQuerier))
    }
}

impl ChunkQueryable for ErrStorage {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        Ok(Box::new(ErrQuerier))
    }
}

impl Appendable for ErrStorage {
    fn appender(&self, _: &Context) -> Box<dyn Appender> {
        unimplemented!("errStorage.Appender returns nil")
    }
}

impl AppendableV2 for ErrStorage {
    fn appender_v2(&self, _: &Context) -> Box<dyn AppenderV2> {
        unimplemented!("errStorage.AppenderV2 returns nil")
    }
}

impl Storage for ErrStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Ok(0)
    }
    fn close(&self) -> Option<Error> {
        None
    }
}

#[test]
fn test_fanout_errors() {
    let working_storage = teststorage_db::new(&[]);
    let working: Arc<dyn Storage> = working_storage.db.clone();
    struct Case {
        primary: Arc<dyn Storage>,
        secondary: Arc<dyn Storage>,
        warning: Option<&'static str>,
        err: Option<&'static str>,
    }
    let cases = vec![
        Case { primary: working.clone(), secondary: Arc::new(ErrStorage), warning: Some(err_select_text()), err: None },
        Case { primary: Arc::new(ErrStorage), secondary: working.clone(), warning: None, err: Some(err_select_text()) },
    ];
    for tc in cases {
        let fanout = new_fanout(None, tc.primary, vec![tc.secondary]);
        // Samples.
        {
            let mut querier = fanout.querier(0, 8000).unwrap();
            let matcher = model::matcher::Matcher::must_new(model::matcher::MatchType::Equal, "a", "b");
            let mut ss = querier.select(&Context::background(), true, None, &[matcher]);
            // Exhaust.
            while ss.next() {
                let _ = ss.at();
            }
            if let Some(e) = tc.err {
                assert_eq!(e, ss.err().expect("an error").to_string());
            }
            if let Some(w) = tc.warning {
                let ws = ss.warnings();
                assert!(!ws.is_empty(), "warnings expected");
                assert_eq!(w, ws.as_errors()[0].to_string());
            }
            drop(ss);
            querier.close();
        }
        // Chunks: Go skips this subtest ("enable once TestStorage and TSDB implements ChunkQuerier").
    }
}

/// Go `fixedQuerierStorage`: a storage whose querier is the given one (here: the noop querier).
struct NoopQuerierStorage(Arc<tsdb::db::DB>);

impl Queryable for NoopQuerierStorage {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok(noop_querier())
    }
}

impl ChunkQueryable for NoopQuerierStorage {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        ChunkQueryable::chunk_querier(&*self.0, mint, maxt)
    }
}

delegate_storage!(NoopQuerierStorage, 0);

impl Storage for NoopQuerierStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Storage::start_time(&*self.0)
    }
    fn close(&self) -> Option<Error> {
        None
    }
}

#[test]
fn test_fanout_querier_searcher_capability() {
    let primary_ts = teststorage_db::new(&[]);
    let mut app = primary_ts.appender(&Context::background());
    app.append(0, &labels::from_strings(&["__name__", "up", "job", "prometheus"]), 0, 1.0).unwrap();
    assert!(app.commit().is_none());
    let primary: Arc<dyn Storage> = primary_ts.db.clone();

    // adapted: Go's `fixedQuerierStorage` embeds a nil `storage.Storage`; only its `Querier` is used. Here the
    // other methods delegate to the primary test storage.
    let cases: Vec<(&str, Arc<dyn Storage>, bool)> = vec![
        ("active unsupported secondary", Arc::new(ErrStorage), true),
        ("noop secondary", Arc::new(NoopQuerierStorage(primary_ts.db.clone())), false),
    ];
    for (name, secondary, warning) in cases {
        let fanout = new_fanout(None, primary.clone(), vec![secondary]);
        let mut querier = fanout.querier(0, 1).unwrap();
        {
            let searcher = querier.as_searcher().expect("a searcher");
            let ctx = Context::background();
            for (method, expected) in [("names", vec!["__name__", "job"]), ("values", vec!["prometheus"])] {
                let mut rs = if method == "names" {
                    searcher.search_label_names(&ctx, None, &[])
                } else {
                    searcher.search_label_values(&ctx, "job", None, &[])
                };
                let mut values = Vec::new();
                while rs.next() {
                    values.push(rs.at().value.clone());
                }
                assert!(rs.err().is_none(), "{name}/{method}");
                assert_eq!(expected, values, "{name}/{method}");
                if warning {
                    let warnings = rs.warnings().as_errors();
                    assert_eq!(1, warnings.len(), "{name}/{method}");
                    assert_eq!(
                        "search is not supported by a secondary storage backend; results may be incomplete",
                        warnings[0].to_string()
                    );
                } else {
                    assert!(rs.warnings().is_empty(), "{name}/{method}");
                }
                assert!(rs.close().is_none(), "{name}/{method}");
            }
        }
        querier.close();
    }
}
