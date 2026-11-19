//! scrape_test.go part 2 (Go lines 2300-4499: TestScrapeLoopAppend_StartTimeSynthesis_OOO_StateMutation through
//! TestScrapeLoopAppend).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use model::exemplar::Exemplar;
use model::histogram::{
    EXPONENTIAL_SCHEMA_MIN, FloatHistogram, Histogram, Span, is_exponential_schema,
};
use model::labels::{Labels, from_strings};
use model::timestamp::from_time;
use model::value::{NORMAL_NAN, STALE_NAN, is_stale_nan};
use scrape::ScrapeLoop;
use scrape::limits::{ERR_BUCKET_LIMIT, ERR_SAMPLE_LIMIT};
use scrape::scrape_loop::{mutate_report_sample_labels, mutate_sample_labels};
use scrape::target::Target;
use scrape::testing::dto::text_to_proto;
use scrape::testing::new_test_scrape_loop;
use storage::{
    AppendOptions, AppendV2Options, Appendable as AppendableT, AppendableV2, Appender, AppenderTransaction,
    AppenderV2, Context, ERR_OUT_OF_ORDER_SAMPLE, Error, SeriesRef,
};
use util::teststorage::{Appendable, Sample, require_equal, require_equal_msg};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

/// Go `withAppendable(app, appV2)`.
fn with_appendable(sl: &mut ScrapeLoop, app: &Appendable, v2: bool) {
    if v2 {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
    } else {
        sl.appendable = Some(Arc::new(app.clone()));
    }
}

/// Go `foreachAppendable`: runs `f` for appV2=false and appV2=true.
fn foreach_appendable(f: impl Fn(bool)) {
    for v2 in [false, true] {
        f(v2);
    }
}

/// Go `readTextParseTestMetrics`.
fn read_text_parse_test_metrics() -> Vec<u8> {
    let p = concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/model/textparse/testdata/alltypes.237mfs.prom.txt");
    let b = std::fs::read(p).unwrap_or_else(|e| panic!("read {p}: {e}"));
    b.into_iter().filter(|&c| c != b'\r').collect()
}

fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN)
}

fn target_with_labels(ls: &[&str]) -> Arc<Target> {
    Arc::new(Target::new(from_strings(ls), Default::default(), Default::default(), Default::default()))
}

fn sample(l: Labels, t: i64, v: f64) -> Sample {
    Sample { l, t, v, ..Default::default() }
}

/// Runs `sl.run(None)` on a thread and waits up to `timeout` for it to return (Go `go func() { sl.run(nil);
/// signal <- struct{}{} }()` and the select on the signal).
fn run_until_stopped(sl: &Arc<ScrapeLoop>, timeout: Duration) {
    let (tx, rx) = mpsc::channel();
    let s = sl.clone();
    std::thread::spawn(move || {
        s.run(None);
        let _ = tx.send(());
    });
    if rx.recv_timeout(timeout).is_err() {
        panic!("Scrape wasn't stopped.");
    }
}

/// Go `appendableFunc`.
struct AppendableFunc<F: Fn(&Context) -> Box<dyn Appender> + Send + Sync>(F);

impl<F: Fn(&Context) -> Box<dyn Appender> + Send + Sync> AppendableT for AppendableFunc<F> {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        (self.0)(ctx)
    }
}

/// Go `appendableV2Func`.
struct AppendableV2Func<F: Fn(&Context) -> Box<dyn AppenderV2> + Send + Sync>(F);

impl<F: Fn(&Context) -> Box<dyn AppenderV2> + Send + Sync> AppendableV2 for AppendableV2Func<F> {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        (self.0)(ctx)
    }
}

/// Go `limitAppender` (scrape/target.go): limits the number of appended samples in a batch.
struct LimitAppender {
    app: Box<dyn Appender>,
    limit: usize,
    i: usize,
}

impl AppenderTransaction for LimitAppender {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl Appender for LimitAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        if r == 0 || !is_stale_nan(v) {
            self.i += 1;
            if self.i > self.limit {
                return Err(ERR_SAMPLE_LIMIT);
            }
        }
        self.app.append(r, l, t, v)
    }
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.app.set_options(opts)
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.app.append_exemplar(r, l, e)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if r == 0 || h.is_some_and(|h| !is_stale_nan(h.sum)) || fh.is_some_and(|fh| !is_stale_nan(fh.sum)) {
            self.i += 1;
            if self.i > self.limit {
                return Err(ERR_SAMPLE_LIMIT);
            }
        }
        self.app.append_histogram(r, l, t, h, fh)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &model::metadata::Metadata) -> Result<SeriesRef, Error> {
        self.app.update_metadata(r, l, m)
    }
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app.append_st_zero_sample(r, l, t, st)
    }
}

/// Go `limitAppenderV2`.
struct LimitAppenderV2 {
    app: Box<dyn AppenderV2>,
    limit: usize,
    i: usize,
}

impl AppenderTransaction for LimitAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl AppenderV2 for LimitAppenderV2 {
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
        if r == 0 || !is_stale_nan(v) {
            self.i += 1;
            if self.i > self.limit {
                return Err(ERR_SAMPLE_LIMIT);
            }
        }
        self.app.append(r, ls, st, t, v, h, fh, opts)
    }
}

/// The histogram part of Go `bucketLimitAppender.AppendHistogram` / `bucketLimitAppenderV2.Append`: returns
/// copies with reduced resolution.
fn bucket_limit(
    limit: usize,
    h: Option<&Histogram>,
    fh: Option<&FloatHistogram>,
) -> Result<(Option<Histogram>, Option<FloatHistogram>), Error> {
    let mut h = h.cloned();
    let mut fh = fh.cloned();
    if let Some(h) = h.as_mut() {
        if h.positive_buckets.len() + h.negative_buckets.len() > limit && !is_exponential_schema(h.schema) {
            return Err(ERR_BUCKET_LIMIT);
        }
        while h.positive_buckets.len() + h.negative_buckets.len() > limit {
            if h.schema <= EXPONENTIAL_SCHEMA_MIN {
                return Err(ERR_BUCKET_LIMIT);
            }
            let s = h.schema - 1;
            h.reduce_resolution(s)?;
        }
    }
    if let Some(fh) = fh.as_mut() {
        if fh.positive_buckets.len() + fh.negative_buckets.len() > limit && !is_exponential_schema(fh.schema) {
            return Err(ERR_BUCKET_LIMIT);
        }
        while fh.positive_buckets.len() + fh.negative_buckets.len() > limit {
            if fh.schema <= EXPONENTIAL_SCHEMA_MIN {
                return Err(ERR_BUCKET_LIMIT);
            }
            let s = fh.schema - 1;
            fh.reduce_resolution(s)?;
        }
    }
    Ok((h, fh))
}

/// Go `bucketLimitAppender`.
struct BucketLimitAppender {
    app: Box<dyn Appender>,
    limit: usize,
}

impl AppenderTransaction for BucketLimitAppender {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl Appender for BucketLimitAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.app.append(r, l, t, v)
    }
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.app.set_options(opts)
    }
    fn append_exemplar(&mut self, r: SeriesRef, l: &Labels, e: &Exemplar) -> Result<SeriesRef, Error> {
        self.app.append_exemplar(r, l, e)
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        let (h, fh) = bucket_limit(self.limit, h, fh)?;
        self.app.append_histogram(r, l, t, h.as_ref(), fh.as_ref())
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
    }
    fn update_metadata(&mut self, r: SeriesRef, l: &Labels, m: &model::metadata::Metadata) -> Result<SeriesRef, Error> {
        self.app.update_metadata(r, l, m)
    }
    fn append_st_zero_sample(&mut self, r: SeriesRef, l: &Labels, t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.app.append_st_zero_sample(r, l, t, st)
    }
}

/// Go `bucketLimitAppenderV2`.
struct BucketLimitAppenderV2 {
    app: Box<dyn AppenderV2>,
    limit: usize,
}

impl AppenderTransaction for BucketLimitAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.app.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.app.rollback()
    }
}

impl AppenderV2 for BucketLimitAppenderV2 {
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
        let (h, fh) = bucket_limit(self.limit, h, fh)?;
        self.app.append(r, ls, st, t, v, h.as_ref(), fh.as_ref(), opts)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Start timestamp synthesis.

#[test]
fn test_scrape_loop_append_start_time_synthesis_ooo_state_mutation() {
    let ts = SystemTime::now();

    let s = Arc::new(tsdb::teststorage::new_default());
    // Simulate OOO error for "test_metric" on the second scrape.
    let return_ooo = Arc::new(AtomicBool::new(false));
    let r = return_ooo.clone();
    let app_test = Appendable::new().with_errs(
        Some(Arc::new(move |ls: &Labels| {
            if r.load(Ordering::SeqCst) && ls.get("__name__") == "test_metric" {
                return Some(ERR_OUT_OF_ORDER_SAMPLE);
            }
            None
        })),
        None,
        None,
    )
    .then(s.clone());

    let (sl, _) = new_test_scrape_loop(|sl| {
        with_appendable(sl, &app_test, true);
        sl.synthesize_st = true;
        sl.parse_st = true;
    });

    // First Scrape: anchor the start time, append is skipped.
    let scrape_a = b"# TYPE test_metric counter\ntest_metric 10\n# EOF\n";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_a, "application/openmetrics-text", ts);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Second Scrape: Counter should be rejected due to OOO.
    let ts2 = ts + Duration::from_secs(1);
    let scrape_b = b"# TYPE test_metric counter\ntest_metric 100\n# EOF\n";
    return_ooo.store(true, Ordering::SeqCst);
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_b, "application/openmetrics-text", ts2);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    // Third Scrape: Counter is valid again; the state was cleared, so it re-anchors.
    let ts3 = ts2 + Duration::from_secs(1);
    let scrape_c = b"# TYPE test_metric counter\ntest_metric 25\n# EOF\n";
    return_ooo.store(false, Ordering::SeqCst);
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_c, "application/openmetrics-text", ts3);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert!(
        got.is_empty(),
        "Expected no samples because the state was cleared and the sample was used to re-anchor: {got:?}"
    );
}

#[test]
fn test_scrape_loop_append_start_time_synthesis_summary() {
    let ts = SystemTime::now();

    let require_sample = |s: &Sample, name: &str, val: f64, ts: i64, st: i64, is_nan: bool| {
        assert_eq!(name, s.l.get("__name__"));
        assert_eq!(ts, s.t);
        if is_nan {
            assert!(is_stale_nan(s.v));
        } else {
            assert_eq!(val, s.v);
        }
        assert_eq!(st, s.st);
    };

    let s = Arc::new(tsdb::teststorage::new_default());
    let app_test = Appendable::new().then(s.clone());
    let (sl, _) = new_test_scrape_loop(|sl| {
        with_appendable(sl, &app_test, true);
        sl.synthesize_st = true;
        sl.parse_st = true;
    });

    let scrape_a = b"# TYPE test_summary summary
test_summary{quantile=\"0.5\"} 10
test_summary_sum 100
test_summary_count 10
# EOF
";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_a, "application/openmetrics-text", ts);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 1, "{got:?}");
    require_sample(&got[0], "test_summary", 10.0, from_time(ts), 0, false);

    let ts2 = ts + Duration::from_secs(1);
    let scrape_b = b"# TYPE test_summary summary
test_summary{quantile=\"0.5\"} 12
test_summary_sum 150
test_summary_count 15
# EOF
";
    let mut app = sl.appender();
    let (_, err) = app.append(scrape_b, "application/openmetrics-text", ts2);
    assert!(err.is_none(), "{err:?}");
    assert!(app.commit().is_none());

    let got = app_test.result_samples();
    assert_eq!(got.len(), 4, "{got:?}");
    require_sample(&got[0], "test_summary", 10.0, from_time(ts), 0, false);
    require_sample(&got[1], "test_summary", 12.0, from_time(ts2), 0, false);
    require_sample(&got[2], "test_summary_sum", 50.0, from_time(ts2), from_time(ts), false);
    require_sample(&got[3], "test_summary_count", 5.0, from_time(ts2), from_time(ts), false);
}

/// Go `requireSampleHist`.
#[track_caller]
fn require_sample_hist(s: &Sample, name: &str, expected_hist: &str, ts: i64, st: i64, is_nan: bool) {
    assert_eq!(name, s.l.get("__name__"));
    assert_eq!(ts, s.t);
    if is_nan {
        assert!(s.is_stale());
    } else if let Some(h) = &s.h {
        assert_eq!(expected_hist, h.to_string());
    } else if let Some(fh) = &s.fh {
        assert_eq!(expected_hist, fh.to_string());
    } else {
        panic!("expected sample to have either H or FH");
    }
    assert_eq!(st, s.st);
}

#[test]
fn test_scrape_loop_append_start_time_synthesis_histograms() {
    // ClassicToNative.
    {
        let ts = SystemTime::now();
        let s = Arc::new(tsdb::teststorage::new_default());
        let app_test = Appendable::new().then(s.clone());
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, true);
            sl.synthesize_st = true;
            sl.convert_classic_hist_to_nhcb = true;
            sl.enable_native_histogram_scraping = true;
            sl.always_scrape_classic_hist = false;
        });

        let scrape_a = b"# TYPE test_hist histogram
# HELP test_hist some help
test_hist_bucket{le=\"0.005\"} 0
test_hist_bucket{le=\"0.01\"} 0
test_hist_bucket{le=\"0.025\"} 0
test_hist_bucket{le=\"0.05\"} 0
test_hist_bucket{le=\"0.1\"} 1
test_hist_bucket{le=\"0.25\"} 1
test_hist_bucket{le=\"0.5\"} 1
test_hist_bucket{le=\"1\"} 2
test_hist_bucket{le=\"2.5\"} 2
test_hist_bucket{le=\"5\"} 2
test_hist_bucket{le=\"10\"} 2
test_hist_bucket{le=\"+Inf\"} 3
test_hist_sum 5.5
test_hist_count 3
# EOF
";
        let mut app = sl.appender();
        let (_, err) = app.append(scrape_a, "application/openmetrics-text", ts);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        let got = app_test.result_samples();
        assert!(got.is_empty(), "{got:?}");

        let ts2 = ts + Duration::from_secs(1);
        let scrape_b = b"# TYPE test_hist histogram
# HELP test_hist some help
test_hist_bucket{le=\"0.005\"} 0
test_hist_bucket{le=\"0.01\"} 0
test_hist_bucket{le=\"0.025\"} 0
test_hist_bucket{le=\"0.05\"} 0
test_hist_bucket{le=\"0.1\"} 2
test_hist_bucket{le=\"0.25\"} 2
test_hist_bucket{le=\"0.5\"} 2
test_hist_bucket{le=\"1\"} 5
test_hist_bucket{le=\"2.5\"} 5
test_hist_bucket{le=\"5\"} 5
test_hist_bucket{le=\"10\"} 5
test_hist_bucket{le=\"+Inf\"} 6
test_hist_sum 10.5
test_hist_count 6
# EOF
";
        let mut app = sl.appender();
        let (_, err) = app.append(scrape_b, "application/openmetrics-text", ts2);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        let got = app_test.result_samples();
        assert_eq!(got.len(), 1, "{got:?}");
        require_sample_hist(
            &got[0],
            "test_hist",
            "{count:3, sum:5, (0.05,0.1]:1, (0.5,1]:2}",
            from_time(ts2),
            from_time(ts),
            false,
        );

        let ts3 = ts2 + Duration::from_secs(1);
        let scrape_c = b"# TYPE test_hist histogram
# HELP test_hist some help
test_hist_bucket{le=\"0.005\"} 0
test_hist_bucket{le=\"0.01\"} 0
test_hist_bucket{le=\"0.025\"} 0
test_hist_bucket{le=\"0.05\"} 0
test_hist_bucket{le=\"0.1\"} 1
test_hist_bucket{le=\"0.25\"} 1
test_hist_bucket{le=\"0.5\"} 1
test_hist_bucket{le=\"1\"} 2
test_hist_bucket{le=\"2.5\"} 2
test_hist_bucket{le=\"5\"} 2
test_hist_bucket{le=\"10\"} 2
test_hist_bucket{le=\"+Inf\"} 2
test_hist_sum 4.0
test_hist_count 2
# EOF
";
        let mut app = sl.appender();
        let (_, err) = app.append(scrape_c, "application/openmetrics-text", ts3);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
        let got = app_test.result_samples();
        assert_eq!(got.len(), 2, "{got:?}");
        require_sample_hist(
            &got[0],
            "test_hist",
            "{count:3, sum:5, (0.05,0.1]:1, (0.5,1]:2}",
            from_time(ts2),
            from_time(ts),
            false,
        );
        require_sample_hist(
            &got[1],
            "test_hist",
            "{count:2, sum:4, (0.05,0.1]:1, (0.5,1]:1}",
            from_time(ts3),
            from_time(ts3) - 1,
            false,
        );
    }

    // FloatNative.
    {
        let ts = SystemTime::now();
        let pb = "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited";
        let s = Arc::new(tsdb::teststorage::new_default());
        let app_test = Appendable::new().then(s.clone());
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, true);
            sl.synthesize_st = true;
            sl.enable_native_histogram_scraping = true;
            sl.always_scrape_classic_hist = false;
            sl.fallback_scrape_protocol = pb.to_string();
        });

        let fam = |count: &str, sum: &str, c1: &str, c2: &str| {
            format!(
                r#"name: "test_float_hist"
help: "test help"
type: HISTOGRAM
metric: <
  histogram: <
    schema: 3
    sample_count_float: {count}
    sample_sum: {sum}
    positive_span: <
      offset: 0
      length: 2
    >
    positive_count: {c1}
    positive_count: {c2}
  >
>
"#
            )
        };

        let mut buf_a = Vec::new();
        text_to_proto(&fam("3.0", "5.0", "1.0", "2.0"), &mut buf_a).unwrap();
        let mut app = sl.appender();
        let (_, err) = app.append(&buf_a, pb, ts);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let ts2 = ts + Duration::from_secs(1);
        let mut buf_b = Vec::new();
        text_to_proto(&fam("6.0", "10.5", "2.0", "4.0"), &mut buf_b).unwrap();
        let mut app = sl.appender();
        let (_, err) = app.append(&buf_b, pb, ts2);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let got = app_test.result_samples();
        assert_eq!(got.len(), 1, "{got:?}");
        require_sample_hist(
            &got[0],
            "test_float_hist",
            "{count:3, sum:5.5, (0.9170040432046711,1]:1, (1,1.0905077326652577]:2}",
            from_time(ts2),
            from_time(ts),
            false,
        );

        let ts3 = ts2 + Duration::from_secs(1);
        let mut buf_c = Vec::new();
        text_to_proto(&fam("2.0", "4.0", "1.0", "1.0"), &mut buf_c).unwrap();
        let mut app = sl.appender();
        let (_, err) = app.append(&buf_c, pb, ts3);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let got = app_test.result_samples();
        assert_eq!(got.len(), 2, "{got:?}");
        require_sample_hist(
            &got[0],
            "test_float_hist",
            "{count:3, sum:5.5, (0.9170040432046711,1]:1, (1,1.0905077326652577]:2}",
            from_time(ts2),
            from_time(ts),
            false,
        );
        require_sample_hist(
            &got[1],
            "test_float_hist",
            "{count:2, sum:4, (0.9170040432046711,1]:1, (1,1.0905077326652577]:1}",
            from_time(ts3),
            from_time(ts3) - 1,
            false,
        );
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Scrape and report, run loop.

#[test]
fn test_scrape_loop_scrape_and_report() {
    foreach_appendable(|v2| {
        let parsable_text = Arc::new(read_text_parse_test_metrics());

        let app_test = Appendable::new();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.fallback_scrape_protocol = "application/openmetrics-text".to_string();
        });
        let p = parsable_text.clone();
        scraper.set_scrape_func(move |_, w| {
            w.extend_from_slice(&p);
            Ok(())
        });

        // Go `time.Time{}` for the append time.
        let ts = go_zero_time();
        sl.scrape_and_report(None, ts, None);
        assert!(scraper.last_error().is_none(), "appV2={v2}: {:?}", scraper.last_error());

        assert_eq!(app_test.result_samples().len(), 1862, "appV2={v2}");
        assert_eq!(app_test.result_metadata().len(), 1862, "appV2={v2}");
    });
}

/// Go `time.Time{}` (year 1, UTC).
fn go_zero_time() -> SystemTime {
    UNIX_EPOCH - Duration::from_secs(62_135_596_800)
}

#[test]
fn test_scrape_loop_run_creates_stale_markers_on_failed_scrape() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let ctx = scrape::Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone();
            sl.fallback_scrape_protocol = "text/plain".to_string();
        });

        // Succeed once, several failures, then stop.
        let num_scrapes = AtomicUsize::new(0);
        let c = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            match n {
                1 => {
                    w.extend_from_slice(b"metric_a 42\n");
                    return Ok(());
                }
                5 => c.cancel(),
                _ => {}
            }
            Err(Error::msg("scrape failed"))
        });

        let sl = Arc::new(sl);
        run_until_stopped(&sl, Duration::from_secs(5));

        let got = app_test.result_samples();
        // 1 successfully scraped sample, 1 stale marker after first fail, 5x 5 report samples.
        assert_eq!(got.len(), 27, "appV2={v2}: Appended samples not as expected:\n{app_test}");
        assert_eq!(42.0, got[0].v, "Appended first sample not as expected");
        assert!(
            is_stale_nan(got[6].v),
            "Appended second sample not as expected. Wanted: stale NaN Got: {:x}",
            got[6].v.to_bits()
        );
    });
}

#[test]
fn test_scrape_loop_run_creates_stale_markers_on_parse_failure() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let ctx = scrape::Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone();
            sl.fallback_scrape_protocol = "text/plain".to_string();
        });

        let num_scrapes = AtomicUsize::new(0);
        let c = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            match n {
                1 => {
                    w.extend_from_slice(b"metric_a 42\n");
                    return Ok(());
                }
                2 => {
                    w.extend_from_slice(b"7&-\n");
                    return Ok(());
                }
                3 => c.cancel(),
                _ => {}
            }
            Err(Error::msg("scrape failed"))
        });

        let sl = Arc::new(sl);
        run_until_stopped(&sl, Duration::from_secs(5));

        let got = app_test.result_samples();
        // 1 successfully scraped sample, 1 stale marker after first fail, 3x 5 report samples.
        assert_eq!(got.len(), 17, "appV2={v2}: Appended samples not as expected:\n{app_test}");
        assert_eq!(42.0, got[0].v, "Appended first sample not as expected");
        assert!(
            is_stale_nan(got[6].v),
            "Appended second sample not as expected. Wanted: stale NaN Got: {:x}",
            got[6].v.to_bits()
        );
    });
}

#[test]
fn test_scrape_loop_run_creates_stale_markers_on_sample_limit() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let ctx = scrape::Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone();
            sl.fallback_scrape_protocol = "text/plain".to_string();
            sl.sample_limit = 4;
        });

        let num_scrapes = AtomicUsize::new(0);
        let c = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            match n {
                1 => {
                    w.extend_from_slice(b"metric_a 10\nmetric_b 10\nmetric_c 10\nmetric_d 10\n");
                    return Ok(());
                }
                2 => {
                    w.extend_from_slice(b"metric_a 20\nmetric_b 20\nmetric_c 20\nmetric_d 20\nmetric_e 999\n");
                    return Ok(());
                }
                3 => {
                    w.extend_from_slice(b"metric_a 30\nmetric_b 30\nmetric_c 30\nmetric_d 30\n");
                    return Ok(());
                }
                4 => c.cancel(),
                _ => {}
            }
            Err(Error::msg("scrape failed"))
        });

        let sl = Arc::new(sl);
        run_until_stopped(&sl, Duration::from_secs(5));

        let got = app_test.result_samples();
        assert_eq!(got.len(), (4 + 5) + 5 + (4 + 5) + (4 + 5), "appV2={v2}: Appended samples not as expected:\n{app_test}");
        for (i, s) in got.iter().enumerate().take(4) {
            assert_eq!(10.0, s.v, "Appended {i} sample not as expected");
        }
        for (i, s) in got.iter().enumerate().take(18).skip(14) {
            assert_eq!(30.0, s.v, "Appended {i} sample not as expected");
        }
        for (i, s) in got.iter().enumerate().take(27).skip(23) {
            assert!(
                is_stale_nan(s.v),
                "Appended {i} sample not as expected. Wanted: stale NaN Got: {:x}",
                s.v.to_bits()
            );
        }
    });
}

#[test]
fn test_scrape_loop_series_ref_change() {
    foreach_appendable(|v2| {
        let scrape_interval = Duration::from_secs(15);
        let first_scrape = UNIX_EPOCH + Duration::from_secs(1_600_000_000);
        let metric_a = from_strings(&["__name__", "metric_a"]);

        let scrape_time = |scrape: u32| first_scrape + scrape_interval * scrape;
        let sample_at_ms = |ms: i64, v: f64| sample(metric_a.clone(), ms, v);
        let sample_at = |scrape: u32, v: f64| sample_at_ms(from_time(scrape_time(scrape)), v);
        let stale_at = |scrape: u32| sample_at(scrape, stale_nan());

        let explicit_ts = from_time(scrape_time(1)) - 1000;
        let explicit_ts_body = format!("metric_a 2 {explicit_ts}\n");

        struct Tc {
            name: &'static str,
            scrapes: Vec<(String, SeriesRef)>,
            expected_samples: Vec<Sample>,
            expected_ref: SeriesRef,
        }
        let s = |b: &str, r: SeriesRef| (b.to_string(), r);
        let cases = vec![
            Tc {
                name: "reference stays the same",
                scrapes: vec![s("metric_a 1\n", 100), s("metric_a 2\n", 100)],
                expected_samples: vec![sample_at(0, 1.0), sample_at(1, 2.0)],
                expected_ref: 100,
            },
            Tc {
                name: "reference changes while the series is still exposed",
                scrapes: vec![s("metric_a 1\n", 100), s("metric_a 2\n", 200), s("metric_a 3\n", 200)],
                expected_samples: vec![sample_at(0, 1.0), sample_at(1, 2.0), sample_at(2, 3.0)],
                expected_ref: 200,
            },
            Tc {
                name: "reference changes on every scrape",
                scrapes: vec![s("metric_a 1\n", 100), s("metric_a 2\n", 200), s("metric_a 3\n", 300)],
                expected_samples: vec![sample_at(0, 1.0), sample_at(1, 2.0), sample_at(2, 3.0)],
                expected_ref: 300,
            },
            Tc {
                name: "series stops being exposed after the reference changed",
                scrapes: vec![s("metric_a 1\n", 100), s("metric_a 2\n", 200), s("", 200)],
                expected_samples: vec![sample_at(0, 1.0), sample_at(1, 2.0), stale_at(2)],
                expected_ref: 200,
            },
            Tc {
                name: "explicit timestamp appears, reference stays the same",
                scrapes: vec![s("metric_a 1\n", 100), s(&explicit_ts_body, 100)],
                expected_samples: vec![sample_at(0, 1.0), sample_at_ms(explicit_ts, 2.0), stale_at(1)],
                expected_ref: 100,
            },
            Tc {
                name: "explicit timestamp appears and the reference changes",
                scrapes: vec![s("metric_a 1\n", 100), s(&explicit_ts_body, 200)],
                expected_samples: vec![sample_at(0, 1.0), sample_at_ms(explicit_ts, 2.0), stale_at(1)],
                expected_ref: 200,
            },
        ];

        for tc in cases {
            let r = Arc::new(Mutex::new(0 as SeriesRef));
            let rr = r.clone();
            let app = Appendable::new().with_ref_fn(Arc::new(move |_: &Labels| *rr.lock().unwrap()));
            let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app, v2));

            for (i, (body, sref)) in tc.scrapes.iter().enumerate() {
                *r.lock().unwrap() = *sref;
                let mut appender = sl.appender();
                let (_, err) = appender.append(body.as_bytes(), "text/plain", scrape_time(i as u32));
                assert!(err.is_none(), "{}: {err:?}", tc.name);
                assert!(appender.commit().is_none(), "{}", tc.name);
            }

            require_equal_msg(&tc.expected_samples, &app.result_samples(), &format!("appV2={v2}/{}", tc.name));

            let cache = sl.cache.series();
            let ce = cache.peek(b"metric_a");
            assert!(ce.is_some(), "{}", tc.name);
            assert_eq!(tc.expected_ref, ce.unwrap().ref_, "{}", tc.name);
        }
    });
}

#[test]
fn test_scrape_loop_cache() {
    foreach_appendable(|v2| {
        let s = Arc::new(tsdb::teststorage::new_default());
        let app_test = Appendable::new().then(s.clone());
        let ctx = scrape::Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone();
            sl.fallback_scrape_protocol = "text/plain".to_string();
            // Decreasing the scrape interval could make the test fail, as multiple scrapes might be initiated at
            // identical millisecond timestamps.
            sl.interval = Duration::from_millis(100);
        });

        // adapted: Go's require inside the scrape function fails the test; here failures are recorded and checked
        // after the loop stopped (a panic on the scrape thread would not reach the test).
        let failures = Arc::new(Mutex::new(Vec::<String>::new()));
        let f = failures.clone();
        let cache = sl.cache.clone();
        let num_scrapes = AtomicUsize::new(0);
        let c = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            let n0 = num_scrapes.load(Ordering::SeqCst);
            {
                let series = cache.series();
                let has_a = series.has_series(b"metric_a");
                let has_b = series.has_series(b"metric_b");
                match n0 {
                    1 | 2 => {
                        if !has_a {
                            f.lock().unwrap().push(format!("metric_a missing from cache after scrape {n0}"));
                        }
                        if !has_b {
                            f.lock().unwrap().push(format!("metric_b missing from cache after scrape {n0}"));
                        }
                    }
                    3 => {
                        if !has_a {
                            f.lock().unwrap().push(format!("metric_a missing from cache after scrape {n0}"));
                        }
                        if has_b {
                            f.lock().unwrap().push(format!("metric_b present in cache after scrape {n0}"));
                        }
                    }
                    _ => {}
                }
            }

            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            match n {
                1 => {
                    w.extend_from_slice(b"metric_a 42\nmetric_b 43\n");
                    return Ok(());
                }
                3 => {
                    w.extend_from_slice(b"metric_a 44\n");
                    return Ok(());
                }
                4 => c.cancel(),
                _ => {}
            }
            Err(Error::msg("scrape failed"))
        });

        let sl = Arc::new(sl);
        run_until_stopped(&sl, Duration::from_secs(5));

        let fails = failures.lock().unwrap().clone();
        assert!(fails.is_empty(), "appV2={v2}: {fails:?}");
        // 3 successfully scraped samples, 3 stale markers after samples were missing, 4x 5 report samples.
        assert_eq!(app_test.result_samples().len(), 26, "appV2={v2}: Appended samples not as expected:\n{app_test}");
    });
}

#[test]
fn test_scrape_loop_cache_memory_exhaustion_protection() {
    foreach_appendable(|v2| {
        let s = Arc::new(tsdb::teststorage::new_default());
        let app_test = Appendable::new().then(s.clone());
        let ctx = scrape::Ctx::background().child();
        let (sl, scraper) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.ctx = ctx.clone();
        });
        let num_scrapes = AtomicUsize::new(0);
        let c = ctx.clone();
        scraper.set_scrape_func(move |_, w| {
            let n = num_scrapes.fetch_add(1, Ordering::SeqCst) + 1;
            if n < 5 {
                let mut s = String::new();
                for i in 0..500 {
                    s.push_str(&format!("metric_{i}_{n} 42\n"));
                }
                w.extend_from_slice(s.as_bytes());
                w.extend_from_slice(b"&");
            } else {
                c.cancel();
            }
            Ok(())
        });

        let sl = Arc::new(sl);
        run_until_stopped(&sl, Duration::from_secs(5));

        let n = sl.cache.series().series_len();
        assert!(n <= 2000, "appV2={v2}: More than 2000 series cached: {n}");
    });
}

// ---------------------------------------------------------------------------------------------------------------
// Appends.

#[test]
fn test_scrape_loop_append_honor_labels() {
    foreach_appendable(|v2| {
        struct Tc {
            title: &'static str,
            honor_labels: bool,
            scrape_text: &'static str,
            discovery_labels: Vec<&'static str>,
            exp_lset: Labels,
        }
        let cases = vec![
            // On label collision, when "honor_labels" is not set, prefix is added.
            Tc {
                title: "HonorLabels=false",
                honor_labels: false,
                scrape_text: r#"metric{n="1"} 1"#,
                discovery_labels: vec!["n", "2"],
                exp_lset: from_strings(&["__name__", "metric", "exported_n", "1", "n", "2"]),
            },
            // Case where SD already has the prefixed label - it shouldn't be overridden.
            Tc {
                title: "HonorLabels=false;exported prefix already exists in SD",
                honor_labels: false,
                scrape_text: r#"metric{n="1"} 1"#,
                discovery_labels: vec!["n", "2", "exported_n", "2"],
                exp_lset: from_strings(&["__name__", "metric", "n", "2", "exported_n", "2", "exported_exported_n", "1"]),
            },
            // Labels with no value need to be removed as these should not be ingested.
            Tc {
                title: "HonorLabels=false;empty label",
                honor_labels: false,
                scrape_text: r#"metric{n=""} 1"#,
                discovery_labels: vec![],
                exp_lset: from_strings(&["__name__", "metric"]),
            },
            // On label collision, when "honor_labels" is true, label is overridden.
            Tc {
                title: "HonorLabels=true",
                honor_labels: true,
                scrape_text: r#"metric{n="1"} 1"#,
                discovery_labels: vec!["n", "2"],
                exp_lset: from_strings(&["__name__", "metric", "n", "1"]),
            },
        ];
        for test in cases {
            let discovery_labels = target_with_labels(&test.discovery_labels);
            let app_test = Appendable::new();
            let (dl1, dl2, honor) = (discovery_labels.clone(), discovery_labels.clone(), test.honor_labels);
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &app_test, v2);
                sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &dl1, honor, &[]));
                sl.report_sample_mutator = Arc::new(move |l| mutate_report_sample_labels(l, &dl2));
            });

            let now = SystemTime::now();
            let mut app = sl.appender();
            let (_, err) = app.append(test.scrape_text.as_bytes(), "text/plain", now);
            assert!(err.is_none(), "{}: {err:?}", test.title);
            assert!(app.commit().is_none());

            let expected = vec![sample(test.exp_lset.clone(), from_time(now), 1.0)];
            require_equal_msg(&expected, &app_test.result_samples(), &format!("appV2={v2}/{}", test.title));
        }
    });
}

#[test]
fn test_scrape_loop_append_for_conflicting_prefixed_labels() {
    foreach_appendable(|v2| {
        let cases: Vec<(&str, Vec<&str>, &str, Vec<&str>)> = vec![
            (
                "One target label collides with existing label",
                vec!["foo", "2"],
                r#"metric{foo="1"} 0"#,
                vec!["__name__", "metric", "exported_foo", "1", "foo", "2"],
            ),
            (
                "One target label collides with existing label, plus target label already with prefix 'exported'",
                vec!["foo", "2", "exported_foo", "3"],
                r#"metric{foo="1"} 0"#,
                vec!["__name__", "metric", "exported_exported_foo", "1", "exported_foo", "3", "foo", "2"],
            ),
            (
                "One target label collides with existing label, plus existing label already with prefix 'exported",
                vec!["foo", "3"],
                r#"metric{foo="1", exported_foo="2"} 0"#,
                vec!["__name__", "metric", "exported_exported_foo", "1", "exported_foo", "2", "foo", "3"],
            ),
            (
                "One target label collides with existing label, both already with prefix 'exported'",
                vec!["exported_foo", "2"],
                r#"metric{exported_foo="1"} 0"#,
                vec!["__name__", "metric", "exported_exported_foo", "1", "exported_foo", "2"],
            ),
            (
                "Two target labels collide with existing labels, both with and without prefix 'exported'",
                vec!["foo", "3", "exported_foo", "4"],
                r#"metric{foo="1", exported_foo="2"} 0"#,
                vec![
                    "__name__",
                    "metric",
                    "exported_exported_foo",
                    "1",
                    "exported_exported_exported_foo",
                    "2",
                    "exported_foo",
                    "4",
                    "foo",
                    "3",
                ],
            ),
            (
                "Extreme example",
                vec!["foo", "0", "exported_exported_foo", "1", "exported_exported_exported_foo", "2"],
                r#"metric{foo="3", exported_foo="4", exported_exported_exported_foo="5"} 0"#,
                vec![
                    "__name__",
                    "metric",
                    "exported_exported_exported_exported_exported_foo",
                    "5",
                    "exported_exported_exported_exported_foo",
                    "3",
                    "exported_exported_exported_foo",
                    "2",
                    "exported_exported_foo",
                    "1",
                    "exported_foo",
                    "4",
                    "foo",
                    "0",
                ],
            ),
        ];
        for (name, target_labels, exposed, expected) in cases {
            let app_test = Appendable::new();
            let t = target_with_labels(&target_labels);
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &app_test, v2);
                sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &t, false, &[]));
            });

            // time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)
            let ts = UNIX_EPOCH + Duration::from_secs(946_688_400);
            let mut app = sl.appender();
            let (_, err) = app.append(exposed.as_bytes(), "text/plain", ts);
            assert!(err.is_none(), "{name}: {err:?}");
            assert!(app.commit().is_none());

            require_equal_msg(
                &[sample(from_strings(&expected), from_time(ts), 0.0)],
                &app_test.result_samples(),
                &format!("appV2={v2}/{name}"),
            );
        }
    });
}

#[test]
fn test_scrape_loop_append_cache_entry_but_err_not_found() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));

        let fake_ref: SeriesRef = 1;
        let exp_value = 1.0;
        let metric: &[u8] = br#"metric{n="1"} 1"#;
        let (p, warning) = textparse::new(metric, "text/plain", &textparse::ParserOptions::default());
        assert!(p.is_some());
        assert!(warning.is_none(), "{warning:?}");
        let mut p = p.unwrap();

        let mut lset = Labels::default();
        let r = p.next();
        assert!(r.is_ok(), "{:?}", r.err());
        p.labels(&mut lset);

        // Create a fake entry in the cache.
        sl.cache.series().add_ref(metric, fake_ref, lset.clone());
        let now = SystemTime::now();

        let mut app = sl.appender();
        let (_, err) = app.append(metric, "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        require_equal(&[sample(lset, from_time(now), exp_value)], &app_test.result_samples());
    });
}

#[test]
fn test_scrape_loop_append_sample_limit() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            let a = app_test.clone();
            if v2 {
                sl.appendable_v2 = Some(Arc::new(AppendableV2Func(move |ctx: &Context| {
                    // Chain appTest to verify what samples passed through.
                    Box::new(LimitAppenderV2 { app: a.appender_v2(ctx), limit: 1, i: 0 }) as Box<dyn AppenderV2>
                })));
            } else {
                sl.appendable = Some(Arc::new(AppendableFunc(move |ctx: &Context| {
                    // Chain appTest to verify what samples passed through.
                    Box::new(LimitAppender { app: a.appender(ctx), limit: 1, i: 0 }) as Box<dyn Appender>
                })));
            }
            sl.sample_mutator = Arc::new(|l: Labels| if l.has("deleteme") { Labels::default() } else { l });
            sl.sample_limit = 1; // Same as limitAppender.limit
        });

        // Get the value of the Counter before performing append.
        let before_metric_value = sl.metrics.target_scrape_sample_limit.get().get();

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (stats, err) = app.append(b"metric_a 1\nmetric_b 1\nmetric_c 1\n", "text/plain", now);
        assert!(err.as_ref().is_some_and(|e| e.is(&ERR_SAMPLE_LIMIT)), "{err:?}");
        assert!(app.rollback().is_none());
        assert_eq!(3, stats.total);
        assert_eq!(3, stats.added);
        assert_eq!(1, stats.series_added);

        // Check that the Counter has been incremented a single time for the scrape, not multiple times for each
        // sample.
        let v = sl.metrics.target_scrape_sample_limit.get().get();
        let change = v - before_metric_value;
        assert_eq!(1.0, change, "Unexpected change of sample limit metric: {change}");

        // And verify that we got the samples that fit under the limit.
        let want = vec![sample(from_strings(&["__name__", "metric_a"]), from_time(now), 1.0)];
        require_equal_msg(&want, &app_test.rolledback_samples(), &format!("Appended samples not as expected:\n{app_test}"));

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (stats, err) = app.append(
            b"metric_a 1\nmetric_b 1\nmetric_c{deleteme=\"yes\"} 1\nmetric_d 1\nmetric_e 1\nmetric_f 1\nmetric_g 1\nmetric_h{deleteme=\"yes\"} 1\nmetric_i{deleteme=\"yes\"} 1\n",
            "text/plain",
            now,
        );
        assert!(err.as_ref().is_some_and(|e| e.is(&ERR_SAMPLE_LIMIT)), "{err:?}");
        assert!(app.rollback().is_none());
        assert_eq!(9, stats.total);
        assert_eq!(6, stats.added);
        assert_eq!(1, stats.series_added);
    });
}

/// The `testing_example_native_histogram` family that Go's client_golang gathers in
/// TestScrapeLoop_HistogramBucketLimit (NativeHistogramBucketFactor 1.1 gives schema 3). `l_extra_spans` are the
/// additional positive spans of the "L" series (one bucket each, delta 0).
// adapted: Go builds the family with client_golang (prometheus.NewHistogramVec + registry.Gather); there is no
// client library here, so the gathered family is written out (as dumped from client_golang v1.25.0) with the
// created timestamps of this run.
fn bucket_limit_family(l_count: u64, l_sum: f64, l_extra_offsets: &[i32], created: SystemTime) -> Vec<u8> {
    let d = created.duration_since(UNIX_EPOCH).unwrap();
    let created = format!("created_timestamp: < seconds: {} nanos: {} >", d.as_secs(), d.subsec_nanos());
    let series = |size: &str, count: u64, sum: f64, spans: &[(i32, u32)], deltas: &[i64]| {
        let mut s = format!(
            "metric: <\n  label: < name: \"size\" value: \"{size}\" >\n  label: < name: \"some\" value: \"value\" >\n  histogram: <\n    sample_count: {count}\n    sample_sum: {sum}\n    {created}\n    schema: 3\n    zero_threshold: 2.938735877055719e-39\n    zero_count: 0\n"
        );
        for (o, l) in spans {
            s.push_str(&format!("    positive_span: < offset: {o} length: {l} >\n"));
        }
        for d in deltas {
            s.push_str(&format!("    positive_delta: {d}\n"));
        }
        s.push_str("  >\n>\n");
        s
    };
    let mut l_spans = vec![(0, 1), (26, 1)];
    let mut l_deltas = vec![1, 0];
    for o in l_extra_offsets {
        l_spans.push((*o, 1));
        l_deltas.push(0);
    }
    let mut text = String::from(
        "name: \"testing_example_native_histogram\"\nhelp: \"This is used for testing\"\ntype: HISTOGRAM\n",
    );
    text.push_str(&series("L", l_count, l_sum, &l_spans, &l_deltas));
    text.push_str(&series("M", 2, 11.0, &[(0, 1), (26, 1)], &[1, 0]));
    text.push_str(&series("S", 1, 1.0, &[(0, 1)], &[1]));
    // Go MetricFamilyToProtobuf: varint length then the message.
    let mut b = Vec::new();
    text_to_proto(&text, &mut b).unwrap();
    b
}

#[test]
fn test_scrape_loop_histogram_bucket_limit() {
    foreach_appendable(|v2| {
        let (sl, _) = new_test_scrape_loop(|sl| {
            if v2 {
                sl.appendable_v2 = Some(Arc::new(AppendableV2Func(|ctx: &Context| {
                    Box::new(BucketLimitAppenderV2 { app: Appendable::new().appender_v2(ctx), limit: 2 })
                        as Box<dyn AppenderV2>
                })));
            } else {
                sl.appendable = Some(Arc::new(AppendableFunc(|ctx: &Context| {
                    Box::new(BucketLimitAppender { app: Appendable::new().appender(ctx), limit: 2 }) as Box<dyn Appender>
                })));
            }
            sl.enable_native_histogram_scraping = true;
            sl.sample_mutator = Arc::new(|l: Labels| if l.has("deleteme") { Labels::default() } else { l });
        });
        let mut app = sl.appender();

        let counter = || sl.metrics.target_scrape_native_histogram_bucket_limit.get().get();
        let mut before_metric_value = counter();
        let created = SystemTime::now();

        // Observations: S: 1; M: 1, 10; L: 1, 10.
        let msg = bucket_limit_family(2, 11.0, &[], created);
        let now = SystemTime::now();
        let (stats, err) = app.append(&msg, "application/vnd.google.protobuf", now);
        assert!(err.is_none(), "appV2={v2}: {err:?}");
        assert_eq!(3, stats.total);
        assert_eq!(3, stats.added);
        assert_eq!(3, stats.series_added);

        let metric_value = counter();
        assert_eq!(before_metric_value, metric_value);
        before_metric_value = metric_value;

        // L observes 100 (in a different bucket since > 10*1.1).
        let msg = bucket_limit_family(3, 111.0, &[26], created);
        let now = SystemTime::now();
        let (stats, err) = app.append(&msg, "application/vnd.google.protobuf", now);
        assert!(err.is_none(), "appV2={v2}: {err:?}");
        assert_eq!(3, stats.total);
        assert_eq!(3, stats.added);
        assert_eq!(0, stats.series_added); // Series are cached.

        let metric_value = counter();
        assert_eq!(before_metric_value, metric_value);
        before_metric_value = metric_value;

        // L observes 100000.
        let msg = bucket_limit_family(4, 100111.0, &[26, 78], created);
        let now = SystemTime::now();
        let (stats, err) = app.append(&msg, "application/vnd.google.protobuf", now);
        if !err.as_ref().is_some_and(|e| e.is(&ERR_BUCKET_LIMIT)) {
            panic!("Did not see expected histogram bucket limit error: {err:?}");
        }
        assert!(app.rollback().is_none());
        assert_eq!(3, stats.total);
        assert_eq!(3, stats.added);
        assert_eq!(0, stats.series_added); // Series are cached.

        let metric_value = counter();
        assert_eq!(before_metric_value + 1.0, metric_value);
    });
}

#[test]
fn test_scrape_loop_changing_metric_string() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(br#"metric_a{a="1",b="1"} 1"#, "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let mut app = sl.appender();
        let (_, err) = app.append(br#"metric_a{b="1",a="1"} 2"#, "text/plain", now + Duration::from_secs(60));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let l = from_strings(&["__name__", "metric_a", "a", "1", "b", "1"]);
        let want = vec![
            sample(l.clone(), from_time(now), 1.0),
            sample(l, from_time(now + Duration::from_secs(60)), 2.0),
        ];
        require_equal_msg(&want, &app_test.result_samples(), &format!("Appended samples not as expected:\n{app_test}"));
    });
}

#[test]
fn test_scrape_loop_append_fails_with_no_content_type() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            // Explicitly setting the lack of fallback protocol here to make it obvious.
            sl.fallback_scrape_protocol = String::new();
        });

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a 1\n", "", now);
        let msg = err.map(|e| e.to_string()).unwrap_or_default();
        assert!(
            msg.contains(
                "non-compliant scrape target sending blank Content-Type and no fallback_scrape_protocol specified for target"
            ),
            "Expected \"non-compliant scrape\" error but got: {msg}"
        );
    });
}

#[test]
fn test_scrape_loop_append_empty_with_no_content_type() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            // Explicitly setting the lack of fallback protocol here to make it obvious.
            sl.fallback_scrape_protocol = String::new();
        });

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"", "", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());
    });
}

#[test]
fn test_scrape_loop_append_staleness() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a 1\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let mut app = sl.appender();
        let (_, err) = app.append(b"", "", now + Duration::from_secs(1));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let l = from_strings(&["__name__", "metric_a"]);
        let want = vec![
            sample(l.clone(), from_time(now), 1.0),
            sample(l, from_time(now + Duration::from_secs(1)), stale_nan()),
        ];
        require_equal_msg(&want, &app_test.result_samples(), &format!("Appended samples not as expected:\n{app_test}"));
    });
}

#[test]
fn test_scrape_loop_append_no_staleness_if_timestamp() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| with_appendable(sl, &app_test, v2));
        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a 1 1000\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let mut app = sl.appender();
        let (_, err) = app.append(b"", "", now + Duration::from_secs(1));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let want = vec![sample(from_strings(&["__name__", "metric_a"]), 1000, 1.0)];
        require_equal_msg(&want, &app_test.result_samples(), &format!("Appended samples not as expected:\n{app_test}"));
    });
}

#[test]
fn test_scrape_loop_append_staleness_if_track_timestamp_staleness() {
    foreach_appendable(|v2| {
        let app_test = Appendable::new();
        let (sl, _) = new_test_scrape_loop(|sl| {
            with_appendable(sl, &app_test, v2);
            sl.track_timestamps_staleness = true;
        });

        let now = SystemTime::now();
        let mut app = sl.appender();
        let (_, err) = app.append(b"metric_a 1 1000\n", "text/plain", now);
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let mut app = sl.appender();
        let (_, err) = app.append(b"", "", now + Duration::from_secs(1));
        assert!(err.is_none(), "{err:?}");
        assert!(app.commit().is_none());

        let l = from_strings(&["__name__", "metric_a"]);
        let want = vec![
            sample(l.clone(), 1000, 1.0),
            sample(l, from_time(now + Duration::from_secs(1)), stale_nan()),
        ];
        require_equal_msg(&want, &app_test.result_samples(), &format!("Appended samples not as expected:\n{app_test}"));
    });
}

// ---------------------------------------------------------------------------------------------------------------
// TestScrapeLoopAppend: the main table test of scrape appends, including histograms, exemplars and metadata.

const NH_WITH_CLASSIC_BUCKETS: &str = r#"name: "test_histogram"
help: "Test histogram with many buckets removed to keep it manageable in size."
type: HISTOGRAM
metric: <
  histogram: <
    created_timestamp: <
      seconds: 111111
      nanos: 1000000
    >
    sample_count: 175
    sample_sum: 0.0008280461746287094
    bucket: <
      cumulative_count: 2
      upper_bound: -0.0004899999999999998
    >
    bucket: <
      cumulative_count: 4
      upper_bound: -0.0003899999999999998
      exemplar: <
        label: <
          name: "dummyID"
          value: "59727"
        >
        value: -0.00039
        timestamp: <
          seconds: 1625851155
          nanos: 146848499
        >
      >
    >
    bucket: <
      cumulative_count: 16
      upper_bound: -0.0002899999999999998
      exemplar: <
        label: <
          name: "dummyID"
          value: "5617"
        >
        value: -0.00029
      >
    >
    bucket: <
      cumulative_count: 32
      upper_bound: -0.0001899999999999998
      exemplar: <
        label: <
          name: "dummyID"
          value: "58215"
        >
        value: -0.00019
        timestamp: <
          seconds: 1625851055
          nanos: 146848599
        >
      >
    >
    schema: 3
    zero_threshold: 2.938735877055719e-39
    zero_count: 2
    negative_span: <
      offset: -162
      length: 1
    >
    negative_span: <
      offset: 23
      length: 4
    >
    negative_delta: 1
    negative_delta: 3
    negative_delta: -2
    negative_delta: -1
    negative_delta: 1
    positive_span: <
      offset: -161
      length: 1
    >
    positive_span: <
      offset: 8
      length: 3
    >
    positive_delta: 1
    positive_delta: 2
    positive_delta: -1
    positive_delta: -1
  >
  timestamp_ms: 1234568
>

"#;

const NH_NO_CLASSIC_BUCKETS: &str = r#"name: "test_histogram"
help: "Test histogram."
type: HISTOGRAM
metric: <
  histogram: <
    created_timestamp: <
      seconds: 111111
      nanos: 1000000
    >
    sample_count: 175
    sample_sum: 0.0008280461746287094
    schema: 3
    zero_threshold: 2.938735877055719e-39
    zero_count: 2
    negative_span: <
      offset: -162
      length: 1
    >
    negative_span: <
      offset: 23
      length: 4
    >
    negative_delta: 1
    negative_delta: 3
    negative_delta: -2
    negative_delta: -1
    negative_delta: 1
    positive_span: <
      offset: -161
      length: 1
    >
    positive_span: <
      offset: 8
      length: 3
    >
    positive_delta: 1
    positive_delta: 2
    positive_delta: -1
    positive_delta: -1
	exemplars: <
	  label: <
        name: "dummyID"
        value: "59732"
      >
      value: -0.00039
      timestamp: <
        seconds: 1625851155
        nanos: 146848499
      >
	>
	exemplars: <
	  label: <
        name: "dummyID"
        value: "58242"
      >
      value: -0.00019
      timestamp: <
        seconds: 1625851055
        nanos: 146848599
      >
	>
	exemplars: <
      label: <
        name: "dummyID"
        value: "5617"
      >
      value: -0.00029
    >
  >
  timestamp_ms: 1234568
>

"#;

fn ex(l: &[&str], value: f64, ts: i64, has_ts: bool) -> Exemplar {
    Exemplar { labels: from_strings(l), value, ts, has_ts }
}

fn test_histogram() -> Histogram {
    Histogram {
        count: 175,
        zero_count: 2,
        sum: 0.0008280461746287094,
        zero_threshold: 2.938735877055719e-39,
        schema: 3,
        positive_spans: vec![Span { offset: -161, length: 1 }, Span { offset: 8, length: 3 }],
        negative_spans: vec![Span { offset: -162, length: 1 }, Span { offset: 23, length: 4 }],
        positive_buckets: vec![1, 2, -1, -1],
        negative_buckets: vec![1, 3, -2, -1, 1],
        ..Default::default()
    }
}

#[test]
fn test_scrape_loop_append() {
    foreach_appendable(|v2| {
        let st: i64 = if v2 { 111111001 } else { 0 };

        struct Tc {
            title: &'static str,
            always_scrape_classic_hist: bool,
            enable_native_histograms_ingestion: bool,
            scrape_text: &'static str,
            content_type: &'static str,
            discovery_labels: Vec<&'static str>,
            samples: Vec<Sample>,
        }
        let base = |title, scrape_text, content_type, samples| Tc {
            title,
            always_scrape_classic_hist: false,
            enable_native_histograms_ingestion: false,
            scrape_text,
            content_type,
            discovery_labels: vec![],
            samples,
        };
        let s = |l: &[&str], v: f64| Sample { l: from_strings(l), v, ..Default::default() };
        let s_st = |l: &[&str], v: f64, es: Vec<Exemplar>| Sample {
            l: from_strings(l),
            st,
            t: 1234568,
            v,
            es,
            ..Default::default()
        };

        let cases = vec![
            base(
                "Normal NaN scraped",
                "metric_total{n=\"1\"} NaN\n# EOF",
                "application/openmetrics-text",
                vec![s(&["__name__", "metric_total", "n", "1"], f64::from_bits(NORMAL_NAN))],
            ),
            Tc {
                discovery_labels: vec!["n", "2"],
                ..base(
                    "Metric without exemplars",
                    "metric_total{n=\"1\"} 0\n# EOF",
                    "application/openmetrics-text",
                    vec![s(&["__name__", "metric_total", "exported_n", "1", "n", "2"], 0.0)],
                )
            },
            Tc {
                discovery_labels: vec!["n", "2"],
                ..base(
                    "Metric with exemplars",
                    "metric_total{n=\"1\"} 0 # {a=\"abc\"} 1.0\n# EOF",
                    "application/openmetrics-text",
                    vec![Sample {
                        es: vec![ex(&["a", "abc"], 1.0, 0, false)],
                        ..s(&["__name__", "metric_total", "exported_n", "1", "n", "2"], 0.0)
                    }],
                )
            },
            Tc {
                discovery_labels: vec!["n", "2"],
                ..base(
                    "Metric with exemplars and TS",
                    "metric_total{n=\"1\"} 0 # {a=\"abc\"} 1.0 10000\n# EOF",
                    "application/openmetrics-text",
                    vec![Sample {
                        es: vec![ex(&["a", "abc"], 1.0, 10000000, true)],
                        ..s(&["__name__", "metric_total", "exported_n", "1", "n", "2"], 0.0)
                    }],
                )
            },
            base(
                "Metric with ST",
                "# TYPE metric counter\nmetric_total{n=\"1\"} 1.1\nmetric_created{n=\"1\"} 9999.999\n# EOF",
                "application/openmetrics-text",
                if !v2 {
                    vec![
                        s(&["__name__", "metric_total", "n", "1"], 1.1),
                        s(&["__name__", "metric_created", "n", "1"], 9999.999),
                    ]
                } else {
                    vec![Sample { st: 9999999, ..s(&["__name__", "metric_total", "n", "1"], 1.1) }]
                },
            ),
            base(
                "Two metrics and exemplars",
                "metric_total{n=\"1\"} 1 # {t=\"1\"} 1.0 10000\nmetric_total{n=\"2\"} 2 # {t=\"2\"} 2.0 20000\n# EOF",
                "application/openmetrics-text",
                vec![
                    Sample {
                        es: vec![ex(&["t", "1"], 1.0, 10000000, true)],
                        ..s(&["__name__", "metric_total", "n", "1"], 1.0)
                    },
                    Sample {
                        es: vec![ex(&["t", "2"], 2.0, 20000000, true)],
                        ..s(&["__name__", "metric_total", "n", "2"], 2.0)
                    },
                ],
            ),
            Tc {
                enable_native_histograms_ingestion: true,
                ..base(
                    "Native histogram with ST and three exemplars from classic buckets",
                    NH_WITH_CLASSIC_BUCKETS,
                    "application/vnd.google.protobuf",
                    vec![Sample {
                        t: 1234568,
                        st,
                        l: from_strings(&["__name__", "test_histogram"]),
                        h: Some(test_histogram()),
                        // Native histogram exemplars are arranged by timestamp, and those with missing timestamps
                        // are dropped.
                        es: vec![
                            ex(&["dummyID", "58215"], -0.00019, 1625851055146, true),
                            ex(&["dummyID", "59727"], -0.00039, 1625851155146, true),
                        ],
                        ..Default::default()
                    }],
                )
            },
            Tc {
                enable_native_histograms_ingestion: true,
                always_scrape_classic_hist: true,
                ..base(
                    "Native histogram with ST and three exemplars scraped as classic histogram",
                    NH_WITH_CLASSIC_BUCKETS,
                    "application/vnd.google.protobuf",
                    vec![
                        Sample {
                            t: 1234568,
                            st,
                            l: from_strings(&["__name__", "test_histogram"]),
                            h: Some(test_histogram()),
                            // Native histogram one is arranged by timestamp. Exemplars with missing timestamps are
                            // dropped for native histograms.
                            es: vec![
                                ex(&["dummyID", "58215"], -0.00019, 1625851055146, true),
                                ex(&["dummyID", "59727"], -0.00039, 1625851155146, true),
                            ],
                            ..Default::default()
                        },
                        s_st(&["__name__", "test_histogram_count"], 175.0, vec![]),
                        s_st(&["__name__", "test_histogram_sum"], 0.0008280461746287094, vec![]),
                        s_st(&["__name__", "test_histogram_bucket", "le", "-0.0004899999999999998"], 2.0, vec![]),
                        s_st(
                            &["__name__", "test_histogram_bucket", "le", "-0.0003899999999999998"],
                            4.0,
                            vec![ex(&["dummyID", "59727"], -0.00039, 1625851155146, true)],
                        ),
                        s_st(
                            &["__name__", "test_histogram_bucket", "le", "-0.0002899999999999998"],
                            16.0,
                            vec![ex(&["dummyID", "5617"], -0.00029, 1234568, false)],
                        ),
                        s_st(
                            &["__name__", "test_histogram_bucket", "le", "-0.0001899999999999998"],
                            32.0,
                            vec![ex(&["dummyID", "58215"], -0.00019, 1625851055146, true)],
                        ),
                        s_st(&["__name__", "test_histogram_bucket", "le", "+Inf"], 175.0, vec![]),
                    ],
                )
            },
            Tc {
                enable_native_histograms_ingestion: true,
                ..base(
                    "Native histogram with ST, exemplars and no classic buckets",
                    NH_NO_CLASSIC_BUCKETS,
                    "application/vnd.google.protobuf",
                    vec![Sample {
                        t: 1234568,
                        st,
                        l: from_strings(&["__name__", "test_histogram"]),
                        h: Some(test_histogram()),
                        // Exemplars with missing timestamps are dropped for native histograms.
                        es: vec![
                            ex(&["dummyID", "58242"], -0.00019, 1625851055146, true),
                            ex(&["dummyID", "59732"], -0.00039, 1625851155146, true),
                        ],
                        ..Default::default()
                    }],
                )
            },
            base(
                "Native histogram with ST, exemplars but ingestion disabled",
                NH_NO_CLASSIC_BUCKETS,
                "application/vnd.google.protobuf",
                vec![
                    s_st(&["__name__", "test_histogram_count"], 175.0, vec![]),
                    s_st(&["__name__", "test_histogram_sum"], 0.0008280461746287094, vec![]),
                    s_st(&["__name__", "test_histogram_bucket", "le", "+Inf"], 175.0, vec![]),
                ],
            ),
        ];

        for mut test in cases {
            let discovery_labels = target_with_labels(&test.discovery_labels);
            let app_test = Appendable::new();
            let (dl1, dl2) = (discovery_labels.clone(), discovery_labels.clone());
            let (enable_nh, always_classic) =
                (test.enable_native_histograms_ingestion, test.always_scrape_classic_hist);
            let (sl, _) = new_test_scrape_loop(|sl| {
                with_appendable(sl, &app_test, v2);
                sl.enable_native_histogram_scraping = enable_nh;
                sl.sample_mutator = Arc::new(move |l| mutate_sample_labels(l, &dl1, false, &[]));
                sl.report_sample_mutator = Arc::new(move |l| mutate_report_sample_labels(l, &dl2));
                sl.always_scrape_classic_hist = always_classic;
                // This test does not care about metadata.
                sl.append_metadata_to_wal = false;
            });
            let mut app = sl.appender();

            let now = SystemTime::now();

            // Process expected samples.
            for smp in test.samples.iter_mut() {
                if !v2 && !smp.mf.is_empty() {
                    // AppenderV1 does not support metric family passing.
                    smp.mf = String::new();
                }
                if smp.t != 0 {
                    continue;
                }
                smp.t = from_time(now);
                // We need to set the timestamp for expected exemplars that do not have a timestamp.
                for e in smp.es.iter_mut() {
                    if e.ts == 0 {
                        e.ts = from_time(now);
                    }
                }
            }

            let mut buf = Vec::new();
            if test.content_type == "application/vnd.google.protobuf" {
                text_to_proto(test.scrape_text, &mut buf).unwrap();
            } else {
                buf.extend_from_slice(test.scrape_text.as_bytes());
            }

            let (_, err) = app.append(&buf, test.content_type, now);
            assert!(err.is_none(), "appV2={v2}/{}: {err:?}", test.title);
            assert!(app.commit().is_none());
            require_equal_msg(&test.samples, &app_test.result_samples(), &format!("appV2={v2}/{}", test.title));
        }
    });
}
