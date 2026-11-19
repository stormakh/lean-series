//! Port of `web/federate_test.go`.

use std::sync::Arc;
use std::time::Duration;

use http::Request;
use model::histogram::{FloatHistogram, Histogram, Span, custom_values};
use model::labels::{Labels, from_strings};
use storage::{AppendV2Options, BoxQuerier, Context, Error, Queryable};
use textparse::{Entry, Parser};
use web::web::{Handler, Options};

struct Scenario {
    name: &'static str,
    params: &'static str,
    external_labels: Labels,
    code: u16,
    body: &'static str,
}

fn scenarios() -> Vec<Scenario> {
    let s = |name, params, external_labels, code, body| Scenario { name, params, external_labels, code, body };
    vec![
        s("empty", "", Labels::empty(), 200, ""),
        s("match nothing", "match[]=does_not_match_anything", Labels::empty(), 200, ""),
        s(
            "invalid params from the beginning",
            "match[]=-not-a-valid-metric-name",
            Labels::empty(),
            400,
            "1:1: parse error: unexpected <op:->\n",
        ),
        s(
            "invalid params somewhere in the middle",
            "match[]=not-a-valid-metric-name",
            Labels::empty(),
            400,
            "1:4: parse error: unexpected <op:->\n",
        ),
        s(
            "test_metric1",
            "match[]=test_metric1",
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n",
        ),
        s(
            "test_metric2",
            "match[]=test_metric2",
            Labels::empty(),
            200,
            "# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n",
        ),
        s(
            "test_metric_without_labels",
            "match[]=test_metric_without_labels",
            Labels::empty(),
            200,
            "# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{instance=\"\"} 1001 6000000\n",
        ),
        s("test_stale_metric", "match[]=test_metric_stale", Labels::empty(), 200, ""),
        s(
            "test_old_metric",
            "match[]=test_metric_old",
            Labels::empty(),
            200,
            "# TYPE test_metric_old untyped\ntest_metric_old{instance=\"\"} 981 5880000\n",
        ),
        s(
            "{foo='boo'}",
            "match[]={foo='boo'}",
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n",
        ),
        s(
            "two matchers",
            "match[]=test_metric1&match[]=test_metric2",
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n",
        ),
        s(
            "two matchers with overlap",
            "match[]={__name__=~'test_metric1'}&match[]={foo='bar'}",
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n",
        ),
        s(
            "everything",
            "match[]={__name__=~'.%2b'}", // '%2b' is an URL-encoded '+'.
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric_old untyped\ntest_metric_old{instance=\"\"} 981 5880000\n# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{instance=\"\"} 1001 6000000\n",
        ),
        s(
            "empty label value matches everything that doesn't have that label",
            "match[]={foo='',__name__=~'.%2b'}",
            Labels::empty(),
            200,
            "# TYPE test_metric_old untyped\ntest_metric_old{instance=\"\"} 981 5880000\n# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{instance=\"\"} 1001 6000000\n",
        ),
        s(
            "empty label value for a label that doesn't exist at all, matches everything",
            "match[]={bar='',__name__=~'.%2b'}",
            Labels::empty(),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric_old untyped\ntest_metric_old{instance=\"\"} 981 5880000\n# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{instance=\"\"} 1001 6000000\n",
        ),
        s(
            "external labels are added if not already present",
            "match[]={__name__=~'.%2b'}", // '%2b' is an URL-encoded '+'.
            from_strings(&["foo", "baz", "zone", "ie"]),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\",zone=\"ie\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\",zone=\"ie\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\",zone=\"ie\"} 1 6000000\n# TYPE test_metric_old untyped\ntest_metric_old{foo=\"baz\",instance=\"\",zone=\"ie\"} 981 5880000\n# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{foo=\"baz\",instance=\"\",zone=\"ie\"} 1001 6000000\n",
        ),
        s(
            // This makes no sense as a configuration, but we should know what it does anyway.
            "instance is an external label",
            "match[]={__name__=~'.%2b'}", // '%2b' is an URL-encoded '+'.
            from_strings(&["instance", "baz"]),
            200,
            "# TYPE test_metric1 untyped\ntest_metric1{foo=\"bar\",instance=\"i\"} 10000 6000000\ntest_metric1{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric2 untyped\ntest_metric2{foo=\"boo\",instance=\"i\"} 1 6000000\n# TYPE test_metric_old untyped\ntest_metric_old{instance=\"baz\"} 981 5880000\n# TYPE test_metric_without_labels untyped\ntest_metric_without_labels{instance=\"baz\"} 1001 6000000\n",
        ),
    ]
}

/// The handler fields the Go tests set: local storage, a 5m lookback delta and a clock at 101min after epoch.
fn handler(local: Arc<dyn Queryable>) -> Arc<Handler> {
    Handler::new(Options {
        local_storage: Some(local),
        lookback_delta: Duration::from_secs(5 * 60),
        now: Some(Arc::new(|| 101 * 60 * 1000)),
        ..Default::default()
    })
}

fn with_external_labels(h: &Handler, ls: &Labels) {
    let mut c = config::Config::default();
    c.global_config.external_labels = ls.clone();
    h.apply_config(Arc::new(c));
}

/// `httptest.NewRequest(GET, "http://example.org/federate?"+params)`.
fn federate_request(params: &str) -> Request {
    Request { method: "GET".to_string(), path: "/federate".to_string(), query: params.to_string(), ..Default::default() }
}

#[test]
fn test_federation() {
    let storage = promql::promqltest::loaded_storage(
        r#"
		load 1m
			test_metric1{foo="bar",instance="i"}    0+100x100
			test_metric1{foo="boo",instance="i"}    1+0x100
			test_metric2{foo="boo",instance="i"}    1+0x100
			test_metric_without_labels 1+10x100
			test_metric_stale                       1+10x99 stale
			test_metric_old                         1+10x98
	"#,
    )
    .expect("load storage");
    let h = handler(Arc::new(storage));

    for sc in scenarios() {
        with_external_labels(&h, &sc.external_labels);
        let res = h.federation(&federate_request(sc.params));
        assert_eq!(sc.code, res.status, "{}", sc.name);
        assert_eq!(sc.body, normalize_body(&res.into_body_vec()), "{}", sc.name);
    }
}

struct NotReadyReadStorage;

impl Queryable for NotReadyReadStorage {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        Err(Error::wrap("wrap", tsdb::db::ERR_NOT_READY))
    }
}

/// Regression test for https://github.com/prometheus/prometheus/issues/7181.
#[test]
fn test_federation_not_ready() {
    for sc in scenarios() {
        let h = handler(Arc::new(NotReadyReadStorage));
        with_external_labels(&h, &sc.external_labels);
        let res = h.federation(&federate_request(sc.params));
        if sc.code == 400 {
            // Request are expected to be checked before DB readiness.
            assert_eq!(400, res.status, "{}", sc.name);
            continue;
        }
        assert_eq!(503, res.status, "{}", sc.name);
    }
}

/// Go `normalizeBody`: sorts the lines within a metric family.
fn normalize_body(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut lines: Vec<String> = Vec::new();
    let mut last_hash = 0usize;
    // Go reads complete lines only (a last line without '\n' is dropped).
    let mut rest: &str = &text;
    while let Some(i) = rest.find('\n') {
        let line = &rest[..=i];
        rest = &rest[i + 1..];
        if line.starts_with('#') && !lines.is_empty() {
            lines[last_hash + 1..].sort();
            last_hash = lines.len();
        }
        lines.push(line.to_string());
    }
    if !lines.is_empty() {
        let start = (last_hash + 1).min(lines.len());
        lines[start..].sort();
    }
    lines.concat()
}

#[derive(Debug, PartialEq)]
struct Sample {
    metric: Labels,
    t: i64,
    f: f64,
    h: Option<FloatHistogram>,
}

#[test]
fn test_federation_with_native_histograms() {
    let mut exp_vec: Vec<Sample> = Vec::new();

    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("federate_test").join("native_histograms");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let db = tsdb::db::open(&dir, None, None, None).expect("open tsdb");

    let spans = || vec![Span { offset: 0, length: 2 }, Span { offset: 1, length: 2 }];
    let mut hist = Histogram {
        count: 12,
        zero_count: 2,
        zero_threshold: 0.001,
        sum: 39.4,
        schema: 1,
        positive_spans: spans(),
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: spans(),
        negative_buckets: vec![1, 1, -1, 0],
        ..Default::default()
    };
    let hist_without_zero_bucket = Histogram {
        count: 20,
        sum: 99.23,
        schema: 1,
        positive_spans: spans(),
        positive_buckets: vec![2, 2, -2, 0],
        negative_spans: spans(),
        negative_buckets: vec![2, 2, -2, 0],
        ..Default::default()
    };
    let nhcb = Histogram {
        count: 6,
        sum: 1.234,
        schema: -53,
        positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 2, length: 1 }],
        positive_buckets: vec![3, -1, -1],
        custom_values: custom_values(&[0.1, 0.2, 0.5, 1.0, 2.0]),
        ..Default::default()
    };
    let ctx = Context::background();
    let mut app = storage::AppendableV2::appender_v2(&*db, &ctx);
    let opts = AppendV2Options::default();
    let t = 100 * 60 * 1000;
    for i in 0..7 {
        let is = i.to_string();
        let l = from_strings(&["__name__", "test_metric", "foo", &is]);
        let exp_l = from_strings(&["__name__", "test_metric", "instance", "", "foo", &is]);
        let r = match i {
            0 | 3 => {
                let r = app.append(0, &l, 0, t, (i * 100) as f64, None, None, &opts);
                exp_vec.push(Sample { metric: exp_l, t, f: (i * 100) as f64, h: None });
                r
            }
            1 | 2 => {
                // Use two float histograms to ensure buffer reuse does not overwrite the first.
                hist.zero_count += 1;
                hist.count += 1;
                let fh = hist.to_float();
                let r = app.append(0, &l, 0, t, 0.0, None, Some(&fh.clone()), &opts);
                exp_vec.push(Sample { metric: exp_l, t, f: 0.0, h: Some(fh) });
                r
            }
            4 => {
                let r = app.append(0, &l, 0, t, 0.0, Some(&hist_without_zero_bucket.clone()), None, &opts);
                exp_vec.push(Sample { metric: exp_l, t, f: 0.0, h: Some(hist_without_zero_bucket.to_float()) });
                r
            }
            5 => {
                hist.zero_count += 1;
                hist.count += 1;
                let r = app.append(0, &l, 0, t, 0.0, Some(&hist.clone()), None, &opts);
                exp_vec.push(Sample { metric: exp_l, t, f: 0.0, h: Some(hist.to_float()) });
                r
            }
            _ => {
                let r = app.append(0, &l, 0, t, 0.0, Some(&nhcb.clone()), None, &opts);
                let mut push = |ls: &[&str], f: f64| exp_vec.push(Sample { metric: from_strings(ls), t, f, h: None });
                push(&["__name__", "test_metric_count", "instance", "", "foo", &is], 6.0);
                push(&["__name__", "test_metric_sum", "instance", "", "foo", &is], 1.234);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "0.1"], 3.0);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "0.2"], 5.0);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "0.5"], 5.0);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "1.0"], 5.0);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "2.0"], 6.0);
                push(&["__name__", "test_metric_bucket", "instance", "", "foo", &is, "le", "+Inf"], 6.0);
                r
            }
        };
        assert!(r.is_ok(), "append {}: {:?}", i, r.err().map(|e| e.to_string()));
    }
    assert!(app.commit().is_none());

    let h = handler(Arc::new(web::web::TsdbAdapter(db.clone())));
    let mut req = federate_request("match[]=test_metric");
    req.headers.push((
        "accept".to_string(),
        "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited,application/openmetrics-text;version=1.0.0;q=0.8,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1".to_string(),
    ));
    let res = h.federation(&req);
    assert_eq!(200, res.status);
    let body = res.into_body_vec();

    let mut p = textparse::ProtobufParser::new(&body, false, false, false, false);
    let mut act_vec: Vec<Sample> = Vec::new();
    let mut metric_families = 0;
    let mut l = Labels::empty();
    loop {
        let et = match p.next() {
            Ok(et) => et,
            Err(textparse::Error::Eof) => break,
            Err(e) => panic!("parse: {:?}", e),
        };
        if et == Entry::Histogram || et == Entry::Series {
            p.labels(&mut l);
        }
        match et {
            Entry::Help => metric_families += 1,
            Entry::Histogram => {
                let (_, ts, hh, fh) = p.histogram();
                assert!(hh.is_none());
                let (ts, fh) = (ts.expect("timestamp"), fh.cloned());
                act_vec.push(Sample { metric: l.clone(), t: ts, f: 0.0, h: fh });
            }
            Entry::Series => {
                let (_, ts, f) = p.series();
                act_vec.push(Sample { metric: l.clone(), t: ts.expect("timestamp"), f, h: None });
            }
            _ => {}
        }
    }

    // TODO(codesome): Once PromQL is able to set the CounterResetHint on histograms,
    // test it with switching histogram types for metric families.
    assert_eq!(4, metric_families);
    assert_eq!(exp_vec, act_vec);
    db.close().expect("close db");
}
