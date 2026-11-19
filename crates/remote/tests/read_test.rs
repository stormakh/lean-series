//! Port of `storage/remote/read_test.go`.

mod common;

use std::sync::Arc;

use common::*;
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use remote::client::ReadClient;
use remote::codec::{from_query_result, to_query_result};
use remote::prompb::{self, LabelMatcher, MatcherType};
use remote::read::{new_sample_and_chunk_queryable_client, new_series_set_filter, RemoteQuerier, StartTimeCallback};
use remote::storage::{RemoteQueryable, Storage};
use storage::{chained_series_merge, new_merge_querier, AppendV2Options, Context, Queryable};

fn read_test_dir(name: &str) -> std::path::PathBuf {
    let d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/tmp/w3-remote/qm")
        .join(format!("read-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn test_storage_cost_sampling_queryable_config_reload() {
    use storage::CostSamplingQueryable;
    let s = Arc::new(remote::write::new_storage(
        None,
        Some(discovery::metrics::Registry::new()),
        None,
        &read_test_dir("cost"),
        std::time::Duration::from_secs(60),
        None,
        false,
        None,
    ));
    let empty = config::Config { global_config: config::default_global_config(), ..Default::default() };
    let mut configured = config::Config { global_config: config::default_global_config(), ..Default::default() };
    let mut reader = config::default_remote_read_config();
    reader.url = yaml::Url::parse("http://example.invalid").unwrap();
    configured.remote_read_configs = vec![reader];
    for cfg in [&empty, &configured, &empty] {
        s.apply_config(cfg).unwrap();
        let (source, supported) = s.cost_sampling_queryable();
        assert!(source.is_none());
        assert_eq!(cfg.remote_read_configs.is_empty(), supported);
    }
    let s2 = s.clone();
    let wg = std::thread::spawn(move || {
        for _ in 0..50 {
            let _ = s2.cost_sampling_queryable();
        }
    });
    for _ in 0..10 {
        s.apply_config(&configured).unwrap();
        s.apply_config(&empty).unwrap();
    }
    wg.join().unwrap();
    assert!(storage::Storage::close(&*s).is_none());
}

#[test]
fn test_no_duplicate_read_configs() {
    let dir = read_test_dir("nodup");
    let rr = |name: &str, host: &str| config::RemoteReadConfig {
        name: name.to_string(),
        url: yaml::Url::parse(&format!("http://{host}")).unwrap(),
        ..Default::default()
    };
    let cfg1 = rr("write-1", "localhost1");
    let cfg2 = rr("write-2", "localhost2");
    let cfg3 = rr("", "localhost3");

    for (cfgs, want_err) in [
        // Duplicates but with different names, we should not get an error.
        (vec![cfg1.clone(), cfg2.clone()], false),
        // Duplicates but one with no name, we should not get an error.
        (vec![cfg1.clone(), cfg3.clone()], false),
        // Duplicates both with no name, we should get an error.
        (vec![cfg3.clone(), cfg3.clone()], true),
    ] {
        let s = remote::write::new_storage(None, None, None, &dir, std::time::Duration::from_secs(60), None, false, None);
        let conf = config::Config { global_config: config::default_global_config(), remote_read_configs: cfgs, ..Default::default() };
        let err = s.apply_config(&conf);
        // Go `prometheus.Unregister(s.rws.highestTimestamp)`: no registry here, nothing to unregister.
        assert_eq!(want_err, err.is_err());
        assert!(storage::Storage::close(&s).is_none());
    }
}

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn key(ms: &[Matcher]) -> Vec<(MatchType, String, String)> {
    ms.iter().map(|m| (m.typ, m.name.clone(), m.value.clone())).collect()
}

#[test]
fn test_external_labels_querier_add_external_labels() {
    let tests: Vec<(Labels, Vec<Matcher>, Vec<Matcher>, Vec<&str>)> = vec![
        (
            Labels::empty(),
            vec![m(MatchType::Equal, "job", "api-server")],
            vec![m(MatchType::Equal, "job", "api-server")],
            vec![],
        ),
        (
            labels::from_strings(&["dc", "berlin-01", "region", "europe"]),
            vec![m(MatchType::Equal, "job", "api-server")],
            vec![
                m(MatchType::Equal, "job", "api-server"),
                m(MatchType::Equal, "region", "europe"),
                m(MatchType::Equal, "dc", "berlin-01"),
            ],
            vec!["dc", "region"],
        ),
        (
            labels::from_strings(&["dc", "berlin-01", "region", "europe"]),
            vec![m(MatchType::Equal, "job", "api-server"), m(MatchType::Equal, "dc", "munich-02")],
            vec![
                m(MatchType::Equal, "job", "api-server"),
                m(MatchType::Equal, "region", "europe"),
                m(MatchType::Equal, "dc", "munich-02"),
            ],
            vec!["region"],
        ),
    ];

    for (i, (el, in_matchers, mut out_matchers, added)) in tests.into_iter().enumerate() {
        let q = RemoteQuerier::with_external_labels(Arc::new(MockedRemoteClient::new(vec![])), el);
        let (mut matchers, got_added) = q.add_external_labels(&in_matchers);

        out_matchers.sort_by(|a, b| a.name.cmp(&b.name));
        matchers.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(key(&out_matchers), key(&matchers), "{i}");
        assert_eq!(added, got_added, "{i}");
    }
}

#[test]
fn test_series_set_filter() {
    let tests = vec![(
        vec!["foo".to_string()],
        prompb::QueryResult {
            timeseries: vec![prompb::TimeSeries {
                labels: prompb::labels_to_label_protos(&labels::from_strings(&["foo", "bar", "a", "b"])),
                ..Default::default()
            }],
        },
        prompb::QueryResult {
            timeseries: vec![prompb::TimeSeries {
                labels: prompb::labels_to_label_protos(&labels::from_strings(&["a", "b"])),
                ..Default::default()
            }],
        },
    )];

    for (to_remove, input, expected) in tests {
        let mut filtered = new_series_set_filter(from_query_result(true, input), to_remove);
        let (act, ws, err) = to_query_result(&mut *filtered, 1_000_000);
        assert!(err.is_none(), "{err:?}");
        assert!(ws.is_empty());
        assert_eq!(expected, act);
    }
}

fn lm(t: MatcherType, n: &str, v: &str) -> LabelMatcher {
    LabelMatcher { r#type: t, name: n.to_string(), value: v.to_string() }
}

fn q(start: i64, end: i64, matchers: Vec<LabelMatcher>) -> Option<prompb::Query> {
    Some(prompb::Query { start_timestamp_ms: start, end_timestamp_ms: end, matchers, hints: None })
}

fn cb(v: i64) -> Option<StartTimeCallback> {
    Some(Arc::new(move || Ok(v)))
}

// NOTE: ChunkQuerier is not tested, as it uses the querier for all operations anyway.
#[test]
fn test_sample_and_chunk_queryable_client() {
    let mock = Arc::new(MockedRemoteClient::new(vec![
        // Samples do not matter for the tests below.
        prompb::TimeSeries { labels: vec![l("a", "b")], ..Default::default() },
        prompb::TimeSeries { labels: vec![l("a", "b3"), l("region", "us")], ..Default::default() },
        prompb::TimeSeries { labels: vec![l("a", "b2"), l("region", "europe")], ..Default::default() },
    ]));

    struct Tc {
        name: &'static str,
        matchers: Vec<Matcher>,
        mint: i64,
        maxt: i64,
        external_labels: Labels,
        required_matchers: Vec<Matcher>,
        read_recent: bool,
        callback: Option<StartTimeCallback>,
        expected_query: Option<prompb::Query>,
        expected_series: Vec<Labels>,
    }
    let base = || Tc {
        name: "",
        matchers: vec![],
        mint: 1,
        maxt: 2,
        external_labels: Labels::empty(),
        required_matchers: vec![],
        read_recent: true,
        callback: None,
        expected_query: None,
        expected_series: vec![],
    };
    let fs = |s: &[&str]| labels::from_strings(s);
    let all = || vec![fs(&["a", "b"]), fs(&["a", "b2", "region", "europe"]), fs(&["a", "b3", "region", "us"])];

    let cases = vec![
        Tc {
            name: "empty",
            matchers: vec![m(MatchType::NotEqual, "a", "something")],
            expected_query: q(1, 2, vec![lm(MatcherType::NEQ, "a", "something")]),
            expected_series: all(),
            ..base()
        },
        Tc {
            name: "external labels specified, not explicitly requested",
            matchers: vec![m(MatchType::NotEqual, "a", "something")],
            external_labels: fs(&["region", "europe"]),
            expected_query: q(
                1,
                2,
                vec![lm(MatcherType::NEQ, "a", "something"), lm(MatcherType::EQ, "region", "europe")],
            ),
            expected_series: vec![fs(&["a", "b"]), fs(&["a", "b2"])],
            ..base()
        },
        Tc {
            name: "external labels specified, explicitly requested europe",
            matchers: vec![m(MatchType::NotEqual, "a", "something"), m(MatchType::Equal, "region", "europe")],
            external_labels: fs(&["region", "europe"]),
            expected_query: q(
                1,
                2,
                vec![lm(MatcherType::NEQ, "a", "something"), lm(MatcherType::EQ, "region", "europe")],
            ),
            expected_series: vec![fs(&["a", "b"]), fs(&["a", "b2", "region", "europe"])],
            ..base()
        },
        Tc {
            name: "external labels specified, explicitly requested not europe",
            matchers: vec![m(MatchType::NotEqual, "a", "something"), m(MatchType::Equal, "region", "us")],
            external_labels: fs(&["region", "europe"]),
            expected_query: q(1, 2, vec![lm(MatcherType::NEQ, "a", "something"), lm(MatcherType::EQ, "region", "us")]),
            expected_series: vec![fs(&["a", "b"]), fs(&["a", "b3", "region", "us"])],
            ..base()
        },
        Tc {
            name: "prefer local storage",
            mint: 0,
            maxt: 50,
            callback: cb(100),
            read_recent: false,
            expected_query: q(0, 50, vec![]),
            expected_series: all(),
            ..base()
        },
        Tc {
            name: "prefer local storage, limited time",
            mint: 0,
            maxt: 50,
            callback: cb(20),
            read_recent: false,
            expected_query: q(0, 20, vec![]),
            expected_series: all(),
            ..base()
        },
        Tc {
            name: "prefer local storage, skipped",
            mint: 30,
            maxt: 50,
            callback: cb(20),
            read_recent: false,
            expected_query: None,
            expected_series: vec![], // Noop should be used.
            ..base()
        },
        Tc {
            name: "required matcher specified, user also specifies same",
            matchers: vec![m(MatchType::Equal, "a", "b2")],
            required_matchers: vec![m(MatchType::Equal, "a", "b2")],
            expected_query: q(1, 2, vec![lm(MatcherType::EQ, "a", "b2")]),
            expected_series: vec![fs(&["a", "b2", "region", "europe"])],
            ..base()
        },
        Tc {
            name: "required matcher specified",
            matchers: vec![m(MatchType::Equal, "a", "b2")],
            required_matchers: vec![m(MatchType::Equal, "a", "b2")],
            expected_query: q(1, 2, vec![lm(MatcherType::EQ, "a", "b2")]),
            expected_series: vec![fs(&["a", "b2", "region", "europe"])],
            ..base()
        },
        Tc {
            name: "required matcher specified, given matcher does not match",
            matchers: vec![m(MatchType::NotEqual, "a", "something")],
            required_matchers: vec![m(MatchType::Equal, "a", "b2")],
            expected_query: None,
            expected_series: vec![], // Given matchers do not match the required ones, noop expected.
            ..base()
        },
        Tc {
            name: "required matcher specified, given matcher does not match2",
            matchers: vec![m(MatchType::NotEqual, "x", "something")],
            required_matchers: vec![m(MatchType::Equal, "a", "b2")],
            expected_query: None,
            expected_series: vec![], // Given matchers do not match the required ones, noop expected.
            ..base()
        },
    ];

    for tc in cases {
        mock.reset();
        let c = new_sample_and_chunk_queryable_client(
            mock.clone() as Arc<dyn ReadClient>,
            tc.external_labels,
            tc.required_matchers,
            tc.read_recent,
            tc.callback,
        );
        let mut qr = c.querier(tc.mint, tc.maxt).expect(tc.name);

        let mut ss = qr.select(&Context::background(), true, None, &tc.matchers);
        assert!(ss.warnings().is_empty(), "{}", tc.name);

        assert_eq!(tc.expected_query, *mock.got.lock().unwrap(), "{}", tc.name);

        let mut got = Vec::new();
        while ss.next() {
            got.push(ss.at().labels().clone());
        }
        assert!(ss.err().is_none(), "{}: {:?}", tc.name, ss.err());
        assert_eq!(tc.expected_series, got, "{}", tc.name);
        assert!(qr.close().is_none());
    }

    // search warnings
    {
        let local = new_test_db();
        let mut app = storage::AppendableV2::appender_v2(&*local.db, &Context::background());
        let lbls = labels::from_strings(&["__name__", "up", "job", "prometheus"]);
        let r = app.append(0, &lbls, 0, 1000, 1.0, None, None, &AppendV2Options::default()).unwrap();
        app.append(r, &lbls, 0, 1500, 1.0, None, None, &AppendV2Options::default()).unwrap();
        assert!(app.commit().is_none());

        struct Stc {
            name: &'static str,
            mint: i64,
            maxt: i64,
            read_recent: bool,
            warning: bool,
            empty: bool,
        }
        let cases = [
            Stc { name: "overlapping local history", mint: 0, maxt: 2000, read_recent: false, warning: true, empty: false },
            Stc { name: "before local history", mint: 0, maxt: 999, read_recent: false, warning: true, empty: true },
            Stc { name: "local start boundary", mint: 1000, maxt: 2000, read_recent: false, warning: true, empty: false },
            Stc { name: "recent range excludes remote", mint: 1001, maxt: 2000, read_recent: false, warning: false, empty: false },
            Stc { name: "read recent includes remote", mint: 1001, maxt: 2000, read_recent: true, warning: true, empty: false },
        ];
        for tc in cases {
            let client = Arc::new(MockedRemoteClient::new(vec![]));
            let remote = Storage::for_reads(vec![RemoteQueryable {
                queryable: Arc::new(new_sample_and_chunk_queryable_client(
                    client.clone() as Arc<dyn ReadClient>,
                    Labels::empty(),
                    vec![],
                    tc.read_recent,
                    cb(1000),
                )),
                external_labels: Labels::empty(),
            }]);
            let primary = local.db.querier(tc.mint, tc.maxt).unwrap();
            let secondary = remote.querier(tc.mint, tc.maxt).unwrap();
            let mut q = new_merge_querier(vec![primary], vec![secondary], Arc::new(chained_series_merge));
            {
                let searcher = q.as_searcher().expect("a Searcher");
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
                    assert!(rs.err().is_none(), "{}/{method}", tc.name);
                    if tc.empty {
                        assert!(values.is_empty(), "{}/{method}: {values:?}", tc.name);
                    } else {
                        assert_eq!(expected, values, "{}/{method}", tc.name);
                    }
                    if tc.warning {
                        let warnings = rs.warnings().as_errors();
                        assert_eq!(1, warnings.len(), "{}/{method}", tc.name);
                        assert_eq!(
                            "search is not supported by a secondary storage backend; results may be incomplete",
                            warnings[0].to_string()
                        );
                    } else {
                        assert!(rs.warnings().is_empty(), "{}/{method}", tc.name);
                    }
                    assert!(client.got.lock().unwrap().is_none());
                    assert!(client.got_multiple.lock().unwrap().is_empty());
                    assert!(rs.close().is_none());
                }
            }
            assert!(q.close().is_none());
        }
    }
}
