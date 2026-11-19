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
use storage::{Context, Queryable};

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

    // The "search warnings" subtest needs a TSDB (teststorage.New): waiting for tsdb.
}
