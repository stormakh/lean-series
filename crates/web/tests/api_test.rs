//! Port of `web/api/v1/api_test.go`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::doubles::*;
use common::*;
use model::exemplar::{Exemplar, QueryResult as ExemplarQueryResult};
use model::labels::{Labels, from_strings};
use promql::{PromqlError, QueryOpts};
use storage::{Context, Error};
use web::api::*;
use web::gotime::{SECOND, Time};

fn sample_config() -> Arc<config::Config> {
    Arc::new(config::Config::default())
}

fn base_opts() -> ApiOptions {
    ApiOptions { parser: Some(Arc::new(promql::parser::Parser::new(Default::default()))), ..Default::default() }
}

#[test]
fn test_scrape_pool_config() {
    let api = Api::new(ApiOptions { target_retriever: Some(Arc::new(TestTargetRetriever::default())), ..base_opts() });
    // returns redacted effective config
    let res = api.scrape_pool_config(&ApiRequest::get(&[("scrapePool", "testpool")]));
    assert!(res.err.is_none());
    let Some(Data::Config(y)) = &res.data else { panic!("expected a config, got {:?}", res.data) };
    assert!(y.contains("job_name: testpool"), "{}", y);
    assert!(y.contains("credentials: <secret>"), "{}", y);
    assert!(!y.contains("credential helper secret"), "{}", y);

    // requires scrape pool
    let res = api.scrape_pool_config(&ApiRequest::get(&[]));
    assert_api_error(&res, ERROR_BAD_DATA);
    assert_eq!("no scrapePool parameter provided", res.err.unwrap().err.to_string());

    // reports unknown scrape pool
    let api = Api::new(ApiOptions {
        target_retriever: Some(Arc::new(TestTargetRetriever { scrape_pool_config_err: Some(Error::msg("scrape pool not found")), ..Default::default() })),
        ..base_opts()
    });
    let res = api.scrape_pool_config(&ApiRequest::get(&[("scrapePool", "missing")]));
    assert_api_error(&res, ERROR_BAD_DATA);
    assert_eq!("error retrieving scrape config: scrape pool not found", res.err.unwrap().err.to_string());
}

/// Go `http.NewRequest(method, url?params, nil)` with a form content type: the parameters stay in the URL.
fn request_url(method: &str, params: &[(&str, &str)]) -> ApiRequest {
    ApiRequest::new(method, params, &[])
}

fn sort_labels(v: &mut [Labels]) {
    v.sort_by(|a, b| model::labels::compare(a, b).cmp(&0));
}

#[test]
fn test_get_series() {
    let s = loaded_storage(
        r#"
		load 1m
			test_metric1{foo1="bar", baz="abc"} 0+100x100
			test_metric1{foo2="boo"} 1+0x100
			test_metric2{foo="boo"} 1+0x100
			test_metric2{foo="boo", xyz="qwerty"} 1+0x100
			test_metric2{foo="baz", abc="qwerty"} 1+0x100
	"#,
    );
    let api = Api::new(ApiOptions { queryable: Some(s.db.clone()), ..base_opts() });
    let exec_api = Api::new(ApiOptions { queryable: Some(Arc::new(ErrorTestQueryable::err(Error::msg("generic")))), ..base_opts() });
    let storage_api = Api::new(ApiOptions {
        queryable: Some(Arc::new(ErrorTestQueryable::err(Error::other(promql::ErrStorage(Error::msg("generic")))))),
        ..base_opts()
    });
    let cases: Vec<(&str, &Api, Vec<&str>, Vec<Labels>, ErrorType)> = vec![
        ("no matchers", &api, vec![], vec![], ERROR_BAD_DATA),
        (
            "non empty label matcher",
            &api,
            vec![r#"{foo=~".+"}"#],
            vec![
                from_strings(&["__name__", "test_metric2", "abc", "qwerty", "foo", "baz"]),
                from_strings(&["__name__", "test_metric2", "foo", "boo"]),
                from_strings(&["__name__", "test_metric2", "foo", "boo", "xyz", "qwerty"]),
            ],
            ERROR_NONE,
        ),
        (
            "exact label matcher",
            &api,
            vec![r#"{foo="boo"}"#],
            vec![from_strings(&["__name__", "test_metric2", "foo", "boo"]), from_strings(&["__name__", "test_metric2", "foo", "boo", "xyz", "qwerty"])],
            ERROR_NONE,
        ),
        (
            "two matchers",
            &api,
            vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#],
            vec![
                from_strings(&["__name__", "test_metric2", "abc", "qwerty", "foo", "baz"]),
                from_strings(&["__name__", "test_metric2", "foo", "boo"]),
                from_strings(&["__name__", "test_metric2", "foo", "boo", "xyz", "qwerty"]),
            ],
            ERROR_NONE,
        ),
        ("exec error type", &exec_api, vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#], vec![], ERROR_EXEC),
        ("storage error type", &storage_api, vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#], vec![], ERROR_INTERNAL),
    ];
    for (name, api, matchers, mut expected, err_type) in cases {
        let params: Vec<(&str, &str)> = matchers.iter().map(|m| ("match[]", *m)).collect();
        let res = api.series(&request_url("GET", &params));
        assert_api_error(&res, err_type);
        if err_type == ERROR_NONE {
            let Some(Data::Series(mut r)) = res.data else { panic!("{}: {:?}", name, res.data) };
            sort_labels(&mut expected);
            sort_labels(&mut r);
            assert_eq!(expected, r, "{}", name);
        }
    }
}

#[test]
fn test_query_exemplars() {
    let s = loaded_storage(
        r#"
		load 1m
			test_metric1{foo="bar"} 0+100x100
			test_metric1{foo="boo"} 1+0x100
			test_metric2{foo="boo"} 1+0x100
			test_metric3{foo="bar", dup="1"} 1+0x100
			test_metric3{foo="boo", dup="1"} 1+0x100
			test_metric4{foo="bar", dup="1"} 1+0x100
			test_metric4{foo="boo", dup="1"} 1+0x100
			test_metric4{foo="boo"} 1+0x100
	"#,
    );
    let api = Api::new(ApiOptions {
        queryable: Some(s.db.clone()),
        query_engine: Some(Arc::new(test_engine())),
        exemplar_queryable: Some(s.db.clone()),
        ..base_opts()
    });
    let exec_api = Api::new(ApiOptions { exemplar_queryable: Some(Arc::new(ErrorTestQueryable::err(Error::msg("generic")))), ..base_opts() });
    let storage_api = Api::new(ApiOptions {
        exemplar_queryable: Some(Arc::new(ErrorTestQueryable::err(Error::other(promql::ErrStorage(Error::msg("generic")))))),
        ..base_opts()
    });
    let q = [("query", r#"test_metric3{foo="boo"} - test_metric4{foo="bar"}"#), ("start", "0"), ("end", "4")];
    let ex = vec![
        ExemplarQueryResult {
            series_labels: from_strings(&["__name__", "test_metric3", "foo", "boo", "dup", "1"]),
            exemplars: vec![Exemplar { labels: from_strings(&["id", "abc"]), value: 10.0, ts: 0, has_ts: false }],
        },
        ExemplarQueryResult {
            series_labels: from_strings(&["__name__", "test_metric4", "foo", "bar", "dup", "1"]),
            exemplars: vec![Exemplar { labels: from_strings(&["id", "lul"]), value: 10.0, ts: 3000, has_ts: false }],
        },
    ];
    let cases: Vec<(&str, &Api, Vec<ExemplarQueryResult>, ErrorType)> = vec![
        ("no error", &api, ex, ERROR_NONE),
        ("should return errorExec upon genetic error", &exec_api, vec![], ERROR_EXEC),
        ("should return errorInternal err type is ErrStorage", &storage_api, vec![], ERROR_INTERNAL),
    ];
    for (name, api, exemplars, err_type) in cases {
        append_exemplars(&s.db, &exemplars);
        let res = api.query_exemplars(&ApiRequest::get(&q));
        assert_api_error(&res, err_type);
        if err_type == ERROR_NONE {
            let Some(Data::Exemplars(got)) = res.data else { panic!("{}: {:?}", name, res.data) };
            // Go compares with cmp: has_ts is not part of Go's struct.
            let strip = |v: &[ExemplarQueryResult]| -> Vec<(Labels, Vec<(Labels, f64, i64)>)> {
                v.iter().map(|r| (r.series_labels.clone(), r.exemplars.iter().map(|e| (e.labels.clone(), e.value, e.ts)).collect())).collect()
            };
            assert_eq!(strip(&exemplars), strip(&got), "{}", name);
        }
    }
}

#[test]
fn test_label_names() {
    let s = loaded_storage(
        r#"
		load 1m
			test_metric1{foo1="bar", baz="abc"} 0+100x100
			test_metric1{foo2="boo"} 1+0x100
			test_metric2{foo="boo"} 1+0x100
			test_metric2{foo="boo", xyz="qwerty"} 1+0x100
			test_metric2{foo="baz", abc="qwerty"} 1+0x100
	"#,
    );
    let api = Api::new(ApiOptions { queryable: Some(s.db.clone()), ..base_opts() });
    let exec_api = Api::new(ApiOptions { queryable: Some(Arc::new(ErrorTestQueryable::err(Error::msg("generic")))), ..base_opts() });
    let storage_api = Api::new(ApiOptions {
        queryable: Some(Arc::new(ErrorTestQueryable::err(Error::other(promql::ErrStorage(Error::msg("generic")))))),
        ..base_opts()
    });
    let cases: Vec<(&str, &Api, Vec<&str>, &str, Vec<&str>, ErrorType)> = vec![
        ("no matchers", &api, vec![], "", vec!["__name__", "abc", "baz", "foo", "foo1", "foo2", "xyz"], ERROR_NONE),
        ("non empty label matcher", &api, vec![r#"{foo=~".+"}"#], "", vec!["__name__", "abc", "foo", "xyz"], ERROR_NONE),
        ("non empty label matcher with limit", &api, vec![r#"{foo=~".+"}"#], "2", vec!["__name__", "abc"], ERROR_NONE),
        ("exact label matcher", &api, vec![r#"{foo="boo"}"#], "", vec!["__name__", "foo", "xyz"], ERROR_NONE),
        ("two matchers", &api, vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#], "", vec!["__name__", "abc", "foo", "xyz"], ERROR_NONE),
        ("exec error type", &exec_api, vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#], "", vec![], ERROR_EXEC),
        ("storage error type", &storage_api, vec![r#"{foo="boo"}"#, r#"{foo="baz"}"#], "", vec![], ERROR_INTERNAL),
    ];
    for (name, api, matchers, limit, expected, err_type) in cases {
        for method in ["GET", "POST"] {
            let mut params: Vec<(&str, &str)> = matchers.iter().map(|m| ("match[]", *m)).collect();
            if !limit.is_empty() {
                params.push(("limit", limit));
            }
            let res = api.label_names(&request_url(method, &params));
            assert_api_error(&res, err_type);
            if err_type == ERROR_NONE {
                assert_eq!(Some(Data::Strings(expected.iter().map(|s| s.to_string()).collect())), res.data, "{} {}", name, method);
            }
        }
    }
}

#[test]
fn test_stats() {
    let s = new_test_storage();
    type Check = Box<dyn Fn(&Data)>;
    let qd = |d: &Data| -> QueryData {
        match d {
            Data::Query(q) => q.clone(),
            _ => panic!("expected QueryData, got {:?}", d),
        }
    };
    let builtin_checks = move |per_step: bool| -> Check {
        Box::new(move |d: &Data| {
            let q = qd(d);
            let st = q.stats.expect("stats");
            let qs = st.builtin();
            assert!(qs.timings.eval_total_time >= 0.0);
            let samples = qs.samples.expect("samples");
            if per_step {
                assert!(samples.total_queryable_samples_per_step.is_some());
                assert!(samples.samples_read_per_step.is_some());
            } else {
                assert!(samples.total_queryable_samples_per_step.is_none());
            }
            assert!(samples.samples_read >= 0);
        })
    };
    let cases: Vec<(&str, Option<StatsRenderer>, &str, bool, Check)> = vec![
        ("stats is blank", None, "", false, Box::new(move |d: &Data| assert!(qd(d).stats.is_none()))),
        ("stats is an unsupported value", None, "foo", true, Box::new(move |d: &Data| assert!(qd(d).stats.is_some()))),
        ("stats is an unsupported truthy value", None, "1", true, Box::new(move |d: &Data| assert!(qd(d).stats.is_some()))),
        ("stats is true", None, "true", false, builtin_checks(false)),
        ("stats is all", None, "all", false, builtin_checks(true)),
        (
            "custom handler with known value",
            Some(Arc::new(|_ctx: &Context, _s: Option<&util::stats::Statistics>, p: &str| {
                if p == "known" { Some(QueryStats::Custom(r#"{"custom":"Custom Value"}"#.to_string())) } else { None }
            })),
            "known",
            false,
            Box::new(move |d: &Data| {
                let q = qd(d);
                let st = q.stats.expect("stats");
                let mut j = String::new();
                st.write_json(&mut j);
                assert_eq!(json::Json::parse(r#"{"custom":"Custom Value"}"#).unwrap(), json::Json::parse(&j).unwrap());
            }),
        ),
    ];
    for (name, renderer, param, want_warning, expected) in cases {
        let api = Api::new(ApiOptions {
            queryable: Some(s.db.clone()),
            query_engine: Some(Arc::new(test_engine())),
            now: Some(Arc::new(|| Time::unix(123, 0))),
            stats_renderer: renderer,
            ..base_opts()
        });
        let check_warnings = |res: &ApiFuncResult| {
            if want_warning {
                assert_eq!(1, res.warnings.len(), "{}", name);
                let (w, _) = res.warnings.as_strings("", 0, 0);
                for msg in w {
                    assert!(msg.contains("deprecated"), "{}: {}", name, msg);
                }
            } else {
                assert!(res.warnings.is_empty(), "{}", name);
            }
        };
        for method in ["GET", "POST"] {
            let params = [("stats", param), ("query", "up"), ("start", "0"), ("end", "100"), ("step", "10")];
            let req = request_url(method, &params);
            let res = api.query(&req);
            assert_api_error(&res, ERROR_NONE);
            expected(res.data.as_ref().unwrap());
            check_warnings(&res);
            let res = api.query_range(&req);
            assert_api_error(&res, ERROR_NONE);
            expected(res.data.as_ref().unwrap());
            check_warnings(&res);
        }
    }
}

#[test]
fn test_admin_endpoints() {
    let ok = || Arc::new(FakeDb::default());
    let with_err = || Arc::new(FakeDb { err: Some(Error::msg("some error")), ..Default::default() });
    let not_ready = || Arc::new(FakeDb { err: Some(Error::wrap("wrap", tsdb::db::ERR_NOT_READY)), ..Default::default() });
    #[derive(Clone, Copy)]
    enum Ep {
        Snapshot,
        Clean,
        Delete,
    }
    let cases: Vec<(Arc<FakeDb>, bool, Ep, Vec<(&str, &str)>, ErrorType)> = vec![
        (ok(), false, Ep::Snapshot, vec![], ERROR_UNAVAILABLE),
        (ok(), true, Ep::Snapshot, vec![], ERROR_NONE),
        (ok(), true, Ep::Snapshot, vec![("skip_head", "true")], ERROR_NONE),
        (ok(), true, Ep::Snapshot, vec![("skip_head", "xxx")], ERROR_BAD_DATA),
        (with_err(), true, Ep::Snapshot, vec![], ERROR_INTERNAL),
        (not_ready(), true, Ep::Snapshot, vec![], ERROR_UNAVAILABLE),
        (ok(), false, Ep::Clean, vec![], ERROR_UNAVAILABLE),
        (ok(), true, Ep::Clean, vec![], ERROR_NONE),
        (with_err(), true, Ep::Clean, vec![], ERROR_INTERNAL),
        (not_ready(), true, Ep::Clean, vec![], ERROR_UNAVAILABLE),
        (ok(), false, Ep::Delete, vec![], ERROR_UNAVAILABLE),
        (ok(), true, Ep::Delete, vec![], ERROR_BAD_DATA),
        (ok(), true, Ep::Delete, vec![("match[]", "123")], ERROR_BAD_DATA),
        (ok(), true, Ep::Delete, vec![("match[]", "up"), ("start", "xxx")], ERROR_BAD_DATA),
        (ok(), true, Ep::Delete, vec![("match[]", "up"), ("end", "xxx")], ERROR_BAD_DATA),
        (ok(), true, Ep::Delete, vec![("match[]", "up")], ERROR_NONE),
        (ok(), true, Ep::Delete, vec![("match[]", r#"up{job!="foo"}"#), ("match[]", r#"{job=~"bar.+"}"#), ("match[]", r#"up{instance!~"fred.+"}"#)], ERROR_NONE),
        (with_err(), true, Ep::Delete, vec![("match[]", "up")], ERROR_INTERNAL),
        (not_ready(), true, Ep::Delete, vec![("match[]", "up")], ERROR_UNAVAILABLE),
    ];
    for (db, enable_admin, ep, values, err_type) in cases {
        let dir = temp_dir("admin");
        let api = Api::new(ApiOptions { db: Some(db), db_dir: dir.to_string_lossy().into_owned(), enable_admin, ..base_opts() });
        let req = ApiRequest::get(&values);
        let res = match ep {
            Ep::Snapshot => api.snapshot(&req),
            Ep::Clean => api.clean_tombstones(&req),
            Ep::Delete => api.delete_series(&req),
        };
        let res = set_unavail_status_on_tsdb_not_ready(res);
        assert_api_error(&res, err_type);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Go `testCodec`.
struct TestCodec {
    content_type: MimeType,
    can_encode: bool,
}

impl Codec for TestCodec {
    fn content_type(&self) -> MimeType {
        self.content_type.clone()
    }
    fn can_encode(&self, _resp: &Response) -> bool {
        self.can_encode
    }
    fn encode(&self, _resp: &Response) -> Result<Vec<u8>, String> {
        Ok(format!("response from {} codec", self.content_type).into_bytes())
    }
}

fn get_with_accept(accept: &str) -> http::Request {
    let mut r = http::Request { method: "GET".into(), path: "/".into(), ..Default::default() };
    if !accept.is_empty() {
        r.headers.push(("accept".into(), accept.into()));
    }
    r
}

#[test]
fn test_respond_success() {
    let mut api = Api::new(ApiOptions::default());
    api.clear_codecs();
    api.install_codec(Arc::new(JsonCodec));
    api.install_codec(Arc::new(TestCodec { content_type: MimeType::new("test", "cannot-encode"), can_encode: false }));
    api.install_codec(Arc::new(TestCodec { content_type: MimeType::new("test", "can-encode"), can_encode: true }));
    api.install_codec(Arc::new(TestCodec { content_type: MimeType::new("test", "can-encode-2"), can_encode: true }));
    let cases = [
        ("no Accept header", "", "application/json", r#"{"status":"success","data":"test"}"#),
        ("Accept header with single content type which is suitable", "test/can-encode", "test/can-encode", "response from test/can-encode codec"),
        ("Accept header with single content type which is not available", "test/not-registered", "application/json", r#"{"status":"success","data":"test"}"#),
        (
            "Accept header with single content type which cannot encode the response payload",
            "test/cannot-encode",
            "application/json",
            r#"{"status":"success","data":"test"}"#,
        ),
        (
            "Accept header with multiple content types, all of which are suitable",
            "test/can-encode, test/can-encode-2",
            "test/can-encode",
            "response from test/can-encode codec",
        ),
        (
            "Accept header with multiple content types, only one of which is available",
            "test/not-registered, test/can-encode",
            "test/can-encode",
            "response from test/can-encode codec",
        ),
        (
            "Accept header with multiple content types, only one of which can encode the response payload",
            "test/cannot-encode, test/can-encode",
            "test/can-encode",
            "response from test/can-encode codec",
        ),
        (
            "Accept header with multiple content types, none of which are available",
            "test/not-registered, test/also-not-registered",
            "application/json",
            r#"{"status":"success","data":"test"}"#,
        ),
    ];
    for (name, accept, ct, body) in cases {
        let resp = api.respond(&get_with_accept(accept), Data::String("test".into()), &Default::default(), "");
        assert_eq!(200, resp.status, "{}", name);
        assert_eq!(Some(ct), resp.header("content-type"), "{}", name);
        assert_eq!(body, String::from_utf8_lossy(resp.body_bytes()), "{}", name);
    }
}

#[test]
fn test_respond_success_default_codec_cannot_encode_response() {
    let mut api = Api::new(ApiOptions::default());
    api.clear_codecs();
    api.install_codec(Arc::new(TestCodec { content_type: MimeType::new("application", "default-format"), can_encode: false }));
    let resp = api.respond(&get_with_accept(""), Data::String("test".into()), &Default::default(), "");
    assert_eq!(406, resp.status);
    assert_eq!(Some("application/json"), resp.header("content-type"));
    assert_eq!(
        json::Json::parse(r#"{"status":"error","errorType":"not_acceptable","error":"cannot encode response as application/default-format"}"#).unwrap(),
        json::Json::parse(&String::from_utf8_lossy(resp.body_bytes())).unwrap()
    );
}

#[test]
fn test_serve_tsdb_blocks() {
    let block_meta = tsdb::BlockMeta {
        ulid: tsdb::Ulid::new(),
        min_time: 0,
        max_time: 1000,
        stats: tsdb::BlockStats { num_series: 10, ..Default::default() },
        ..Default::default()
    };
    let api = Api::new(ApiOptions { db: Some(Arc::new(FakeDb { block_metas: vec![block_meta.clone()], ..Default::default() })), ..base_opts() });
    let result = api.serve_tsdb_blocks(&ApiRequest::get(&[]));
    let mut out = String::new();
    result.data.as_ref().unwrap().write_json(&mut out);
    let v = json::Json::parse(&out).unwrap();
    let blocks = v.get("blocks").and_then(|b| b.as_arr()).unwrap();
    assert_eq!(1, blocks.len());
    assert_eq!(block_meta, tsdb::BlockMeta::from_json(&blocks[0]).unwrap());
}

#[test]
fn test_respond_error() {
    let api = Api::new(ApiOptions::default());
    let resp = api.respond_error(&ApiError::msg(ERROR_TIMEOUT, "message"), Some(&Data::String("test".into())));
    assert_eq!(503, resp.status);
    assert_eq!(Some("application/json"), resp.header("content-type"));
    assert_eq!(
        json::Json::parse(r#"{"status": "error", "data": "test", "errorType": "timeout", "error": "message"}"#).unwrap(),
        json::Json::parse(&String::from_utf8_lossy(resp.body_bytes())).unwrap()
    );
}

#[test]
fn test_parse_time_param() {
    let ts = parse_time("1582468023986").unwrap();
    let cases: Vec<(&str, &str, Time, Result<Time, String>)> = vec![
        ("start", "1582468023986", min_time(), Ok(ts)),
        ("end", "", max_time(), Ok(max_time())),
        (
            "foo",
            "baz",
            max_time(),
            Err(format!("invalid time value for '{}': {}", "foo", parse_time("baz").unwrap_err())),
        ),
    ];
    for (name, value, default, result) in cases {
        let req = ApiRequest::get(&[(name, value)]);
        match (parse_time_param(&req, name, default), result) {
            (Ok(t), Ok(want)) => assert_eq!(want, t, "{}", value),
            (Err(e), Err(want)) => assert_eq!(want, e),
            (got, want) => panic!("{}: got {:?}, want {:?}", value, got, want),
        }
    }
}

#[test]
fn test_parse_time() {
    let ts = Time::parse_rfc3339("2015-06-03T13:21:58.555Z").unwrap();
    let cases: Vec<(String, bool, Time)> = vec![
        ("".into(), true, Time::ZERO),
        ("abc".into(), true, Time::ZERO),
        ("30s".into(), true, Time::ZERO),
        ("123".into(), false, Time::unix(123, 0)),
        ("123.123".into(), false, Time::unix(123, 123000000)),
        ("2015-06-03T13:21:58.555Z".into(), false, ts),
        ("2015-06-03T14:21:58.555+01:00".into(), false, ts),
        ("1543578564.705".into(), false, Time::unix(1543578564, 705 * 1_000_000)),
        (min_time().format_rfc3339_nano(), false, min_time()),
        (max_time().format_rfc3339_nano(), false, max_time()),
    ];
    for (input, fail, result) in cases {
        let r = parse_time(&input);
        if !fail {
            let t = r.unwrap_or_else(|e| panic!("Unexpected error for {:?}: {}", input, e));
            assert_eq!(result, t, "Expected time {:?} for input {:?} but got {:?}", result, input, t);
            continue;
        }
        assert!(r.is_err(), "Expected error for {:?} but got none", input);
    }
}

#[test]
fn test_parse_duration() {
    let cases: Vec<(&str, bool, i64)> = vec![
        ("", true, 0),
        ("abc", true, 0),
        ("2015-06-03T13:21:58.555Z", true, 0),
        ("-148966367200.372", true, 0),
        ("148966367200.372", true, 0),
        ("9223372036.854776", true, 0),
        ("NaN", true, 0),
        ("9223372036.85477", false, 9223372036854770688),
        ("-9223372036.854775808", false, i64::MIN),
        ("123", false, 123 * SECOND),
        ("123.333", false, 123 * SECOND + 333 * 1_000_000),
        ("15s", false, 15 * SECOND),
        ("5m", false, 5 * 60 * SECOND),
    ];
    for (input, fail, result) in cases {
        let r = parse_duration(input);
        if !fail {
            let d = r.unwrap_or_else(|e| panic!("Unexpected error for {:?}: {}", input, e));
            assert_eq!(result, d, "Expected duration {} for input {:?} but got {}", result, input, d);
            continue;
        }
        assert!(r.is_err(), "Expected error for {:?} but got none", input);
    }
}

#[test]
fn test_options_method() {
    let api = Arc::new(Api::new(ApiOptions::default()));
    let r = api.router("");
    let resp = r.serve(http::Request { method: "OPTIONS".into(), path: "/any_path".into(), ..Default::default() });
    assert_eq!(204, resp.status);
}

#[test]
fn test_tsdb_status() {
    let cases: Vec<(Vec<(&str, &str)>, ErrorType)> = vec![
        (vec![], ERROR_NONE),
        (vec![("limit", "20")], ERROR_NONE),
        (vec![("limit", "0")], ERROR_BAD_DATA),
        (vec![("limit", "10000")], ERROR_NONE),
        (vec![("limit", "10001")], ERROR_BAD_DATA),
    ];
    for (i, (values, err_type)) in cases.into_iter().enumerate() {
        let api = Api::new(ApiOptions { db: Some(Arc::new(FakeDb::default())), gatherer: Some(web::metrics::default_gatherer()), ..base_opts() });
        let res = api.serve_tsdb_status(&ApiRequest::get(&values));
        assert_api_error(&res, err_type);
        let _ = i;
    }
}

#[test]
fn test_self_metrics() {
    let api = Api::new(ApiOptions { gatherer: Some(web::metrics::default_gatherer()), ..base_opts() });
    let families = |res: &ApiFuncResult| -> Vec<String> {
        match &res.data {
            Some(Data::RawList(v)) => v.clone(),
            d => panic!("expected raw JSON messages, got {:?}", d),
        }
    };

    // returns all metrics without filter
    let res = api.self_metrics(&ApiRequest::get(&[]));
    assert_api_error(&res, ERROR_NONE);
    assert!(!families(&res).is_empty(), "expected at least one metric family from default gatherer");

    // filters metrics by regex
    let res = api.self_metrics(&ApiRequest::get(&[("metric_name_pattern", "go_.*")]));
    assert_api_error(&res, ERROR_NONE);
    let f = families(&res);
    assert!(!f.is_empty());
    for raw in f {
        let v = json::Json::parse(&raw).unwrap();
        let name = v.str_of("name");
        assert!(name.starts_with("go_"), "expected metric name to start with 'go_', got {:?}", name);
    }

    // non-matching pattern returns empty
    let res = api.self_metrics(&ApiRequest::get(&[("metric_name_pattern", "nonexistent_prefix_xyz_")]));
    assert_api_error(&res, ERROR_NONE);
    assert!(families(&res).is_empty());

    // invalid regex returns error
    let res = api.self_metrics(&ApiRequest::get(&[("metric_name_pattern", "[invalid")]));
    assert_api_error(&res, ERROR_BAD_DATA);

    // metric families have expected ProtoJSON structure
    let res = api.self_metrics(&ApiRequest::get(&[]));
    assert_api_error(&res, ERROR_NONE);
    for raw in families(&res) {
        let v = json::Json::parse(&raw).unwrap();
        let name = v.str_of("name");
        assert!(!name.is_empty(), "metric family name should not be empty");
        assert!(!v.str_of("type").is_empty(), "metric family type should not be empty");
        let metrics = v.get("metric").and_then(|m| m.as_arr()).cloned().unwrap_or_default();
        assert!(!metrics.is_empty(), "metric family {:?} should have at least one metric", name);
    }
}

#[test]
fn test_return_api_error() {
    let cases: Vec<(Error, ErrorType)> = vec![
        (Error::other(promql::ErrStorage(Error::msg("storage error"))), ERROR_INTERNAL),
        (Error::wrap("wrapped", Error::other(promql::ErrStorage(Error::msg("storage error")))), ERROR_INTERNAL),
        (PromqlError::QueryTimeout("timeout error".into()).err(), ERROR_TIMEOUT),
        (Error::wrap("wrapped", PromqlError::QueryTimeout("timeout error".into()).err()), ERROR_TIMEOUT),
        (PromqlError::QueryCanceled("canceled error".into()).err(), ERROR_CANCELED),
        (Error::wrap("wrapped", PromqlError::QueryCanceled("canceled error".into()).err()), ERROR_CANCELED),
        (Error::msg("exec error"), ERROR_EXEC),
        (storage::ERR_CANCELED, ERROR_CANCELED),
    ];
    for (ix, (err, expected)) in cases.into_iter().enumerate() {
        let actual = return_api_error(err);
        assert_eq!(expected, actual.typ, "{}", ix);
    }
}

#[test]
fn test_get_global_url() {
    let p = |u: &str| yaml::url::parse(u).unwrap();
    let o = |l: &str, h: &str, s: &str| GlobalUrlOptions { listen_address: l.into(), host: h.into(), scheme: s.into() };
    let cases = vec![
        (p("http://127.0.0.1:9090"), o("127.0.0.1:9090", "127.0.0.1:9090", "http"), p("http://127.0.0.1:9090"), false),
        (p("http://127.0.0.1:9090"), o("127.0.0.1:9090", "prometheus.io", "https"), p("https://prometheus.io"), false),
        (p("http://example.com"), o("127.0.0.1:9090", "prometheus.io", "https"), p("http://example.com"), false),
        (p("http://localhost:8080"), o("127.0.0.1:9090", "prometheus.io", "https"), p("http://prometheus.io:8080"), false),
        (p("http://[::1]:8080"), o("127.0.0.1:9090", "prometheus.io", "https"), p("http://prometheus.io:8080"), false),
        (p("http://localhost"), o("127.0.0.1:9090", "prometheus.io", "https"), p("http://prometheus.io"), false),
        (p("http://localhost:9091"), o("[::1]:9090", "[::1]", "https"), p("http://[::1]:9091"), false),
        (p("http://localhost:9091"), o("[::1]:9090", "[::1]:9090", "https"), p("http://[::1]:9091"), false),
    ];
    for (i, (input, opts, expected, errorful)) in cases.into_iter().enumerate() {
        let out = get_global_url(&input, &opts);
        if errorful {
            assert!(out.is_err(), "Test {}", i);
            continue;
        }
        let out = out.unwrap_or_else(|(_, e)| panic!("Test {}: {}", i, e));
        assert_eq!(expected, out, "Test {}", i);
    }
}

fn opts_eq(a: &QueryOpts, b: &QueryOpts) -> bool {
    a.enable_per_step_stats == b.enable_per_step_stats
        && a.lookback_delta == b.lookback_delta
        && a.use_start_timestamps == b.use_start_timestamps
        && a.max_series == b.max_series
        && a.max_samples_read == b.max_samples_read
        && a.max_query_duration == b.max_query_duration
}

#[test]
fn test_extract_query_opts() {
    let cases: Vec<(&str, Vec<(&str, &str)>, Vec<(&str, &str)>, Result<QueryOpts, &str>)> = vec![
        ("with stats all", vec![("stats", "all")], vec![], Ok(QueryOpts::new(true, Duration::ZERO, None))),
        ("with stats none", vec![("stats", "none")], vec![], Ok(QueryOpts::new(false, Duration::ZERO, None))),
        ("with lookback delta", vec![("stats", "all"), ("lookback_delta", "30s")], vec![], Ok(QueryOpts::new(true, Duration::from_secs(30), None))),
        (
            "with X-Prometheus-Use-Start-Timestamps header true",
            vec![],
            vec![("X-Prometheus-Use-Start-Timestamps", "true")],
            Ok(QueryOpts::new(false, Duration::ZERO, Some(true))),
        ),
        (
            "with X-Prometheus-Use-Start-Timestamps header false",
            vec![],
            vec![("X-Prometheus-Use-Start-Timestamps", "false")],
            Ok(QueryOpts::new(false, Duration::ZERO, Some(false))),
        ),
        (
            "with invalid X-Prometheus-Use-Start-Timestamps header",
            vec![],
            vec![("X-Prometheus-Use-Start-Timestamps", "invalid")],
            Err(r#"error parsing X-Prometheus-Use-Start-Timestamps header: strconv.ParseBool: parsing "invalid": invalid syntax"#),
        ),
        (
            "with invalid lookback delta",
            vec![("lookback_delta", "invalid")],
            vec![],
            Err(r#"error parsing lookback delta duration: cannot parse "invalid" to a valid duration"#),
        ),
    ];
    for enable_query_cost in [false, true] {
        for (name, form, headers, expect) in &cases {
            let mut req = ApiRequest::new("GET", &[], form);
            for (k, v) in headers {
                req = req.with_header(k, v);
            }
            let api = Api::new(ApiOptions { enable_query_cost, ..Default::default() });
            match (api.extract_query_opts(&req), expect) {
                (Ok(got), Ok(want)) => assert!(opts_eq(&got, want), "query-cost={} {}: {:?} != {:?}", enable_query_cost, name, got, want),
                (Err(e), Err(want)) => assert_eq!(*want, e, "query-cost={} {}", enable_query_cost, name),
                (got, want) => panic!("query-cost={} {}: got {:?}, want {:?}", enable_query_cost, name, got, want),
            }
        }
    }
}

/// Go `fakeQuery`: records the contexts of its executions.
struct FakeQuery {
    exec_calls: Arc<std::sync::Mutex<Vec<Context>>>,
}

impl ApiQuery for FakeQuery {
    fn exec(&mut self, ctx: &Context) -> promql::QueryResult {
        self.exec_calls.lock().unwrap().push(ctx.clone());
        promql::QueryResult { value: Some(promql::Value::String(promql::StringValue { t: 0, v: "test".into() })), ..Default::default() }
    }
    fn close(&mut self) {}
    fn stats(&self) -> Option<util::stats::Statistics> {
        None
    }
}

/// Go `fakeEngine`.
struct FakeEngine {
    exec_calls: Arc<std::sync::Mutex<Vec<Context>>>,
}

impl QueryEngine for FakeEngine {
    fn new_instant_query(&self, _: &Context, _: Arc<dyn storage::Queryable>, _: Option<&QueryOpts>, _: &str, _: i64) -> Result<Box<dyn ApiQuery>, Error> {
        Ok(Box::new(FakeQuery { exec_calls: self.exec_calls.clone() }))
    }
    fn new_range_query(&self, _: &Context, _: Arc<dyn storage::Queryable>, _: Option<&QueryOpts>, _: &str, _: i64, _: i64, _: i64) -> Result<Box<dyn ApiQuery>, Error> {
        Ok(Box::new(FakeQuery { exec_calls: self.exec_calls.clone() }))
    }
}

#[test]
fn test_query_timeout() {
    let s = loaded_storage(
        r#"
		load 1m
			test_metric1{foo="bar"} 0+100x100
	"#,
    );
    let now = Time::now();
    for method in ["GET", "POST"] {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = Api::new(ApiOptions {
            queryable: Some(s.db.clone()),
            query_engine: Some(Arc::new(FakeEngine { exec_calls: calls.clone() })),
            exemplar_queryable: Some(s.db.clone()),
            alertmanager_retriever: Some(Arc::new(TestAlertmanagerRetriever)),
            now: Some(Arc::new(move || now)),
            config: Some(Arc::new(sample_config)),
            ..base_opts()
        });
        // Go builds the request with the parameters in the URL for both methods.
        let req = ApiRequest::new(method, &[("query", "2"), ("timeout", "1s")], &[]);
        let res = api.query(&req);
        assert_api_error(&res, ERROR_NONE);
        let calls = calls.lock().unwrap();
        assert_eq!(1, calls.len());
        let deadline = calls[0].deadline().expect("deadline");
        // adapted: Go compares the deadline with now+1s exactly; contexts here keep monotonic instants, so the
        // deadline is the instant at now+1s as seen from the real clock when the query started.
        let want = std::time::Instant::now() + Duration::from_secs(1) - Duration::from_nanos(Time::now().sub(&now).max(0) as u64);
        let diff = if deadline > want { deadline - want } else { want - deadline };
        assert!(diff < Duration::from_millis(50), "deadline off by {:?}", diff);
    }
}

#[test]
fn test_delete_series_endpoint_removed() {
    let api = Arc::new(Api::new(ApiOptions::default()));
    let r = api.router("/api/v1");
    let resp = r.serve(http::Request { method: "DELETE".into(), path: "/api/v1/series".into(), ..Default::default() });
    assert_ne!(500, resp.status, "DELETE /api/v1/series should no longer return 500; the endpoint has been removed");
}

#[test]
fn test_get_rule_group_next_token() {
    let token = get_rule_group_next_token("/path/to/file", "group");
    assert_eq!(64, token.len());
    assert_eq!(token, get_rule_group_next_token("/path/to/file", "group"));
    assert_ne!(token, get_rule_group_next_token("/path/to/file", "other"));
    assert_ne!(token, get_rule_group_next_token("/other/file", "group"));
}
