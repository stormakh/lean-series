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

// ---------------------------------------------------------------------------------------------------------------
// Query cost.

/// The test API of the query-cost tests and the parts tests change.
struct CostApi {
    st: TestDb,
    engine: Arc<promql::Engine>,
    opts: ApiOptions,
}

impl CostApi {
    fn api(&self) -> Api {
        Api::new(self.opts.clone())
    }
}

/// Go `newQueryCostTestAPI`.
fn new_query_cost_test_api(enabled: bool, is_agent: bool) -> CostApi {
    let st = loaded_storage(
        r#"
		load 1m
			test_metric1{foo="bar"} 0+100x100
			test_metric1{foo="boo"} 1+0x100
			test_metric2{foo="boo"} 1+0x100
	"#,
    );
    let now = Time::now();
    let engine = Arc::new(promql::promqltest::new_test_engine_with_opts(promql::EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60 * 1000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats: true,
        enable_query_cost: enabled,
        ..Default::default()
    }));
    let opts = ApiOptions {
        queryable: Some(st.db.clone()),
        cost_sampling: Some(st.db.clone()),
        query_engine: Some(engine.clone()),
        now: Some(Arc::new(move || now)),
        config: Some(Arc::new(sample_config)),
        enable_query_cost: enabled,
        is_agent,
        ..base_opts()
    };
    CostApi { st, engine, opts }
}

/// Go `queryCostConfig`.
fn query_cost_config(max_series: u64, max_samples_read: u64, max_duration: Duration) -> Arc<config::Config> {
    let mut cfg = config::Config::default();
    cfg.global_config.query_max_series = max_series;
    cfg.global_config.query_max_samples_read = max_samples_read;
    cfg.global_config.query_max_duration = yaml::types::Duration(max_duration.as_nanos() as i64);
    Arc::new(cfg)
}

/// Go `queryCostRequest`: parameters in the URL (GET) or as a form body (POST).
fn query_cost_request(method: &str, params: &[(String, String)]) -> ApiRequest {
    let p: Vec<(&str, &str)> = params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    if method == "GET" { ApiRequest::new(method, &p, &[]) } else { ApiRequest::new(method, &[], &p) }
}

/// Go `withParams`: a copy of `base` with the pairs set.
fn with_params(base: &[(String, String)], kv: &[&str]) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = base.to_vec();
    for p in kv.chunks(2) {
        if p.len() < 2 {
            break;
        }
        match out.iter_mut().find(|(k, _)| k == p[0]) {
            Some(e) => e.1 = p[1].to_string(),
            None => out.push((p[0].to_string(), p[1].to_string())),
        }
    }
    out
}

/// Go `jsonFields`: the field names of the JSON object.
fn json_fields(write: impl Fn(&mut String)) -> Vec<String> {
    let mut s = String::new();
    write(&mut s);
    let v = json::Json::parse(&s).unwrap();
    v.as_obj().unwrap().iter().map(|(k, _)| k.clone()).collect()
}

fn elements_match(a: &[&str], b: &[String]) -> bool {
    let mut x: Vec<String> = a.iter().map(|s| s.to_string()).collect();
    let mut y = b.to_vec();
    x.sort();
    y.sort();
    x == y
}

type Handler = fn(&Api, &ApiRequest) -> ApiFuncResult;

/// Go `queryCostEndpoints`.
fn query_cost_endpoints() -> Vec<(&'static str, Handler, Vec<(String, String)>)> {
    let p = |v: &[(&str, &str)]| v.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>();
    let instant = p(&[("query", "test_metric1"), ("time", "120")]);
    let range = p(&[("query", "test_metric1"), ("start", "0"), ("end", "120"), ("step", "60")]);
    vec![
        ("query", Api::query as Handler, instant.clone()),
        ("query_range", Api::query_range as Handler, range.clone()),
        ("query_cost", Api::query_cost as Handler, instant),
        ("query_range_cost", Api::query_range_cost as Handler, range),
    ]
}

fn run(res: &mut ApiFuncResult) {
    if let Some(f) = res.finalizer.take() {
        f();
    }
}

#[test]
fn test_query_cost_estimate_endpoint() {
    let c = new_query_cost_test_api(true, false);
    let api = c.api();
    for (name, h, params) in query_cost_endpoints() {
        if name != "query_cost" && name != "query_range_cost" {
            continue;
        }
        for method in ["GET", "POST"] {
            let res = h(&api, &query_cost_request(method, &params));
            assert!(res.err.is_none(), "{} {}: {:?}", name, method, res.err);
            let Some(Data::QueryCost(data)) = &res.data else { panic!("expected QueryCostData, got {:?}", res.data) };
            assert!(data.estimate.series_touched >= 1);
            assert!(data.estimate.samples_read >= 1);
            assert_eq!(0, data.estimate.peak_samples);
            assert_eq!(vec!["estimate".to_string()], json_fields(|s| res.data.as_ref().unwrap().write_json(s)));
            assert!(elements_match(&["seriesTouched", "samplesRead"], &json_fields(|s| data.estimate.write_json(s))));
        }
    }
}

#[test]
fn test_query_cost_endpoint_parameter_parity() {
    let c = new_query_cost_test_api(true, false);
    let api = c.api();
    let cases: Vec<(&str, Vec<&str>, ErrorType, bool)> = vec![
        ("valid limit", vec!["limit", "10"], ERROR_NONE, false),
        ("invalid limit", vec!["limit", "notanumber"], ERROR_BAD_DATA, false),
        ("negative limit", vec!["limit", "-1"], ERROR_BAD_DATA, false),
        ("valid stats", vec!["stats", "all"], ERROR_NONE, false),
        ("deprecated stats value warns", vec!["stats", "bogus"], ERROR_NONE, true),
        ("valid cost", vec!["cost", "true"], ERROR_NONE, false),
        ("invalid cost", vec!["cost", "maybe"], ERROR_BAD_DATA, false),
        ("invalid max_series", vec!["max_series", "notanumber"], ERROR_BAD_DATA, false),
        ("invalid max_samples_read", vec!["max_samples_read", "-1"], ERROR_BAD_DATA, false),
        ("invalid max_query_duration", vec!["max_query_duration", "notaduration"], ERROR_BAD_DATA, false),
        ("malformed query", vec!["query", "up{"], ERROR_BAD_DATA, false),
        ("zero timeout", vec!["timeout", "0s"], ERROR_BAD_DATA, false),
        ("negative timeout", vec!["timeout", "-1"], ERROR_BAD_DATA, false),
        ("negative max_query_duration", vec!["max_query_duration", "-1"], ERROR_BAD_DATA, false),
    ];
    for (name, params, err_type, warning) in cases {
        for (ep, h, base) in query_cost_endpoints() {
            let mut res = h(&api, &query_cost_request("GET", &with_params(&base, &params)));
            assert_api_error(&res, err_type);
            if warning {
                assert!(!res.warnings.is_empty(), "{} {}", name, ep);
            }
            run(&mut res);
        }
    }
}

#[test]
fn test_query_range_cost_endpoint_validation() {
    let c = new_query_cost_test_api(true, false);
    let api = c.api();
    let cases: Vec<(&str, Vec<(&str, &str)>, ErrorType)> = vec![
        ("zero step", vec![("query", "test_metric1"), ("start", "0"), ("end", "120"), ("step", "0")], ERROR_BAD_DATA),
        ("negative step", vec![("query", "test_metric1"), ("start", "0"), ("end", "120"), ("step", "-1")], ERROR_BAD_DATA),
        ("end before start", vec![("query", "test_metric1"), ("start", "120"), ("end", "0"), ("step", "60")], ERROR_BAD_DATA),
        ("matrix expression", vec![("query", "test_metric1[5m]"), ("start", "0"), ("end", "120"), ("step", "60")], ERROR_BAD_DATA),
        ("string expression", vec![("query", r#""hello""#), ("start", "0"), ("end", "120"), ("step", "60")], ERROR_BAD_DATA),
        ("valid range", vec![("query", "test_metric1"), ("start", "0"), ("end", "120"), ("step", "60")], ERROR_NONE),
    ];
    for (name, q, err_type) in cases {
        let res = api.query_range_cost(&ApiRequest::get(&q));
        assert_api_error(&res, err_type);
        if err_type == ERROR_NONE {
            assert!(matches!(res.data, Some(Data::QueryCost(_))), "{}: {:?}", name, res.data);
        }
    }
}

/// Go `failEstimateQueryable`: fails the chunk probes of the cost estimation.
struct FailEstimateQueryable {
    inner: Arc<tsdb::db::DB>,
}

impl storage::Queryable for FailEstimateQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxQuerier, Error> {
        storage::Queryable::querier(self.inner.as_ref(), mint, maxt)
    }
}

impl storage::ChunkQueryable for FailEstimateQueryable {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxChunkQuerier, Error> {
        let inner = storage::ChunkQueryable::chunk_querier(self.inner.as_ref(), mint, maxt)?;
        Ok(Box::new(FailEstimateChunkQuerier { inner }))
    }
}

impl storage::CostSamplingQueryable for FailEstimateQueryable {
    fn cost_sampling_queryable(&self) -> (Option<&dyn storage::SampleAndChunkQueryable>, bool) {
        (Some(self), true)
    }
}

struct FailEstimateChunkQuerier {
    inner: storage::BoxChunkQuerier,
}

impl storage::LabelQuerier for FailEstimateChunkQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&storage::LabelHints>, ms: &[model::labels::Matcher]) -> storage::LabelResult {
        self.inner.label_values(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&storage::LabelHints>, ms: &[model::labels::Matcher]) -> storage::LabelResult {
        self.inner.label_names(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }
}

impl storage::ChunkQuerier for FailEstimateChunkQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&storage::SelectHints>, _: &[model::labels::Matcher]) -> storage::BoxChunkSeriesSet {
        storage::err_chunk_series_set(Error::msg("injected estimation failure"))
    }
}

#[test]
fn test_query_cost_comparison_gating() {
    let cases: Vec<(&str, bool, &str, bool, ErrorType, bool)> = vec![
        ("present when cost=1 and feature enabled", true, "1", false, ERROR_NONE, true),
        ("absent without cost param", true, "", false, ERROR_NONE, false),
        ("absent when feature disabled even with cost=1", false, "1", false, ERROR_NONE, false),
        ("absent when cost=false", true, "false", false, ERROR_NONE, false),
        ("rejected when cost is not a boolean", true, "maybe", false, ERROR_BAD_DATA, false),
        ("absent with a warning when estimation fails", true, "1", true, ERROR_NONE, false),
    ];
    for (name, enabled, cost, fail_estimate, err_type, want_cost) in cases {
        let mut c = new_query_cost_test_api(enabled, false);
        if fail_estimate {
            let q = Arc::new(FailEstimateQueryable { inner: c.st.db.clone() });
            c.opts.queryable = Some(q.clone());
            c.opts.cost_sampling = Some(q);
        }
        let api = c.api();
        for (ep, h, base) in query_cost_endpoints() {
            if ep != "query" && ep != "query_range" {
                continue;
            }
            let params = if cost.is_empty() { base.clone() } else { with_params(&base, &["cost", cost]) };
            let mut res = h(&api, &query_cost_request("GET", &params));
            assert_api_error(&res, err_type);
            if err_type == ERROR_NONE {
                let Some(Data::Query(data)) = &res.data else { panic!("{} {}: expected QueryData, got {:?}", name, ep, res.data) };
                if want_cost {
                    let cst = data.cost.as_ref().expect("cost");
                    assert!(cst.estimated.series_touched >= 1);
                    assert!(cst.actual.series_touched >= 1);
                } else {
                    assert!(data.cost.is_none(), "{} {}", name, ep);
                }
                if fail_estimate {
                    assert!(!res.warnings.is_empty(), "{} {}", name, ep);
                }
            }
            run(&mut res);
        }
    }
}

#[test]
fn test_query_cost_routes_unavailable_in_agent_mode() {
    // agent mode returns error
    let c = new_query_cost_test_api(true, true);
    let api = Arc::new(c.api());
    let r = api.router("");
    for (path, query) in [("/query_cost", "query=test_metric1&time=120"), ("/query_range_cost", "query=test_metric1&start=0&end=120&step=60")] {
        let resp = r.serve(http::Request { method: "GET".into(), path: path.into(), query: query.into(), ..Default::default() });
        let body = String::from_utf8_lossy(resp.body_bytes()).into_owned();
        assert!(body.contains("unavailable with Prometheus Agent"), "{}", body);
    }

    // routes not registered when feature disabled
    let c = new_query_cost_test_api(false, false);
    let api = Arc::new(c.api());
    let r = api.router("");
    let resp = r.serve(http::Request { method: "GET".into(), path: "/query_cost".into(), query: "query=test_metric1&time=120".into(), ..Default::default() });
    assert_eq!(404, resp.status);
}

#[test]
fn test_query_cost_limit_exceeded() {
    for (name, max_series, max_samples_read) in [("max_series", 1u64, 0u64), ("max_samples_read", 0, 1)] {
        // instant query
        let c = new_query_cost_test_api(true, false);
        c.engine.set_query_limits(max_series, max_samples_read, Duration::ZERO);
        let res = c.api().query(&ApiRequest::get(&[("query", "test_metric1"), ("time", "120")]));
        let e = res.err.as_ref().unwrap_or_else(|| panic!("{}: expected an error", name));
        assert_eq!(ERROR_COST_LIMIT, e.typ, "{}", name);
        assert_eq!(422, default_error_code(e.typ));

        // range query
        let c = new_query_cost_test_api(true, false);
        c.engine.set_query_limits(max_series, max_samples_read, Duration::ZERO);
        let res = c.api().query_range(&ApiRequest::get(&[("query", "test_metric1"), ("start", "0"), ("end", "120"), ("step", "60")]));
        let e = res.err.as_ref().unwrap_or_else(|| panic!("{}: expected an error", name));
        assert_eq!(ERROR_COST_LIMIT, e.typ, "{}", name);
        assert_eq!(422, default_error_code(e.typ));
    }
}

#[test]
fn test_query_cost_comparison_shape() {
    let c = new_query_cost_test_api(true, false);
    let api = c.api();
    for (ep, h, base) in query_cost_endpoints() {
        if ep != "query" && ep != "query_range" {
            continue;
        }
        for method in ["GET", "POST"] {
            let mut res = h(&api, &query_cost_request(method, &with_params(&base, &["cost", "true"])));
            assert!(res.err.is_none(), "{} {}: {:?}", ep, method, res.err);
            let Some(Data::Query(data)) = res.data.clone() else { panic!("expected QueryData") };
            let cost = data.cost.clone().expect("cost");
            assert!(elements_match(&["estimated", "actual"], &json_fields(|s| cost.write_json(s))));
            assert!(elements_match(&["seriesTouched", "samplesRead"], &json_fields(|s| cost.estimated.write_json(s))));
            assert!(elements_match(&["seriesTouched", "samplesRead", "peakSamples"], &json_fields(|s| cost.actual.write_json(s))));
            assert!(cost.actual.peak_samples > 0);
            run(&mut res);

            let mut stats_result = h(&api, &query_cost_request(method, &with_params(&base, &["stats", "true"])));
            assert!(stats_result.err.is_none());
            let Some(Data::Query(stats_data)) = stats_result.data.clone() else { panic!("expected QueryData") };
            assert!(stats_data.cost.is_none());
            let samples = stats_data.stats.as_ref().unwrap().builtin().samples.unwrap();
            assert_eq!(cost.actual.series_touched, samples.series_touched);
            assert_eq!(cost.actual.samples_read, samples.samples_read);
            assert!(samples.series_touched > 0);
            assert!(json_fields(|s| samples.write_json(s)).contains(&"seriesTouched".to_string()));
            run(&mut stats_result);
        }
    }
}

#[test]
fn test_query_cost_override_above_ceiling_rejected() {
    const SERIES_CEILING: u64 = 10;
    const SAMPLES_CEILING: u64 = 100;
    let duration_ceiling = Duration::from_secs(30);
    let new_api = || {
        let mut c = new_query_cost_test_api(true, false);
        let cfg = query_cost_config(SERIES_CEILING, SAMPLES_CEILING, duration_ceiling);
        c.opts.config = Some(Arc::new(move || cfg.clone()));
        c
    };
    for (limit, above, at_ceiling) in [("max_series", "11", "10"), ("max_samples_read", "101", "100"), ("max_query_duration", "31s", "30s")] {
        let c = new_api();
        let api = c.api();
        for (ep, h, base) in query_cost_endpoints() {
            let mut res = h(&api, &query_cost_request("GET", &with_params(&base, &[limit, above])));
            let e = res.err.as_ref().unwrap_or_else(|| panic!("{} {}: expected an error", limit, ep));
            assert_eq!(ERROR_BAD_DATA, e.typ, "{} {}", limit, ep);
            assert_eq!(400, default_error_code(e.typ));
            assert!(e.err.to_string().contains(limit), "{}", e.err);
            run(&mut res);

            let mut res = h(&api, &query_cost_request("GET", &with_params(&base, &[limit, at_ceiling])));
            assert!(res.err.is_none(), "{} {}: {:?}", limit, ep, res.err);
            run(&mut res);
        }
    }

    // below the ceiling the override applies
    let c = new_api();
    let api = c.api();
    let req = ApiRequest::get(&[("max_series", "5"), ("max_samples_read", "50"), ("max_query_duration", "10s")]);
    let opts = api.extract_query_opts(&req).unwrap();
    assert_eq!(5, opts.max_series);
    assert_eq!(50, opts.max_samples_read);
    assert_eq!(Duration::from_secs(10), opts.max_query_duration);

    // below the ceiling the override is enforced
    let c = new_api();
    c.engine.set_query_limits(SERIES_CEILING, SAMPLES_CEILING, Duration::ZERO);
    let api = c.api();
    for (ep, h, base) in query_cost_endpoints() {
        if ep != "query" && ep != "query_range" {
            continue;
        }
        let res = h(&api, &query_cost_request("GET", &with_params(&base, &["max_series", "1"])));
        let e = res.err.as_ref().unwrap_or_else(|| panic!("{}: expected an error", ep));
        assert_eq!(ERROR_COST_LIMIT, e.typ, "{}", ep);
    }
}

#[test]
fn test_query_cost_disabled_ignores_ceilings() {
    let mut c = new_query_cost_test_api(false, false);
    let cfg = query_cost_config(10, 100, Duration::from_secs(30));
    c.opts.config = Some(Arc::new(move || cfg.clone()));
    let api = c.api();
    for (name, params) in [
        ("above the ceiling", [("max_series", "1000000"), ("max_samples_read", "1000000"), ("max_query_duration", "1h")]),
        ("malformed", [("max_series", "notanumber"), ("max_samples_read", "notanumber"), ("max_query_duration", "notaduration")]),
    ] {
        let opts = api.extract_query_opts(&ApiRequest::get(&params)).unwrap_or_else(|e| panic!("{}: {}", name, e));
        assert_eq!(0, opts.max_series, "{}", name);
        assert_eq!(0, opts.max_samples_read, "{}", name);
        assert_eq!(Duration::ZERO, opts.max_query_duration, "{}", name);
    }
}

#[test]
fn test_query_cost_construction_error_mapped() {
    // limit above the ceiling is bad data
    let c = new_query_cost_test_api(true, false);
    c.engine.set_query_limits(10, 0, Duration::ZERO);
    let api = c.api();
    for (ep, h, base) in query_cost_endpoints() {
        if ep != "query" && ep != "query_range" {
            continue;
        }
        let res = h(&api, &query_cost_request("GET", &with_params(&base, &["max_series", "11"])));
        let e = res.err.as_ref().unwrap_or_else(|| panic!("{}: expected an error", ep));
        assert_eq!(ERROR_BAD_DATA, e.typ);
        assert!(e.err.to_string().contains("max_series"));
        assert!(!e.err.to_string().contains(r#"invalid parameter "query""#));
    }

    // waiting for a query slot
    let dir = temp_dir("tracker");
    let tracker = Arc::new(promql::query_logger::ActiveQueryTracker::new(&dir, 1).unwrap());
    let mut c = new_query_cost_test_api(true, false);
    let eng = Arc::new(promql::promqltest::new_test_engine_with_opts(promql::EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_millis(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60 * 1000)),
        active_query_tracker: Some(tracker),
        enable_query_cost: true,
        ..Default::default()
    }));
    c.opts.query_engine = Some(eng.clone());
    let api = c.api();
    let held = eng.acquire_query_slot(&Context::background(), "held").unwrap();
    let (canceled, cancel) = Context::background().with_cancel();
    cancel.cancel();
    for (name, ctx, want) in [("deadline exceeded", Context::background(), ERROR_TIMEOUT), ("request canceled", canceled, ERROR_CANCELED)] {
        for (ep, h, base) in query_cost_endpoints() {
            if ep != "query" && ep != "query_range" {
                continue;
            }
            let req = query_cost_request("GET", &base).with_ctx(ctx.clone());
            let res = h(&api, &req);
            let e = res.err.as_ref().unwrap_or_else(|| panic!("{} {}: expected an error", name, ep));
            assert_eq!(want, e.typ, "{} {}: {}", name, ep, e.err);
            assert!(!e.err.to_string().contains(r#"invalid parameter "query""#));
        }
    }
    drop(held);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go `estimationSlotEngine`: tracks the slot reserved for estimation.
#[derive(Default)]
struct SlotState {
    active: bool,
    acquired: usize,
    released: usize,
    wait: bool,
    executed: bool,
    acquired_after_exec: bool,
    built: bool,
    built_at: Option<std::time::Instant>,
    acquired_before_build: bool,
}

struct EstimationSlotEngine {
    inner: Arc<promql::Engine>,
    st: Arc<std::sync::Mutex<SlotState>>,
}

struct ExecTrackingQuery {
    inner: Box<dyn ApiQuery>,
    st: Arc<std::sync::Mutex<SlotState>>,
}

impl ApiQuery for ExecTrackingQuery {
    fn exec(&mut self, ctx: &Context) -> promql::QueryResult {
        self.st.lock().unwrap().executed = true;
        self.inner.exec(ctx)
    }
    fn close(&mut self) {
        self.inner.close()
    }
    fn stats(&self) -> Option<util::stats::Statistics> {
        self.inner.stats()
    }
}

struct SlotRelease(Arc<std::sync::Mutex<SlotState>>);

impl Drop for SlotRelease {
    fn drop(&mut self) {
        let mut s = self.0.lock().unwrap();
        s.active = false;
        s.released += 1;
    }
}

impl QueryEngine for EstimationSlotEngine {
    fn new_instant_query(&self, ctx: &Context, q: Arc<dyn storage::Queryable>, opts: Option<&QueryOpts>, qs: &str, ts: i64) -> Result<Box<dyn ApiQuery>, Error> {
        let qry = QueryEngine::new_instant_query(self.inner.as_ref(), ctx, q, opts, qs, ts)?;
        {
            let mut s = self.st.lock().unwrap();
            s.built = true;
            s.built_at = Some(std::time::Instant::now());
        }
        Ok(Box::new(ExecTrackingQuery { inner: qry, st: self.st.clone() }))
    }
    fn new_range_query(&self, ctx: &Context, q: Arc<dyn storage::Queryable>, opts: Option<&QueryOpts>, qs: &str, start: i64, end: i64, step: i64) -> Result<Box<dyn ApiQuery>, Error> {
        let qry = QueryEngine::new_range_query(self.inner.as_ref(), ctx, q, opts, qs, start, end, step)?;
        {
            let mut s = self.st.lock().unwrap();
            s.built = true;
            s.built_at = Some(std::time::Instant::now());
        }
        Ok(Box::new(ExecTrackingQuery { inner: qry, st: self.st.clone() }))
    }
    fn acquire_query_slot(&self, ctx: &Context, _qs: &str) -> Option<Result<QuerySlotGuard, Error>> {
        let wait = {
            let mut s = self.st.lock().unwrap();
            s.acquired += 1;
            s.acquired_after_exec = s.acquired_after_exec || s.executed;
            s.acquired_before_build = s.acquired_before_build || !s.built;
            s.wait
        };
        let release = SlotRelease(self.st.clone());
        if wait {
            while !ctx.is_done() {
                std::thread::sleep(Duration::from_millis(1));
            }
            let e = ctx.err().unwrap();
            drop(release);
            return Some(Err(e));
        }
        self.st.lock().unwrap().active = true;
        Some(Ok(Box::new(release)))
    }
}

/// Go `estimationBoundsQueryable`: checks the slot and the deadline of the estimation probes.
struct EstimationBoundsQueryable {
    inner: Arc<tsdb::db::DB>,
    st: Arc<std::sync::Mutex<SlotState>>,
    duration: Duration,
    observed: Arc<std::sync::atomic::AtomicBool>,
}

impl storage::Queryable for EstimationBoundsQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxQuerier, Error> {
        storage::Queryable::querier(self.inner.as_ref(), mint, maxt)
    }
}

impl storage::ChunkQueryable for EstimationBoundsQueryable {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<storage::BoxChunkQuerier, Error> {
        let inner = storage::ChunkQueryable::chunk_querier(self.inner.as_ref(), mint, maxt)?;
        Ok(Box::new(EstimationBoundsQuerier { inner, st: self.st.clone(), duration: self.duration, observed: self.observed.clone() }))
    }
}

impl storage::CostSamplingQueryable for EstimationBoundsQueryable {
    fn cost_sampling_queryable(&self) -> (Option<&dyn storage::SampleAndChunkQueryable>, bool) {
        (Some(self), true)
    }
}

struct EstimationBoundsQuerier {
    inner: storage::BoxChunkQuerier,
    st: Arc<std::sync::Mutex<SlotState>>,
    duration: Duration,
    observed: Arc<std::sync::atomic::AtomicBool>,
}

impl storage::LabelQuerier for EstimationBoundsQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&storage::LabelHints>, ms: &[model::labels::Matcher]) -> storage::LabelResult {
        self.inner.label_values(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&storage::LabelHints>, ms: &[model::labels::Matcher]) -> storage::LabelResult {
        self.inner.label_names(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }
}

impl storage::ChunkQuerier for EstimationBoundsQuerier {
    fn select(&self, ctx: &Context, sorted: bool, hints: Option<&storage::SelectHints>, ms: &[model::labels::Matcher]) -> storage::BoxChunkSeriesSet {
        self.observed.store(true, std::sync::atomic::Ordering::Relaxed);
        let s = self.st.lock().unwrap();
        assert!(s.active, "estimation must hold a concurrency slot");
        let deadline = ctx.deadline().expect("estimation must have a deadline");
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(remaining <= self.duration, "estimation must honor the shortest duration limit");
        assert!(remaining > self.duration / 2, "estimation must not apply a shorter duration limit");
        if s.built {
            assert!(deadline <= s.built_at.unwrap() + self.duration, "estimation must not outlast the query's duration limit");
        }
        drop(s);
        self.inner.select(ctx, sorted, hints, ms)
    }
}

#[test]
fn test_query_cost_estimation_bounds() {
    let cases: Vec<(&str, Duration, Vec<&str>, Duration)> = vec![
        ("flag fallback", Duration::ZERO, vec![], Duration::from_secs(60)),
        ("configured ceiling", Duration::from_secs(30), vec![], Duration::from_secs(30)),
        ("query duration", Duration::from_secs(30), vec!["max_query_duration", "1s"], Duration::from_secs(1)),
        ("request timeout", Duration::from_secs(30), vec!["timeout", "1s"], Duration::from_secs(1)),
        ("request timeout above the configured ceiling", Duration::from_secs(30), vec!["timeout", "1m"], Duration::from_secs(30)),
        ("request timeout above the flag", Duration::ZERO, vec!["timeout", "5m"], Duration::from_secs(60)),
    ];
    for (name, ceiling, params, duration) in cases {
        let mut c = new_query_cost_test_api(true, false);
        let cfg = query_cost_config(0, 0, ceiling);
        c.opts.config = Some(Arc::new(move || cfg.clone()));
        c.opts.query_timeout_flag = 60 * SECOND;
        let st = Arc::new(std::sync::Mutex::new(SlotState::default()));
        c.opts.query_engine = Some(Arc::new(EstimationSlotEngine { inner: c.engine.clone(), st: st.clone() }));
        let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let q = Arc::new(EstimationBoundsQueryable { inner: c.st.db.clone(), st: st.clone(), duration, observed: observed.clone() });
        c.opts.queryable = Some(q.clone());
        c.opts.cost_sampling = Some(q);
        let api = c.api();
        for (ep, h, base) in query_cost_endpoints() {
            observed.store(false, std::sync::atomic::Ordering::Relaxed);
            {
                let mut s = st.lock().unwrap();
                s.executed = false;
                s.acquired_after_exec = false;
                s.built = false;
                s.acquired_before_build = false;
            }
            let p = with_params(&with_params(&base, &["cost", "true"]), &params);
            let mut res = h(&api, &query_cost_request("GET", &p));
            assert!(res.err.is_none(), "{} {}: {:?}", name, ep, res.err);
            assert!(observed.load(std::sync::atomic::Ordering::Relaxed), "{} {}", name, ep);
            {
                let s = st.lock().unwrap();
                assert!(!s.active);
                assert_eq!(s.acquired, s.released);
                assert!(!s.acquired_after_exec, "estimation must run before the query executes");
                if ep == "query" || ep == "query_range" {
                    assert!(!s.acquired_before_build, "the query must be built before it is estimated");
                }
            }
            run(&mut res);
        }
    }

    // malformed query is rejected before estimating
    let mut c = new_query_cost_test_api(true, false);
    let st = Arc::new(std::sync::Mutex::new(SlotState::default()));
    c.opts.query_engine = Some(Arc::new(EstimationSlotEngine { inner: c.engine.clone(), st: st.clone() }));
    let api = c.api();
    for (ep, h, base) in query_cost_endpoints() {
        if ep != "query" && ep != "query_range" {
            continue;
        }
        st.lock().unwrap().acquired = 0;
        let res = h(&api, &query_cost_request("GET", &with_params(&base, &["cost", "true", "query", "sum("])));
        assert_api_error(&res, ERROR_BAD_DATA);
        assert_eq!(0, st.lock().unwrap().acquired, "a malformed query must not wait for an estimation slot");
    }
}

#[test]
fn test_query_cost_estimation_queue_timeout() {
    let (canceled, cancel) = Context::background().with_cancel();
    cancel.cancel();
    let cases: Vec<(&str, Duration, Vec<&str>, Context, ErrorType, ErrorType)> = vec![
        ("duration limit", Duration::from_millis(10), vec![], Context::background(), ERROR_NONE, ERROR_TIMEOUT),
        ("request timeout", Duration::ZERO, vec!["timeout", "50ms"], Context::background(), ERROR_TIMEOUT, ERROR_TIMEOUT),
        ("request canceled", Duration::ZERO, vec![], canceled, ERROR_CANCELED, ERROR_CANCELED),
    ];
    for (name, ceiling, params, ctx, query_err, cost_err) in cases {
        let mut c = new_query_cost_test_api(true, false);
        let cfg = query_cost_config(0, 0, ceiling);
        c.opts.config = Some(Arc::new(move || cfg.clone()));
        let st = Arc::new(std::sync::Mutex::new(SlotState { wait: true, ..Default::default() }));
        c.opts.query_engine = Some(Arc::new(EstimationSlotEngine { inner: c.engine.clone(), st: st.clone() }));
        let api = c.api();
        for (ep, h, base) in query_cost_endpoints() {
            let p = with_params(&with_params(&base, &["cost", "true"]), &params);
            let mut res = h(&api, &query_cost_request("GET", &p).with_ctx(ctx.clone()));
            let want = if ep == "query" || ep == "query_range" { query_err } else { cost_err };
            assert_api_error(&res, want);
            if want == ERROR_NONE {
                let Some(Data::Query(d)) = &res.data else { panic!("{} {}: expected QueryData", name, ep) };
                assert!(d.cost.is_none());
                assert!(!res.warnings.is_empty());
            }
            {
                let s = st.lock().unwrap();
                assert_eq!(s.acquired, s.released, "{} {}", name, ep);
            }
            run(&mut res);
        }
    }
}

// ---- TestEndpoints ----

/// One case of Go `testEndpoints`.
struct EpCase {
    endpoint: &'static str,
    params: Vec<(&'static str, &'static str)>,
    query: Vec<(&'static str, String)>,
    response: Option<Data>,
    /// If nonzero, check only the length; `response` is ignored.
    response_len: usize,
    response_metadata_total: usize,
    response_as_json: &'static str,
    warnings_count: usize,
    err_type: ErrorType,
    sorter: Option<fn(&mut Data)>,
    metadata: Vec<(&'static str, Vec<scrape::MetricMetadata>)>,
    zero_func: Option<fn(&mut Data)>,
}

impl Default for EpCase {
    fn default() -> EpCase {
        EpCase {
            endpoint: "",
            params: vec![],
            query: vec![],
            response: None,
            response_len: 0,
            response_metadata_total: 0,
            response_as_json: "",
            warnings_count: 0,
            err_type: ERROR_NONE,
            sorter: None,
            metadata: vec![],
            zero_func: None,
        }
    }
}

fn qv(pairs: &[(&'static str, &str)]) -> Vec<(&'static str, String)> {
    pairs.iter().map(|(k, v)| (*k, v.to_string())).collect()
}

fn ep_func(name: &str) -> ApiFunc {
    match name {
        "query" => Api::query,
        "query_range" => Api::query_range,
        "format_query" => Api::format_query,
        "series" => Api::series,
        "targets" => Api::targets,
        "target_relabel_steps" => Api::target_relabel_steps,
        "target_metadata" => Api::target_metadata,
        "alertmanagers" => Api::alertmanagers,
        "metric_metadata" => Api::metric_metadata,
        "serve_config" => Api::serve_config,
        "serve_flags" => Api::serve_flags,
        "alerts" => Api::alerts,
        "rules" => Api::rules,
        "query_exemplars" => Api::query_exemplars,
        "label_values" => Api::label_values,
        "label_names" => Api::label_names,
        _ => panic!("unknown endpoint {}", name),
    }
}

fn scalar_data(v: f64, t: i64) -> Data {
    Data::Query(QueryData {
        result_type: promql::parser::ValueType::Scalar,
        result: promql::Value::Scalar(promql::Scalar { t, v }),
        stats: None,
        cost: None,
    })
}

fn matrix_data(series: Vec<(Labels, Vec<(f64, i64)>)>) -> Data {
    Data::Query(QueryData {
        result_type: promql::parser::ValueType::Matrix,
        result: promql::Value::Matrix(
            series
                .into_iter()
                .map(|(metric, pts)| promql::Series {
                    metric,
                    floats: pts.into_iter().map(|(f, t)| promql::FPoint { t, f }).collect(),
                    ..Default::default()
                })
                .collect(),
        ),
        stats: None,
        cost: None,
    })
}

fn strings(v: &[&str]) -> Data {
    Data::Strings(v.iter().map(|s| s.to_string()).collect())
}

fn series_data(v: Vec<Labels>) -> Data {
    Data::Series(v)
}

fn lmap(pairs: &[(&str, &str)]) -> Labels {
    model::labels::from_map(pairs.iter().copied())
}

fn mm(family: &str, typ: model::metadata::MetricType, help: &str) -> scrape::MetricMetadata {
    scrape::MetricMetadata { metric_family: family.to_string(), typ, help: help.to_string(), unit: String::new() }
}

fn md(typ: &str, help: &str) -> Metadata {
    Metadata { typ: typ.to_string(), help: help.to_string(), unit: String::new() }
}

fn metadata_data(v: Vec<(&str, Vec<Metadata>)>) -> Data {
    Data::Metadata(v.into_iter().map(|(k, m)| (k.to_string(), m)).collect())
}

fn tmd(target: Labels, family: &str, help: &str) -> MetricMetadataItem {
    MetricMetadataItem { target, metric_family: family.to_string(), typ: "gauge".to_string(), help: help.to_string(), unit: String::new() }
}

/// The two active targets of `setup_test_target_retriever`, as the API reports them.
fn expected_active_targets() -> Vec<Target> {
    let last_scrape = Time::from_system(scrape_start());
    vec![
        Target {
            discovered_labels: from_strings(&[
                "__always_scrape_classic_histograms__",
                "false",
                "__convert_classic_histograms_to_nhcb__",
                "false",
                "__param_target",
                "example.com",
                "__scrape_interval__",
                "0s",
                "__scrape_native_histograms__",
                "false",
                "__scrape_timeout__",
                "0s",
            ]),
            labels: from_strings(&["job", "blackbox"]),
            scrape_pool: "blackbox".to_string(),
            scrape_url: "http://localhost:9115/probe?target=example.com".to_string(),
            global_url: "http://localhost:9115/probe?target=example.com".to_string(),
            health: "down".to_string(),
            last_error: "failed: missing port in address".to_string(),
            last_scrape,
            last_scrape_duration: 0.1,
            scrape_interval: "20s".to_string(),
            scrape_timeout: "10s".to_string(),
        },
        Target {
            discovered_labels: from_strings(&[
                "__always_scrape_classic_histograms__",
                "false",
                "__convert_classic_histograms_to_nhcb__",
                "false",
                "__scrape_interval__",
                "0s",
                "__scrape_native_histograms__",
                "false",
                "__scrape_timeout__",
                "0s",
            ]),
            labels: from_strings(&["job", "test"]),
            scrape_pool: "test".to_string(),
            scrape_url: "http://example.com:8080/metrics".to_string(),
            global_url: "http://example.com:8080/metrics".to_string(),
            health: "up".to_string(),
            last_error: String::new(),
            last_scrape,
            last_scrape_duration: 0.07,
            scrape_interval: "15s".to_string(),
            scrape_timeout: "5s".to_string(),
        },
    ]
}

fn expected_dropped_targets() -> Vec<DroppedTarget> {
    vec![DroppedTarget {
        discovered_labels: from_strings(&[
            "__address__",
            "http://dropped.example.com:9115",
            "__always_scrape_classic_histograms__",
            "false",
            "__convert_classic_histograms_to_nhcb__",
            "false",
            "__metrics_path__",
            "/probe",
            "__scheme__",
            "http",
            "job",
            "blackbox",
            "__scrape_interval__",
            "30s",
            "__scrape_native_histograms__",
            "false",
            "__scrape_timeout__",
            "15s",
        ]),
        scrape_pool: "blackbox".to_string(),
    }]
}

fn dropped_counts() -> Option<std::collections::BTreeMap<String, usize>> {
    Some(std::collections::BTreeMap::from([("blackbox".to_string(), 1)]))
}

fn relabel_step(rule: model::relabel::Config, output: Labels, keep: bool) -> RelabelStep {
    RelabelStep { rule_json: rule.to_json().stringify(), output, keep }
}

fn tm5_alert() -> Alert {
    Alert {
        labels: from_strings(&["alertname", "test_metric5", "name", "tm5"]),
        annotations: Labels::empty(),
        state: "pending".to_string(),
        active_at: None,
        keep_firing_since: None,
        value: "1e+00".to_string(),
    }
}

/// An expected `AlertingRule` after `rulesZeroFunc` (duration 1s, no annotations, health "ok").
fn arule(state: &str, name: &str, query: &str, labels: Labels, alerts: Option<Vec<Alert>>) -> Rule {
    Rule::Alerting(AlertingRule {
        state: state.to_string(),
        name: name.to_string(),
        query: query.to_string(),
        duration: 1.0,
        keep_firing_for: 0.0,
        labels,
        annotations: Labels::empty(),
        alerts,
        health: "ok".to_string(),
        last_error: String::new(),
        evaluation_time: 0.0,
        last_evaluation: Time::ZERO,
    })
}

fn rrule(name: &str, labels: Labels) -> Rule {
    Rule::Recording(RecordingRule {
        name: name.to_string(),
        query: "vector(1)".to_string(),
        labels,
        health: "ok".to_string(),
        last_error: String::new(),
        evaluation_time: 0.0,
        last_evaluation: Time::ZERO,
    })
}

fn rgroup(name: &str, rules: Vec<Rule>) -> RuleGroup {
    RuleGroup {
        name: name.to_string(),
        file: "/path/to/file".to_string(),
        rules,
        interval: 1.0,
        limit: 0,
        evaluation_time: 0.0,
        last_evaluation: Time::ZERO,
    }
}

fn rules_data(groups: Vec<RuleGroup>, next: &str) -> Data {
    Data::Rules(RuleDiscovery { rule_groups: groups, group_next_token: next.to_string() })
}

// The rules of group "grp" as the API reports them; `alerts(true)` gives `[]` lists, `alerts(false)` gives nil.
fn r_tm3(with_alerts: bool) -> Rule {
    arule("inactive", "test_metric3", "absent(test_metric3) != 1", Labels::empty(), with_alerts.then(Vec::new))
}
fn r_tm4(with_alerts: bool) -> Rule {
    arule("inactive", "test_metric4", "up == 1", Labels::empty(), with_alerts.then(Vec::new))
}
fn r_tm5(with_alerts: bool) -> Rule {
    arule("pending", "test_metric5", "vector(1)", from_strings(&["name", "tm5"]), with_alerts.then(|| vec![tm5_alert()]))
}
fn r_tm6(with_alerts: bool) -> Rule {
    arule("inactive", "test_metric6", "up == 1", from_strings(&["testlabel", "rule"]), with_alerts.then(Vec::new))
}
fn r_tm7(with_alerts: bool) -> Rule {
    arule("inactive", "test_metric7", "up == 1", from_strings(&["templatedlabel", "{{ $externalURL }}"]), with_alerts.then(Vec::new))
}
fn r_rec1() -> Rule {
    rrule("recording-rule-1", Labels::empty())
}
fn r_rec2() -> Rule {
    rrule("recording-rule-2", from_strings(&["testlabel", "rule"]))
}

/// Go `rulesZeroFunc`.
fn rules_zero_func(d: &mut Data) {
    if let Data::Rules(v) = d {
        for g in &mut v.rule_groups {
            g.evaluation_time = 0.0;
            g.last_evaluation = Time::ZERO;
            for rule in &mut g.rules {
                match rule {
                    Rule::Alerting(r) => {
                        r.last_evaluation = Time::ZERO;
                        r.evaluation_time = 0.0;
                        r.last_error = String::new();
                        r.health = "ok".to_string();
                        if let Some(alerts) = &mut r.alerts {
                            for a in alerts {
                                a.active_at = None;
                            }
                        }
                    }
                    Rule::Recording(r) => {
                        r.last_evaluation = Time::ZERO;
                        r.evaluation_time = 0.0;
                        r.last_error = String::new();
                        r.health = "ok".to_string();
                    }
                }
            }
        }
    }
}

fn alerts_zero_func(d: &mut Data) {
    if let Data::Alerts(v) = d {
        for a in v {
            a.active_at = None;
        }
    }
}

fn sort_target_metadata_by_family(d: &mut Data) {
    if let Data::TargetMetadata(v) = d {
        v.sort_by(|a, b| a.metric_family.cmp(&b.metric_family));
    }
}

fn sort_go_threads_by_help(d: &mut Data) {
    if let Data::Metadata(m) = d
        && let Some(v) = m.get_mut("go_threads")
    {
        v.sort_by(|a, b| a.help.cmp(&b.help));
    }
}

/// Go `reflect.ValueOf(got).Len()`.
fn data_len(d: &Option<Data>) -> usize {
    match d {
        Some(Data::Strings(v)) => v.len(),
        Some(Data::Series(v)) => v.len(),
        Some(Data::Metadata(m)) => m.len(),
        Some(Data::TargetMetadata(v)) => v.len(),
        Some(Data::Exemplars(v)) => v.len(),
        other => panic!("no length for {:?}", other),
    }
}

/// Order-insensitive JSON for `require.JSONEq` (objects compare as maps, numbers as float64).
fn json_norm(j: json::Json) -> json::Json {
    match j {
        json::Json::Obj(v) => {
            let mut v: Vec<(String, json::Json)> = v.into_iter().map(|(k, x)| (k, json_norm(x))).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            json::Json::Obj(v)
        }
        json::Json::Arr(v) => json::Json::Arr(v.into_iter().map(json_norm).collect()),
        json::Json::Int(i) => json::Json::Float(i as f64),
        o => o,
    }
}

#[test]
fn test_endpoints() {
    let s = loaded_storage(
        "
		load 1m
			test_metric1{foo=\"bar\"} 0+100x100
			test_metric1{foo=\"boo\"} 1+0x100
			test_metric2{foo=\"boo\"} 1+0x100
			test_metric3{foo=\"bar\", dup=\"1\"} 1+0x100
			test_metric3{foo=\"boo\", dup=\"1\"} 1+0x100
			test_metric4{foo=\"bar\", dup=\"1\"} 1+0x100
			test_metric4{foo=\"boo\", dup=\"1\"} 1+0x100
			test_metric4{foo=\"boo\"} 1+0x100
			test_metric5{\"host.name\"=\"localhost\"} 1+0x100
			test_metric5{\"junk\\n{},=:  chars\"=\"bar\"} 1+0x100
	",
    );

    // Add exemplar testdata here, given promqltest does not support exemplars.
    let ex = |series: &[&str], id: &str, ts: i64| ExemplarQueryResult {
        series_labels: from_strings(series),
        exemplars: vec![Exemplar { labels: from_strings(&["id", id]), value: 10.0, ts, has_ts: false }],
    };
    let exemplars = vec![
        ex(&["__name__", "test_metric3", "foo", "boo", "dup", "1"], "abc", 2000),
        ex(&["__name__", "test_metric4", "foo", "bar", "dup", "1"], "lul", 4000),
        ex(&["__name__", "test_metric3", "foo", "boo", "dup", "1"], "abc2", 4053),
        ex(&["__name__", "test_metric4", "foo", "bar", "dup", "1"], "lul2", 4153),
    ];
    append_exemplars(&s.db, &exemplars);

    let now = Time::now();

    let ng = Arc::new(test_engine());

    // local
    let mut algr = RulesRetrieverMock::new();
    algr.create_alerting_rules();
    algr.create_rule_groups();
    let g = algr.rule_groups.clone();
    g[0].eval(&Context::background(), rules::Time::now());

    let tr = Arc::new(setup_test_target_retriever());
    let api = Api::new(ApiOptions {
        queryable: Some(s.db.clone()),
        query_engine: Some(ng.clone()),
        exemplar_queryable: Some(s.db.clone()),
        target_retriever: Some(tr.clone()),
        alertmanager_retriever: Some(Arc::new(TestAlertmanagerRetriever)),
        flags: sample_flag_map(),
        now: Some(Arc::new(move || now)),
        config: Some(Arc::new(sample_config)),
        rules_retriever: Some(Arc::new(algr)),
        ..base_opts()
    });
    test_endpoints_run(&api, &tr, true, now);

    // Run all the API tests against an API that is wired to forward queries via
    // the remote read client to a test server, which in turn sends them to the
    // data from the test storage.
    // remote
    {
        let server = setup_remote(s.db.clone());

        let u = yaml::Url::parse(&server.url).unwrap();

        let db_dir = temp_dir("remote");

        let remote = Arc::new(remote::write::new_storage(
            None,
            None,
            Some(Arc::new(|| Ok(0))),
            &db_dir,
            Duration::from_secs(1),
            None,
            false,
            None,
        ));

        let cfg = config::Config {
            remote_read_configs: vec![config::RemoteReadConfig {
                url: u,
                remote_timeout: yaml::Duration::from_secs(1),
                read_recent: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        remote.apply_config(&cfg).unwrap();

        let mut algr = RulesRetrieverMock::new();
        algr.create_alerting_rules();
        algr.create_rule_groups();
        let g = algr.rule_groups.clone();
        g[0].eval(&Context::background(), rules::Time::now());

        let tr = Arc::new(setup_test_target_retriever());
        let api = Api::new(ApiOptions {
            queryable: Some(remote.clone()),
            query_engine: Some(ng.clone()),
            exemplar_queryable: Some(s.db.clone()),
            target_retriever: Some(tr.clone()),
            alertmanager_retriever: Some(Arc::new(TestAlertmanagerRetriever)),
            flags: sample_flag_map(),
            now: Some(Arc::new(move || now)),
            config: Some(Arc::new(sample_config)),
            rules_retriever: Some(Arc::new(algr)),
            ..base_opts()
        });
        test_endpoints_run(&api, &tr, false, now);

        let _ = storage::Storage::close(&*remote);
        server.close();
        let _ = std::fs::remove_dir_all(&db_dir);
    }
}

/// Go `httptest.Server` for `setupRemote`: closed with [`RemoteTestServer::close`].
struct RemoteTestServer {
    url: String,
    stop: Arc<std::sync::atomic::AtomicBool>,
    port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RemoteTestServer {
    /// Go `server.Close()`.
    fn close(mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        // Wake the accept loop (see `http::server::Server::run_until`).
        let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

struct RemoteReadService(Arc<tsdb::db::DB>);

/// Go `http.Error`.
fn http_error(msg: &str, code: u16) -> http::Response {
    http::Response::text(code, "text/plain; charset=utf-8", format!("{msg}\n")).with_header("X-Content-Type-Options", "nosniff")
}

impl http::server::Service for RemoteReadService {
    fn handle(&self, r: http::Request) -> http::Response {
        let s = &self.0;
        let req = match remote::codec::decode_read_request(&r) {
            Ok(req) => req,
            Err(e) => return http_error(&e.to_string(), 400),
        };
        let mut resp = remote::prompb::ReadResponse { results: Vec::with_capacity(req.queries.len()) };
        for query in &req.queries {
            let matchers = match remote::codec::from_label_matchers(&query.matchers) {
                Ok(m) => m,
                Err(e) => return http_error(&e.to_string(), 400),
            };

            let hints = query.hints.as_ref().map(|h| storage::SelectHints {
                start: h.start_ms,
                end: h.end_ms,
                step: h.step_ms,
                func: h.func.clone(),
                ..Default::default()
            });

            let mut querier = match s.querier(query.start_timestamp_ms, query.end_timestamp_ms) {
                Ok(q) => q,
                Err(e) => return http_error(&e.to_string(), 500),
            };

            let mut set = querier.select(&Context::background(), false, hints.as_ref(), &matchers);
            let (res, _, err) = remote::codec::to_query_result(&mut *set, 1_000_000);
            drop(set);
            querier.close();
            if let Some(e) = err {
                return http_error(&e.to_string(), 500);
            }
            resp.results.push(res);
        }

        http::Response::text(200, "application/x-protobuf", remote::codec::encode_read_response(&resp))
            .with_header("Content-Encoding", "snappy")
    }
}

/// Go `setupRemote`.
fn setup_remote(s: Arc<tsdb::db::DB>) -> RemoteTestServer {
    // Like Go's httptest.NewServer: 127.0.0.1 with a port the kernel picks.
    // The handler reads the TSDB on the connection thread: give it a full stack (Go stacks grow).
    let cfg = http::server::ServerConfig { stack_size: 8 << 20, ..Default::default() };
    let srv = http::server::Server::bind("127.0.0.1:0", cfg).expect("bind");
    let port = srv.local_port();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop2 = stop.clone();
    let handle = std::thread::spawn(move || srv.run_until(Arc::new(RemoteReadService(s)), stop2));
    RemoteTestServer { url: format!("http://127.0.0.1:{port}"), stop, port, handle: Some(handle) }
}

fn sample_flag_map() -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([("flag1".to_string(), "value1".to_string()), ("flag2".to_string(), "value2".to_string())])
}

/// Go `testEndpoints`.
fn test_endpoints_run(api: &Api, tr: &TestTargetRetriever, test_label_api: bool, now: Time) {
    use model::metadata::MetricType::{Gauge, Summary};
    use model::relabel::Action;
    let d = EpCase::default;
    let lv = |name: &'static str, q: &[(&'static str, &str)], resp: Option<&[&str]>, err: ErrorType| EpCase {
        endpoint: "label_values",
        params: vec![("name", name)],
        query: qv(q),
        response: resp.map(strings),
        err_type: err,
        ..d()
    };
    let ln = |q: &[(&'static str, &str)], resp: Option<&[&str]>, err: ErrorType| EpCase {
        endpoint: "label_names",
        query: qv(q),
        response: resp.map(strings),
        err_type: err,
        ..d()
    };
    let all_names: &[&str] = &["__name__", "dup", "foo", "host.name", "junk\n{},=:  chars"];
    let replace_query = r#"label_replace(vector(42), "foo", "bar", "", "") or label_replace(vector(3.1415), "dings", "bums", "", "")"#;
    let both_series = || {
        matrix_data(vec![
            (lmap(&[("dings", "bums")]), vec![(3.1415, 0), (3.1415, 1000), (3.1415, 2000)]),
            (lmap(&[("foo", "bar")]), vec![(42.0, 0), (42.0, 1000), (42.0, 2000)]),
        ])
    };
    let both_vector_json = r#"{
		"resultType": "vector",
		"result": [
			{
				"metric": {
					"foo": "bar"
				},
				"value": [123.4, "42"]
			},
			{
				"metric": {
					"dings": "bums"
				},
				"value": [123.4, "3.1415"]
			}
		]
	}"#;
    let next_grp2 = get_rule_group_next_token("/path/to/file", "grp2");
    let all_grp_rules = |a: bool| vec![r_tm3(a), r_tm4(a), r_tm5(a), r_tm6(a), r_tm7(a), r_rec1(), r_rec2()];

    let mut tests: Vec<EpCase> = vec![
        EpCase { endpoint: "query", query: qv(&[("query", "2"), ("time", "123.4")]), response: Some(scalar_data(2.0, 123_400)), ..d() },
        // Only matrix and vector responses are limited/truncated. String and scalar responses aren't truncated.
        EpCase {
            endpoint: "query",
            query: qv(&[("query", "2"), ("time", "123.4"), ("limit", "1")]),
            response: Some(scalar_data(2.0, 123_400)),
            warnings_count: 0,
            ..d()
        },
        // When limit = 0, limit is disabled.
        EpCase {
            endpoint: "query",
            query: qv(&[("query", "2"), ("time", "123.4"), ("limit", "0")]),
            response: Some(scalar_data(2.0, 123_400)),
            warnings_count: 0,
            ..d()
        },
        EpCase { endpoint: "query", query: qv(&[("query", "2"), ("time", "123.4"), ("limit", "-1")]), err_type: ERROR_BAD_DATA, ..d() },
        // A non-positive timeout could only produce an already-expired deadline.
        EpCase { endpoint: "query", query: qv(&[("query", "2"), ("time", "123.4"), ("timeout", "0s")]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "time()"), ("start", "0"), ("end", "2"), ("step", "1"), ("timeout", "-1")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        EpCase { endpoint: "query", query: qv(&[("query", "0.333"), ("time", "1970-01-01T00:02:03Z")]), response: Some(scalar_data(0.333, 123_000)), ..d() },
        EpCase {
            endpoint: "query",
            query: qv(&[("query", "0.333"), ("time", "1970-01-01T01:02:03+01:00")]),
            response: Some(scalar_data(0.333, 123_000)),
            ..d()
        },
        EpCase { endpoint: "query", query: qv(&[("query", "0.333")]), response: Some(scalar_data(0.333, now.unix_milli())), ..d() },
        EpCase {
            endpoint: "query",
            query: qv(&[("query", replace_query), ("time", "123.4"), ("limit", "2")]),
            warnings_count: 0,
            response_as_json: both_vector_json,
            ..d()
        },
        EpCase {
            endpoint: "query",
            query: qv(&[("query", replace_query), ("time", "123.4"), ("limit", "1")]),
            warnings_count: 1,
            response_as_json: r#"{
		"resultType": "vector",
		"result": [
			{
				"metric": {
					"foo": "bar"
				},
				"value": [123.4, "42"]
			}
		]
	}"#,
            ..d()
        },
        EpCase {
            endpoint: "query",
            query: qv(&[("query", replace_query), ("time", "123.4"), ("limit", "0")]),
            response_as_json: both_vector_json,
            warnings_count: 0,
            ..d()
        },
        // limit=0 means no limit.
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", replace_query), ("start", "0"), ("end", "2"), ("step", "1"), ("limit", "0")]),
            response: Some(both_series()),
            warnings_count: 0,
            ..d()
        },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", replace_query), ("start", "0"), ("end", "2"), ("step", "1"), ("limit", "1")]),
            response: Some(matrix_data(vec![(lmap(&[("dings", "bums")]), vec![(3.1415, 0), (3.1415, 1000), (3.1415, 2000)])])),
            warnings_count: 1,
            ..d()
        },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", replace_query), ("start", "0"), ("end", "2"), ("step", "1"), ("limit", "2")]),
            response: Some(both_series()),
            warnings_count: 0,
            ..d()
        },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "time()"), ("start", "0"), ("end", "2"), ("step", "1")]),
            response: Some(matrix_data(vec![(Labels::empty(), vec![(0.0, 0), (1.0, 1000), (2.0, 2000)])])),
            ..d()
        },
        // Test empty vector result
        EpCase { endpoint: "query", query: qv(&[("query", "bottomk(2, notExists)")]), response_as_json: r#"{"resultType":"vector","result":[]}"#, ..d() },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "bottomk(2, notExists)"), ("start", "0"), ("end", "2"), ("step", "1"), ("limit", "-1")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        // Test empty matrix result
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "bottomk(2, notExists)"), ("start", "0"), ("end", "2"), ("step", "1")]),
            response_as_json: r#"{"resultType":"matrix","result":[]}"#,
            ..d()
        },
        // Missing query params in range queries.
        EpCase { endpoint: "query_range", query: qv(&[("query", "time()"), ("end", "2"), ("step", "1")]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase { endpoint: "query_range", query: qv(&[("query", "time()"), ("start", "0"), ("step", "1")]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase { endpoint: "query_range", query: qv(&[("query", "time()"), ("start", "0"), ("end", "2")]), err_type: ERROR_BAD_DATA, ..d() },
        // Bad query expression.
        EpCase { endpoint: "query", query: qv(&[("query", "invalid][query"), ("time", "1970-01-01T01:02:03+01:00")]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "invalid][query"), ("start", "0"), ("end", "100"), ("step", "1")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        // Invalid step.
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "time()"), ("start", "1"), ("end", "2"), ("step", "0")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        // Start after end.
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "time()"), ("start", "2"), ("end", "1"), ("step", "1")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        // Start overflows int64 internally.
        EpCase {
            endpoint: "query_range",
            query: qv(&[("query", "time()"), ("start", "148966367200.372"), ("end", "1489667272.372"), ("step", "1")]),
            err_type: ERROR_BAD_DATA,
            ..d()
        },
        EpCase { endpoint: "format_query", query: qv(&[("query", "foo+bar")]), response: Some(Data::String("foo + bar".to_string())), ..d() },
        EpCase { endpoint: "format_query", query: qv(&[("query", "invalid_expression/")]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric2", "foo", "boo"])])),
            ..d()
        },
        EpCase { endpoint: "series", query: qv(&[("match[]", r#"{foo=""}"#)]), err_type: ERROR_BAD_DATA, ..d() },
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", r#"test_metric1{foo=~".+o"}"#)]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric1", "foo", "boo"])])),
            ..d()
        },
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", r#"test_metric1{foo=~".+o$"}"#), ("match[]", r#"test_metric1{foo=~".+o"}"#)]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric1", "foo", "boo"])])),
            ..d()
        },
        // Try to overlap the selected series set as much as possible to test the result de-duplication works well.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", r#"test_metric4{foo=~".+o$"}"#), ("match[]", r#"test_metric4{dup=~"^1"}"#)]),
            response: Some(series_data(vec![
                from_strings(&["__name__", "test_metric4", "dup", "1", "foo", "bar"]),
                from_strings(&["__name__", "test_metric4", "dup", "1", "foo", "boo"]),
                from_strings(&["__name__", "test_metric4", "foo", "boo"]),
            ])),
            ..d()
        },
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", r#"test_metric1{foo=~".+o"}"#), ("match[]", "none")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric1", "foo", "boo"])])),
            ..d()
        },
        // Start and end before series starts.
        EpCase { endpoint: "series", query: qv(&[("match[]", "test_metric2"), ("start", "-2"), ("end", "-1")]), response: Some(series_data(vec![])), ..d() },
        // Start and end after series ends.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2"), ("start", "100000"), ("end", "100001")]),
            response: Some(series_data(vec![])),
            ..d()
        },
        // Start before series starts, end after series ends.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2"), ("start", "-1"), ("end", "100000")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric2", "foo", "boo"])])),
            ..d()
        },
        // Start and end within series.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2"), ("start", "1"), ("end", "100")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric2", "foo", "boo"])])),
            ..d()
        },
        // Start within series, end after.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2"), ("start", "1"), ("end", "100000")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric2", "foo", "boo"])])),
            ..d()
        },
        // Start before series, end within series.
        EpCase {
            endpoint: "series",
            query: qv(&[("match[]", "test_metric2"), ("start", "-1"), ("end", "1")]),
            response: Some(series_data(vec![from_strings(&["__name__", "test_metric2", "foo", "boo"])])),
            ..d()
        },
        // Series request with limit.
        EpCase { endpoint: "series", query: qv(&[("match[]", "test_metric1"), ("limit", "1")]), response_len: 1, warnings_count: 1, ..d() },
        EpCase { endpoint: "series", query: qv(&[("match[]", "test_metric1"), ("limit", "2")]), response_len: 2, warnings_count: 0, ..d() },
        EpCase { endpoint: "series", query: qv(&[("match[]", "test_metric1"), ("limit", "0")]), response_len: 2, warnings_count: 0, ..d() },
        // Missing match[] query params in series requests.
        EpCase { endpoint: "series", err_type: ERROR_BAD_DATA, ..d() },
        EpCase {
            endpoint: "targets",
            response: Some(Data::Targets(TargetDiscovery {
                active_targets: expected_active_targets(),
                dropped_targets: expected_dropped_targets(),
                dropped_target_counts: dropped_counts(),
            })),
            ..d()
        },
        EpCase {
            endpoint: "targets",
            query: qv(&[("state", "any")]),
            response: Some(Data::Targets(TargetDiscovery {
                active_targets: expected_active_targets(),
                dropped_targets: expected_dropped_targets(),
                dropped_target_counts: dropped_counts(),
            })),
            ..d()
        },
        EpCase {
            endpoint: "targets",
            query: qv(&[("state", "active")]),
            response: Some(Data::Targets(TargetDiscovery {
                active_targets: expected_active_targets(),
                dropped_targets: vec![],
                dropped_target_counts: None,
            })),
            ..d()
        },
        EpCase {
            endpoint: "targets",
            query: qv(&[("state", "Dropped")]),
            response: Some(Data::Targets(TargetDiscovery {
                active_targets: vec![],
                dropped_targets: expected_dropped_targets(),
                dropped_target_counts: dropped_counts(),
            })),
            ..d()
        },
        EpCase {
            endpoint: "target_relabel_steps",
            query: qv(&[("scrapePool", "testpool"), ("labels", r#"{"job":"test","__address__":"localhost:9090"}"#)]),
            response: Some(Data::RelabelSteps(vec![
                relabel_step(
                    test_relabel_config(Action::Replace, None, "__address__", "", "example.com:443", true),
                    lmap(&[("job", "test"), ("__address__", "example.com:443")]),
                    true,
                ),
                relabel_step(test_relabel_config(Action::Drop, Some(vec!["__address__"]), "", r"example\.com:.*", "", false), Labels::empty(), false),
            ])),
            ..d()
        },
        EpCase {
            endpoint: "target_relabel_steps",
            query: qv(&[("scrapePool", "testpool3"), ("labels", r#"{"job":"test","__address__":"localhost:9090"}"#)]),
            response: Some(Data::RelabelSteps(vec![
                relabel_step(
                    test_relabel_config(Action::Replace, None, "__address__", "", "example.com:443", true),
                    lmap(&[("job", "test"), ("__address__", "example.com:443")]),
                    true,
                ),
                relabel_step(test_relabel_config(Action::Drop, Some(vec!["__address__"]), "", r"example\.com:.*", "", false), Labels::empty(), false),
                relabel_step(test_relabel_config(Action::Replace, None, "job", ".*", "should_not_apply", false), Labels::empty(), false),
            ])),
            ..d()
        },
        // With a matching metric.
        EpCase {
            endpoint: "target_metadata",
            query: qv(&[("metric", "go_threads")]),
            metadata: vec![("test", vec![mm("go_threads", Gauge, "Number of OS threads created.")])],
            response: Some(Data::TargetMetadata(vec![tmd(lmap(&[("job", "test")]), "", "Number of OS threads created.")])),
            ..d()
        },
        // With a matching target.
        EpCase {
            endpoint: "target_metadata",
            query: qv(&[("match_target", "{job=\"blackbox\"}")]),
            metadata: vec![(
                "blackbox",
                vec![mm(
                    "prometheus_tsdb_storage_blocks_bytes",
                    Gauge,
                    "The number of bytes that are currently used for local storage by all blocks.",
                )],
            )],
            response: Some(Data::TargetMetadata(vec![tmd(
                lmap(&[("job", "blackbox")]),
                "prometheus_tsdb_storage_blocks_bytes",
                "The number of bytes that are currently used for local storage by all blocks.",
            )])),
            ..d()
        },
        // Without a target or metric.
        EpCase {
            endpoint: "target_metadata",
            metadata: vec![
                ("test", vec![mm("go_threads", Gauge, "Number of OS threads created.")]),
                (
                    "blackbox",
                    vec![mm(
                        "prometheus_tsdb_storage_blocks_bytes",
                        Gauge,
                        "The number of bytes that are currently used for local storage by all blocks.",
                    )],
                ),
            ],
            response: Some(Data::TargetMetadata(vec![
                tmd(lmap(&[("job", "test")]), "go_threads", "Number of OS threads created."),
                tmd(
                    lmap(&[("job", "blackbox")]),
                    "prometheus_tsdb_storage_blocks_bytes",
                    "The number of bytes that are currently used for local storage by all blocks.",
                ),
            ])),
            sorter: Some(sort_target_metadata_by_family),
            ..d()
        },
        // Without a matching metric.
        EpCase {
            endpoint: "target_metadata",
            query: qv(&[("match_target", "{job=\"non-existentblackbox\"}")]),
            response: Some(Data::TargetMetadata(vec![])),
            ..d()
        },
        EpCase {
            endpoint: "alertmanagers",
            response: Some(Data::Alertmanagers {
                active: vec!["http://alertmanager.example.com:8080/api/v1/alerts".to_string()],
                dropped: vec!["http://dropped.alertmanager.example.com:8080/api/v1/alerts".to_string()],
            }),
            ..d()
        },
        // With metadata available.
        EpCase {
            endpoint: "metric_metadata",
            metadata: vec![(
                "test",
                vec![mm("prometheus_engine_query_duration_seconds", Summary, "Query timings"), mm("go_info", Gauge, "Information about the Go environment.")],
            )],
            response: Some(metadata_data(vec![
                ("prometheus_engine_query_duration_seconds", vec![md("summary", "Query timings")]),
                ("go_info", vec![md("gauge", "Information about the Go environment.")]),
            ])),
            response_as_json: r#"{"prometheus_engine_query_duration_seconds":[{"type":"summary","unit":"",
"help":"Query timings"}], "go_info":[{"type":"gauge","unit":"",
"help":"Information about the Go environment."}]}"#,
            ..d()
        },
        // With duplicate metadata for a metric that comes from different targets.
        EpCase {
            endpoint: "metric_metadata",
            metadata: vec![
                ("test", vec![mm("go_threads", Gauge, "Number of OS threads created")]),
                ("blackbox", vec![mm("go_threads", Gauge, "Number of OS threads created")]),
            ],
            response: Some(metadata_data(vec![("go_threads", vec![md("gauge", "Number of OS threads created")])])),
            response_as_json: r#"{"go_threads": [{"type":"gauge","unit":"",
"help":"Number of OS threads created"}]}"#,
            ..d()
        },
        // With non-duplicate metadata for the same metric from different targets.
        EpCase {
            endpoint: "metric_metadata",
            metadata: vec![
                ("test", vec![mm("go_threads", Gauge, "Number of OS threads created")]),
                ("blackbox", vec![mm("go_threads", Gauge, "Number of OS threads that were created.")]),
            ],
            response: Some(metadata_data(vec![(
                "go_threads",
                vec![md("gauge", "Number of OS threads created"), md("gauge", "Number of OS threads that were created.")],
            )])),
            response_as_json: r#"{"go_threads": [{"type":"gauge","unit":"",
"help":"Number of OS threads created"},{"type":"gauge","unit":"",
"help":"Number of OS threads that were created."}]}"#,
            sorter: Some(sort_go_threads_by_help),
            ..d()
        },
        // With a limit for the number of metrics returned.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("limit", "2")]),
            metadata: vec![
                (
                    "test",
                    vec![mm("go_threads", Gauge, "Number of OS threads created"), mm("prometheus_engine_query_duration_seconds", Summary, "Query Timings.")],
                ),
                ("blackbox", vec![mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations.")]),
            ],
            response_len: 2,
            ..d()
        },
        // With a limit for the number of metadata per metric.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("limit_per_metric", "1")]),
            metadata: vec![(
                "test",
                vec![
                    mm("go_threads", Gauge, "Number of OS threads created"),
                    mm("go_threads", Gauge, "Repeated metadata"),
                    mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations."),
                ],
            )],
            response: Some(metadata_data(vec![
                ("go_threads", vec![md("gauge", "Number of OS threads created")]),
                ("go_gc_duration_seconds", vec![md("summary", "A summary of the GC invocation durations.")]),
            ])),
            response_as_json: r#"{"go_gc_duration_seconds":[{"help":"A summary of the GC invocation durations.","type":"summary","unit":""}],"go_threads": [{"type":"gauge","unit":"","help":"Number of OS threads created"}]}"#,
            ..d()
        },
        // With a limit for the number of metadata per metric and per metric.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("limit_per_metric", "1"), ("limit", "1")]),
            metadata: vec![(
                "test",
                vec![
                    mm("go_threads", Gauge, "Number of OS threads created"),
                    mm("go_threads", Gauge, "Repeated metadata"),
                    mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations."),
                ],
            )],
            response_len: 1,
            response_metadata_total: 1,
            ..d()
        },
        // With a limit for the number of metadata per metric and per metric, while having multiple targets.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("limit_per_metric", "1"), ("limit", "1")]),
            metadata: vec![
                (
                    "test",
                    vec![
                        mm("go_threads", Gauge, "Number of OS threads created"),
                        mm("go_threads", Gauge, "Repeated metadata"),
                        mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations."),
                    ],
                ),
                (
                    "blackbox",
                    vec![
                        mm("go_threads", Gauge, "Number of OS threads created, but from a different target"),
                        mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations, but from a different target."),
                    ],
                ),
            ],
            response_len: 1,
            response_metadata_total: 1,
            ..d()
        },
        // When requesting a specific metric that is present.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("metric", "go_threads")]),
            metadata: vec![
                ("test", vec![mm("go_threads", Gauge, "Number of OS threads created")]),
                (
                    "blackbox",
                    vec![
                        mm("go_gc_duration_seconds", Summary, "A summary of the GC invocation durations."),
                        mm("go_threads", Gauge, "Number of OS threads that were created."),
                    ],
                ),
            ],
            response: Some(metadata_data(vec![(
                "go_threads",
                vec![md("gauge", "Number of OS threads created"), md("gauge", "Number of OS threads that were created.")],
            )])),
            response_as_json: r#"{"go_threads": [{"type":"gauge","unit":"","help":"Number of OS threads created"},{"type":"gauge","unit":"","help":"Number of OS threads that were created."}]}"#,
            sorter: Some(sort_go_threads_by_help),
            ..d()
        },
        // With a specific metric that is not present.
        EpCase {
            endpoint: "metric_metadata",
            query: qv(&[("metric", "go_gc_duration_seconds")]),
            metadata: vec![("test", vec![mm("go_threads", Gauge, "Number of OS threads created")])],
            response: Some(metadata_data(vec![])),
            ..d()
        },
        // With no available metadata.
        EpCase { endpoint: "metric_metadata", response: Some(metadata_data(vec![])), ..d() },
        EpCase { endpoint: "serve_config", response: Some(Data::Config(sample_config().to_string())), ..d() },
        EpCase { endpoint: "serve_flags", response: Some(Data::Flags(sample_flag_map())), ..d() },
        EpCase { endpoint: "alerts", response: Some(Data::Alerts(vec![tm5_alert()])), zero_func: Some(alerts_zero_func), ..d() },
        EpCase {
            endpoint: "rules",
            response: Some(rules_data(vec![rgroup("grp", all_grp_rules(true)), rgroup("grp2", vec![r_tm3(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("exclude_alerts", "true")]),
            response: Some(rules_data(vec![rgroup("grp", all_grp_rules(false)), rgroup("grp2", vec![r_tm3(false)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("type", "alert")]),
            response: Some(rules_data(
                vec![rgroup("grp", vec![r_tm3(true), r_tm4(true), r_tm5(true), r_tm6(true), r_tm7(true)]), rgroup("grp2", vec![r_tm3(true)])],
                "",
            )),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("type", "record")]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_rec1(), r_rec2()])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("rule_name[]", "test_metric4")]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm4(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase { endpoint: "rules", query: qv(&[("rule_group[]", "respond-with-nothing")]), response: Some(rules_data(vec![], "")), ..d() },
        EpCase {
            endpoint: "rules",
            query: qv(&[("file[]", "/path/to/file"), ("rule_name[]", "test_metric4")]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm4(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("match[]", r#"{testlabel="rule"}"#)]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm6(true), r_rec2()])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("type", "alert"), ("match[]", r#"{templatedlabel="{{ $externalURL }}"}"#)]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm7(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase { endpoint: "rules", query: qv(&[("match[]", r#"{testlabel="abc"}"#)]), response: Some(rules_data(vec![], "")), ..d() },
        // This is testing OR condition, the api response should return rule if it matches one of the label selector
        EpCase {
            endpoint: "rules",
            query: qv(&[("match[]", r#"{testlabel="abc"}"#), ("match[]", r#"{testlabel="rule"}"#)]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm6(true), r_rec2()])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("type", "record"), ("match[]", r#"{testlabel="rule"}"#)]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_rec2()])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("type", "alert"), ("match[]", r#"{testlabel="rule"}"#)]),
            response: Some(rules_data(vec![rgroup("grp", vec![r_tm6(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("group_limit", "1")]),
            response: Some(rules_data(vec![rgroup("grp", all_grp_rules(true))], &next_grp2)),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        EpCase {
            endpoint: "rules",
            query: qv(&[("group_limit", "1"), ("group_next_token", &next_grp2)]),
            response: Some(rules_data(vec![rgroup("grp2", vec![r_tm3(true)])], "")),
            zero_func: Some(rules_zero_func),
            ..d()
        },
        // invalid pagination request
        EpCase { endpoint: "rules", query: qv(&[("group_next_token", &next_grp2)]), err_type: ERROR_BAD_DATA, zero_func: Some(rules_zero_func), ..d() },
        // invalid group_limit
        EpCase {
            endpoint: "rules",
            query: qv(&[("group_limit", "0"), ("group_next_token", &next_grp2)]),
            err_type: ERROR_BAD_DATA,
            zero_func: Some(rules_zero_func),
            ..d()
        },
        // Pagination token is invalid due to changes in the rule groups
        EpCase {
            endpoint: "rules",
            query: qv(&[("group_limit", "1"), ("group_next_token", &get_rule_group_next_token("/removed/file", "notfound"))]),
            err_type: ERROR_BAD_DATA,
            zero_func: Some(rules_zero_func),
            ..d()
        },
        // groupNextToken should not be in empty response
        EpCase {
            endpoint: "rules",
            query: qv(&[("match[]", r#"{testlabel="abc-cannot-find"}"#), ("group_limit", "1")]),
            response_as_json: r#"{"groups":[]}"#,
            ..d()
        },
        EpCase {
            endpoint: "query_exemplars",
            query: qv(&[("query", r#"test_metric3{foo="boo"} - test_metric4{foo="bar"}"#), ("start", "0"), ("end", "4")]),
            // Note extra integer length of timestamps for exemplars because of millisecond preservation
            // of timestamps within Prometheus (see timestamp package).
            response: Some(Data::Exemplars(vec![
                ex1(&["__name__", "test_metric3", "foo", "boo", "dup", "1"], &[("abc", 2000)]),
                ex1(&["__name__", "test_metric4", "foo", "bar", "dup", "1"], &[("lul", 4000)]),
            ])),
            ..d()
        },
        EpCase {
            endpoint: "query_exemplars",
            query: qv(&[("query", r#"{foo="boo"}"#), ("start", "4"), ("end", "4.1")]),
            response: Some(Data::Exemplars(vec![ex1(&["__name__", "test_metric3", "foo", "boo", "dup", "1"], &[("abc2", 4053)])])),
            ..d()
        },
        EpCase {
            endpoint: "query_exemplars",
            query: qv(&[("query", r#"{foo="boo"}"#)]),
            response: Some(Data::Exemplars(vec![ex1(&["__name__", "test_metric3", "foo", "boo", "dup", "1"], &[("abc", 2000), ("abc2", 4053)])])),
            ..d()
        },
        EpCase { endpoint: "query_exemplars", query: qv(&[("query", r#"{__name__="test_metric5"}"#)]), response: Some(Data::Exemplars(vec![])), ..d() },
    ];

    if test_label_api {
        let bar_boo: Option<&[&str]> = Some(&["bar", "boo"]);
        let none: Option<&[&str]> = Some(&[]);
        tests.extend([
            lv("__name__", &[], Some(&["test_metric1", "test_metric2", "test_metric3", "test_metric4", "test_metric5"]), ERROR_NONE),
            lv("foo", &[], bar_boo, ERROR_NONE),
            // Bad name parameter: Go passes "host.name\xff" (invalid UTF-8) and expects errorBadData.
            // Not ported: a Rust `String` (route params of `ApiRequest`) cannot hold invalid UTF-8, so this input
            // cannot be built at this level.
            // Valid utf8 name parameter for utf8 validation.
            lv("host.name", &[], Some(&["localhost"]), ERROR_NONE),
            // Valid escaped utf8 name parameter for utf8 validation.
            lv("U__junk_0a__7b__7d__2c__3d_:_20__20_chars", &[], Some(&["bar"]), ERROR_NONE),
            // Start and end before LabelValues starts.
            lv("foo", &[("start", "-2"), ("end", "-1")], none, ERROR_NONE),
            // Start and end within LabelValues.
            lv("foo", &[("start", "1"), ("end", "100")], bar_boo, ERROR_NONE),
            // Start before LabelValues, end within LabelValues.
            lv("foo", &[("start", "-1"), ("end", "3")], bar_boo, ERROR_NONE),
            // Start before LabelValues starts, end after LabelValues ends.
            lv("foo", &[("start", "1969-12-31T00:00:00Z"), ("end", "1970-02-01T00:02:03Z")], bar_boo, ERROR_NONE),
            // Start with bad data, end within LabelValues.
            lv("foo", &[("start", "boop"), ("end", "1")], None, ERROR_BAD_DATA),
            // Start within LabelValues, end after.
            lv("foo", &[("start", "1"), ("end", "100000000")], bar_boo, ERROR_NONE),
            // Start and end after LabelValues ends.
            lv("foo", &[("start", "148966367200.372"), ("end", "148966367200.972")], none, ERROR_NONE),
            // Only provide Start within LabelValues, don't provide an end time.
            lv("foo", &[("start", "2")], bar_boo, ERROR_NONE),
            // Only provide end within LabelValues, don't provide a start time.
            lv("foo", &[("end", "100")], bar_boo, ERROR_NONE),
            // Label values with bad matchers.
            lv("foo", &[("match[]", r#"{foo="""#), ("match[]", "test_metric2")], None, ERROR_BAD_DATA),
            // Label values with empty matchers.
            lv("foo", &[("match[]", r#"{foo=""}"#)], None, ERROR_BAD_DATA),
            // Label values with matcher.
            lv("foo", &[("match[]", "test_metric2")], Some(&["boo"]), ERROR_NONE),
            // Label values with matcher.
            lv("foo", &[("match[]", "test_metric1")], bar_boo, ERROR_NONE),
            // Label values with matcher using label filter.
            lv("foo", &[("match[]", r#"test_metric1{foo="bar"}"#)], Some(&["bar"]), ERROR_NONE),
            // Label values with matcher and time range.
            lv("foo", &[("match[]", "test_metric1"), ("start", "1"), ("end", "100000000")], bar_boo, ERROR_NONE),
            // Try to overlap the selected series set as much as possible to test that the value de-duplication works.
            lv("foo", &[("match[]", r#"test_metric4{dup=~"^1"}"#), ("match[]", r#"test_metric4{foo=~".+o$"}"#)], bar_boo, ERROR_NONE),
            // Label values with limit.
            EpCase { endpoint: "label_values", params: vec![("name", "__name__")], query: qv(&[("limit", "2")]), response_len: 2, warnings_count: 1, ..d() },
            EpCase { endpoint: "label_values", params: vec![("name", "__name__")], query: qv(&[("limit", "5")]), response_len: 5, warnings_count: 0, ..d() },
            // Label names.
            ln(&[], Some(all_names), ERROR_NONE),
            // Start and end before Label names starts.
            ln(&[("start", "-2"), ("end", "-1")], none, ERROR_NONE),
            // Start and end within Label names.
            ln(&[("start", "1"), ("end", "100")], Some(all_names), ERROR_NONE),
            // Start before Label names, end within Label names.
            ln(&[("start", "-1"), ("end", "10")], Some(all_names), ERROR_NONE),
            // Start before Label names starts, end after Label names ends.
            ln(&[("start", "-1"), ("end", "100000")], Some(all_names), ERROR_NONE),
            // Start with bad data for Label names, end within Label names.
            ln(&[("start", "boop"), ("end", "1")], None, ERROR_BAD_DATA),
            // Start within Label names, end after.
            ln(&[("start", "1"), ("end", "1000000006")], Some(all_names), ERROR_NONE),
            // Start and end after Label names ends.
            ln(&[("start", "148966367200.372"), ("end", "148966367200.972")], none, ERROR_NONE),
            // Only provide Start within Label names, don't provide an end time.
            ln(&[("start", "4")], Some(all_names), ERROR_NONE),
            // Only provide End within Label names, don't provide a start time.
            ln(&[("end", "20")], Some(all_names), ERROR_NONE),
            // Label names with bad matchers.
            ln(&[("match[]", r#"{foo="""#), ("match[]", "test_metric2")], None, ERROR_BAD_DATA),
            // Label values with empty matchers.
            EpCase { params: vec![("name", "foo")], ..ln(&[("match[]", r#"{foo=""}"#)], None, ERROR_BAD_DATA) },
            // Label names with matcher.
            ln(&[("match[]", "test_metric2")], Some(&["__name__", "foo"]), ERROR_NONE),
            // Label names with matcher.
            ln(&[("match[]", "test_metric3")], Some(&["__name__", "dup", "foo"]), ERROR_NONE),
            // Label names with matcher using label filter.
            // There is no matching series.
            ln(&[("match[]", r#"test_metric1{foo="test"}"#)], none, ERROR_NONE),
            // Label names with matcher and time range.
            ln(&[("match[]", "test_metric2"), ("start", "1"), ("end", "100000000")], Some(&["__name__", "foo"]), ERROR_NONE),
            // Label names with limit.
            EpCase { endpoint: "label_names", query: qv(&[("limit", "2")]), response_len: 2, warnings_count: 1, ..d() },
            EpCase { endpoint: "label_names", query: qv(&[("limit", "5")]), response_len: 5, warnings_count: 0, ..d() },
        ]);
    }

    let methods = |ep: &str| -> &'static [&'static str] {
        if ep == "query" || ep == "query_range" || ep == "series" { &["GET", "POST"] } else { &["GET"] }
    };
    let request = |m: &str, q: &[(&str, String)]| -> ApiRequest {
        let pairs: Vec<(&str, &str)> = q.iter().map(|(k, v)| (*k, v.as_str())).collect();
        if m == "POST" { ApiRequest::new(m, &[], &pairs) } else { ApiRequest::new(m, &pairs, &[]) }
    };

    for (i, test) in tests.iter().enumerate() {
        for method in methods(test.endpoint) {
            let name = format!("run {} {} {:?} {}", i, test.endpoint, test.query, method);
            let mut req = request(method, &test.query);
            for (p, v) in &test.params {
                req = req.with_param(p, v);
            }
            tr.reset_metadata_store();
            for (id, md) in &test.metadata {
                tr.set_metadata_store_for_targets(id, Arc::new(TestMetaStore { metadata: md.clone() })).unwrap();
            }
            let mut res = (ep_func(test.endpoint))(api, &req);
            if let Some(f) = res.finalizer.take() {
                // Finalizers were added to ensure closed readers on API panics, ensure they are closed here too.
                f();
            }
            match &res.err {
                None => assert_eq!(test.err_type, ERROR_NONE, "{}: expected error of type {:?} but got none", name, test.err_type.str),
                Some(e) => assert_eq!(test.err_type, e.typ, "{}: ({})", name, e.err),
            }
            if let (Some(sorter), Some(data)) = (test.sorter, res.data.as_mut()) {
                sorter(data);
            }
            if test.response_len != 0 {
                assert_eq!(test.response_len, data_len(&res.data), "{}: Response length does not match", name);
                if test.response_metadata_total != 0 {
                    let Some(Data::Metadata(m)) = &res.data else { panic!("{}: not metadata", name) };
                    let total: usize = m.values().map(|v| v.len()).sum();
                    assert_eq!(test.response_metadata_total, total, "{}: Amount of metadata in the response does not match", name);
                }
            } else {
                if let (Some(zf), Some(data)) = (test.zero_func, res.data.as_mut()) {
                    zf(data);
                }
                if let Some(exp) = &test.response {
                    assert_eq!(Some(exp), res.data.as_ref(), "{}", name);
                }
            }
            if !test.response_as_json.is_empty() {
                let mut got = String::new();
                match &res.data {
                    Some(d) => d.write_json(&mut got),
                    None => got.push_str("null"),
                }
                let exp = json_norm(json::Json::parse(test.response_as_json).expect("expected JSON"));
                let got_j = json_norm(json::Json::parse(&got).expect("response JSON"));
                assert_eq!(exp, got_j, "{}: {}", name, got);
            }
            assert_eq!(test.warnings_count, res.warnings.len(), "{}: warnings {:?}", name, res.warnings.as_errors());
        }
    }
}

fn ex1(series: &[&str], exs: &[(&str, i64)]) -> ExemplarQueryResult {
    ExemplarQueryResult {
        series_labels: from_strings(series),
        exemplars: exs.iter().map(|(id, ts)| Exemplar { labels: from_strings(&["id", id]), value: 10.0, ts: *ts, has_ts: false }).collect(),
    }
}
