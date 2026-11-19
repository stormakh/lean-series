//! Port of `web/api/v1/errors_test.go`.

use std::sync::Arc;
use std::time::Duration;

use model::labels::Matcher;
use promql::{EngineOpts, PromqlError};
use storage::{
    Annotations, BoxChunkQuerier, BoxQuerier, BoxSeriesSet, ChunkQueryable, Context, Error, LabelHints, LabelQuerier, LabelResult, Querier,
    Queryable, SampleAndChunkQueryable, SelectHints, SeriesArc, SeriesSet,
};
use web::api::{Api, ApiOptions, ErrorNum, OverrideErrorCode, STATUS_CLIENT_CLOSED_CONNECTION};
use web::route::Router;

#[derive(Clone)]
struct ErrorTestQueryable {
    q: Option<ErrorTestQuerier>,
    err: Option<Error>,
}

impl Queryable for ErrorTestQueryable {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        if let Some(q) = &self.q {
            return Ok(Box::new(q.clone()));
        }
        Err(self.err.clone().unwrap())
    }
}

impl ChunkQueryable for ErrorTestQueryable {
    fn chunk_querier(&self, _mint: i64, _maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Err(self.err.clone().unwrap())
    }
}

#[derive(Clone)]
struct ErrorTestQuerier {
    s: Option<ErrorTestSeriesSet>,
    err: Option<Error>,
}

impl LabelQuerier for ErrorTestQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), self.err.clone())
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), self.err.clone())
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for ErrorTestQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        if let Some(s) = &self.s {
            return Box::new(s.clone());
        }
        storage::err_series_set(self.err.clone().unwrap())
    }
}

#[derive(Clone)]
struct ErrorTestSeriesSet {
    err: Error,
}

impl SeriesSet for ErrorTestSeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &SeriesArc {
        unreachable!()
    }
    fn err(&self) -> Option<Error> {
        Some(self.err.clone())
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

fn create_prometheus_api(q: Arc<dyn SampleAndChunkQueryable>, override_error_code: Option<OverrideErrorCode>) -> Router {
    let engine = promql::promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 100,
        timeout: Duration::from_secs(5),
        ..Default::default()
    });
    let api = Arc::new(Api::new(ApiOptions {
        query_engine: Some(Arc::new(engine)),
        queryable: Some(q),
        config: Some(Arc::new(|| Arc::new(config::Config::default()))),
        cors_origin: Some(web::httputil::CorsOrigin::new(".*").unwrap()),
        runtime_info: Some(Arc::new(|| Err(Error::msg("not implemented")))),
        lookback_delta: 5 * 60 * 1_000_000_000,
        override_error_code,
        ..Default::default()
    }));
    api.router("/api/v1")
}

#[test]
fn test_api_status_codes() {
    struct Case {
        name: &'static str,
        err: Error,
        expected_string: &'static str,
        expected_code: u16,
        override_error_code: Option<OverrideErrorCode>,
    }
    let cases = vec![
        Case { name: "random error", err: Error::msg("some random error"), expected_string: "some random error", expected_code: 422, override_error_code: None },
        Case {
            name: "promql.ErrTooManySamples",
            err: PromqlError::TooManySamples("some error".into()).err(),
            expected_string: "too many samples",
            expected_code: 422,
            override_error_code: None,
        },
        Case {
            name: "overridden error code for engine error",
            err: PromqlError::TooManySamples("some error".into()).err(),
            expected_string: "too many samples",
            override_error_code: Some(Arc::new(|num: ErrorNum, err: &Error| {
                if num == ErrorNum::Exec {
                    if err.to_string().contains("some error") {
                        return Some(999);
                    }
                    return Some(998);
                }
                None
            })),
            expected_code: 999,
        },
        Case {
            name: "promql.ErrQueryCanceled",
            err: PromqlError::QueryCanceled("some error".into()).err(),
            expected_string: "query was canceled",
            expected_code: STATUS_CLIENT_CLOSED_CONNECTION,
            override_error_code: None,
        },
        Case {
            name: "promql.ErrQueryTimeout",
            err: PromqlError::QueryTimeout("some error".into()).err(),
            expected_string: "query timed out",
            expected_code: 503,
            override_error_code: None,
        },
        Case {
            name: "context.DeadlineExceeded",
            err: storage::ERR_DEADLINE_EXCEEDED,
            expected_string: "context deadline exceeded",
            expected_code: 422,
            override_error_code: None,
        },
        Case {
            name: "context.Canceled",
            err: storage::ERR_CANCELED,
            expected_string: "context canceled",
            expected_code: STATUS_CLIENT_CLOSED_CONNECTION,
            override_error_code: None,
        },
        Case {
            name: "promql.ErrTooManySeries",
            err: PromqlError::TooManySeries("some error".into()).err(),
            expected_string: r#""errorType":"cost_limit""#,
            expected_code: 422,
            override_error_code: None,
        },
        Case {
            name: "promql.ErrTooManySamplesRead",
            err: PromqlError::TooManySamplesRead("some error".into()).err(),
            expected_string: r#""errorType":"cost_limit""#,
            expected_code: 422,
            override_error_code: None,
        },
        Case {
            name: "overridden error code for cost limit",
            err: PromqlError::TooManySeries("some error".into()).err(),
            expected_string: "too many series",
            override_error_code: Some(Arc::new(|num: ErrorNum, _err: &Error| if num == ErrorNum::CostLimit { Some(999) } else { None })),
            expected_code: 999,
        },
    ];
    for tc in cases {
        let queryables: Vec<(&str, ErrorTestQueryable)> = vec![
            ("error from queryable", ErrorTestQueryable { q: None, err: Some(tc.err.clone()) }),
            ("error from querier", ErrorTestQueryable { q: Some(ErrorTestQuerier { s: None, err: Some(tc.err.clone()) }), err: None }),
            (
                "error from seriesset",
                ErrorTestQueryable { q: Some(ErrorTestQuerier { s: Some(ErrorTestSeriesSet { err: tc.err.clone() }), err: None }), err: None },
            ),
        ];
        for (k, q) in queryables {
            let r = create_prometheus_api(Arc::new(q), tc.override_error_code.clone());
            let req = http::Request {
                method: "GET".into(),
                path: "/api/v1/query".into(),
                query: "query=up".into(),
                ..Default::default()
            };
            let resp = r.serve(req);
            let body = String::from_utf8(resp.body_bytes().to_vec()).unwrap();
            assert_eq!(tc.expected_code, resp.status, "{}/{}: {}", tc.name, k, body);
            assert!(body.contains(tc.expected_string), "{}/{}: {}", tc.name, k, body);
        }
    }
}
