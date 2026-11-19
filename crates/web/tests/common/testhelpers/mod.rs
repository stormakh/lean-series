//! Port of `web/api/testhelpers` (api.go, assertions.go, fixtures.go, mocks.go, openapi.go, request.go) and of
//! `newTestAPI` in `web/api/v1/helpers_test.go`.

pub mod assertions;
pub mod fixtures;
pub mod mocks;
pub mod openapi;
pub mod request;

use std::collections::BTreeMap;
use std::sync::Arc;

use storage::{ExemplarQueryable, SampleAndChunkQueryable};
use web::api::{
    AlertmanagerRetriever, Api, ApiOptions, QueryEngine, RulesRetriever, RuntimeInfo,
    ScrapePoolsRetriever, TargetRetriever, TsdbAdminStats,
};
use web::route::Router;

#[allow(unused_imports)]
pub use fixtures::*;
#[allow(unused_imports)]
pub use mocks::*;
#[allow(unused_imports)]
pub use request::*;

/// Go `APIConfig`: the dependencies of a test API; unset ones get the defaults of `PrepareAPI`.
///
/// adapted: Go wraps each dependency in a `LazyLoader` so it is built only when used; here the value is built
/// by the caller (the scenario tests build every dependency they set).
#[derive(Default)]
pub struct ApiConfig {
    pub query_engine: Option<Arc<dyn QueryEngine>>,
    pub queryable: Option<Arc<dyn SampleAndChunkQueryable>>,
    pub exemplar_queryable: Option<Arc<dyn ExemplarQueryable>>,
    pub rules_retriever: Option<Arc<dyn RulesRetriever>>,
    pub target_retriever: Option<Arc<dyn TargetRetriever>>,
    pub scrape_pools_retriever: Option<Arc<dyn ScrapePoolsRetriever>>,
    pub alertmanager_retriever: Option<Arc<dyn AlertmanagerRetriever>>,
    pub tsdb_admin: Option<Arc<dyn TsdbAdminStats>>,
    pub db_dir: String,
    pub config: Option<Arc<dyn Fn() -> Arc<config::Config> + Send + Sync>>,
    pub flags_map: Option<BTreeMap<String, String>>,
}

/// Go `APIWrapper`: the router with the API routes under "/api/v1".
pub struct ApiWrapper {
    pub handler: Router,
}

/// Go `PrepareAPI` with `newTestAPI`: the API with test defaults for every unset dependency, registered under
/// "/api/v1".
pub fn new_test_api(cfg: ApiConfig) -> ApiWrapper {
    let query_engine = cfg
        .query_engine
        .unwrap_or_else(|| Arc::new(super::test_engine()));
    let queryable = cfg
        .queryable
        .unwrap_or_else(|| Arc::new(new_empty_queryable()));
    let exemplar_queryable = cfg
        .exemplar_queryable
        .unwrap_or_else(|| Arc::new(FakeExemplarQueryable));
    let rules_retriever = cfg
        .rules_retriever
        .unwrap_or_else(|| Arc::new(new_empty_rules_retriever()));
    let target_retriever = cfg
        .target_retriever
        .unwrap_or_else(|| Arc::new(new_empty_target_retriever()));
    let scrape_pools_retriever = cfg
        .scrape_pools_retriever
        .unwrap_or_else(|| Arc::new(FakeScrapePoolsRetriever::default()));
    let alertmanager_retriever = cfg
        .alertmanager_retriever
        .unwrap_or_else(|| Arc::new(FakeAlertmanagerRetriever));
    // adapted: Go's `FakeTSDBAdminStats` returns empty statistics; `doubles::FakeDb` without an error does the same.
    let tsdb_admin = cfg
        .tsdb_admin
        .unwrap_or_else(|| Arc::new(super::doubles::FakeDb::default()));
    let config = cfg
        .config
        .unwrap_or_else(|| Arc::new(|| Arc::new(config::Config::default())));
    let db_dir = if cfg.db_dir.is_empty() {
        super::temp_dir("testhelpers")
            .to_string_lossy()
            .into_owned()
    } else {
        cfg.db_dir
    };

    let api = Arc::new(Api::new(ApiOptions {
        query_engine: Some(query_engine),
        queryable: Some(queryable),
        exemplar_queryable: Some(exemplar_queryable),
        scrape_pools_retriever: Some(scrape_pools_retriever),
        target_retriever: Some(target_retriever),
        alertmanager_retriever: Some(alertmanager_retriever),
        rules_retriever: Some(rules_retriever),
        config: Some(config),
        flags: cfg.flags_map.unwrap_or_default(),
        db: Some(tsdb_admin),
        db_dir,
        enable_admin: false,
        enable_search: false,
        max_search_limit: 0,
        is_agent: false,
        runtime_info: Some(Arc::new(|| Ok(RuntimeInfo::default()))),
        notifications_getter: Some(Arc::new(Vec::new)),
        lookback_delta: 5 * 60 * web::gotime::SECOND,
        parser: Some(Arc::new(promql::parser::Parser::new(Default::default()))),
        enable_query_cost: false,
        query_timeout_flag: 0,
        ..Default::default()
    }));
    ApiWrapper {
        handler: api.router("/api/v1"),
    }
}
