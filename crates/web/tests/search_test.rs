//! Port of `web/api/v1/search_test.go`.
//!
//! Go loads the fixtures with `promqltest.LoadedStorage`, a real TSDB. Here the fixtures load into a real
//! `tsdb::db::DB` in a temporary directory, so the searcher is the TSDB's own.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

use config::ScrapeConfig;
use json::Json;
use model::labels::{Labels, Matcher};
use model::labelset::LabelSet;
use model::metadata::MetricType;
use scrape::{MetricMetadata, MetricMetadataStore, Target};
use storage::{
    Annotations, BoxChunkQuerier, BoxQuerier, BoxSearchResultSet, BoxSeriesSet, ChunkQueryable, Context, Error,
    LabelHints, LabelQuerier, LabelResult, Querier, Queryable, SampleAndChunkQueryable, SearchHints, SearchResult,
    SearchResultSet, Searcher, SelectHints, Storage,
};
use web::api::search::{
    MAX_SEARCH_TERMS_PER_REQUEST, NdjsonRecorder, SearchEndpoint, SearchParams, SearchResultStreamer,
    build_search_filter, parse_search_params, serve_search, stream_search_results,
};
use web::api::{Api, ApiOptions, ApiRequest, TargetRetriever};
use web::gotime::Time;

// ---------------------------------------------------------------------------------------------------------------
// Fixtures.

static DIR_SEQ: AtomicUsize = AtomicUsize::new(0);

/// A TSDB in a temporary directory with the data of promqltest `load` commands (Go `promqltest.LoadedStorage`).
struct TestDb {
    db: Arc<tsdb::db::DB>,
    dir: PathBuf,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn loaded_storage(input: &str) -> Arc<TestDb> {
    let dir = std::env::temp_dir().join(format!(
        "web-search-test-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, AtomicOrdering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = tsdb::db::open(&dir, None, Some(tsdb::db::default_options()), None).expect("open tsdb");
    let engine = promql::promqltest::new_test_engine(false, std::time::Duration::from_secs(300), 50_000_000);
    let st = db.clone();
    let open = move || -> Arc<dyn Storage> { st.clone() };
    promql::promqltest::run_test_with_storage(input, &engine, &open, &mut |_| {}).expect("load");
    Arc::new(TestDb { db, dir })
}

impl Queryable for TestDb {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.db.querier(mint, maxt)
    }
}

impl ChunkQueryable for TestDb {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        self.db.chunk_querier(mint, maxt)
    }
}

/// A queryable whose queriers come from a function (Go `errorTestQueryable` and `searchTestQueryable`).
struct FnQueryable(Box<dyn Fn(i64, i64) -> Result<BoxQuerier, Error> + Send + Sync>);

impl Queryable for FnQueryable {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        (self.0)(mint, maxt)
    }
}

impl ChunkQueryable for FnQueryable {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        Err(Error::msg("no chunk querier"))
    }
}

fn fn_queryable(f: impl Fn(i64, i64) -> Result<BoxQuerier, Error> + Send + Sync + 'static) -> Arc<dyn SampleAndChunkQueryable> {
    Arc::new(FnQueryable(Box::new(f)))
}

struct TestMetaStore(Vec<MetricMetadata>);

impl MetricMetadataStore for TestMetaStore {
    fn list_metadata(&self) -> Vec<MetricMetadata> {
        self.0.clone()
    }
    fn get_metadata(&self, mf_name: &str) -> Option<MetricMetadata> {
        self.0.iter().find(|m| m.metric_family == mf_name).cloned()
    }
    fn size_metadata(&self) -> usize {
        self.0.iter().map(|m| m.metric_family.len() + m.help.len() + m.unit.len()).sum()
    }
    fn length_metadata(&self) -> usize {
        self.0.len()
    }
}

fn md(family: &str, typ: MetricType, help: &str, unit: &str) -> MetricMetadata {
    MetricMetadata { metric_family: family.to_string(), typ, help: help.to_string(), unit: unit.to_string() }
}

fn new_target(labels: Labels, metadata: Vec<MetricMetadata>) -> Arc<Target> {
    let t = Target::new(labels, Arc::new(ScrapeConfig::default()), LabelSet::default(), LabelSet::default());
    t.set_metadata_store(Arc::new(TestMetaStore(metadata)));
    Arc::new(t)
}

/// Go `testTargetRetriever` for the active targets.
struct TestTargetRetriever {
    active: HashMap<String, Vec<Arc<Target>>>,
}

impl TargetRetriever for TestTargetRetriever {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.active.clone()
    }
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        HashMap::new()
    }
    fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        HashMap::new()
    }
    fn scrape_pool_config(&self, _pool: &str) -> Result<Arc<ScrapeConfig>, Error> {
        Err(Error::msg("not found"))
    }
}

/// Counts `targets_active` calls (Go `countingTargetRetriever`).
struct CountingTargetRetriever {
    inner: Arc<dyn TargetRetriever>,
    targets_active_cnt: AtomicUsize,
}

impl TargetRetriever for CountingTargetRetriever {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.targets_active_cnt.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.targets_active()
    }
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.inner.targets_dropped()
    }
    fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        self.inner.targets_dropped_counts()
    }
    fn scrape_pool_config(&self, pool: &str) -> Result<Arc<ScrapeConfig>, Error> {
        self.inner.scrape_pool_config(pool)
    }
}

fn fixed_now() -> Option<Arc<dyn Fn() -> Time + Send + Sync>> {
    // The data runs from t=0 to t=6000s; now at t=7200s makes the default 1h lookback cover it.
    Some(Arc::new(|| Time::unix(7200, 0)))
}

const SEARCH_FIXTURE: &str = r#"
    load 1m
        go_gc_duration_seconds{instance="localhost:9090", job="prometheus"} 0+1x100
        go_gc_heap_goal_bytes{instance="localhost:9090", job="prometheus"} 0+1x100
        go_goroutines{instance="localhost:9090", job="prometheus"} 0+1x100
        up{instance="localhost:9090", job="prometheus"} 1+0x100
        up{instance="localhost:9091", job="node"} 1+0x100
        process_cpu_seconds_total{instance="localhost:9090", job="prometheus"} 0+1x100
"#;

fn search_test_retriever() -> Arc<dyn TargetRetriever> {
    let t = new_target(
        model::labels::from_strings(&["__scheme__", "http", "__address__", "localhost:9090", "__metrics_path__", "/metrics", "job", "prometheus"]),
        vec![
            md("go_gc_duration_seconds", MetricType::Gauge, "GC duration.", "seconds"),
            md("go_goroutines", MetricType::Gauge, "Number of goroutines.", ""),
            md("up", MetricType::Gauge, "Target is up.", ""),
        ],
    );
    Arc::new(TestTargetRetriever { active: HashMap::from([("test".to_string(), vec![t])]) })
}

/// Go `newSearchTestAPI`: options with the search fixture; adjust them, then build with [`api`].
fn search_test_options() -> ApiOptions {
    let st = loaded_storage(SEARCH_FIXTURE);
    ApiOptions {
        queryable: Some(st as Arc<dyn SampleAndChunkQueryable>),
        target_retriever: Some(search_test_retriever()),
        now: fixed_now(),
        enable_search: true,
        ..Default::default()
    }
}

fn new_search_test_api() -> Arc<Api> {
    Arc::new(Api::new(search_test_options()))
}

/// Go `minimalSearchAPI`.
fn minimal_search_options(queryable: Arc<dyn SampleAndChunkQueryable>) -> ApiOptions {
    ApiOptions { queryable: Some(queryable), now: fixed_now(), enable_search: true, ..Default::default() }
}

fn api(o: ApiOptions) -> Arc<Api> {
    Arc::new(Api::new(o))
}

// ---------------------------------------------------------------------------------------------------------------
// Requests.

struct Rec {
    code: u16,
    content_type: String,
    body: String,
}

fn encode(params: &[(&str, &str)]) -> String {
    params.iter().map(|(k, v)| format!("{}={}", http::url_encode(k), http::url_encode(v))).collect::<Vec<_>>().join("&")
}

fn build_request(method: &str, path: &str, params: &[(&str, &str)]) -> http::Request {
    let mut req = http::Request { method: method.to_string(), path: path.to_string(), ..Default::default() };
    if method == "POST" {
        req.body = encode(params).into_bytes();
        req.headers.push(("content-type".to_string(), "application/x-www-form-urlencoded".to_string()));
    } else {
        req.query = encode(params);
    }
    req
}

fn finish(resp: http::Response) -> Rec {
    let code = resp.status;
    let content_type = resp.header("Content-Type").unwrap_or("").to_string();
    let body = String::from_utf8(resp.into_body_vec()).unwrap();
    Rec { code, content_type, body }
}

fn do_request(api: &Arc<Api>, method: &str, path: &str, params: &[(&str, &str)]) -> Rec {
    let r = api.router("");
    finish(r.serve(build_request(method, path, params)))
}

/// Go `doSearchRequest`: a GET through the router.
fn do_search_request(api: &Arc<Api>, path: &str, params: &[(&str, &str)]) -> Rec {
    do_request(api, "GET", path, params)
}

/// Go `parseNDJSON`.
fn parse_ndjson(body: &str) -> Vec<Json> {
    body.lines().filter(|l| !l.is_empty()).map(|l| Json::parse(l).expect("valid JSON line")).collect()
}

/// The values of `field` in the results of a batch line.
fn results(line: &Json, field: &str) -> Vec<String> {
    line.get("results").and_then(|r| r.as_arr()).expect("results").iter().map(|r| r.str_of(field).to_string()).collect()
}

fn results_len(line: &Json) -> usize {
    line.get("results").and_then(|r| r.as_arr()).expect("results").len()
}

fn strings_of(line: &Json, key: &str) -> Vec<String> {
    match line.get(key).and_then(|w| w.as_arr()) {
        Some(a) => a.iter().map(|s| s.as_str().unwrap().to_string()).collect(),
        None => Vec::new(),
    }
}

fn status(line: &Json) -> String {
    line.str_of("status").to_string()
}

fn has_more(line: &Json) -> bool {
    line.bool_of("has_more")
}

fn metric_results(line: &Json) -> Vec<(String, Option<f64>, String, String, String)> {
    line.get("results")
        .and_then(|r| r.as_arr())
        .expect("results")
        .iter()
        .map(|r| {
            (
                r.str_of("name").to_string(),
                r.get("score").and_then(|s| s.as_f64()),
                r.str_of("type").to_string(),
                r.str_of("help").to_string(),
                r.str_of("unit").to_string(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------------------------
// Querier doubles.

/// Go `errorTestQuerier`: label calls and selects fail with `err`; not a searcher.
#[derive(Clone)]
struct ErrorTestQuerier {
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
        storage::err_series_set(self.err.clone().unwrap_or_else(|| Error::msg("")))
    }
}

fn error_test_querier() -> BoxQuerier {
    Box::new(ErrorTestQuerier { err: None })
}

/// Go `closeCountingQuerier`: counts `close` calls. Not a searcher (Go embeds only `storage.Querier`).
struct CloseCountingQuerier {
    q: BoxQuerier,
    close_calls: Arc<AtomicUsize>,
}

fn close_counting(q: BoxQuerier) -> (CloseCountingQuerier, Arc<AtomicUsize>) {
    let n = Arc::new(AtomicUsize::new(0));
    (CloseCountingQuerier { q, close_calls: n.clone() }, n)
}

impl LabelQuerier for CloseCountingQuerier {
    fn label_values(&self, ctx: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.q.label_values(ctx, n, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.q.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.close_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.q.close()
    }
}

impl Querier for CloseCountingQuerier {
    fn select(&self, ctx: &Context, s: bool, h: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        self.q.select(ctx, s, h, m)
    }
}

/// Go `closeCountingSearchQuerier`: a close-counting searcher that tracks the close calls of its result sets.
struct CloseCountingSearchQuerier {
    c: CloseCountingQuerier,
    sets: Arc<Mutex<Vec<Arc<AtomicUsize>>>>,
}

struct CloseCountingSearchSet<'a> {
    rs: BoxSearchResultSet<'a>,
    close_calls: Arc<AtomicUsize>,
}

impl SearchResultSet for CloseCountingSearchSet<'_> {
    fn next(&mut self) -> bool {
        self.rs.next()
    }
    fn at(&self) -> &SearchResult {
        self.rs.at()
    }
    fn warnings(&self) -> Annotations {
        self.rs.warnings()
    }
    fn err(&self) -> Option<Error> {
        self.rs.err()
    }
    fn close(&mut self) -> Option<Error> {
        self.close_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.rs.close()
    }
}

impl CloseCountingSearchQuerier {
    fn track<'a>(&self, rs: BoxSearchResultSet<'a>) -> BoxSearchResultSet<'a> {
        let n = Arc::new(AtomicUsize::new(0));
        self.sets.lock().unwrap().push(n.clone());
        Box::new(CloseCountingSearchSet { rs, close_calls: n })
    }
}

impl LabelQuerier for CloseCountingSearchQuerier {
    fn label_values(&self, ctx: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.c.label_values(ctx, n, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.c.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.c.close()
    }
}

impl Querier for CloseCountingSearchQuerier {
    fn select(&self, ctx: &Context, s: bool, h: Option<&SelectHints>, m: &[Matcher]) -> BoxSeriesSet {
        self.c.select(ctx, s, h, m)
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
}

impl Searcher for CloseCountingSearchQuerier {
    fn search_label_names<'a>(&'a self, ctx: &Context, h: Option<&SearchHints>, m: &[Matcher]) -> BoxSearchResultSet<'a> {
        let s = self.c.q.as_searcher().expect("searcher");
        self.track(s.search_label_names(ctx, h, m))
    }
    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        h: Option<&SearchHints>,
        m: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        let s = self.c.q.as_searcher().expect("searcher");
        self.track(s.search_label_values(ctx, name, h, m))
    }
}

/// A searcher whose result sets come from a function (Go `errorSearchQuerier`, `fixedSearchQuerier`,
/// `sequentialSearchQuerier`). The function gets the number of earlier calls of the same method.
struct FnSearchQuerier {
    f: Box<dyn Fn(usize) -> BoxSearchResultSet<'static> + Send + Sync>,
    label_name_count: AtomicUsize,
    label_value_count: AtomicUsize,
}

fn fn_search_querier(f: impl Fn(usize) -> BoxSearchResultSet<'static> + Send + Sync + 'static) -> BoxQuerier {
    Box::new(FnSearchQuerier {
        f: Box::new(f),
        label_name_count: AtomicUsize::new(0),
        label_value_count: AtomicUsize::new(0),
    })
}

impl LabelQuerier for FnSearchQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for FnSearchQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        storage::err_series_set(Error::msg(""))
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
}

impl Searcher for FnSearchQuerier {
    fn search_label_names<'a>(&'a self, _: &Context, _: Option<&SearchHints>, _: &[Matcher]) -> BoxSearchResultSet<'a> {
        (self.f)(self.label_name_count.fetch_add(1, AtomicOrdering::SeqCst))
    }
    fn search_label_values<'a>(
        &'a self,
        _: &Context,
        _: &str,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        (self.f)(self.label_value_count.fetch_add(1, AtomicOrdering::SeqCst))
    }
}

/// Go `sequentialSearchQuerier`: call i gets `calls[i]`, later calls an empty set.
fn sequential_search_querier(calls: Vec<(Vec<SearchResult>, Option<Error>)>) -> BoxQuerier {
    fn_search_querier(move |i| match calls.get(i) {
        Some((_, Some(err))) => storage::err_search_result_set(err.clone(), Vec::new()),
        Some((results, None)) => storage::new_search_result_set_from_slice(results.clone(), Annotations::new()),
        None => storage::new_search_result_set_from_slice(Vec::new(), Annotations::new()),
    })
}

fn sr(value: &str, score: f64) -> SearchResult {
    SearchResult { value: value.to_string(), score }
}

/// A queryable that hands out one querier per call from `make`.
fn querier_queryable(make: impl Fn() -> BoxQuerier + Send + Sync + 'static) -> Arc<dyn SampleAndChunkQueryable> {
    fn_queryable(move |_, _| Ok(make()))
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_api_search_endpoints_unavailable_storage() {
    struct Case {
        name: &'static str,
        /// The queryable, and the close counter of the querier when it counts closes.
        new_queryable: fn() -> (Arc<dyn SampleAndChunkQueryable>, Option<Arc<Mutex<Vec<Arc<AtomicUsize>>>>>),
        expected_error: String,
    }
    let not_ready = tsdb::db::ERR_NOT_READY.to_string();
    let cases = vec![
        Case {
            name: "querier error",
            new_queryable: || (fn_queryable(|_, _| Err(tsdb::db::ERR_NOT_READY)), None),
            expected_error: not_ready.clone(),
        },
        Case {
            name: "search result error",
            new_queryable: || {
                (
                    querier_queryable(|| {
                        fn_search_querier(|_| storage::err_search_result_set(tsdb::db::ERR_NOT_READY, Vec::new()))
                    }),
                    None,
                )
            },
            expected_error: not_ready.clone(),
        },
        Case {
            name: "unsupported primary",
            new_queryable: || {
                let counters = Arc::new(Mutex::new(Vec::new()));
                let c = counters.clone();
                let q = querier_queryable(move || {
                    let (q, n) = close_counting(error_test_querier());
                    c.lock().unwrap().push(n);
                    Box::new(q)
                });
                (q, Some(counters))
            },
            expected_error: "search not supported by storage".to_string(),
        },
        Case {
            name: "unsupported primary in a merge",
            new_queryable: || {
                let counters = Arc::new(Mutex::new(Vec::new()));
                let c = counters.clone();
                let q = querier_queryable(move || {
                    let merged = storage::new_merge_querier(
                        vec![fn_search_querier(|_| storage::empty_search_result_set()), error_test_querier()],
                        Vec::new(),
                        Arc::new(storage::chained_series_merge),
                    );
                    let (q, n) = close_counting(merged);
                    c.lock().unwrap().push(n);
                    Box::new(q)
                });
                (q, Some(counters))
            },
            expected_error: "search not supported by storage".to_string(),
        },
    ];
    let endpoints: [(&str, &str, &[(&str, &str)]); 3] = [
        ("metric names", "/search/metric_names", &[]),
        ("label names", "/search/label_names", &[]),
        ("label values", "/search/label_values", &[("label", "job")]),
    ];
    for tc in &cases {
        for (ename, path, params) in endpoints {
            for method in ["GET", "POST"] {
                let ctx = format!("{} {} {}", tc.name, method, ename);
                let (queryable, counters) = (tc.new_queryable)();
                let api = api(minimal_search_options(queryable));
                let rec = do_request(&api, method, path, params);
                if let Some(c) = counters {
                    let c = c.lock().unwrap();
                    assert_eq!(1, c.len(), "{}", ctx);
                    assert_eq!(1, c[0].load(AtomicOrdering::SeqCst), "{}", ctx);
                }
                assert_eq!(500, rec.code, "{}", ctx);
                assert_eq!("application/json", rec.content_type, "{}", ctx);
                let response = Json::parse(&rec.body).unwrap();
                assert_eq!("error", response.str_of("status"), "{}", ctx);
                assert_eq!("unavailable", response.str_of("errorType"), "{}", ctx);
                assert_eq!(tc.expected_error, response.str_of("error"), "{}", ctx);
            }
        }
    }
}

#[test]
fn test_api_search_endpoints_secondary_warnings() {
    let local = loaded_storage(
        r#"
        load 1m
            up{job="prometheus"} 1+0x2
            requests_total{job="node"} 0+1x2
    "#,
    );

    struct Case {
        name: &'static str,
        nested: bool,
        secondary_only: bool,
        noop_secondary: bool,
        empty: bool,
        empty_range: bool,
    }
    let c = |name, nested, secondary_only, noop_secondary, empty, empty_range| Case {
        name,
        nested,
        secondary_only,
        noop_secondary,
        empty,
        empty_range,
    };
    let cases = [
        c("unsupported secondary", false, false, false, false, false),
        c("nested unsupported secondaries", true, false, false, false, false),
        c("unsupported secondary only", false, true, false, true, false),
        c("empty matching results", false, false, false, true, false),
        c("noop secondary", false, false, true, false, false),
        c("empty time range", false, false, false, true, true),
    ];
    let endpoints: [(&str, &str, &str, &[&str]); 3] = [
        ("metric names", "/search/metric_names", "name", &["requests_total", "up"]),
        ("label names", "/search/label_names", "name", &["__name__", "job"]),
        ("label values", "/search/label_values", "value", &["node", "prometheus"]),
    ];

    for tc in &cases {
        for (ename, path, field, expected_all) in endpoints {
            for method in ["GET", "POST"] {
                for limit in [1usize, 100] {
                    let ctx = format!("{}/{}/{}/limit={}", tc.name, ename, method, limit);
                    let queriers: Arc<Mutex<Vec<Arc<AtomicUsize>>>> = Arc::new(Mutex::new(Vec::new()));
                    let searchers: Arc<Mutex<Vec<Arc<Mutex<Vec<Arc<AtomicUsize>>>>>>> = Arc::new(Mutex::new(Vec::new()));
                    let (nested, secondary_only, noop_secondary, empty_range) =
                        (tc.nested, tc.secondary_only, tc.noop_secondary, tc.empty_range);
                    let (qs, ss, local2) = (queriers.clone(), searchers.clone(), local.clone());
                    let queryable = fn_queryable(move |mint, maxt| {
                        if empty_range {
                            return local2.querier(mint, maxt);
                        }
                        let counting_searcher = |q: BoxQuerier| -> BoxQuerier {
                            let (c, n) = close_counting(q);
                            qs.lock().unwrap().push(n);
                            let sets = Arc::new(Mutex::new(Vec::new()));
                            ss.lock().unwrap().push(sets.clone());
                            Box::new(CloseCountingSearchQuerier { c, sets })
                        };
                        let counting = |q: BoxQuerier| -> BoxQuerier {
                            let (c, n) = close_counting(q);
                            qs.lock().unwrap().push(n);
                            Box::new(c)
                        };
                        let mut primaries = Vec::new();
                        if !secondary_only {
                            primaries.push(counting_searcher(local2.querier(mint, maxt)?));
                        }
                        let secondary = if noop_secondary {
                            storage::noop_querier()
                        } else {
                            let unsupported = counting(error_test_querier());
                            if nested {
                                let other = counting(error_test_querier());
                                storage::new_merge_querier(
                                    Vec::new(),
                                    vec![unsupported, other],
                                    Arc::new(storage::chained_series_merge),
                                )
                            } else {
                                unsupported
                            }
                        };
                        let merged =
                            storage::new_merge_querier(primaries, vec![secondary], Arc::new(storage::chained_series_merge));
                        Ok(counting_searcher(merged))
                    });
                    let api = api(minimal_search_options(queryable));
                    let limit_s = limit.to_string();
                    let mut params: Vec<(&str, &str)> =
                        vec![("start", "0"), ("end", "120"), ("limit", &limit_s), ("batch_size", "1")];
                    if field == "value" {
                        params.push(("label", "job"));
                    }
                    if tc.empty_range {
                        params[0].1 = "-120";
                        params[1].1 = "-60";
                    } else if tc.empty {
                        params.push(("match[]", "absent"));
                    }
                    let rec = do_request(&api, method, path, &params);
                    assert_eq!(200, rec.code, "{}: {}", ctx, rec.body);
                    assert_eq!("application/x-ndjson; charset=utf-8", rec.content_type, "{}", ctx);

                    let lines = parse_ndjson(&rec.body);
                    assert!(lines.len() >= 2, "{}", ctx);
                    let mut got = Vec::new();
                    let mut warnings = Vec::new();
                    for line in &lines[..lines.len() - 1] {
                        got.extend(results(line, field));
                        warnings.extend(strings_of(line, "warnings"));
                    }
                    let trailer = &lines[lines.len() - 1];
                    assert_eq!("success", status(trailer), "{}", ctx);
                    assert!(strings_of(trailer, "warnings").is_empty(), "{}", ctx);
                    let mut expected: Vec<String> =
                        if tc.empty { Vec::new() } else { expected_all.iter().map(|s| s.to_string()).collect() };
                    assert_eq!(!tc.empty && limit < expected.len(), has_more(trailer), "{}", ctx);
                    if limit < expected.len() {
                        expected.truncate(limit);
                    }
                    assert_eq!(expected, got, "{}", ctx);
                    if tc.noop_secondary || tc.empty_range {
                        assert!(warnings.is_empty(), "{}: {:?}", ctx, warnings);
                    } else {
                        assert_eq!(
                            vec!["search is not supported by a secondary storage backend; results may be incomplete"],
                            warnings,
                            "{}",
                            ctx
                        );
                    }
                    for q in queriers.lock().unwrap().iter() {
                        assert_eq!(1, q.load(AtomicOrdering::SeqCst), "{}", ctx);
                    }
                    for s in searchers.lock().unwrap().iter() {
                        let sets = s.lock().unwrap();
                        assert_eq!(1, sets.len(), "{}", ctx);
                        assert_eq!(1, sets[0].load(AtomicOrdering::SeqCst), "{}", ctx);
                    }
                }
            }
        }
    }
}

#[test]
fn test_search_metric_names() {
    let api = new_search_test_api();

    // basic search
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("search[]", "go_gc")]);
        assert_eq!(200, rec.code);
        assert_eq!("application/x-ndjson; charset=utf-8", rec.content_type);
        let lines = parse_ndjson(&rec.body);
        assert!(lines.len() >= 2);
        assert_eq!("success", status(&lines[lines.len() - 1]));
        let names = results(&lines[0], "name");
        assert!(!names.is_empty());
        for n in &names {
            assert!(n.contains("go_gc"), "{}", n);
        }
    }

    // empty search returns all
    {
        let rec = do_search_request(&api, "/search/metric_names", &[]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        assert_eq!(5, results_len(&lines[0]));
    }

    // with limit
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("limit", "2")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        assert_eq!(2, results_len(&lines[0]));
        assert!(has_more(&lines[lines.len() - 1]));
    }

    // with metadata
    struct MdCase {
        name: &'static str,
        metadata: MetricMetadata,
        extra: Vec<MetricMetadata>,
        extra_first: bool,
        search: &'static str,
        matching: Vec<&'static str>,
        missing: Vec<&'static str>,
        filtered: Vec<&'static str>,
    }
    let base = |name, metadata, matching: Vec<&'static str>, missing: Vec<&'static str>| MdCase {
        name,
        metadata,
        extra: Vec::new(),
        extra_first: false,
        search: "",
        matching,
        missing,
        filtered: Vec::new(),
    };
    let md_cases = vec![
        base(
            "gauge",
            md("go_goroutines", MetricType::Gauge, "Number of goroutines.", ""),
            vec!["go_goroutines"],
            vec!["go_goroutines_total", "go_goroutines_count", "go_goroutines_bucket", "go_goroutines_info"],
        ),
        MdCase {
            search: "go_goroutines",
            filtered: vec!["go_threads"],
            ..base(
                "search filter",
                md("go_goroutines", MetricType::Gauge, "Number of goroutines.", ""),
                vec!["go_goroutines"],
                vec![],
            )
        },
        base("name without underscore", md("up", MetricType::Gauge, "Target is up.", ""), vec!["up"], vec!["uptime"]),
        base(
            "OpenMetrics counter",
            md("process_cpu_seconds", MetricType::Counter, "Total CPU time.", "seconds"),
            vec!["process_cpu_seconds_total"],
            vec![
                "process_cpu_seconds_created",
                "process_cpu_seconds_sum",
                "process_cpu_seconds_bucket",
                "process_cpu_seconds_total_total",
            ],
        ),
        base(
            "Prometheus counter",
            md("process_cpu_seconds_total", MetricType::Counter, "Total CPU time.", "seconds"),
            vec!["process_cpu_seconds_total"],
            vec!["process_cpu_seconds"],
        ),
        base(
            "Prometheus counter with doubled suffix",
            md("process_cpu_seconds_total", MetricType::Counter, "Total CPU time.", "seconds"),
            vec!["process_cpu_seconds_total"],
            vec!["process_cpu_seconds_total_total"],
        ),
        MdCase {
            extra: vec![md("requests_total", MetricType::Counter, "Requests.", "")],
            ..base(
                "exact metadata for doubled suffix",
                md("requests_total_total", MetricType::Gauge, "Independent gauge.", ""),
                vec!["requests_total_total"],
                vec![],
            )
        },
        MdCase {
            extra: vec![md("requests", MetricType::Gauge, "In-flight requests.", "")],
            ..base(
                "conflicting types across targets",
                md("requests", MetricType::Counter, "Requests.", ""),
                vec!["requests_total"],
                vec![],
            )
        },
        MdCase {
            extra: vec![md("requests", MetricType::Gauge, "In-flight requests.", "")],
            extra_first: true,
            ..base(
                "conflicting types across targets with incompatible type first",
                md("requests", MetricType::Counter, "Requests.", ""),
                vec!["requests_total"],
                vec![],
            )
        },
        MdCase {
            extra: vec![md("http_request_duration_seconds", MetricType::Histogram, "Request duration.", "seconds")],
            ..base(
                "exact metadata takes precedence",
                md("http_request_duration_seconds_count", MetricType::Gauge, "Independent gauge.", ""),
                vec!["http_request_duration_seconds_count"],
                vec![],
            )
        },
        base(
            "histogram",
            md("http_request_duration_seconds", MetricType::Histogram, "Request duration.", "seconds"),
            vec![
                "http_request_duration_seconds",
                "http_request_duration_seconds_bucket",
                "http_request_duration_seconds_sum",
                "http_request_duration_seconds_count",
            ],
            vec![
                "http_request_duration_seconds_created",
                "http_request_duration_seconds_total",
                "http_request_duration_seconds_gsum",
                "http_request_duration_seconds_gcount",
            ],
        ),
        base(
            "summary",
            md("go_gc_duration_seconds", MetricType::Summary, "GC duration.", "seconds"),
            vec!["go_gc_duration_seconds", "go_gc_duration_seconds_sum", "go_gc_duration_seconds_count"],
            vec!["go_gc_duration_seconds_created", "go_gc_duration_seconds_bucket", "go_gc_duration_seconds_total"],
        ),
        base(
            "gauge histogram",
            md("queue_size", MetricType::GaugeHistogram, "Queue size.", ""),
            vec!["queue_size", "queue_size_bucket", "queue_size_gsum", "queue_size_gcount", "queue_size_sum", "queue_size_count"],
            vec!["queue_size_total", "queue_size_created"],
        ),
        base(
            "info",
            md("target", MetricType::Info, "Target information.", ""),
            vec!["target_info"],
            vec!["target_total", "target_created"],
        ),
        base(
            "Prometheus info with doubled suffix",
            md("go_build_info", MetricType::Info, "Build information.", ""),
            vec!["go_build_info"],
            vec!["go_build_info_info"],
        ),
        base(
            "unknown type",
            md("custom_metric", MetricType::Unknown, "Custom metric.", ""),
            vec!["custom_metric"],
            vec!["custom_metric_total", "custom_metric_sum", "custom_metric_count", "custom_metric_created"],
        ),
    ];
    for tc in &md_cases {
        let mut input = String::from("load 1m\n");
        for names in [&tc.matching, &tc.missing, &tc.filtered] {
            for name in names.iter() {
                input.push_str(&format!("  {}{{job=\"test\"}} 0+1x100\n", name));
            }
        }
        let st = loaded_storage(&input);
        let a = new_target(model::labels::from_strings(&["__address__", "a:9090"]), vec![tc.metadata.clone()]);
        let b = new_target(model::labels::from_strings(&["__address__", "b:9090"]), tc.extra.clone());
        // Both targets in one pool, so their traversal order is deterministic.
        let targets = if tc.extra_first { vec![b, a] } else { vec![a, b] };
        let tr = Arc::new(TestTargetRetriever { active: HashMap::from([("test".to_string(), targets)]) });
        let mut o = minimal_search_options(st);
        o.target_retriever = Some(tr);
        let api = self::api(o);

        for include_metadata in [true, false] {
            let im = include_metadata.to_string();
            let mut params = vec![("include_metadata", im.as_str())];
            if !tc.search.is_empty() {
                params.push(("search[]", tc.search));
            }
            let rec = do_search_request(&api, "/search/metric_names", &params);
            assert_eq!(200, rec.code, "{}", tc.name);
            let lines = parse_ndjson(&rec.body);
            let mut want = Vec::new();
            for name in &tc.matching {
                if include_metadata {
                    want.push((
                        name.to_string(),
                        None,
                        tc.metadata.typ.as_str().to_string(),
                        tc.metadata.help.clone(),
                        tc.metadata.unit.clone(),
                    ));
                } else {
                    want.push((name.to_string(), None, String::new(), String::new(), String::new()));
                }
            }
            for name in &tc.missing {
                want.push((name.to_string(), None, String::new(), String::new(), String::new()));
            }
            let mut got = metric_results(&lines[0]);
            // ElementsMatch.
            let key = |r: &(String, Option<f64>, String, String, String)| format!("{:?}", r);
            want.sort_by_key(key);
            got.sort_by_key(key);
            assert_eq!(want, got, "{} include_metadata={}", tc.name, include_metadata);
        }
    }

    // with include_score
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("search[]", "go_gc"), ("include_score", "true")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        let r = metric_results(&lines[0]);
        assert!(!r.is_empty());
        for x in &r {
            assert!(x.1.is_some(), "score should be set when include_score=true");
        }
    }

    // without include_score score is absent
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("search[]", "go_gc")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        let r = metric_results(&lines[0]);
        assert!(!r.is_empty());
        for x in &r {
            assert!(x.1.is_none(), "score should be absent when include_score is not set");
        }
    }

    // unknown params are ignored
    {
        let rec =
            do_search_request(&api, "/search/metric_names", &[("search[]", "up"), ("include_cardinality", "true")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        assert_eq!(vec!["up".to_string()], results(&lines[0], "name"));
    }

    // sort by alpha ascending
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("sort_by", "alpha")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        for i in 1..names.len() {
            assert!(names[i - 1] <= names[i]);
        }
    }

    // sort by alpha descending
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("sort_by", "alpha"), ("sort_dir", "dsc")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        for i in 1..names.len() {
            assert!(names[i - 1] >= names[i]);
        }
    }

    // sort_by=score without search[] is rejected
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("sort_by", "score")]).code);

    // case insensitive search
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("search[]", "GO_GC"), ("case_sensitive", "false")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        assert!(!names.is_empty());
        for n in &names {
            assert!(n.to_lowercase().contains("go_gc"));
        }
    }

    // fuzzy search
    {
        let rec =
            do_search_request(&api, "/search/metric_names", &[("search[]", "go_goroutins"), ("fuzz_threshold", "80")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        assert!(names.iter().any(|n| n == "go_goroutines"), "expected to find go_goroutines via fuzzy match");
    }

    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("sort_by", "invalid")]).code);
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("sort_dir", "invalid")]).code);
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("fuzz_threshold", "200")]).code);

    // invalid bool params
    for param in ["case_sensitive", "include_score", "include_metadata"] {
        let rec = do_search_request(&api, "/search/metric_names", &[(param, "maybe")]);
        assert_eq!(400, rec.code, "{}", param);
        assert!(rec.body.contains("bad_data"), "{}", param);
        assert!(rec.body.contains(&format!("invalid {}", param)), "{}", param);
    }

    // sort_dir without sort_by
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("sort_dir", "asc")]).code);
    // unsupported fuzz_alg
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("fuzz_alg", "levenshtein")]).code);
    // valid fuzz_alg
    assert_eq!(
        200,
        do_search_request(&api, "/search/metric_names", &[("search[]", "go_gc"), ("fuzz_alg", "jarowinkler")]).code
    );

    for v in ["0", "-1"] {
        let rec = do_search_request(&api, "/search/metric_names", &[("batch_size", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
        let rec = do_search_request(&api, "/search/metric_names", &[("limit", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
    }

    // sort_by cardinality is invalid
    assert_eq!(400, do_search_request(&api, "/search/metric_names", &[("search[]", "up"), ("sort_by", "cardinality")]).code);
    // sort_dir with sort_by=score is invalid
    assert_eq!(
        400,
        do_search_request(&api, "/search/metric_names", &[("search[]", "up"), ("sort_by", "score"), ("sort_dir", "asc")])
            .code
    );

    // batch_size splitting
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("batch_size", "2")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        // 5 metrics with batch_size=2: 3 batch lines (2+2+1) and the trailer.
        assert_eq!(4, lines.len());
        for line in &lines[..2] {
            assert_eq!(2, results_len(line));
        }
        assert_eq!(1, results_len(&lines[2]));
    }

    // end before start is rejected
    {
        let rec = do_search_request(&api, "/search/metric_names", &[("start", "7200"), ("end", "3600")]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("end timestamp must not be before start timestamp"));
    }

    // limit cap rejects excessive limits
    {
        let mut o = search_test_options();
        o.max_search_limit = 50;
        let capped = Arc::new(Api::new(o));
        let rec = do_search_request(&capped, "/search/metric_names", &[("limit", "5000")]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("exceeds the configured maximum"));
    }

    // limit cap below default still serves request
    {
        let mut o = search_test_options();
        o.max_search_limit = 3;
        let capped = Arc::new(Api::new(o));
        let rec = do_search_request(&capped, "/search/metric_names", &[]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        assert!(!lines.is_empty());
        assert!(results_len(&lines[0]) <= 3, "default limit must be clamped to maxSearchLimit when smaller");
        let trailer = &lines[lines.len() - 1];
        assert_eq!("success", status(trailer));
        assert!(has_more(trailer), "has_more must be true when the cap truncated a fixture with more values than the cap");
    }

    // limit cap allows in-range explicit limit
    {
        let mut o = search_test_options();
        o.max_search_limit = 50;
        let capped = Arc::new(Api::new(o));
        assert_eq!(200, do_search_request(&capped, "/search/metric_names", &[("limit", "2")]).code);
    }

    // limit cap zero disables the cap
    assert_eq!(200, do_search_request(&api, "/search/metric_names", &[("limit", "100000")]).code);

    // multiple match sets are merged and deduplicated
    {
        let rec = do_search_request(
            &api,
            "/search/metric_names",
            &[("match[]", "up"), ("match[]", r#"{job="prometheus"}"#), ("sort_by", "alpha")],
        );
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        assert_eq!(5, names.len());
        let mut seen = std::collections::HashSet::new();
        for (i, n) in names.iter().enumerate() {
            assert!(seen.insert(n.clone()), "duplicate result {:?}", n);
            if i > 0 {
                assert!(names[i - 1] <= *n);
            }
        }
        assert!(seen.contains("up"));
    }
}

#[test]
fn test_search_label_names() {
    let api = new_search_test_api();

    // basic search
    {
        let rec = do_search_request(&api, "/search/label_names", &[("search[]", "inst")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        assert!(!names.is_empty());
        assert!(names.iter().any(|n| n == "instance"));
    }

    // empty search returns all
    {
        let rec = do_search_request(&api, "/search/label_names", &[]);
        assert_eq!(200, rec.code);
        assert!(results_len(&parse_ndjson(&rec.body)[0]) >= 3);
    }

    // search by exact name
    {
        let rec = do_search_request(&api, "/search/label_names", &[("search[]", "job")]);
        assert_eq!(200, rec.code);
        assert_eq!(vec!["job".to_string()], results(&parse_ndjson(&rec.body)[0], "name"));
    }

    // sort by alpha
    {
        let rec = do_search_request(&api, "/search/label_names", &[("sort_by", "alpha")]);
        assert_eq!(200, rec.code);
        let names = results(&parse_ndjson(&rec.body)[0], "name");
        for i in 1..names.len() {
            assert!(names[i - 1] <= names[i]);
        }
    }

    assert_eq!(400, do_search_request(&api, "/search/label_names", &[("sort_by", "invalid")]).code);
    for v in ["0", "-1"] {
        let rec = do_search_request(&api, "/search/label_names", &[("limit", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
        let rec = do_search_request(&api, "/search/label_names", &[("batch_size", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
    }
}

#[test]
fn test_search_label_values() {
    let api = new_search_test_api();

    // basic search
    {
        let rec = do_search_request(&api, "/search/label_values", &[("label", "job"), ("search[]", "prom")]);
        assert_eq!(200, rec.code);
        assert_eq!(vec!["prometheus".to_string()], results(&parse_ndjson(&rec.body)[0], "value"));
    }

    // missing label parameter
    assert_eq!(400, do_search_request(&api, "/search/label_values", &[]).code);

    // all values for label
    {
        let rec = do_search_request(&api, "/search/label_values", &[("label", "job")]);
        assert_eq!(200, rec.code);
        assert_eq!(2, results_len(&parse_ndjson(&rec.body)[0]));
    }

    // search exact value
    {
        let rec = do_search_request(&api, "/search/label_values", &[("label", "job"), ("search[]", "prometheus")]);
        assert_eq!(200, rec.code);
        assert_eq!(vec!["prometheus".to_string()], results(&parse_ndjson(&rec.body)[0], "value"));
    }

    // sort by alpha descending
    {
        let rec =
            do_search_request(&api, "/search/label_values", &[("label", "job"), ("sort_by", "alpha"), ("sort_dir", "dsc")]);
        assert_eq!(200, rec.code);
        assert_eq!(
            vec!["prometheus".to_string(), "node".to_string()],
            results(&parse_ndjson(&rec.body)[0], "value")
        );
    }

    // with limit and has_more
    {
        let rec = do_search_request(&api, "/search/label_values", &[("label", "instance"), ("limit", "1")]);
        assert_eq!(200, rec.code);
        let lines = parse_ndjson(&rec.body);
        assert_eq!(1, results_len(&lines[0]));
        assert!(has_more(&lines[lines.len() - 1]));
    }

    assert_eq!(400, do_search_request(&api, "/search/label_values", &[("label", "job"), ("sort_by", "cardinality")]).code);
    assert_eq!(400, do_search_request(&api, "/search/label_values", &[("label", "job"), ("sort_by", "frequency")]).code);
    for v in ["0", "-1"] {
        let rec = do_search_request(&api, "/search/label_values", &[("label", "job"), ("limit", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
        let rec = do_search_request(&api, "/search/label_values", &[("label", "job"), ("batch_size", v)]);
        assert_eq!(400, rec.code);
        assert!(rec.body.contains("must be a positive integer"));
    }

    // multiple search terms OR logic
    {
        let rec = do_search_request(
            &api,
            "/search/label_values",
            &[("label", "job"), ("search[]", "prometheus"), ("search[]", "node")],
        );
        assert_eq!(200, rec.code);
        assert_eq!(2, results_len(&parse_ndjson(&rec.body)[0]));
    }
}

#[test]
fn test_match_name() {
    // (name, search, fuzz threshold, case sensitive, want matched, want score)
    let tests: &[(&str, &str, i64, bool, bool, f64)] = &[
        // Empty searches must produce no filter.
        ("go_goroutines", "", 100, true, false, 0.0),
        ("go_goroutines", "go_gor", 100, true, true, 1.0),
        ("go_goroutines", "GO_GOR", 100, true, false, 0.0),
        ("go_goroutines", "GO_GOR", 100, false, true, 1.0),
        ("go_goroutines", "xyz_not_found", 100, true, false, 0.0),
        ("go_goroutines", "go_goroutins", 80, true, true, 0.0),
        // Fuzzy disabled with threshold=100.
        ("go_goroutines", "go_goroutins", 100, true, false, 0.0),
        // idx=3, maxIdx=13-9=4 -> 1.0 - 0.9*3/4 = 0.325.
        ("go_goroutines", "goroutine", 100, true, true, 0.325),
    ];
    for &(name, search, ft, cs, want_matched, want_score) in tests {
        let searches: Vec<String> = if search.is_empty() { Vec::new() } else { vec![search.to_string()] };
        let filter = build_search_filter(&searches, ft, "jarowinkler", cs);
        if search.is_empty() {
            assert!(filter.is_none(), "empty searches must yield a nil filter");
            continue;
        }
        let (matched, score) = storage::Filter::accept(&filter.unwrap(), name);
        assert_eq!(want_matched, matched, "{}_{}", name, search);
        if want_matched && want_score > 0.0 {
            assert!((want_score - score).abs() <= 0.01, "{}_{}: {}", name, search, score);
        }
    }
}

fn counting_api() -> (Arc<Api>, Arc<CountingTargetRetriever>) {
    let mut o = search_test_options();
    let counting = Arc::new(CountingTargetRetriever {
        inner: o.target_retriever.take().unwrap(),
        targets_active_cnt: AtomicUsize::new(0),
    });
    o.target_retriever = Some(counting.clone());
    (Arc::new(Api::new(o)), counting)
}

#[test]
fn test_search_metric_names_metadata_map_built_once() {
    let (api, counting) = counting_api();
    let rec = do_search_request(&api, "/search/metric_names", &[("include_metadata", "true")]);
    assert_eq!(200, rec.code);
    assert!(results_len(&parse_ndjson(&rec.body)[0]) >= 5);
    assert_eq!(
        1,
        counting.targets_active_cnt.load(AtomicOrdering::SeqCst),
        "buildMetricMetadataMap must call TargetsActive exactly once per request"
    );
}

#[test]
fn test_search_metric_names_metadata_map_cached_across_requests() {
    let (api, counting) = counting_api();
    let rec = do_search_request(&api, "/search/metric_names", &[("include_metadata", "true")]);
    assert_eq!(200, rec.code);
    assert_eq!(1, counting.targets_active_cnt.load(AtomicOrdering::SeqCst), "first request must build the metadata map exactly once");

    let rec = do_search_request(&api, "/search/metric_names", &[("include_metadata", "true")]);
    assert_eq!(200, rec.code);
    assert_eq!(
        1,
        counting.targets_active_cnt.load(AtomicOrdering::SeqCst),
        "second request within TTL must hit the cache; targetsActiveCnt must not grow"
    );
}

#[test]
fn test_search_metric_names_metadata_map_not_built_when_empty() {
    let (api, counting) = counting_api();
    let rec = do_search_request(
        &api,
        "/search/metric_names",
        &[("include_metadata", "true"), ("search[]", "this_metric_definitely_does_not_exist_xyz")],
    );
    assert_eq!(200, rec.code);
    assert_eq!(
        0,
        counting.targets_active_cnt.load(AtomicOrdering::SeqCst),
        "buildMetricMetadataMap must not be called when no results are emitted"
    );
}

#[test]
fn test_search_endpoints_partial_matcher_set_error() {
    // Two match[] sets: the first yields three values, the second errors.
    let api = api(minimal_search_options(querier_queryable(|| {
        sequential_search_querier(vec![
            (vec![sr("alpha", 1.0), sr("beta", 1.0), sr("gamma", 1.0)], None),
            (Vec::new(), Some(Error::msg("simulated set failure"))),
        ])
    })));
    let rec = do_search_request(
        &api,
        "/search/metric_names",
        &[("match[]", "up"), ("match[]", r#"{job="prometheus"}"#), ("batch_size", "2")],
    );
    assert_eq!(200, rec.code);
    let lines = parse_ndjson(&rec.body);
    assert!(!lines.is_empty());
    let mut values = Vec::new();
    let mut last_is_error = false;
    for (i, line) in lines.iter().enumerate() {
        let st = status(line);
        if !st.is_empty() && i == lines.len() - 1 && st == "error" {
            last_is_error = true;
            continue;
        }
        if line.get("results").and_then(|r| r.as_arr()).is_some() {
            values.extend(results(line, "name"));
        }
    }
    assert_eq!(vec!["alpha", "beta", "gamma"], values, "values from the surviving match[] set must reach the client");
    assert!(last_is_error, "stream must terminate with an NDJSON error line, not a success trailer");
}

#[test]
fn test_search_endpoints_ctx_cancelled() {
    // adapted: Go passes a cancelled request context through the router; the Rust handler takes the context as an
    // argument of `serve_search` (the router has no request context).
    let api = api(minimal_search_options(querier_queryable(|| {
        sequential_search_querier(vec![(vec![sr("alpha", 1.0), sr("beta", 1.0), sr("gamma", 1.0)], None)])
    })));
    let (ctx, cancel) = Context::background().with_cancel();
    cancel.cancel();
    let resp = serve_search(
        &api,
        build_request("GET", "/search/metric_names", &[("batch_size", "1")]),
        SearchEndpoint::MetricNames,
        ctx,
    );
    let rec = finish(resp);
    assert_eq!(200, rec.code);
    let lines = parse_ndjson(&rec.body);
    assert!(!lines.is_empty(), "expected at least the first batch line to be written before ctx-cancel takes effect");
    for (i, line) in lines.iter().enumerate() {
        assert_ne!("success", status(line), "line {} is a success trailer but ctx was cancelled before iteration began", i);
    }
}

#[test]
fn test_search_stream_first_batch_error() {
    let api = api(minimal_search_options(querier_queryable(|| {
        fn_search_querier(|_| {
            storage::new_search_result_set_from_slice_and_error(
                vec![sr("alpha", 1.0), sr("beta", 1.0)],
                Annotations::new(),
                Some(Error::msg("tail failure")),
            )
        })
    })));
    // batch_size=5: both results land in the first batch, then the tail error surfaces in the same call.
    let rec = do_search_request(&api, "/search/metric_names", &[("batch_size", "5")]);
    assert_eq!(200, rec.code, "partial first-batch results must not be lost to a JSON error response");
    let lines = parse_ndjson(&rec.body);
    assert!(lines.len() >= 2, "expected a batch line followed by an error line");
    assert_eq!(vec!["alpha", "beta"], results(&lines[0], "name"));
    assert_eq!("error", status(&lines[lines.len() - 1]), "stream must terminate with an error line, not a success trailer");
}

#[test]
fn test_stream_search_results_warnings_sorted() {
    let mut warns = Annotations::new();
    warns.add(Error::msg("c-warn"));
    warns.add(Error::msg("a-warn"));
    warns.add(Error::msg("b-warn"));
    let rs = storage::new_search_result_set_from_slice(vec![sr("alpha", 1.0), sr("beta", 0.9)], warns);

    let sp = SearchParams { limit: 100, batch_size: 100, ..Default::default() };
    let mut rec = NdjsonRecorder::default();
    stream_search_results(&Context::background(), &mut rec, rs, &sp, |sr| sr.value.clone());

    let body = String::from_utf8(rec.body).unwrap();
    let lines = parse_ndjson(&body);
    assert!(lines.len() >= 2, "expected at least a batch and a trailer line");
    assert_eq!(
        vec!["a-warn", "b-warn", "c-warn"],
        strings_of(&lines[0], "warnings"),
        "first batch must carry warnings in sorted order so the on-wire format is deterministic"
    );
    let trailer = &lines[lines.len() - 1];
    assert_eq!("success", status(trailer));
    assert!(strings_of(trailer, "warnings").is_empty(), "trailer must not re-emit warnings already carried by the first batch");
}

/// Go `warningsOnExhaustResultSet`: extra warnings appear once `next` returned false.
struct WarningsOnExhaustResultSet {
    inner: BoxSearchResultSet<'static>,
    extra: Annotations,
    finished: bool,
}

impl SearchResultSet for WarningsOnExhaustResultSet {
    fn next(&mut self) -> bool {
        let ok = self.inner.next();
        if !ok {
            self.finished = true;
        }
        ok
    }
    fn at(&self) -> &SearchResult {
        self.inner.at()
    }
    fn warnings(&self) -> Annotations {
        let base = self.inner.warnings();
        if !self.finished {
            return base;
        }
        let mut merged = Annotations::new();
        merged.merge(base);
        merged.merge(self.extra.clone());
        merged
    }
    fn err(&self) -> Option<Error> {
        self.inner.err()
    }
    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }
}

#[test]
fn test_stream_search_results_warnings_trailer_diff() {
    let base = Annotations::new().with(Error::msg("base-warn"));
    let extra = Annotations::new().with(Error::msg("late-warn"));
    let rs = Box::new(WarningsOnExhaustResultSet {
        inner: storage::new_search_result_set_from_slice(vec![sr("alpha", 0.0), sr("beta", 0.0), sr("gamma", 0.0)], base),
        extra,
        finished: false,
    });
    // batch_size=1 forces iteration past the first batch before the inner set is exhausted.
    let sp = SearchParams { limit: 100, batch_size: 1, ..Default::default() };
    let mut rec = NdjsonRecorder::default();
    stream_search_results(&Context::background(), &mut rec, rs, &sp, |sr| sr.value.clone());

    let body = String::from_utf8(rec.body).unwrap();
    let lines = parse_ndjson(&body);
    assert_eq!(4, lines.len(), "expected three batch lines plus a trailer");
    assert_eq!(
        vec!["base-warn"],
        strings_of(&lines[0], "warnings"),
        "first batch must see only the warnings present before iteration exhausts"
    );
    let trailer = &lines[lines.len() - 1];
    assert_eq!("success", status(trailer));
    assert_eq!(
        vec!["base-warn", "late-warn"],
        strings_of(trailer, "warnings"),
        "trailer must surface warnings that appeared only after iteration exhausted"
    );
}

#[test]
fn test_search_params_rejects_excess_search_terms() {
    let api = new_search_test_api();
    let too_many: Vec<(&str, &str)> = vec![("search[]", "t"); MAX_SEARCH_TERMS_PER_REQUEST + 1];
    let rec = do_search_request(&api, "/search/metric_names", &too_many);
    assert_eq!(400, rec.code);
    let response = Json::parse(&rec.body).unwrap();
    assert_eq!("error", response.str_of("status"));
    assert_eq!("bad_data", response.str_of("errorType"));
    assert!(response.str_of("error").contains("too many search[] terms"));

    // At the cap the request must still succeed.
    let at_cap: Vec<(&str, &str)> = vec![("search[]", "t"); MAX_SEARCH_TERMS_PER_REQUEST];
    let rec = do_search_request(&api, "/search/metric_names", &at_cap);
    assert_eq!(200, rec.code, "a request with exactly maxSearchTermsPerRequest terms must be accepted");
}

#[test]
fn test_search_batch_size() {
    // oversized batch on all endpoints
    {
        let api = new_search_test_api();
        let max = i64::MAX.to_string();
        for path in ["/search/metric_names", "/search/label_names", "/search/label_values"] {
            let rec = do_search_request(&api, path, &[("batch_size", &max), ("limit", "1"), ("label", "job")]);
            assert_eq!(200, rec.code, "{}", path);
            let lines = parse_ndjson(&rec.body);
            assert_eq!(2, lines.len(), "{}", path);
            assert_eq!(1, results_len(&lines[0]), "{}", path);
            assert_eq!("success", status(&lines[1]), "{}", path);
            assert!(has_more(&lines[1]), "{}", path);
        }
    }

    let max = i64::MAX.to_string();
    // (name, batch size, limit, max limit, want)
    let cases: &[(&str, &str, &str, i64, i64)] = &[
        ("default", "", "", 0, 100),
        ("small batch", "2", "", 0, 2),
        ("default limited by result limit", "", "1", 0, 1),
        ("default limited by operator cap", "", "", 3, 3),
        ("large batch limited by result limit", &max, "1", 0, 1),
        ("large batch limited by operator cap", &max, "", 3, 3),
        ("large batch with uncapped limit", &max, &max, 0, 1000),
        ("batch at maximum", "1000", "2000", 0, 1000),
        ("batch above maximum", "1001", "2000", 0, 1000),
    ];
    let o = || ApiOptions { now: fixed_now(), enable_search: true, ..Default::default() };
    for &(name, batch_size, limit, max_limit, want) in cases {
        let mut opts = o();
        opts.max_search_limit = max_limit;
        let api = Api::new(opts);
        let req = ApiRequest::get(&[("batch_size", batch_size), ("limit", limit)]);
        let sp = parse_search_params(&api, &req).unwrap_or_else(|e| panic!("{}: {}", name, e));
        assert_eq!(want, sp.batch_size, "{}", name);
    }
}

#[test]
fn test_search_result_streamer_batch_capacity() {
    for count in [3usize, 4] {
        let rs = storage::new_search_result_set_from_slice(vec![SearchResult::default(); count], Annotations::new());
        let mut streamer = SearchResultStreamer::new(rs, 3, 2, |sr: &SearchResult| sr.value.clone());
        for want in [2usize, 1, 0] {
            let (batch, err) = streamer.next_batch();
            assert!(err.is_none(), "{}", count);
            assert_eq!(want, batch.len(), "{}", count);
            assert!(batch.capacity() <= want, "{}: capacity {}", count, batch.capacity());
        }
        assert_eq!(count > 3, streamer.has_more, "{}", count);
        assert!(streamer.rs.close().is_none());
    }
}
