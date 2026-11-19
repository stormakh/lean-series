//! Port of `web/api/v1/search.go`: the streaming search endpoints (`/search/metric_names`, `/search/label_names`,
//! `/search/label_values`).
//!
//! Stream contract (as upstream):
//! - Successful responses use Content-Type "application/x-ndjson; charset=utf-8", one JSON object per line.
//! - Zero or more batch lines (`results`, optional `warnings`). The first batch is always written, even when empty.
//! - Then EITHER a trailer line (status "success", optional `warnings` delta, `has_more`) OR an error line (status
//!   "error", `errorType`, `error`) when storage failed after the first batch.
//! - Errors before the first batch are the usual JSON error object with a 4xx/5xx status code.
//!
//! The search result set borrows the querier, and the response body is written after the handler returned, so a
//! request runs its search on its own thread that owns the querier. The thread sends the lines over a bounded
//! channel to the streamed body; the first message decides between the error response and the stream. When the
//! client goes away the body stops reading, which cancels the search (Go: `r.Context()` done).

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

use http::{Request, Response};
use model::labels::Matcher;
use scrape::MetricMetadata;
use model::metadata::MetricType;
use storage::{BoxQuerier, BoxSearchResultSet, Context, Error, Filter, Ordering, SearchHints, SearchResult, Searcher};

use super::params::{ApiRequest, parse_bool, parse_matchers_param, parse_time_param};
use super::search_filters::{FuzzyFilter, MemoizingFilter, OrFilter, OrSearchesFilter, SubsequenceFilter, SubstringFilter};
use super::search_filters::{CaseFoldingFilter, go_to_lower};
use super::{
    Api, ApiError, ApiFuncResult, ERROR_BAD_DATA, ERROR_EXEC, ERROR_INTERNAL, ERROR_UNAVAILABLE, TargetRetriever,
    return_api_error, set_unavail_status_on_tsdb_not_ready,
};
use crate::gotime::{GoDuration, HOUR, SECOND, Time, go_quote};

/// Go `defaultFuzzAlg`.
pub const DEFAULT_FUZZ_ALG: &str = "subsequence";
/// Go `defaultSearchLimit`.
pub const DEFAULT_SEARCH_LIMIT: i64 = 100;
/// Go `defaultSearchBatchSize`.
pub const DEFAULT_SEARCH_BATCH_SIZE: i64 = 100;
/// Go `maxSearchBatchSize`.
pub const MAX_SEARCH_BATCH_SIZE: i64 = 1000;
/// Go `maxSearchTermsPerRequest`.
pub const MAX_SEARCH_TERMS_PER_REQUEST: usize = 32;
/// Go `metricMetadataCacheTTL`.
pub const METRIC_METADATA_CACHE_TTL: GoDuration = 5 * SECOND;

const FUZZ_ALGORITHMS: [&str; 2] = [DEFAULT_FUZZ_ALG, "jarowinkler"];

/// Go `FuzzAlgorithms`: the supported fuzzy matching algorithms.
pub fn fuzz_algorithms() -> Vec<String> {
    FUZZ_ALGORITHMS.iter().map(|s| s.to_string()).collect()
}

/// The parsed parameters common to all search endpoints (Go `searchParams`).
#[derive(Clone, Debug, Default)]
pub struct SearchParams {
    pub matcher_sets: Vec<Vec<Matcher>>,
    pub searches: Vec<String>,
    /// 0-100, default 0.
    pub fuzz_threshold: i64,
    /// "subsequence" (default) or "jarowinkler".
    pub fuzz_alg: String,
    /// Default true.
    pub case_sensitive: bool,
    pub sort_by: String,
    /// "asc" (default) or "dsc".
    pub sort_dir: String,
    pub include_score: bool,
    pub start: Time,
    pub end: Time,
    /// Default 100.
    pub limit: i64,
    /// Default 100.
    pub batch_size: i64,
}

// ---------------------------------------------------------------------------------------------------------------
// Records and NDJSON lines.

/// A result record that writes itself as a JSON object (Go's per-endpoint record types with `encoding/json`).
pub trait SearchRecord {
    fn write_json(&self, out: &mut String);
}

/// A plain string record (Go tests stream `string` records).
impl SearchRecord for String {
    fn write_json(&self, out: &mut String) {
        json::write_str(self, out);
    }
}

fn write_score(score: Option<f64>, out: &mut String) {
    if let Some(s) = score {
        out.push_str(",\"score\":");
        json::write_f64(s, out);
    }
}

fn write_omitempty_str(key: &str, v: &str, out: &mut String) {
    if !v.is_empty() {
        out.push_str(",\"");
        out.push_str(key);
        out.push_str("\":");
        json::write_str(v, out);
    }
}

/// Go `searchMetricNameResult`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchMetricNameResult {
    pub name: String,
    pub score: Option<f64>,
    pub typ: String,
    pub help: String,
    pub unit: String,
}

impl SearchRecord for SearchMetricNameResult {
    fn write_json(&self, out: &mut String) {
        out.push_str("{\"name\":");
        json::write_str(&self.name, out);
        write_score(self.score, out);
        write_omitempty_str("type", &self.typ, out);
        write_omitempty_str("help", &self.help, out);
        write_omitempty_str("unit", &self.unit, out);
        out.push('}');
    }
}

/// Go `searchLabelNameResult`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchLabelNameResult {
    pub name: String,
    pub score: Option<f64>,
}

impl SearchRecord for SearchLabelNameResult {
    fn write_json(&self, out: &mut String) {
        out.push_str("{\"name\":");
        json::write_str(&self.name, out);
        write_score(self.score, out);
        out.push('}');
    }
}

/// Go `searchLabelValueResult`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchLabelValueResult {
    pub value: String,
    pub score: Option<f64>,
}

impl SearchRecord for SearchLabelValueResult {
    fn write_json(&self, out: &mut String) {
        out.push_str("{\"value\":");
        json::write_str(&self.value, out);
        write_score(self.score, out);
        out.push('}');
    }
}

fn write_warnings_field(warnings: &[String], out: &mut String) {
    if !warnings.is_empty() {
        out.push_str(",\"warnings\":");
        super::jsonutil::write_strings(warnings, out);
    }
}

/// A batch line (Go `searchBatch`), with the newline.
fn batch_line<T: SearchRecord>(results: &[T], warnings: &[String]) -> String {
    let mut out = String::with_capacity(32 + results.len() * 32);
    out.push_str("{\"results\":[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        r.write_json(&mut out);
    }
    out.push(']');
    write_warnings_field(warnings, &mut out);
    out.push_str("}\n");
    out
}

/// The trailer line (Go `searchTrailer`), with the newline.
fn trailer_line(has_more: bool, warnings: &[String]) -> String {
    let mut out = String::with_capacity(64);
    out.push_str("{\"status\":\"success\",\"has_more\":");
    out.push_str(if has_more { "true" } else { "false" });
    write_warnings_field(warnings, &mut out);
    out.push_str("}\n");
    out
}

/// An error line (Go `searchErrorResponse`), with the newline.
fn error_line(error_type: &str, err: &str) -> String {
    let mut out = String::with_capacity(64 + err.len());
    out.push_str("{\"status\":\"error\",\"errorType\":");
    json::write_str(error_type, &mut out);
    out.push_str(",\"error\":");
    json::write_str(err, &mut out);
    out.push_str("}\n");
    out
}

/// Where [`stream_search_results`] writes: a JSON error response before the stream, or NDJSON lines.
pub trait SearchSink {
    /// The error response before any line was written (Go `respondPreStreamSearchError`).
    fn pre_stream_error(&mut self, err: ApiError);
    /// Writes and flushes one line (Go `ndjsonWriter.writeLine`).
    fn write_line(&mut self, line: &str) -> Result<(), Error>;
}

/// A [`SearchSink`] that keeps everything in memory (tests and in-process callers).
#[derive(Debug, Default)]
pub struct NdjsonRecorder {
    pub body: Vec<u8>,
    pub pre_stream_error: Option<ApiError>,
}

impl SearchSink for NdjsonRecorder {
    fn pre_stream_error(&mut self, err: ApiError) {
        self.pre_stream_error = Some(err);
    }
    fn write_line(&mut self, line: &str) -> Result<(), Error> {
        self.body.extend_from_slice(line.as_bytes());
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Parameters.

fn bad_data(msg: String) -> ApiError {
    ApiError::msg(ERROR_BAD_DATA, msg)
}

/// Go `strconv.Atoi` success (any error is reported by the caller's own message).
fn atoi(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

/// Go `parseSearchParams`: the parameters common to the search endpoints.
pub fn parse_search_params(api: &Api, r: &ApiRequest) -> Result<SearchParams, ApiError> {
    let mut sp = SearchParams::default();
    let now = api.now();
    sp.start = parse_time_param(r, "start", now.add(-HOUR)).map_err(bad_data)?;
    sp.end = parse_time_param(r, "end", now).map_err(bad_data)?;
    // end == start is a zero-duration snapshot search; only inverted ranges are rejected.
    if sp.end.before(&sp.start) {
        return Err(bad_data("end timestamp must not be before start timestamp".to_string()));
    }

    let matchers = r.form_values("match[]");
    if !matchers.is_empty() {
        sp.matcher_sets = parse_matchers_param(&api.parser, &matchers).map_err(bad_data)?;
    }

    sp.searches = r.form_values("search[]");
    if sp.searches.len() > MAX_SEARCH_TERMS_PER_REQUEST {
        return Err(bad_data(format!(
            "too many search[] terms: got {}, maximum is {}",
            sp.searches.len(),
            MAX_SEARCH_TERMS_PER_REQUEST
        )));
    }

    sp.fuzz_threshold = 0;
    let v = r.form_value("fuzz_threshold");
    if !v.is_empty() {
        match atoi(v) {
            Some(ft) if (0..=100).contains(&ft) => sp.fuzz_threshold = ft,
            _ => return Err(bad_data(format!("invalid fuzz_threshold {}: must be 0-100", go_quote(v)))),
        }
    }

    sp.fuzz_alg = DEFAULT_FUZZ_ALG.to_string();
    let v = r.form_value("fuzz_alg");
    if !v.is_empty() {
        if !FUZZ_ALGORITHMS.contains(&v) {
            return Err(bad_data(format!(
                "unsupported fuzz_alg {}: must be one of [{}]",
                go_quote(v),
                FUZZ_ALGORITHMS.join(" ")
            )));
        }
        sp.fuzz_alg = v.to_string();
    }

    sp.case_sensitive = parse_search_bool_param(r, "case_sensitive", true)?;
    sp.include_score = parse_search_bool_param(r, "include_score", false)?;

    sp.sort_by = r.form_value("sort_by").to_string();
    sp.sort_dir = r.form_value("sort_dir").to_string();
    if !sp.sort_dir.is_empty() && sp.sort_by.is_empty() {
        return Err(bad_data("sort_dir is only valid when sort_by is set".to_string()));
    }
    if !sp.sort_dir.is_empty() && sp.sort_by == "score" {
        return Err(bad_data("sort_dir is not supported for sort_by=score".to_string()));
    }
    if sp.sort_dir.is_empty() {
        sp.sort_dir = "asc".to_string();
    }
    if sp.sort_dir != "asc" && sp.sort_dir != "dsc" {
        return Err(bad_data(format!("invalid sort_dir {}: must be \"asc\" or \"dsc\"", go_quote(&sp.sort_dir))));
    }
    if sp.sort_by == "score" && sp.searches.is_empty() {
        return Err(bad_data("sort_by=score requires search[] to be set".to_string()));
    }

    // The default limit shrinks to a smaller configured cap, so a request without "limit" still works.
    let max_limit = api.o.max_search_limit;
    sp.limit = DEFAULT_SEARCH_LIMIT;
    if max_limit > 0 && sp.limit > max_limit {
        sp.limit = max_limit;
    }
    let v = r.form_value("limit");
    if !v.is_empty() {
        let l = match atoi(v) {
            Some(l) if l > 0 => l,
            _ => return Err(bad_data(format!("invalid limit {}: must be a positive integer", go_quote(v)))),
        };
        if max_limit > 0 && l > max_limit {
            return Err(bad_data(format!(
                "limit {} exceeds the configured maximum ({}, see --web.search.max-limit)",
                l, max_limit
            )));
        }
        sp.limit = l;
    }

    sp.batch_size = DEFAULT_SEARCH_BATCH_SIZE;
    let v = r.form_value("batch_size");
    if !v.is_empty() {
        match atoi(v) {
            Some(bs) if bs > 0 => sp.batch_size = bs,
            _ => return Err(bad_data(format!("invalid batch_size {}: must be a positive integer", go_quote(v)))),
        }
    }
    sp.batch_size = sp.batch_size.min(sp.limit).min(MAX_SEARCH_BATCH_SIZE);
    Ok(sp)
}

/// Go `searchHintsLimit`: one above the limit to detect `has_more`, saturating at the maximum.
pub fn search_hints_limit(sp_limit: i64) -> i64 {
    if sp_limit >= i64::MAX { i64::MAX } else { sp_limit + 1 }
}

/// Go `parseSearchBoolParam`.
pub fn parse_search_bool_param(r: &ApiRequest, name: &str, default: bool) -> Result<bool, ApiError> {
    let v = r.form_value(name);
    if v.is_empty() {
        return Ok(default);
    }
    parse_bool(v).map_err(|_| bad_data(format!("invalid {} {}: must be boolean", name, go_quote(v))))
}

// ---------------------------------------------------------------------------------------------------------------
// Metric metadata.

/// The metric metadata of all active targets by family name.
pub type MetricMetadataMap = HashMap<String, Vec<MetricMetadata>>;

struct MetadataCacheEntry {
    built: Time,
    data: Arc<MetricMetadataMap>,
}

/// Caches the metric metadata map for [`METRIC_METADATA_CACHE_TTL`] (Go `searchMetadataCache`). Clones share the
/// cache.
#[derive(Clone, Default)]
pub struct SearchMetadataCache {
    entry: Arc<Mutex<Option<Arc<MetadataCacheEntry>>>>,
}

impl SearchMetadataCache {
    fn load(&self) -> Option<Arc<MetadataCacheEntry>> {
        self.entry.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn store(&self, e: MetadataCacheEntry) {
        *self.entry.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(e));
    }
}

/// What the metadata lookup reads from the API; owned, so the search thread can keep it.
#[derive(Clone)]
struct MetadataSource {
    retriever: Option<Arc<dyn TargetRetriever>>,
    cache: SearchMetadataCache,
    now: Option<Arc<dyn Fn() -> Time + Send + Sync>>,
}

impl MetadataSource {
    fn from_api(api: &Api) -> MetadataSource {
        MetadataSource {
            retriever: api.o.target_retriever.clone(),
            cache: api.meta_cache.clone(),
            now: api.o.now.clone(),
        }
    }

    fn now(&self) -> Time {
        match &self.now {
            Some(f) => f(),
            None => Time::now(),
        }
    }

    /// Go `buildMetricMetadataMap`: the metadata of all active targets by family name, keeping the first metadata
    /// seen for each distinct type. Cached for the TTL; a partial map from a done context is not cached.
    fn build_metric_metadata_map(&self, ctx: &Context) -> Option<Arc<MetricMetadataMap>> {
        if let Some(e) = self.cache.load()
            && self.now().sub(&e.built) < METRIC_METADATA_CACHE_TTL
        {
            return Some(e.data.clone());
        }
        let tr = self.retriever.as_ref()?;
        let mut out = MetricMetadataMap::new();
        'targets: for targets in tr.targets_active().values() {
            if ctx.is_done() {
                break;
            }
            for t in targets {
                if ctx.is_done() {
                    break 'targets;
                }
                for md in t.list_metadata() {
                    if ctx.is_done() {
                        break 'targets;
                    }
                    let mds = out.entry(md.metric_family.clone()).or_default();
                    if !mds.iter().any(|m| m.typ == md.typ) {
                        mds.push(md);
                    }
                }
            }
        }
        let out = Arc::new(out);
        if !ctx.is_done() {
            self.cache.store(MetadataCacheEntry { built: self.now(), data: out.clone() });
        }
        Some(out)
    }
}

/// Go `metadataForMetric`: the metadata of the family a metric name belongs to. An exact family match wins;
/// otherwise the last suffix is stripped if the family type allows it.
pub fn metadata_for_metric<'a>(meta_map: &'a MetricMetadataMap, name: &str) -> Option<&'a MetricMetadata> {
    if let Some(mds) = meta_map.get(name)
        && let Some(md) = mds.first()
    {
        return Some(md);
    }
    let i = name.rfind('_')?;
    let (family, suffix) = (&name[..i], &name[i..]);
    if (suffix == "_total" || suffix == "_info") && family.ends_with(suffix) {
        return None;
    }
    meta_map.get(family)?.iter().find(|md| type_allows_suffix(md.typ, suffix))
}

/// Go `typeAllowsSuffix`.
pub fn type_allows_suffix(typ: MetricType, suffix: &str) -> bool {
    match suffix {
        "_total" => typ == MetricType::Counter,
        "_bucket" => matches!(typ, MetricType::Histogram | MetricType::GaugeHistogram),
        "_sum" | "_count" => matches!(typ, MetricType::Histogram | MetricType::GaugeHistogram | MetricType::Summary),
        "_gsum" | "_gcount" => typ == MetricType::GaugeHistogram,
        "_info" => typ == MetricType::Info,
        _ => false,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Streaming.

/// Go `sortOrdering`.
pub fn sort_ordering(sort_by: &str, sort_dir: &str) -> Ordering {
    match sort_by {
        "score" => Ordering::ByScoreDesc,
        "alpha" if sort_dir == "dsc" => Ordering::ByValueDesc,
        _ => Ordering::ByValueAsc,
    }
}

/// Go `searchAPIError`.
pub fn search_api_error(err: Error) -> ApiError {
    let r = set_unavail_status_on_tsdb_not_ready(ApiFuncResult::error(return_api_error(err)));
    r.err.expect("an error result")
}

fn write_stream_search_error(sink: &mut dyn SearchSink, err: Error) {
    let e = search_api_error(err);
    let _ = sink.write_line(&error_line(e.typ.str, &e.err.to_string()));
}

fn write_stream_internal_error(sink: &mut dyn SearchSink, err: Error) {
    let _ = sink.write_line(&error_line(ERROR_INTERNAL.str, &err.to_string()));
}

/// Go `searchWarnings`.
fn search_warnings(rs: &dyn storage::SearchResultSet) -> Vec<String> {
    rs.warnings().iter().map(|w| w.to_string()).collect()
}

/// Batches the results of a set and detects `has_more` (Go `searchResultStreamer`).
pub struct SearchResultStreamer<'a, T, F> {
    pub rs: BoxSearchResultSet<'a>,
    pub limit: i64,
    pub batch_size: i64,
    pub emitted: i64,
    pub has_more: bool,
    pub to_result: F,
    _t: PhantomData<T>,
}

impl<'a, T, F: FnMut(&SearchResult) -> T> SearchResultStreamer<'a, T, F> {
    pub fn new(rs: BoxSearchResultSet<'a>, limit: i64, batch_size: i64, to_result: F) -> Self {
        SearchResultStreamer { rs, limit, batch_size, emitted: 0, has_more: false, to_result, _t: PhantomData }
    }

    /// Go `nextBatch`.
    pub fn next_batch(&mut self) -> (Vec<T>, Option<Error>) {
        if self.has_more {
            return (Vec::new(), None);
        }
        let mut capacity = self.batch_size;
        if self.limit > 0 {
            capacity = capacity.min(self.limit - self.emitted);
        }
        let mut batch = Vec::with_capacity(capacity.max(0) as usize);
        while (batch.len() as i64) < self.batch_size {
            if self.limit > 0 && self.emitted >= self.limit {
                if self.rs.next() {
                    self.has_more = true;
                }
                return (batch, self.rs.err());
            }
            if !self.rs.next() {
                return (batch, self.rs.err());
            }
            self.emitted += 1;
            batch.push((self.to_result)(self.rs.at()));
        }
        (batch, None)
    }
}

/// Go `streamSearchResults`: writes the batches and the trailer (or an error) of `rs` to `sink`, and closes `rs`.
pub fn stream_search_results<T: SearchRecord>(
    ctx: &Context,
    sink: &mut dyn SearchSink,
    rs: BoxSearchResultSet<'_>,
    sp: &SearchParams,
    to_result: impl FnMut(&SearchResult) -> T,
) {
    let mut streamer = SearchResultStreamer::new(rs, sp.limit, sp.batch_size, to_result);
    stream_batches(ctx, sink, &mut streamer);
    let _ = streamer.rs.close();
}

fn stream_batches<T: SearchRecord, F: FnMut(&SearchResult) -> T>(
    ctx: &Context,
    sink: &mut dyn SearchSink,
    streamer: &mut SearchResultStreamer<'_, T, F>,
) {
    let (first_batch, first_err) = streamer.next_batch();
    // An error with no results is the usual JSON error; with partial results the stream opens so they are kept.
    if let Some(e) = &first_err
        && first_batch.is_empty()
    {
        sink.pre_stream_error(search_api_error(e.clone()));
        return;
    }
    // Sorted, so the wire order is deterministic and the trailer dedup is order-independent.
    let mut first_warnings = search_warnings(&*streamer.rs);
    first_warnings.sort();

    if let Err(e) = sink.write_line(&batch_line(&first_batch, &first_warnings)) {
        write_stream_internal_error(sink, e);
        return;
    }
    if let Some(e) = first_err {
        write_stream_search_error(sink, e);
        return;
    }
    if first_batch.is_empty() {
        let _ = sink.write_line(&trailer_line(streamer.has_more, &[]));
        return;
    }
    loop {
        // Stop pulling from storage as soon as the client goes away.
        if ctx.is_done() {
            return;
        }
        let (batch, err) = streamer.next_batch();
        if !batch.is_empty()
            && let Err(e) = sink.write_line(&batch_line(&batch, &[]))
        {
            write_stream_internal_error(sink, e);
            return;
        }
        if let Some(e) = err {
            write_stream_search_error(sink, e);
            return;
        }
        if batch.is_empty() {
            break;
        }
    }
    // Warnings may appear while the merge tree drains; send only a changed set.
    let mut trailer_warnings = search_warnings(&*streamer.rs);
    trailer_warnings.sort();
    if trailer_warnings == first_warnings {
        trailer_warnings.clear();
    }
    let _ = sink.write_line(&trailer_line(streamer.has_more, &trailer_warnings));
}

/// Go `searchLabelValues` (the helper): values of `name`, merging the results of several matcher sets.
pub fn label_values_search<'a>(
    ctx: &'a Context,
    searcher: &'a dyn Searcher,
    name: &'a str,
    matcher_sets: &'a [Vec<Matcher>],
    hints: &'a SearchHints,
) -> BoxSearchResultSet<'a> {
    if matcher_sets.len() > 1 {
        let sets = matcher_sets
            .iter()
            .map(|ms| {
                storage::new_lazy_search_result_set(move || searcher.search_label_values(ctx, name, Some(hints), ms))
            })
            .collect();
        return storage::merge_search_result_sets(sets, Some(hints));
    }
    let ms: &[Matcher] = matcher_sets.first().map_or(&[], |m| m.as_slice());
    searcher.search_label_values(ctx, name, Some(hints), ms)
}

/// Go `searchLabelNames` (the helper): label names, merging the results of several matcher sets.
pub fn label_names_search<'a>(
    ctx: &'a Context,
    searcher: &'a dyn Searcher,
    matcher_sets: &'a [Vec<Matcher>],
    hints: &'a SearchHints,
) -> BoxSearchResultSet<'a> {
    if matcher_sets.len() > 1 {
        let sets = matcher_sets
            .iter()
            .map(|ms| storage::new_lazy_search_result_set(move || searcher.search_label_names(ctx, Some(hints), ms)))
            .collect();
        return storage::merge_search_result_sets(sets, Some(hints));
    }
    let ms: &[Matcher] = matcher_sets.first().map_or(&[], |m| m.as_slice());
    searcher.search_label_names(ctx, Some(hints), ms)
}

// ---------------------------------------------------------------------------------------------------------------
// Filters.

/// The filter [`build_search_filter`] builds: memoized when the chain scores expensively.
pub enum SearchFilter {
    Memoizing(MemoizingFilter),
    Plain(Arc<dyn Filter>),
}

impl Filter for SearchFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        match self {
            SearchFilter::Memoizing(f) => f.accept(value),
            SearchFilter::Plain(f) => f.accept(value),
        }
    }
}

/// Go `buildSearchFilter`: OR of the non-empty search terms; `None` when there are none. Case-insensitive search
/// lowercases the terms and wraps the chain in a [`CaseFoldingFilter`].
pub fn build_search_filter(
    searches: &[String],
    fuzz_threshold: i64,
    fuzz_alg: &str,
    case_sensitive: bool,
) -> Option<SearchFilter> {
    let terms: Vec<String> = searches
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| if case_sensitive { s.clone() } else { go_to_lower(s) })
        .collect();
    if terms.is_empty() {
        return None;
    }
    let threshold = fuzz_threshold as f64 / 100.0;
    let mut filters: Vec<Arc<dyn Filter>> = terms
        .iter()
        .map(|s| -> Arc<dyn Filter> {
            if fuzz_alg == "subsequence" {
                Arc::new(SubsequenceFilter::new(s, threshold))
            } else {
                // Jaro-Winkler: substring OR Jaro-Winkler fuzzy.
                Arc::new(OrFilter {
                    substring_filter: Some(SubstringFilter::new(s)),
                    fuzzy_filter: if fuzz_threshold > 0 { Some(FuzzyFilter::new(s, threshold)) } else { None },
                })
            }
        })
        .collect();
    let mut combined: Arc<dyn Filter> =
        if filters.len() == 1 { filters.pop().expect("one filter") } else { Arc::new(OrSearchesFilter::new(filters)) };
    if !case_sensitive {
        combined = Arc::new(CaseFoldingFilter::new(combined));
    }
    if filter_chain_has_expensive_scoring(fuzz_threshold, fuzz_alg) {
        return Some(SearchFilter::Memoizing(MemoizingFilter::new(combined)));
    }
    Some(SearchFilter::Plain(combined))
}

/// Go `filterChainHasExpensiveScoring`.
pub fn filter_chain_has_expensive_scoring(fuzz_threshold: i64, fuzz_alg: &str) -> bool {
    fuzz_alg == "subsequence" || fuzz_threshold > 0
}

// ---------------------------------------------------------------------------------------------------------------
// Handlers.

/// The search endpoints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchEndpoint {
    MetricNames,
    LabelNames,
    LabelValues,
}

impl SearchEndpoint {
    fn name(self) -> &'static str {
        match self {
            SearchEndpoint::MetricNames => "metric_names",
            SearchEndpoint::LabelNames => "label_names",
            SearchEndpoint::LabelValues => "label_values",
        }
    }
}

/// GET/POST /search/metric_names.
pub fn search_metric_names(api: &Api, req: Request) -> Response {
    serve_search(api, req, SearchEndpoint::MetricNames, Context::background())
}

/// GET/POST /search/label_names.
pub fn search_label_names(api: &Api, req: Request) -> Response {
    serve_search(api, req, SearchEndpoint::LabelNames, Context::background())
}

/// GET/POST /search/label_values.
pub fn search_label_values(api: &Api, req: Request) -> Response {
    serve_search(api, req, SearchEndpoint::LabelValues, Context::background())
}

/// A search request with the request context `ctx` (Go `r.Context()`); the stream also stops when the client
/// disconnects.
pub fn serve_search(api: &Api, req: Request, endpoint: SearchEndpoint, ctx: Context) -> Response {
    let mut resp = serve_search_inner(api, &req, endpoint, ctx);
    crate::httputil::set_cors(&mut resp, api.o.cors_origin.as_ref(), &req);
    resp
}

/// What the search thread sends to the response body.
enum Msg {
    PreStreamError(ApiError),
    Line(String),
}

struct ChannelSink {
    tx: SyncSender<Msg>,
}

impl SearchSink for ChannelSink {
    fn pre_stream_error(&mut self, err: ApiError) {
        let _ = self.tx.send(Msg::PreStreamError(err));
    }
    fn write_line(&mut self, line: &str) -> Result<(), Error> {
        self.tx.send(Msg::Line(line.to_string())).map_err(|_| Error::msg("search response closed"))
    }
}

/// Everything the search thread owns.
struct SearchJob {
    endpoint: SearchEndpoint,
    q: BoxQuerier,
    sp: SearchParams,
    hints: SearchHints,
    label_name: String,
    include_metadata: bool,
    meta: MetadataSource,
    ctx: Context,
}

fn serve_search_inner(api: &Api, req: &Request, endpoint: SearchEndpoint, ctx: Context) -> Response {
    if !api.o.enable_search {
        return api.respond_error(&ApiError::msg(ERROR_UNAVAILABLE, "search API disabled"), None);
    }
    if api.o.is_agent {
        return api.respond_error(&ApiError::msg(ERROR_EXEC, "unavailable with Prometheus Agent"), None);
    }
    let r = ApiRequest::from_http(req).with_ctx(ctx.clone());
    let sp = match parse_search_params(api, &r) {
        Ok(sp) => sp,
        Err(e) => return api.respond_error(&e, None),
    };
    if !sp.sort_by.is_empty() && sp.sort_by != "alpha" && sp.sort_by != "score" {
        return api.respond_error(
            &bad_data(format!(
                "invalid sort_by {} for {}: must be \"alpha\" or \"score\"",
                go_quote(&sp.sort_by),
                endpoint.name()
            )),
            None,
        );
    }
    let queryable = match api.queryable() {
        Ok(q) => q,
        Err(e) => return api.respond_error(&e, None),
    };
    let mut q = match queryable.querier(sp.start.unix_milli(), sp.end.unix_milli()) {
        Ok(q) => q,
        Err(e) => return api.respond_error(&search_api_error(e), None),
    };
    if q.as_searcher().is_none() {
        let _ = q.close();
        return api.respond_error(&ApiError::msg(ERROR_UNAVAILABLE, "search not supported by storage"), None);
    }
    let hints = SearchHints {
        filter: build_search_filter(&sp.searches, sp.fuzz_threshold, &sp.fuzz_alg, sp.case_sensitive)
            .map(|f| Arc::new(f) as Arc<dyn Filter>),
        // One extra to detect has_more.
        limit: search_hints_limit(sp.limit),
        order_by: sort_ordering(&sp.sort_by, &sp.sort_dir),
    };

    let mut include_metadata = false;
    let mut label_name = String::new();
    match endpoint {
        SearchEndpoint::MetricNames => match parse_search_bool_param(&r, "include_metadata", false) {
            Ok(b) => include_metadata = b,
            Err(e) => {
                let _ = q.close();
                return api.respond_error(&e, None);
            }
        },
        SearchEndpoint::LabelValues => {
            label_name = r.form_value("label").to_string();
            if label_name.is_empty() {
                let _ = q.close();
                return api.respond_error(&bad_data("missing required parameter \"label\"".to_string()), None);
            }
        }
        SearchEndpoint::LabelNames => {}
    }

    let (job_ctx, cancel) = ctx.with_cancel();
    let job = SearchJob {
        endpoint,
        q,
        sp,
        hints,
        label_name,
        include_metadata,
        meta: MetadataSource::from_api(api),
        ctx: job_ctx,
    };
    let (tx, rx) = sync_channel::<Msg>(1);
    let handle = match std::thread::Builder::new().name("search".to_string()).spawn(move || run_search(job, tx)) {
        Ok(h) => h,
        Err(e) => return api.respond_error(&ApiError::msg(ERROR_INTERNAL, e.to_string()), None),
    };
    let first = match rx.recv() {
        Ok(Msg::Line(l)) => l,
        Ok(Msg::PreStreamError(e)) => {
            let _ = handle.join();
            return api.respond_error(&e, None);
        }
        Err(_) => {
            let _ = handle.join();
            return api.respond_error(&ApiError::msg(ERROR_INTERNAL, "search failed"), None);
        }
    };
    let mut resp = Response::new(200);
    resp.set_header("Content-Type", "application/x-ndjson; charset=utf-8");
    resp.body = http::Body::Stream(Box::new(move |w: &mut dyn http::StreamWriter| {
        let res = (|| -> std::io::Result<()> {
            w.write_all(first.as_bytes())?;
            w.flush()?;
            while let Ok(m) = rx.recv() {
                if w.peer_closed() {
                    break;
                }
                if let Msg::Line(l) = m {
                    w.write_all(l.as_bytes())?;
                    w.flush()?;
                }
            }
            Ok(())
        })();
        // Stops the search when the client went away; the thread exits at its next send.
        cancel.cancel();
        drop(rx);
        let _ = handle.join();
        res
    }));
    resp
}

/// The search thread: runs the search, streams it into `tx`, and closes the querier.
fn run_search(job: SearchJob, tx: SyncSender<Msg>) {
    let SearchJob { endpoint, mut q, sp, hints, label_name, include_metadata, meta, ctx } = job;
    let mut sink = ChannelSink { tx };
    {
        let searcher = q.as_searcher().expect("checked before");
        let include_score = sp.include_score;
        let score = move |sr: &SearchResult| if include_score { Some(sr.score) } else { None };
        match endpoint {
            SearchEndpoint::MetricNames => {
                // Built on the first result, so an empty search never reads the targets.
                let mut meta_map: Option<Option<Arc<MetricMetadataMap>>> = None;
                let rs = label_values_search(&ctx, searcher, model::labels::METRIC_NAME, &sp.matcher_sets, &hints);
                stream_search_results(&ctx, &mut sink, rs, &sp, |sr| {
                    let mut result = SearchMetricNameResult { name: sr.value.clone(), score: score(sr), ..Default::default() };
                    if include_metadata {
                        let m = meta_map.get_or_insert_with(|| meta.build_metric_metadata_map(&ctx));
                        if let Some(m) = m
                            && let Some(md) = metadata_for_metric(m, &sr.value)
                        {
                            result.typ = md.typ.as_str().to_string();
                            result.help = md.help.clone();
                            result.unit = md.unit.clone();
                        }
                    }
                    result
                });
            }
            SearchEndpoint::LabelNames => {
                let rs = label_names_search(&ctx, searcher, &sp.matcher_sets, &hints);
                stream_search_results(&ctx, &mut sink, rs, &sp, |sr| SearchLabelNameResult {
                    name: sr.value.clone(),
                    score: score(sr),
                });
            }
            SearchEndpoint::LabelValues => {
                let rs = label_values_search(&ctx, searcher, &label_name, &sp.matcher_sets, &hints);
                stream_search_results(&ctx, &mut sink, rs, &sp, |sr| SearchLabelValueResult {
                    value: sr.value.clone(),
                    score: score(sr),
                });
            }
        }
    }
    let _ = q.close();
}
