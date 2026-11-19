//! Port of `promql/estimate_resource_test.go`.
//!
//! Units: times are milliseconds, durations nanoseconds. Go's type assertion `q.(storage.CostSamplingQueryable)`
//! is the explicit `sampling` argument of `estimate_cost`.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use chunkenc::{AnyIterator, new_nop_iterator};
use model::labels;
use model::matcher::Matcher;
use promql::estimate::{CostEstimate, ExprParser, estimate_cost};
use promql::parser::{self, Expr, Parser};
use promql::promqltest::{self, TestStorage};
use remote::prompb;
use storage::{
    Annotations, BoxChunkQuerier, BoxChunkSeriesSet, BoxMetaIterator, BoxQuerier, BoxSeriesSet, CancelToken,
    ChunkQuerier, ChunkQueryable, ChunkSeriesArc, ChunkSeriesEntry, ChunkSeriesSet, Context, CostSamplingQueryable,
    ERR_CANCELED, ERR_COST_SAMPLING_UNSUPPORTED, Error, LabelHints, LabelQuerier, LabelResult, Querier, Queryable,
    SampleAndChunkQueryable, SelectHints, SeriesArc, SeriesEntry, SeriesSet, Storage,
};

const MS: i64 = 1_000_000;
const SECOND: i64 = 1000 * MS;
const MINUTE: i64 = 60 * SECOND;

fn estimate_test_parser() -> Parser {
    Parser::new(parser::Options::default())
}

#[test]
fn test_estimate_cost_remote_sampling_fallback() {
    for kind in ["float", "histogram"] {
        const POINTS: usize = 200_000;
        let series: SeriesArc = if kind == "float" {
            let timestamps: Vec<i64> = (0..POINTS as i64).collect();
            let values: Vec<f64> = (0..POINTS).map(|i| i as f64).collect();
            Arc::new(SeriesEntry {
                lset: labels::from_strings(&["__name__", "metric"]),
                data: (timestamps, values),
                sample_iterator_fn: |d, _| {
                    AnyIterator::Other(Box::new(chunkenc::mock_series_iterator(Vec::new(), d.0.clone(), d.1.clone())))
                },
            })
        } else {
            let hs = storage::tsdbutil::generate_test_histograms(1000);
            let histograms: Vec<prompb::Histogram> =
                hs.iter().enumerate().map(|(i, h)| prompb::from_int_histogram(i as i64, h)).collect();
            let mut set = remote::codec::from_query_result(
                false,
                prompb::QueryResult {
                    timeseries: vec![prompb::TimeSeries {
                        labels: vec![prompb::Label::new("__name__", "metric")],
                        histograms,
                        ..Default::default()
                    }],
                },
            );
            assert!(set.next(), "{kind}");
            set.at().clone()
        };
        let counted = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(CostProbeReadClient {
            series: Arc::new(SampleReadSeries { inner: series, owner: counted.clone() }),
            requests: Mutex::new(Vec::new()),
        });
        let q = remote::read::new_sample_and_chunk_queryable_client(
            client.clone(),
            labels::Labels::default(),
            Vec::new(),
            true,
            None,
        );
        let ts = 200_000;
        // Go's type assertion q.(storage.CostSamplingQueryable) fails for the remote client: no sampling source.
        let (estimate, annos, err) = estimate_cost(
            &Context::background(),
            &q,
            None,
            &estimate_test_parser(),
            "sum_over_time(metric[200s]) + sum_over_time(metric[200s])",
            ts,
            ts,
            0,
            MINUTE,
            MINUTE,
            MS,
        );
        assert!(err.is_none(), "{kind}: {err:?}");
        assert_eq!(2, estimate.series_touched, "{kind}");
        assert_eq!((2 * (POINTS + 1)) as i64, estimate.samples_read, "{kind}");
        let requests = client.requests.lock().unwrap();
        assert_eq!(2, requests.len(), "{kind}: one enumeration request per selector; no sampling requests");
        for req in requests.iter() {
            assert_eq!("series", req.hints.as_ref().unwrap().func, "{kind}");
        }
        assert_eq!(
            0,
            counted.load(Ordering::SeqCst),
            "{kind}: remote adapter must never decode and re-encode a whole series"
        );
        assert_eq!(1, annos.as_errors().len(), "{kind}: fallback warning is deduplicated");
        assert!(annos.as_errors()[0].to_string().contains("bounded storage sampling is unavailable"), "{kind}");
    }
}

/// Go `costProbeReadClient`.
struct CostProbeReadClient {
    series: SeriesArc,
    requests: Mutex<Vec<prompb::Query>>,
}

impl remote::client::ReadClient for CostProbeReadClient {
    fn read(&self, _: &Context, query: &prompb::Query, _: bool) -> Result<BoxSeriesSet, Error> {
        self.requests.lock().unwrap().push(query.clone());
        Ok(Box::new(CostProbeSeriesSet { series: self.series.clone(), limit: 1, position: 0, on_done: None, on_next: None }))
    }
    fn read_multiple(&self, ctx: &Context, queries: &[prompb::Query], sorted: bool) -> Result<BoxSeriesSet, Error> {
        self.read(ctx, &queries[0], sorted)
    }
}

/// Go `sampleReadSeries` (engine_test.go): counts sample accesses through the series' iterators.
struct SampleReadSeries {
    inner: SeriesArc,
    owner: Arc<AtomicUsize>,
}

impl storage::Series for SampleReadSeries {
    fn labels(&self) -> &labels::Labels {
        self.inner.labels()
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(SampleReadIterator { inner: self.inner.iterator(None), owner: &self.owner }))
    }
}

/// Go `sampleReadIterator`.
struct SampleReadIterator<'a> {
    inner: AnyIterator<'a>,
    owner: &'a AtomicUsize,
}

impl chunkenc::ChunkIterator for SampleReadIterator<'_> {
    fn next(&mut self) -> chunkenc::ValueType {
        self.inner.next()
    }
    fn seek(&mut self, t: i64) -> chunkenc::ValueType {
        self.inner.seek(t)
    }
    fn at(&self) -> (i64, f64) {
        self.owner.fetch_add(1, Ordering::SeqCst);
        self.inner.at()
    }
    fn at_histogram(&self, h: Option<model::histogram::Histogram>) -> (i64, model::histogram::Histogram) {
        self.owner.fetch_add(1, Ordering::SeqCst);
        self.inner.at_histogram(h)
    }
    fn at_float_histogram(
        &self,
        h: Option<model::histogram::FloatHistogram>,
    ) -> (i64, model::histogram::FloatHistogram) {
        self.owner.fetch_add(1, Ordering::SeqCst);
        self.inner.at_float_histogram(h)
    }
    fn at_t(&self) -> i64 {
        self.inner.at_t()
    }
    fn at_st(&self) -> i64 {
        self.inner.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.inner.err()
    }
}

type Hook = Arc<dyn Fn() + Send + Sync>;

/// Go `costProbeSeriesSet`.
struct CostProbeSeriesSet {
    series: SeriesArc,
    limit: usize,
    position: usize,
    on_done: Option<Hook>,
    on_next: Option<Hook>,
}

impl SeriesSet for CostProbeSeriesSet {
    fn next(&mut self) -> bool {
        if let Some(f) = &self.on_next {
            f();
        }
        if self.position == self.limit {
            if let Some(f) = &self.on_done {
                f();
            }
            return false;
        }
        self.position += 1;
        true
    }
    fn at(&self) -> &SeriesArc {
        &self.series
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `costProbeChunkSet`.
struct CostProbeChunkSet {
    series: ChunkSeriesArc,
    limit: usize,
    position: usize,
    on_done: Option<Hook>,
    on_next: Option<Hook>,
}

impl ChunkSeriesSet for CostProbeChunkSet {
    fn next(&mut self) -> bool {
        if let Some(f) = &self.on_next {
            f();
        }
        if self.position == self.limit {
            if let Some(f) = &self.on_done {
                f();
            }
            return false;
        }
        self.position += 1;
        true
    }
    fn at(&self) -> &ChunkSeriesArc {
        &self.series
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

type SelectSeries = Arc<dyn Fn(&SelectHints) -> BoxSeriesSet + Send + Sync>;
type SelectChunks = Arc<dyn Fn() -> BoxChunkSeriesSet + Send + Sync>;
type OnClose = Arc<dyn Fn() -> Option<Error> + Send + Sync>;

/// Go `costProbeQuerier`. Go hands out the same querier value on every `Querier` call; here every call gets a
/// querier with the same functions.
#[derive(Clone)]
struct CostProbeQuerier {
    select_series: SelectSeries,
    on_close: Option<OnClose>,
}

impl LabelQuerier for CostProbeQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        match &self.on_close {
            Some(f) => f(),
            None => None,
        }
    }
}

impl Querier for CostProbeQuerier {
    fn select(&self, _: &Context, _: bool, hints: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        (self.select_series)(hints.expect("hints"))
    }
}

/// Go `costProbeChunkQuerier`; `None` is Go's `storage.NoopChunkedQuerier()`.
#[derive(Clone)]
struct CostProbeChunkQuerier {
    select_chunks: Option<SelectChunks>,
}

impl LabelQuerier for CostProbeChunkQuerier {
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

impl ChunkQuerier for CostProbeChunkQuerier {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxChunkSeriesSet {
        match &self.select_chunks {
            Some(f) => f(),
            None => storage::noop_chunked_series_set(),
        }
    }
}

/// Go `costProbeQueryable`: its own cost sampling source.
struct CostProbeQueryable {
    q: CostProbeQuerier,
    cq: CostProbeChunkQuerier,
}

impl Queryable for CostProbeQueryable {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(self.q.clone()))
    }
}

impl ChunkQueryable for CostProbeQueryable {
    fn chunk_querier(&self, _: i64, _: i64) -> Result<BoxChunkQuerier, Error> {
        Ok(Box::new(self.cq.clone()))
    }
}

impl CostSamplingQueryable for CostProbeQueryable {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        (Some(self), true)
    }
}

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn get(c: &AtomicUsize) -> usize {
    c.load(Ordering::SeqCst)
}

#[test]
fn test_estimate_cost_sampling_series_budgets() {
    let (density_next, point_next, point_iterators) = (counter(), counter(), counter());
    let series: SeriesArc = Arc::new(SeriesEntry {
        lset: labels::from_strings(&["__name__", "metric"]),
        data: point_iterators.clone(),
        sample_iterator_fn: |n, _| {
            n.fetch_add(1, Ordering::SeqCst);
            AnyIterator::Other(Box::new(new_nop_iterator()))
        },
    });
    let empty_chunks: ChunkSeriesArc = Arc::new(ChunkSeriesEntry {
        lset: series.labels().clone(),
        data: (),
        chunk_iterator_fn: |_, _| -> BoxMetaIterator<'_> { storage::new_list_chunk_series_iterator(Vec::new()) },
    });
    let (s, pn) = (series.clone(), point_next.clone());
    let (ec, dn) = (empty_chunks.clone(), density_next.clone());
    let q = CostProbeQueryable {
        q: CostProbeQuerier {
            select_series: Arc::new(move |hints| {
                let mut set =
                    CostProbeSeriesSet { series: s.clone(), limit: 100, position: 0, on_done: None, on_next: None };
                if hints.func != "series" {
                    let pn = pn.clone();
                    set.on_next = Some(Arc::new(move || {
                        pn.fetch_add(1, Ordering::SeqCst);
                    }));
                }
                Box::new(set)
            }),
            on_close: None,
        },
        cq: CostProbeChunkQuerier {
            select_chunks: Some(Arc::new(move || {
                let dn = dn.clone();
                Box::new(CostProbeChunkSet {
                    series: ec.clone(),
                    limit: 100,
                    position: 0,
                    on_done: None,
                    on_next: Some(Arc::new(move || {
                        dn.fetch_add(1, Ordering::SeqCst);
                    })),
                })
            })),
        },
    };
    let ts = 200_000;
    let (estimate, _, err) = estimate_cost(
        &Context::background(),
        &q,
        Some(&q),
        &estimate_test_parser(),
        "metric[200s]",
        ts,
        ts,
        0,
        MINUTE,
        MINUTE,
        SECOND,
    );
    assert!(err.is_none(), "{err:?}");
    assert_eq!(100, estimate.series_touched);
    assert_eq!(50, get(&density_next), "empty/tombstoned series must not escape the density series budget");
    assert_eq!(50, get(&point_next), "must not materialize a 51st series");
    assert_eq!(50, get(&point_iterators));
}

/// Go `costResourceParser`: calls a hook before parsing.
struct CostResourceParser {
    parser: Parser,
    before_parse: Box<dyn Fn()>,
}

impl ExprParser for CostResourceParser {
    fn parse_expr(&self, input: &str) -> Result<Expr, parser::Error> {
        (self.before_parse)();
        self.parser.parse_expr(input)
    }
}

#[test]
fn test_estimate_cost_cancellation_at_boundaries() {
    for stage in [
        "before parsing",
        "selector-free parsing",
        "invalid query parsing",
        "empty series",
        "series exhaustion",
        "empty density",
        "empty points",
        "reader close",
    ] {
        let (ctx, cancel) = Context::background().with_cancel();
        let (selects, chunk_selects, parses) = (counter(), counter(), counter());
        let series = storage::new_list_series(
            labels::from_strings(&["__name__", "metric"]),
            storage::chunks::generate_samples(0, 1),
        );
        let samples = storage::chunks::generate_samples(0, 1);
        let chunk_series = storage::new_list_chunk_series_from_samples(series.labels().clone(), &[&samples]);
        let cancel_hook = |c: &CancelToken| -> Hook {
            let c = c.clone();
            Arc::new(move || c.cancel())
        };

        let (sel, s, ch) = (selects.clone(), series.clone(), cancel.clone());
        let ch2 = cancel.clone();
        let (csel, cs, ch3) = (chunk_selects.clone(), chunk_series.clone(), cancel.clone());
        let q = CostProbeQueryable {
            q: CostProbeQuerier {
                select_series: Arc::new(move |hints| {
                    sel.fetch_add(1, Ordering::SeqCst);
                    let mut set =
                        CostProbeSeriesSet { series: s.clone(), limit: 1, position: 0, on_done: None, on_next: None };
                    if hints.func == "series" {
                        if stage == "empty series" {
                            set.limit = 0;
                        }
                        if stage == "empty series" || stage == "series exhaustion" {
                            set.on_done = Some(cancel_hook(&ch));
                        }
                    } else if stage == "empty points" {
                        set.limit = 0;
                        set.on_done = Some(cancel_hook(&ch));
                    }
                    Box::new(set)
                }),
                on_close: Some(Arc::new(move || {
                    if stage == "reader close" {
                        ch2.cancel();
                    }
                    None
                })),
            },
            cq: CostProbeChunkQuerier {
                select_chunks: Some(Arc::new(move || {
                    csel.fetch_add(1, Ordering::SeqCst);
                    let mut set =
                        CostProbeChunkSet { series: cs.clone(), limit: 1, position: 0, on_done: None, on_next: None };
                    if stage == "empty density" {
                        set.limit = 0;
                        set.on_done = Some(cancel_hook(&ch3));
                    }
                    Box::new(set)
                })),
            },
        };
        let (ps, ch4) = (parses.clone(), cancel.clone());
        let p = CostResourceParser {
            parser: estimate_test_parser(),
            before_parse: Box::new(move || {
                ps.fetch_add(1, Ordering::SeqCst);
                if stage == "selector-free parsing" || stage == "invalid query parsing" {
                    ch4.cancel();
                }
            }),
        };
        let mut expr = "metric[200s]";
        if stage == "selector-free parsing" {
            expr = "1";
        }
        if stage == "invalid query parsing" {
            expr = "broken(";
        }
        if stage == "before parsing" {
            cancel.cancel();
        }
        let ts = 200_000;
        let (estimate, _, err) = estimate_cost(&ctx, &q, Some(&q), &p, expr, ts, ts, 0, MINUTE, MINUTE, SECOND);
        let err = err.unwrap_or_else(|| panic!("{stage}: no error"));
        assert!(err.is(&ERR_CANCELED), "{stage}: {err}");
        assert_eq!(CostEstimate::default(), estimate, "{stage}: no successful or partial estimate after cancellation");
        if stage == "invalid query parsing" {
            assert!(
                matches!(err.downcast_ref::<parser::Error>(), Some(parser::Error::Parse(_))),
                "{stage}: {err}"
            );
        }
        if stage == "before parsing" {
            assert_eq!(0, get(&parses), "{stage}");
        }
        if stage == "before parsing" || stage == "selector-free parsing" || stage == "invalid query parsing" {
            assert_eq!(0, get(&selects), "{stage}");
        }
        if stage == "empty series" || stage == "series exhaustion" {
            assert_eq!(0, get(&chunk_selects), "{stage}");
        }
        if stage == "empty density" {
            assert_eq!(1, get(&selects), "{stage}: no point sampling after cancellation");
        }
    }
}

#[test]
fn test_estimate_cost_querier_close_errors() {
    let want = Error::msg("reader close failed");
    let w = want.clone();
    let q = CostProbeQueryable {
        q: CostProbeQuerier {
            select_series: Arc::new(|_| storage::noop_series_set()),
            on_close: Some(Arc::new(move || Some(w.clone()))),
        },
        cq: CostProbeChunkQuerier { select_chunks: None },
    };
    let ts = 200_000;
    let (estimate, _, err) = estimate_cost(
        &Context::background(),
        &q,
        Some(&q),
        &estimate_test_parser(),
        "metric",
        ts,
        ts,
        0,
        MINUTE,
        MINUTE,
        SECOND,
    );
    let err = err.expect("an error");
    assert!(err.is(&want), "{err}");
    assert_eq!(CostEstimate::default(), estimate);
}

/// Go `costSamplingOverrideQueryable`.
struct CostSamplingOverrideQueryable<'a> {
    queryable: &'a dyn Queryable,
    source: UnsupportedPointCostQueryable<'a>,
}

impl Queryable for CostSamplingOverrideQueryable<'_> {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.queryable.querier(mint, maxt)
    }
}

impl CostSamplingQueryable for CostSamplingOverrideQueryable<'_> {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        (Some(&self.source), true)
    }
}

/// Go `unsupportedPointCostQueryable`.
struct UnsupportedPointCostQueryable<'a>(&'a dyn SampleAndChunkQueryable);

impl Queryable for UnsupportedPointCostQueryable<'_> {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Err(ERR_COST_SAMPLING_UNSUPPORTED)
    }
}

impl ChunkQueryable for UnsupportedPointCostQueryable<'_> {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        self.0.chunk_querier(mint, maxt)
    }
}

#[test]
fn test_estimate_cost_sampling_fallback_discards_density() {
    let store: TestStorage = promqltest::loaded_storage("\nload 10s\n  metric 0+1x20\n").unwrap();
    let (source, supported) = store.as_cost_sampling_queryable().unwrap().cost_sampling_queryable();
    assert!(supported);
    let q = CostSamplingOverrideQueryable {
        queryable: &store,
        source: UnsupportedPointCostQueryable(source.unwrap()),
    };
    let ts = 200_000;
    let (estimate, annos, err) = estimate_cost(
        &Context::background(),
        &q,
        Some(&q),
        &estimate_test_parser(),
        "metric[200s]",
        ts,
        ts,
        0,
        MINUTE,
        MINUTE,
        SECOND,
    );
    assert!(err.is_none(), "{err:?}");
    assert_eq!(1, estimate.series_touched);
    assert_eq!(201, estimate.samples_read, "later unsupported point sampling must discard the measured density and window cap");
    assert_eq!(1, annos.as_errors().len());
    assert!(annos.as_errors()[0].to_string().contains("bounded storage sampling is unavailable"));
    assert!(store.close().is_none());
}
