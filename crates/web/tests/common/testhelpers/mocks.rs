//! Port of `web/api/testhelpers/mocks.go`: doubles of the API dependencies.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use chunkenc::{AnyIterator, ChunkIterator, ValueType};
use config::ScrapeConfig;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, Matcher};
use promql::{FPoint, HPoint};
use scrape::Target;
use storage::chunks::{BoxMetaIterator, Meta, MetaIterator};
use storage::{
    Annotations, BoxChunkQuerier, BoxChunkSeriesSet, BoxQuerier, BoxSeriesSet, ChunkQuerier,
    ChunkQueryable, ChunkSeries, ChunkSeriesArc, ChunkSeriesSet, Context, Error, ExemplarQuerier,
    ExemplarQueryable, LabelHints, LabelQuerier, LabelResult, Querier, Queryable, SelectHints,
    Series, SeriesArc, SeriesSet,
};
use web::api::{
    AlertingRuleView, AlertmanagerRetriever, RuleGroupView, RuleRef, RulesRetriever,
    ScrapePoolsRetriever, TargetRetriever,
};

/// Go `FakeQueryable`.
pub struct FakeQueryable {
    pub series: Vec<SeriesArc>,
}

impl Queryable for FakeQueryable {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(FakeQuerier {
            series: self.series.clone(),
        }))
    }
}

impl ChunkQueryable for FakeQueryable {
    fn chunk_querier(&self, _mint: i64, _maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Ok(Box::new(FakeQuerier {
            series: self.series.clone(),
        }))
    }
}

/// Go `FakeQuerier` and `FakeChunkQuerier` (the same label methods). Label values and names are collected in a
/// set (Go: a map, so its order is random; the API sorts them).
pub struct FakeQuerier {
    pub series: Vec<SeriesArc>,
}

impl LabelQuerier for FakeQuerier {
    fn label_values(
        &self,
        _: &Context,
        name: &str,
        _: Option<&LabelHints>,
        _: &[Matcher],
    ) -> LabelResult {
        let mut values = BTreeSet::new();
        for s in &self.series {
            let v = s.labels().get(name);
            if !v.is_empty() {
                values.insert(v.to_string());
            }
        }
        (values.into_iter().collect(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        let mut names = BTreeSet::new();
        for s in &self.series {
            s.labels().range(|n, _| {
                names.insert(n.to_string());
            });
        }
        (names.into_iter().collect(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for FakeQuerier {
    /// Every series, whatever the matchers (as Go's fake).
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, _: &[Matcher]) -> BoxSeriesSet {
        Box::new(FakeSeriesSet {
            series: self.series.clone(),
            idx: -1,
        })
    }
}

impl ChunkQuerier for FakeQuerier {
    fn select(
        &self,
        _: &Context,
        _: bool,
        _: Option<&SelectHints>,
        _: &[Matcher],
    ) -> BoxChunkSeriesSet {
        Box::new(FakeChunkSeriesSet {
            series: self
                .series
                .iter()
                .map(|s| Arc::new(FakeChunkSeries { series: s.clone() }) as ChunkSeriesArc)
                .collect(),
            idx: -1,
        })
    }
}

/// Go `FakeSeriesSet`.
pub struct FakeSeriesSet {
    series: Vec<SeriesArc>,
    idx: isize,
}

impl SeriesSet for FakeSeriesSet {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.series.len()
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `FakeChunkSeriesSet`.
pub struct FakeChunkSeriesSet {
    series: Vec<ChunkSeriesArc>,
    idx: isize,
}

impl ChunkSeriesSet for FakeChunkSeriesSet {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.series.len()
    }
    fn at(&self) -> &ChunkSeriesArc {
        &self.series[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// Go `FakeChunkSeries`: no chunks.
pub struct FakeChunkSeries {
    series: SeriesArc,
}

impl ChunkSeries for FakeChunkSeries {
    fn labels(&self) -> &Labels {
        self.series.labels()
    }
    fn iterator<'a>(&'a self, _reuse: Option<BoxMetaIterator<'a>>) -> BoxMetaIterator<'a> {
        Box::new(FakeChunkSeriesIterator)
    }
}

/// Go `FakeChunkSeriesIterator`.
pub struct FakeChunkSeriesIterator;

impl MetaIterator for FakeChunkSeriesIterator {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &Meta {
        unreachable!("FakeChunkSeriesIterator has no chunks")
    }
    fn err(&self) -> Option<Error> {
        None
    }
}

/// Go `FakeSeries`.
pub struct FakeSeries {
    pub labels: Labels,
    pub samples: Vec<FPoint>,
}

impl Series for FakeSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(FakeSeriesIterator {
            samples: &self.samples,
            idx: -1,
        }))
    }
}

/// Go `FakeSeriesIterator`.
pub struct FakeSeriesIterator<'a> {
    samples: &'a [FPoint],
    idx: isize,
}

impl ChunkIterator for FakeSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        self.idx += 1;
        if (self.idx as usize) < self.samples.len() {
            ValueType::Float
        } else {
            ValueType::None
        }
    }
    fn seek(&mut self, t: i64) -> ValueType {
        while self.idx < self.samples.len() as isize - 1 {
            self.idx += 1;
            if self.samples[self.idx as usize].t >= t {
                return ValueType::Float;
            }
        }
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        let s = &self.samples[self.idx as usize];
        (s.t, s.f)
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("not implemented")
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        panic!("not implemented")
    }
    fn at_t(&self) -> i64 {
        self.samples[self.idx as usize].t
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

/// Go `FakeHistogramSeries`.
pub struct FakeHistogramSeries {
    pub labels: Labels,
    pub histograms: Vec<HPoint>,
}

impl Series for FakeHistogramSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(FakeHistogramSeriesIterator {
            histograms: &self.histograms,
            idx: -1,
        }))
    }
}

/// Go `FakeHistogramSeriesIterator`.
pub struct FakeHistogramSeriesIterator<'a> {
    histograms: &'a [HPoint],
    idx: isize,
}

impl ChunkIterator for FakeHistogramSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        self.idx += 1;
        if (self.idx as usize) < self.histograms.len() {
            ValueType::FloatHistogram
        } else {
            ValueType::None
        }
    }
    fn seek(&mut self, t: i64) -> ValueType {
        while self.idx < self.histograms.len() as isize - 1 {
            self.idx += 1;
            if self.histograms[self.idx as usize].t >= t {
                return ValueType::FloatHistogram;
            }
        }
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        panic!("not a float value")
    }
    fn at_histogram(&self, _h: Option<Histogram>) -> (i64, Histogram) {
        panic!("not implemented")
    }
    fn at_float_histogram(&self, _fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        let h = &self.histograms[self.idx as usize];
        (h.t, (*h.h).clone())
    }
    fn at_t(&self) -> i64 {
        self.histograms[self.idx as usize].t
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

/// Go `FakeExemplarQueryable`.
pub struct FakeExemplarQueryable;

impl ExemplarQueryable for FakeExemplarQueryable {
    fn exemplar_querier(&self, _ctx: &Context) -> Result<Box<dyn ExemplarQuerier>, Error> {
        Ok(Box::new(FakeExemplarQuerier))
    }
}

/// Go `FakeExemplarQuerier`.
pub struct FakeExemplarQuerier;

impl ExemplarQuerier for FakeExemplarQuerier {
    fn select(
        &self,
        _start: i64,
        _end: i64,
        _matchers: &[Vec<Matcher>],
    ) -> Result<Vec<model::exemplar::QueryResult>, Error> {
        Ok(Vec::new())
    }
}

/// Go `FakeRulesRetriever`.
pub struct FakeRulesRetriever {
    pub groups: Vec<Arc<dyn RuleGroupView>>,
}

impl RulesRetriever for FakeRulesRetriever {
    fn rule_groups(&self) -> Vec<Arc<dyn RuleGroupView>> {
        self.groups.clone()
    }
    fn alerting_rules(&self) -> Vec<Arc<dyn AlertingRuleView>> {
        let mut out = Vec::new();
        for g in &self.groups {
            for r in g.rules(&[]) {
                if let RuleRef::Alerting(a) = r {
                    out.push(a);
                }
            }
        }
        out
    }
}

/// Go `FakeTargetRetriever`.
#[derive(Default)]
pub struct FakeTargetRetriever {
    pub active: HashMap<String, Vec<Arc<Target>>>,
    pub dropped: HashMap<String, Vec<Arc<Target>>>,
    pub dropped_counts: HashMap<String, usize>,
    pub scrape_config: HashMap<String, Arc<ScrapeConfig>>,
}

impl TargetRetriever for FakeTargetRetriever {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.active.clone()
    }
    fn targets_dropped(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.dropped.clone()
    }
    fn targets_dropped_counts(&self) -> HashMap<String, usize> {
        self.dropped_counts.clone()
    }
    /// adapted: Go returns a nil config without an error for an unknown pool; the Rust trait has no nil config,
    /// so an unknown pool is an error.
    fn scrape_pool_config(&self, name: &str) -> Result<Arc<ScrapeConfig>, Error> {
        self.scrape_config
            .get(name)
            .cloned()
            .ok_or_else(|| Error::msg(format!("scrape pool {name:?} not found")))
    }
}

/// Go `FakeScrapePoolsRetriever`.
#[derive(Default)]
pub struct FakeScrapePoolsRetriever {
    pub pools: Vec<String>,
}

impl ScrapePoolsRetriever for FakeScrapePoolsRetriever {
    fn scrape_pools(&self) -> Vec<String> {
        self.pools.clone()
    }
}

/// Go `FakeAlertmanagerRetriever`.
pub struct FakeAlertmanagerRetriever;

impl AlertmanagerRetriever for FakeAlertmanagerRetriever {
    fn alertmanagers(&self) -> Vec<String> {
        Vec::new()
    }
    fn dropped_alertmanagers(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Go `TSDBNotReadyQueryable`: every querier fails with `tsdb.ErrNotReady`.
pub struct TsdbNotReadyQueryable;

impl Queryable for TsdbNotReadyQueryable {
    fn querier(&self, _mint: i64, _maxt: i64) -> Result<BoxQuerier, Error> {
        Err(tsdb::db::ERR_NOT_READY)
    }
}

impl ChunkQueryable for TsdbNotReadyQueryable {
    fn chunk_querier(&self, _mint: i64, _maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Err(tsdb::db::ERR_NOT_READY)
    }
}

/// Go `NewEmptyQueryable`.
pub fn new_empty_queryable() -> FakeQueryable {
    FakeQueryable { series: Vec::new() }
}

/// Go `NewQueryableWithSeries`.
pub fn new_queryable_with_series(series: Vec<SeriesArc>) -> FakeQueryable {
    FakeQueryable { series }
}

/// Go `NewEmptyRulesRetriever`.
pub fn new_empty_rules_retriever() -> FakeRulesRetriever {
    FakeRulesRetriever { groups: Vec::new() }
}

/// Go `NewRulesRetrieverWithGroups`.
pub fn new_rules_retriever_with_groups(groups: Vec<Arc<dyn RuleGroupView>>) -> FakeRulesRetriever {
    FakeRulesRetriever { groups }
}

/// Go `NewEmptyTargetRetriever`.
pub fn new_empty_target_retriever() -> FakeTargetRetriever {
    FakeTargetRetriever::default()
}
