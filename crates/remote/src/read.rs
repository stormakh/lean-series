//! Port of `storage/remote/read.go`: a queryable over a remote read client, with external labels, required
//! matchers and the "prefer local storage" time range cut.

use std::sync::Arc;

use chunkenc::AnyIterator;
use model::labels::{Builder, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{
    err_series_set, new_series_set_to_chunk_set, noop_chunked_querier, noop_querier, noop_series_set, Annotations,
    BoxChunkQuerier, BoxChunkSeriesSet, BoxQuerier, BoxSeriesSet, ChunkQuerier, ChunkQueryable, Context, Error,
    LabelHints, LabelQuerier, LabelResult, Querier, Queryable, SelectHints, Series, SeriesArc, SeriesSet,
};

use crate::client::ReadClient;
use crate::codec::to_query;

/// Go `startTimeCallback`: the oldest timestamp stored in the local storage.
pub type StartTimeCallback = Arc<dyn Fn() -> Result<i64, Error> + Send + Sync>;

/// Go `sampleAndChunkQueryableClient`.
pub struct SampleAndChunkQueryableClient {
    client: Arc<dyn ReadClient>,
    external_labels: Labels,
    required_matchers: Vec<Matcher>,
    read_recent: bool,
    callback: Option<StartTimeCallback>,
}

/// Go `NewSampleAndChunkQueryableClient`. `callback` may be `None` only when `read_recent` is true.
pub fn new_sample_and_chunk_queryable_client(
    c: Arc<dyn ReadClient>,
    external_labels: Labels,
    required_matchers: Vec<Matcher>,
    read_recent: bool,
    callback: Option<StartTimeCallback>,
) -> SampleAndChunkQueryableClient {
    SampleAndChunkQueryableClient { client: c, external_labels, required_matchers, read_recent, callback }
}

impl SampleAndChunkQueryableClient {
    fn new_querier(&self, mint: i64, maxt: i64) -> RemoteQuerier {
        RemoteQuerier {
            mint,
            maxt,
            client: self.client.clone(),
            external_labels: self.external_labels.clone(),
            required_matchers: self.required_matchers.clone(),
        }
    }

    /// Go `preferLocalStorage`: `noop` when the local TSDB answers the whole range; else the reduced maxt.
    fn prefer_local_storage(&self, mint: i64, maxt: i64) -> Result<(i64, bool), Error> {
        let cb = self.callback.as_ref().expect("remote read: no start time callback");
        let local_start_time = cb()?;
        let mut cmaxt = maxt;
        // Avoid queries whose time range is later than the first timestamp in local DB.
        if mint > local_start_time {
            return Ok((0, true));
        }
        // Query only samples older than the first timestamp in local DB.
        if maxt > local_start_time {
            cmaxt = local_start_time;
        }
        Ok((cmaxt, false))
    }
}

impl Queryable for SampleAndChunkQueryableClient {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let mut q = self.new_querier(mint, maxt);
        if self.read_recent {
            return Ok(Box::new(q));
        }
        let (cmaxt, noop) = self.prefer_local_storage(mint, maxt)?;
        if noop {
            return Ok(noop_querier());
        }
        q.maxt = cmaxt;
        Ok(Box::new(q))
    }
}

impl ChunkQueryable for SampleAndChunkQueryableClient {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let mut q = self.new_querier(mint, maxt);
        if self.read_recent {
            return Ok(Box::new(RemoteChunkQuerier(q)));
        }
        let (cmaxt, noop) = self.prefer_local_storage(mint, maxt)?;
        if noop {
            return Ok(noop_chunked_querier());
        }
        q.maxt = cmaxt;
        Ok(Box::new(RemoteChunkQuerier(q)))
    }
}

/// Go `querier`: reads series from the client.
pub struct RemoteQuerier {
    pub mint: i64,
    pub maxt: i64,
    client: Arc<dyn ReadClient>,
    external_labels: Labels,
    required_matchers: Vec<Matcher>,
}

impl RemoteQuerier {
    /// A querier with only external labels, for tests of [`RemoteQuerier::add_external_labels`].
    pub fn with_external_labels(client: Arc<dyn ReadClient>, external_labels: Labels) -> RemoteQuerier {
        RemoteQuerier { mint: 0, maxt: 0, client, external_labels, required_matchers: Vec::new() }
    }

    /// Go `addExternalLabels`: adds an equality matcher for each external label that has no matcher of the
    /// same name yet. Returns the matchers and the names of the added labels (sorted).
    pub fn add_external_labels(&self, ms: &[Matcher]) -> (Vec<Matcher>, Vec<String>) {
        let mut el: Vec<(&str, &str)> = self.external_labels.iter().collect();
        for m in ms {
            el.retain(|l| l.0 != m.name);
        }
        let mut out = ms.to_vec();
        for (n, v) in &el {
            out.push(Matcher::new(MatchType::Equal, n, v).expect("equality matcher"));
        }
        let names = el.iter().map(|l| l.0.to_string()).collect();
        (out, names)
    }

    fn select_series(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxSeriesSet {
        if !self.required_matchers.is_empty() {
            // Copy, so that the configured list is not changed.
            let mut required = self.required_matchers.clone();
            for m in matchers {
                if let Some(i) = required
                    .iter()
                    .position(|r| m.typ == MatchType::Equal && m.name == r.name && m.value == r.value)
                {
                    // Requirement matched.
                    required.remove(i);
                }
                if required.is_empty() {
                    break;
                }
            }
            if !required.is_empty() {
                return noop_series_set();
            }
        }

        let (m, added) = self.add_external_labels(matchers);
        let query = match to_query(self.mint, self.maxt, &m, hints) {
            Ok(q) => q,
            Err(e) => return err_series_set(Error::wrap("toQuery", e)),
        };
        match self.client.read(ctx, &query, sort_series) {
            Ok(res) => new_series_set_filter(res, added),
            Err(e) => err_series_set(Error::wrap("remote_read", e)),
        }
    }
}

impl LabelQuerier for RemoteQuerier {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        // Not implemented upstream: https://github.com/prometheus/prometheus/issues/3351
        (Vec::new(), Annotations::new(), Some(Error::msg("not implemented")))
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), Some(Error::msg("not implemented")))
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for RemoteQuerier {
    /// Adds equality matchers for the external labels before calling the remote endpoint and removes those
    /// labels from the result. With required matchers, returns an empty set unless every one is given.
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxSeriesSet {
        self.select_series(ctx, sort_series, hints, matchers)
    }
}

/// Go `chunkQuerier`: the querier as a chunk querier (series are encoded into chunks).
pub struct RemoteChunkQuerier(pub RemoteQuerier);

impl LabelQuerier for RemoteChunkQuerier {
    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_values(ctx, name, hints, ms)
    }
    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, ms: &[Matcher]) -> LabelResult {
        self.0.label_names(ctx, hints, ms)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl ChunkQuerier for RemoteChunkQuerier {
    fn select(&self, ctx: &Context, sort_series: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> BoxChunkSeriesSet {
        new_series_set_to_chunk_set(self.0.select_series(ctx, sort_series, hints, matchers))
    }
}

/// Go `newSeriesSetFilter`: removes the labels `to_filter` (sorted) from every series.
pub fn new_series_set_filter(ss: BoxSeriesSet, to_filter: Vec<String>) -> BoxSeriesSet {
    Box::new(SeriesSetFilter { ss, to_filter: Arc::new(to_filter), cur: None })
}

/// Go `seriesSetFilter`.
pub struct SeriesSetFilter {
    ss: BoxSeriesSet,
    to_filter: Arc<Vec<String>>,
    cur: Option<SeriesArc>,
}

impl SeriesSet for SeriesSetFilter {
    fn next(&mut self) -> bool {
        if !self.ss.next() {
            self.cur = None;
            return false;
        }
        let inner = self.ss.at().clone();
        let mut b = Builder::new(inner.labels().clone());
        b.del_many(&self.to_filter);
        let labels = b.labels();
        self.cur = Some(Arc::new(SeriesFilter { inner, labels }));
        true
    }
    fn at(&self) -> &SeriesArc {
        self.cur.as_ref().expect("At called before Next")
    }
    fn err(&self) -> Option<Error> {
        self.ss.err()
    }
    fn warnings(&self) -> Annotations {
        self.ss.warnings()
    }
}

/// Go `seriesFilter`.
struct SeriesFilter {
    inner: SeriesArc,
    labels: Labels,
}

impl Series for SeriesFilter {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        self.inner.iterator(reuse)
    }
}
