//! An in-memory test storage with the observable behavior of the TSDB head that `util/teststorage` opens
//! (`tsdb.DefaultOptions` with 24h blocks, out-of-order window 0, exemplars on).
//!
//! It is a stand-in for the real TSDB until the `tsdb` crate exists; it implements [`storage::Storage`], so it can
//! be swapped for it. What queries can observe is ported from the head:
//! - Series are created on append (`headAppenderBase.getOrCreate`, same errors) and get increasing references.
//!   Unsorted selects return series in reference order (Go: postings order), sorted selects by labels.
//! - Samples go into real `chunkenc` chunks with the head's chunk cutting (`memSeries.appendPreprocessor`,
//!   `histogramsAppendPreprocessor`, `appendHistogram`, `appendFloatHistogram`): 120 samples per float chunk,
//!   size-based histogram chunks, cuts at chunk range boundaries, on encoding changes and on counter resets or
//!   layout changes the histogram appender cannot recode. Iterators are the `chunkenc` iterators, so counter reset
//!   hints are the chunk header hints as in Prometheus.
//! - Appends: validation, `ErrOutOfBounds` below `max(head maxt - chunkRange/2, minValidTime)`, out-of-order and
//!   duplicate errors at append time against committed data, batching by sample type, stale float markers turned
//!   into histogram stale markers for histogram series, and the re-check at commit that silently drops in-batch
//!   out-of-order samples and duplicates (`commitFloats`, `commitHistograms`, `commitFloatHistograms`).
//! - Selects trim samples to the hinted (or querier) time range unless `disable_trimming` is set (Go
//!   `blockBaseSeriesSet`), and keep stale markers.
//! - Optional start-timestamp storage (`EnableSTStorage`, XOR2 float chunks, histogram ST chunks) as
//!   `promqltest.NewBuiltinTestStorage` configures it.
//!
//! Not ported: out-of-order ingestion (window > 0), the WAL, compaction into blocks (in Go a background
//! compaction moves data older than 36h into blocks; queries see the same samples and chunks), exemplar storage
//! (exemplars are accepted and dropped), metadata storage, isolation between a querier and later commits (a select
//! sees what was committed when it ran).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chunkenc::{AnyIterator, Chunk, ChunkIterator, Encoding, ValueType};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::Matcher;
use model::metadata::Metadata;
use model::value::is_stale_nan;
use storage::error::Sentinel;
use storage::{
    Annotations, AppendOptions, AppendV2Options, Appendable, AppendableV2, Appender,
    AppenderTransaction, AppenderV2, BoxChunkQuerier, BoxQuerier, BoxSeriesSet, ChunkQuerier,
    ChunkQueryable, Context, Error, LabelHints, LabelQuerier, LabelResult, Querier, Queryable,
    SelectHints, Series, SeriesArc, SeriesRef, SeriesSet, Storage,
};

/// Go `tsdb.ErrInvalidSample`.
pub static INVALID_SAMPLE: Sentinel = Sentinel::new("invalid sample");
/// Go `tsdb.ErrInvalidSample`.
pub const ERR_INVALID_SAMPLE: Error = Error::Sentinel(&INVALID_SAMPLE);
/// Go `tsdb.ErrAppenderClosed`.
pub static APPENDER_CLOSED: Sentinel = Sentinel::new("appender closed");
/// Go `tsdb.ErrAppenderClosed`.
pub const ERR_APPENDER_CLOSED: Error = Error::Sentinel(&APPENDER_CLOSED);

/// The options of the test storage (the parts of `tsdb.Options` that matter here).
#[derive(Clone, Debug)]
pub struct Options {
    /// The head chunk range in milliseconds (`MinBlockDuration`; teststorage sets 24h).
    pub chunk_range: i64,
    /// Go `SamplesPerChunk` (default 120).
    pub samples_per_chunk: usize,
    /// Go `EnableSTStorage`: keep start timestamps.
    pub enable_st_storage: bool,
    /// Go `FloatChunkEncoding == chunkenc.EncXOR2`.
    pub use_xor2: bool,
    /// Go `EnableHistogramSTEncoding`.
    pub enable_histogram_st_encoding: bool,
}

impl Default for Options {
    /// The options of Go `teststorage.New(t)` without options.
    fn default() -> Options {
        Options {
            chunk_range: 24 * 3600 * 1000,
            samples_per_chunk: 120,
            enable_st_storage: false,
            use_xor2: false,
            enable_histogram_st_encoding: false,
        }
    }
}

impl Options {
    /// The options of Go `promqltest.NewBuiltinTestStorage` (ST storage, XOR2 floats, histogram ST chunks).
    pub fn builtin() -> Options {
        Options {
            enable_st_storage: true,
            use_xor2: true,
            enable_histogram_st_encoding: true,
            ..Options::default()
        }
    }
}

/// An in-memory storage with the TSDB head's behavior (Go `teststorage.TestStorage`). Cheap to clone; clones
/// share the data.
#[derive(Clone)]
pub struct TestStorage {
    inner: Arc<Inner>,
}

struct Inner {
    opts: Options,
    head: RwLock<Head>,
}

struct Head {
    series: Vec<MemSeries>,
    by_hash: HashMap<u64, Vec<usize>>,
    min_time: i64,
    max_time: i64,
    min_valid_time: i64,
}

/// A closed chunk with its time range.
#[derive(Clone)]
struct ClosedChunk {
    chunk: Arc<dyn Chunk>,
    min_time: i64,
    max_time: i64,
}

struct HeadChunk {
    chunk: Box<dyn Chunk>,
    min_time: i64,
    max_time: i64,
}

struct MemSeries {
    lset: Labels,
    /// Chunks that were cut (Go mmapped chunks and older head chunks), oldest first.
    closed: Vec<ClosedChunk>,
    /// The open head chunk.
    head: Option<HeadChunk>,
    app: Option<chunkenc::Appender>,
    next_at: i64,
    histogram_chunk_end_computed: bool,
    last_value: f64,
    last_histogram: Option<Histogram>,
    last_float_histogram: Option<FloatHistogram>,
}

#[derive(Clone, Copy)]
struct ChunkOpts {
    chunk_range: i64,
    samples_per_chunk: usize,
    use_xor2: bool,
    use_histogram_st: bool,
    store_st: bool,
}

/// Go `rangeForTimestamp`.
fn range_for_timestamp(t: i64, width: i64) -> i64 {
    (t / width) * width + width
}

/// Go `computeChunkEndTime`.
fn compute_chunk_end_time(start: i64, cur: i64, max_t: i64, ratio_to_full: f64) -> i64 {
    let n = (max_t - start) as f64 / ((cur - start + 1) as f64 * ratio_to_full);
    if n <= 1.0 {
        return max_t;
    }
    (start as f64 + (max_t - start) as f64 / n.floor()) as i64
}

impl MemSeries {
    fn new(lset: Labels) -> MemSeries {
        MemSeries {
            lset,
            closed: Vec::new(),
            head: None,
            app: None,
            next_at: i64::MIN,
            histogram_chunk_end_computed: false,
            last_value: 0.0,
            last_histogram: None,
            last_float_histogram: None,
        }
    }

    /// Go `memSeries.maxTime`.
    fn max_time(&self) -> i64 {
        if let Some(h) = &self.head {
            return h.max_time;
        }
        self.closed.last().map(|c| c.max_time).unwrap_or(i64::MIN)
    }

    /// Go `cutNewHeadChunk`.
    fn cut_new_head_chunk(&mut self, mint: i64, e: Encoding, chunk_range: i64) {
        if let Some(old) = self.head.take() {
            self.closed.push(ClosedChunk {
                chunk: Arc::from(old.chunk),
                min_time: old.min_time,
                max_time: old.max_time,
            });
        }
        let mut chunk = if chunkenc::is_valid_encoding(e) {
            chunkenc::new_empty_chunk(e).expect("valid encoding")
        } else {
            chunkenc::new_empty_chunk(chunkenc::ENC_XOR).expect("valid encoding")
        };
        self.next_at = range_for_timestamp(mint, chunk_range);
        self.app = Some(chunk.appender().expect("appender of an empty chunk"));
        self.head = Some(HeadChunk {
            chunk,
            min_time: mint,
            max_time: i64::MIN,
        });
    }

    /// Go `memSeries.appendPreprocessor`. Returns (sampleInOrder, chunkCreated).
    fn append_preprocessor(&mut self, t: i64, e: Encoding, o: ChunkOpts) -> (bool, bool) {
        let mut chunk_created = false;
        if self.head.is_none() {
            if let Some(last) = self.closed.last() {
                if last.max_time >= t {
                    return (false, false);
                }
            }
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        if self.head.as_ref().unwrap().max_time >= t {
            return (false, chunk_created);
        }
        if !chunk_created
            && self.head.as_ref().unwrap().chunk.bytes().len()
                > chunkenc::MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
        {
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        let ce = self.head.as_ref().unwrap().chunk.encoding();
        if ce != e && (!chunkenc::compatible_values(ce, e) || o.store_st) {
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        let num_samples = self.head.as_ref().unwrap().chunk.num_samples();
        if num_samples == 0 {
            let c = self.head.as_mut().unwrap();
            c.min_time = t;
            self.next_at = range_for_timestamp(t, o.chunk_range);
        }
        if num_samples == o.samples_per_chunk / 4 {
            let c = self.head.as_ref().unwrap();
            self.next_at = compute_chunk_end_time(c.min_time, c.max_time, self.next_at, 4.0);
        }
        if t >= self.next_at || num_samples >= o.samples_per_chunk * 2 {
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        (true, chunk_created)
    }

    /// Go `memSeries.histogramsAppendPreprocessor`. Returns (sampleInOrder, chunkCreated).
    fn histograms_append_preprocessor(
        &mut self,
        t: i64,
        e: Encoding,
        o: ChunkOpts,
    ) -> (bool, bool) {
        let mut chunk_created = false;
        if self.head.is_none() {
            if let Some(last) = self.closed.last() {
                if last.max_time >= t {
                    return (false, false);
                }
            }
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        if self.head.as_ref().unwrap().max_time >= t {
            return (false, chunk_created);
        }
        if self.head.as_ref().unwrap().chunk.encoding() != e {
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        let num_samples = self.head.as_ref().unwrap().chunk.num_samples();
        let target_bytes = chunkenc::TARGET_BYTES_PER_HISTOGRAM_CHUNK;
        let num_bytes = self.head.as_ref().unwrap().chunk.bytes().len();
        if num_samples == 0 {
            self.head.as_mut().unwrap().min_time = t;
            self.next_at = range_for_timestamp(t, o.chunk_range);
        }
        let c_min = self.head.as_ref().unwrap().min_time;
        let c_max = self.head.as_ref().unwrap().max_time;
        let next_chunk_range_start = if self.histogram_chunk_end_computed {
            range_for_timestamp(c_min, o.chunk_range)
        } else {
            self.next_at
        };
        if !self.histogram_chunk_end_computed && num_bytes >= target_bytes / 4 {
            let ratio_to_full = target_bytes as f64 / num_bytes as f64;
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio_to_full);
            self.histogram_chunk_end_computed = true;
        }
        if (t >= self.next_at || num_bytes >= target_bytes * 2)
            && (num_samples >= chunkenc::MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                || t >= next_chunk_range_start)
        {
            self.cut_new_head_chunk(t, e, o.chunk_range);
            chunk_created = true;
        }
        if chunk_created {
            self.histogram_chunk_end_computed = false;
        }
        (true, chunk_created)
    }

    /// Go `memSeries.append`. Returns (sampleInOrder, chunkCreated).
    fn append(&mut self, st: i64, t: i64, v: f64, o: ChunkOpts) -> (bool, bool) {
        let e = ValueType::Float.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, created) = self.append_preprocessor(t, e, o);
        if !in_order {
            return (in_order, created);
        }
        let c = self.head.as_mut().unwrap();
        self.app.as_mut().unwrap().append(&mut *c.chunk, st, t, v);
        c.max_time = t;
        self.last_value = v;
        self.last_histogram = None;
        self.last_float_histogram = None;
        (true, created)
    }

    /// Applies the result of a histogram append (the tail of Go `appendHistogram`/`appendFloatHistogram`).
    fn finish_histogram_append(
        &mut self,
        t: i64,
        res: chunkenc::AppendResult,
        created: bool,
        o: ChunkOpts,
    ) -> (bool, bool) {
        match res {
            (None, _) => {
                self.head.as_mut().unwrap().max_time = t;
                (true, created)
            }
            (Some((chunk, app)), true) => {
                let c = self.head.as_mut().unwrap();
                c.max_time = t;
                c.chunk = chunk;
                self.app = Some(app);
                (true, false)
            }
            (Some((chunk, app)), false) => {
                if let Some(old) = self.head.take() {
                    self.closed.push(ClosedChunk {
                        chunk: Arc::from(old.chunk),
                        min_time: old.min_time,
                        max_time: old.max_time,
                    });
                }
                self.head = Some(HeadChunk {
                    chunk,
                    min_time: t,
                    max_time: t,
                });
                self.app = Some(app);
                self.next_at = range_for_timestamp(t, o.chunk_range);
                (true, true)
            }
        }
    }

    /// Go `memSeries.appendHistogram`.
    fn append_histogram(&mut self, st: i64, t: i64, h: &Histogram, o: ChunkOpts) -> (bool, bool) {
        let mut prev_app = self.app.clone();
        let e = ValueType::Histogram.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, created) = self.histograms_append_preprocessor(t, e, o);
        if !in_order {
            return (in_order, created);
        }
        if !created {
            prev_app = None;
        }
        let mut h2 = h.clone();
        let c = self.head.as_mut().unwrap();
        let res = self
            .app
            .as_mut()
            .unwrap()
            .append_histogram(&mut *c.chunk, prev_app.as_ref(), st, t, &mut h2, false)
            .expect("append_histogram with append_only=false does not fail");
        self.last_histogram = Some(h2);
        self.last_float_histogram = None;
        self.finish_histogram_append(t, res, created, o)
    }

    /// Go `memSeries.appendFloatHistogram`.
    fn append_float_histogram(
        &mut self,
        st: i64,
        t: i64,
        fh: &FloatHistogram,
        o: ChunkOpts,
    ) -> (bool, bool) {
        let mut prev_app = self.app.clone();
        let e = ValueType::FloatHistogram.chunk_encoding(o.use_xor2, o.use_histogram_st);
        let (in_order, created) = self.histograms_append_preprocessor(t, e, o);
        if !in_order {
            return (in_order, created);
        }
        if !created {
            prev_app = None;
        }
        let mut h2 = fh.clone();
        let c = self.head.as_mut().unwrap();
        let res = self
            .app
            .as_mut()
            .unwrap()
            .append_float_histogram(&mut *c.chunk, prev_app.as_ref(), st, t, &mut h2, false)
            .expect("append_float_histogram with append_only=false does not fail");
        self.last_histogram = None;
        self.last_float_histogram = Some(h2);
        self.finish_histogram_append(t, res, created, o)
    }

    /// Go `memSeries.appendable` with the out-of-order window 0.
    fn appendable(&self, t: i64, v: f64, min_valid_time: i64) -> Result<(), Error> {
        if t >= min_valid_time {
            if self.head.is_none() {
                return Ok(());
            }
            let ms_maxt = self.max_time();
            if t > ms_maxt {
                return Ok(());
            }
            if t == ms_maxt {
                if self.last_histogram.is_some() || self.last_float_histogram.is_some() {
                    return Err(storage::new_duplicate_histogram_to_float_err(t, v));
                }
                if self.last_value.to_bits() != v.to_bits() {
                    return Err(storage::new_duplicate_float_err(t, self.last_value, v));
                }
                return Ok(());
            }
        }
        if t < min_valid_time {
            return Err(storage::ERR_OUT_OF_BOUNDS);
        }
        Err(storage::ERR_OUT_OF_ORDER_SAMPLE)
    }

    /// Go `memSeries.appendableHistogram` / `appendableFloatHistogram` with the out-of-order window 0. `same` tells
    /// whether the sample equals the last histogram sample.
    fn appendable_histogram(
        &self,
        t: i64,
        same: impl FnOnce(&MemSeries) -> bool,
        min_valid_time: i64,
    ) -> Result<(), Error> {
        if t >= min_valid_time {
            if self.head.is_none() {
                return Ok(());
            }
            let ms_maxt = self.max_time();
            if t > ms_maxt {
                return Ok(());
            }
            if t == ms_maxt {
                if !same(self) {
                    return Err(storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP);
                }
                return Ok(());
            }
        }
        if t < min_valid_time {
            return Err(storage::ERR_OUT_OF_BOUNDS);
        }
        Err(storage::ERR_OUT_OF_ORDER_SAMPLE)
    }

    fn appendable_h(&self, t: i64, h: &Histogram, min_valid_time: i64) -> Result<(), Error> {
        self.appendable_histogram(
            t,
            |s| s.last_histogram.as_ref().is_some_and(|l| h.equals(l)),
            min_valid_time,
        )
    }

    fn appendable_fh(&self, t: i64, fh: &FloatHistogram, min_valid_time: i64) -> Result<(), Error> {
        self.appendable_histogram(
            t,
            |s| {
                s.last_float_histogram
                    .as_ref()
                    .is_some_and(|l| fh.equals(l))
            },
            min_valid_time,
        )
    }

    /// The chunks that overlap [mint, maxt], with the open head chunk copied (Go `appendSeriesChunks` plus the
    /// prefilter of `blockBaseSeriesSet`). The open chunk's max time is `i64::MAX`, as the head index reports it.
    fn snapshot(&self, qmint: i64, qmaxt: i64, mint: i64, maxt: i64) -> Vec<ClosedChunk> {
        let mut out = Vec::new();
        for c in &self.closed {
            if c.min_time <= qmaxt
                && qmint <= c.max_time
                && c.max_time >= mint
                && c.min_time <= maxt
            {
                out.push(c.clone());
            }
        }
        if let Some(h) = &self.head {
            if h.min_time <= qmaxt && qmint <= h.max_time && h.min_time <= maxt {
                let copy = chunkenc::from_data(h.chunk.encoding(), h.chunk.bytes().to_vec())
                    .expect("valid chunk");
                out.push(ClosedChunk {
                    chunk: Arc::from(copy),
                    min_time: h.min_time,
                    max_time: i64::MAX,
                });
            }
        }
        out
    }
}

impl Head {
    fn new() -> Head {
        Head {
            series: Vec::new(),
            by_hash: HashMap::new(),
            min_time: i64::MAX,
            max_time: i64::MIN,
            min_valid_time: i64::MIN,
        }
    }

    fn initialized(&self) -> bool {
        self.min_time != i64::MAX
    }

    /// Go `Head.initTime`.
    fn init_time(&mut self, t: i64) {
        if self.min_time != i64::MAX {
            return;
        }
        self.min_time = t;
        if self.max_time == i64::MIN {
            self.max_time = t;
        }
    }

    /// Go `appendableMinValidTime`.
    fn appendable_min_valid_time(&self, chunk_range: i64) -> i64 {
        let cw_end = self.max_time.saturating_sub(chunk_range / 2);
        cw_end.max(self.min_valid_time)
    }

    fn get_by_id(&self, r: SeriesRef) -> Option<usize> {
        if r == 0 || r as usize > self.series.len() {
            return None;
        }
        Some(r as usize - 1)
    }

    fn get_by_labels(&self, lset: &Labels) -> Option<usize> {
        self.by_hash
            .get(&lset.hash())?
            .iter()
            .copied()
            .find(|&i| labels::equal(&self.series[i].lset, lset))
    }

    /// Go `headAppenderBase.getOrCreate`.
    fn get_or_create(&mut self, lset: &Labels) -> Result<usize, Error> {
        let lset = lset.without_empty();
        if lset.is_empty() {
            return Err(Error::wrap("empty labelset", ERR_INVALID_SAMPLE));
        }
        if let Some(l) = lset.has_duplicate_label_names() {
            return Err(Error::wrapped(
                format!("label name \"{}\" is not unique: invalid sample", l),
                ERR_INVALID_SAMPLE,
            ));
        }
        if let Some(i) = self.get_by_labels(&lset) {
            return Ok(i);
        }
        let i = self.series.len();
        self.by_hash.entry(lset.hash()).or_default().push(i);
        self.series.push(MemSeries::new(lset));
        Ok(i)
    }

    /// The indexes of the series that match all matchers (Go `PostingsForMatchers`), in reference order.
    fn postings_for_matchers(&self, ms: &[Matcher]) -> Vec<usize> {
        if ms.is_empty() {
            return Vec::new();
        }
        (0..self.series.len())
            .filter(|&i| {
                let ls = &self.series[i].lset;
                ms.iter().all(|m| m.matches(ls.get(&m.name)))
            })
            .collect()
    }
}

impl Default for TestStorage {
    fn default() -> TestStorage {
        TestStorage::new()
    }
}

impl TestStorage {
    /// Go `teststorage.New(t)`.
    pub fn new() -> TestStorage {
        TestStorage::with_options(Options::default())
    }

    /// Go `teststorage.New(t, opts...)`.
    pub fn with_options(opts: Options) -> TestStorage {
        TestStorage {
            inner: Arc::new(Inner {
                opts,
                head: RwLock::new(Head::new()),
            }),
        }
    }

    /// Go `promqltest.NewBuiltinTestStorage`.
    pub fn new_builtin() -> TestStorage {
        TestStorage::with_options(Options::builtin())
    }

    /// The options of the storage.
    pub fn options(&self) -> &Options {
        &self.inner.opts
    }

    /// A new V2 appender (Go `DB.AppenderV2`).
    pub fn appender_v2(&self) -> HeadAppender {
        HeadAppender::new(self.inner.clone())
    }

    /// A new V1 appender (Go `DB.Appender`).
    pub fn appender_v1(&self) -> HeadAppender {
        HeadAppender::new(self.inner.clone())
    }

    /// The minimum and maximum timestamps in the head (`i64::MAX`, `i64::MIN` when empty).
    pub fn head_time_range(&self) -> (i64, i64) {
        let h = self.inner.head.read().unwrap();
        (h.min_time, h.max_time)
    }

    /// The number of chunks of the series with these labels (for tests that check chunk cutting).
    pub fn num_chunks(&self, lset: &Labels) -> usize {
        let h = self.inner.head.read().unwrap();
        match h.get_by_labels(lset) {
            Some(i) => h.series[i].closed.len() + h.series[i].head.is_some() as usize,
            None => 0,
        }
    }
}

impl Queryable for TestStorage {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let empty = {
            let h = self.inner.head.read().unwrap();
            maxt < h.min_time
        };
        if empty {
            return Ok(Box::new(storage::NoopQuerier));
        }
        Ok(Box::new(HeadQuerier {
            inner: self.inner.clone(),
            mint,
            maxt,
        }))
    }
}

impl ChunkQueryable for TestStorage {
    /// Chunk queries re-encode the selected samples (Go copies head chunks that need no trimming as they are).
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        Ok(Box::new(HeadChunkQuerier {
            q: HeadQuerier {
                inner: self.inner.clone(),
                mint,
                maxt,
            },
        }))
    }
}

impl Appendable for TestStorage {
    fn appender(&self, _ctx: &Context) -> Box<dyn Appender> {
        Box::new(self.appender_v1())
    }
}

impl AppendableV2 for TestStorage {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(TestStorage::appender_v2(self))
    }
}

impl Storage for TestStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Ok(self.inner.head.read().unwrap().min_time)
    }
    fn close(&self) -> Option<Error> {
        None
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn storage::CostSamplingQueryable> {
        Some(self)
    }
}

/// Go `tsdb.DB.CostSamplingQueryable`: a head-only DB gives one native block querier, so the source is the
/// storage itself.
impl storage::CostSamplingQueryable for TestStorage {
    fn cost_sampling_queryable(&self) -> (Option<&dyn storage::SampleAndChunkQueryable>, bool) {
        (Some(self), true)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Queriers.

struct HeadQuerier {
    inner: Arc<Inner>,
    mint: i64,
    maxt: i64,
}

impl HeadQuerier {
    fn overlaps_head(&self, h: &Head) -> bool {
        !(self.maxt < h.min_time || self.mint > h.max_time)
    }

    fn label_values_impl(
        &self,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Vec<String> {
        let h = self.inner.head.read().unwrap();
        if !self.overlaps_head(&h) {
            return Vec::new();
        }
        let idx: Vec<usize> = if matchers.is_empty() {
            (0..h.series.len()).collect()
        } else {
            h.postings_for_matchers(matchers)
        };
        let mut vals: Vec<String> = idx
            .into_iter()
            .map(|i| h.series[i].lset.get(name))
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
            .collect();
        vals.sort();
        vals.dedup();
        let (v, _) = LabelHints::apply_limit(hints, &vals);
        v.into_owned()
    }
}

impl LabelQuerier for HeadQuerier {
    fn label_values(
        &self,
        _ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> LabelResult {
        (
            self.label_values_impl(name, hints, matchers),
            Annotations::new(),
            None,
        )
    }

    fn label_names(
        &self,
        _ctx: &Context,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> LabelResult {
        let h = self.inner.head.read().unwrap();
        if !self.overlaps_head(&h) {
            return (Vec::new(), Annotations::new(), None);
        }
        let idx: Vec<usize> = if matchers.is_empty() {
            (0..h.series.len()).collect()
        } else {
            h.postings_for_matchers(matchers)
        };
        let mut names: Vec<String> = Vec::new();
        for i in idx {
            h.series[i].lset.range(|n, _| names.push(n.to_string()));
        }
        names.sort();
        names.dedup();
        let (v, _) = LabelHints::apply_limit(hints, &names);
        (v.into_owned(), Annotations::new(), None)
    }

    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for HeadQuerier {
    fn select(
        &self,
        _ctx: &Context,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[Matcher],
    ) -> BoxSeriesSet {
        let h = self.inner.head.read().unwrap();
        let mut idx = h.postings_for_matchers(matchers);
        if sort_series {
            idx.sort_by(|&a, &b| labels::compare(&h.series[a].lset, &h.series[b].lset).cmp(&0));
        }
        let (mut mint, mut maxt, mut disable_trimming, mut no_chunks) =
            (self.mint, self.maxt, false, false);
        if let Some(hints) = hints {
            mint = hints.start;
            maxt = hints.end;
            disable_trimming = hints.disable_trimming;
            no_chunks = hints.func == "series";
        }
        let mut out: Vec<SeriesArc> = Vec::with_capacity(idx.len());
        for i in idx {
            let s = &h.series[i];
            let chunks = s.snapshot(self.mint, self.maxt, mint, maxt);
            if chunks.is_empty() {
                continue;
            }
            let mut trim_front = false;
            let mut trim_back = false;
            if !disable_trimming {
                for c in &chunks {
                    trim_front |= c.min_time < mint;
                    trim_back |= c.max_time > maxt;
                }
            }
            out.push(Arc::new(HeadSeries {
                lset: s.lset.clone(),
                chunks: if no_chunks { Vec::new() } else { chunks },
                lo: if trim_front { mint } else { i64::MIN },
                hi: if trim_back { maxt } else { i64::MAX },
            }));
        }
        Box::new(ListSeriesSet { series: out, i: 0 })
    }
}

struct HeadChunkQuerier {
    q: HeadQuerier,
}

impl LabelQuerier for HeadChunkQuerier {
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> LabelResult {
        self.q.label_values(ctx, name, hints, matchers)
    }
    fn label_names(
        &self,
        ctx: &Context,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> LabelResult {
        self.q.label_names(ctx, hints, matchers)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl ChunkQuerier for HeadChunkQuerier {
    fn select(
        &self,
        ctx: &Context,
        sort_series: bool,
        hints: Option<&SelectHints>,
        matchers: &[Matcher],
    ) -> storage::BoxChunkSeriesSet {
        storage::new_series_set_to_chunk_set(Querier::select(
            &self.q,
            ctx,
            sort_series,
            hints,
            matchers,
        ))
    }
}

/// A series set over a list (the set never fails).
struct ListSeriesSet {
    series: Vec<SeriesArc>,
    i: usize,
}

impl SeriesSet for ListSeriesSet {
    fn next(&mut self) -> bool {
        if self.i >= self.series.len() {
            return false;
        }
        self.i += 1;
        true
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.i - 1]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// A selected series: its chunks and the kept time range [lo, hi] (Go deletion intervals from trimming).
struct HeadSeries {
    lset: Labels,
    chunks: Vec<ClosedChunk>,
    lo: i64,
    hi: i64,
}

impl Series for HeadSeries {
    fn labels(&self) -> &Labels {
        &self.lset
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(HeadSeriesIterator {
            s: self,
            next_chunk: 0,
            cur: None,
        }))
    }
}

/// Go `populateWithDelSeriesIterator` with a `DeletedIterator` for the trimmed ranges.
struct HeadSeriesIterator<'a> {
    s: &'a HeadSeries,
    next_chunk: usize,
    cur: Option<AnyIterator<'a>>,
}

impl<'a> HeadSeriesIterator<'a> {
    fn deleted(&self, t: i64) -> bool {
        t < self.s.lo || t > self.s.hi
    }

    /// Go `DeletedIterator.Next` on the current chunk.
    fn cur_next(&mut self) -> ValueType {
        let Some(cur) = self.cur.as_mut() else {
            return ValueType::None;
        };
        loop {
            let vt = cur.next();
            if vt == ValueType::None {
                return vt;
            }
            let t = cur.at_t();
            if !(t < self.s.lo || t > self.s.hi) {
                return vt;
            }
        }
    }

    /// Go `DeletedIterator.Seek` on the current chunk.
    fn cur_seek(&mut self, t: i64) -> ValueType {
        let Some(cur) = self.cur.as_mut() else {
            return ValueType::None;
        };
        if cur.err().is_some() {
            return ValueType::None;
        }
        let vt = cur.seek(t);
        if vt == ValueType::None {
            return vt;
        }
        if self.deleted(self.cur.as_ref().unwrap().at_t()) {
            return self.cur_next();
        }
        vt
    }

    fn open_next_chunk(&mut self) -> bool {
        if self.next_chunk >= self.s.chunks.len() {
            return false;
        }
        let c = &self.s.chunks[self.next_chunk];
        self.next_chunk += 1;
        let reuse = self.cur.take();
        self.cur = Some(c.chunk.iterator(reuse));
        true
    }
}

impl ChunkIterator for HeadSeriesIterator<'_> {
    fn next(&mut self) -> ValueType {
        if self.cur.is_some() {
            let vt = self.cur_next();
            if vt != ValueType::None {
                return vt;
            }
        }
        while self.open_next_chunk() {
            let vt = self.cur_next();
            if vt != ValueType::None {
                return vt;
            }
        }
        ValueType::None
    }

    fn seek(&mut self, t: i64) -> ValueType {
        if self.cur.is_some() {
            let vt = self.cur_seek(t);
            if vt != ValueType::None {
                return vt;
            }
        }
        while self.next() != ValueType::None {
            let vt = self.cur_seek(t);
            if vt != ValueType::None {
                return vt;
            }
        }
        ValueType::None
    }

    fn at(&self) -> (i64, f64) {
        self.cur.as_ref().map(|c| c.at()).unwrap_or((i64::MIN, 0.0))
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        match &self.cur {
            Some(c) => c.at_histogram(h),
            None => (i64::MIN, Histogram::default()),
        }
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        match &self.cur {
            Some(c) => c.at_float_histogram(fh),
            None => (i64::MIN, FloatHistogram::default()),
        }
    }
    fn at_t(&self) -> i64 {
        self.cur.as_ref().map(|c| c.at_t()).unwrap_or(i64::MIN)
    }
    fn at_st(&self) -> i64 {
        self.cur.as_ref().map(|c| c.at_st()).unwrap_or(0)
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.cur.as_ref().and_then(|c| c.err())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Appender.

/// Go `sampleType` of the head appender batches.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SampleType {
    None,
    Float,
    Histogram,
    FloatHistogram,
    CustomBucketHistogram,
    CustomBucketFloatHistogram,
}

#[derive(Default)]
struct Batch {
    floats: Vec<(usize, i64, i64, f64)>,
    histograms: Vec<(usize, i64, i64, Histogram)>,
    float_histograms: Vec<(usize, i64, i64, FloatHistogram)>,
}

/// The head appender (Go `initAppenderV2`/`headAppenderV2`, and the V1 `headAppender` on the same core). Not
/// thread-safe; commit or roll back once.
pub struct HeadAppender {
    inner: Arc<Inner>,
    /// Set by the first append (Go `initAppender` creates the real appender lazily).
    state: Option<AppenderState>,
    batches: Vec<Batch>,
    types_in_batch: HashMap<usize, SampleType>,
    closed: bool,
}

#[derive(Clone, Copy)]
struct AppenderState {
    min_valid_time: i64,
}

impl HeadAppender {
    fn new(inner: Arc<Inner>) -> HeadAppender {
        let state = {
            let h = inner.head.read().unwrap();
            if h.initialized() {
                Some(AppenderState {
                    min_valid_time: h.appendable_min_valid_time(inner.opts.chunk_range),
                })
            } else {
                None
            }
        };
        HeadAppender {
            inner,
            state,
            batches: Vec::new(),
            types_in_batch: HashMap::new(),
            closed: false,
        }
    }

    fn state(&mut self, t: i64) -> AppenderState {
        if let Some(s) = self.state {
            return s;
        }
        let mut h = self.inner.head.write().unwrap();
        h.init_time(t);
        let s = AppenderState {
            min_valid_time: h.appendable_min_valid_time(self.inner.opts.chunk_range),
        };
        self.state = Some(s);
        s
    }

    /// Go `getCurrentBatch`.
    fn current_batch(&mut self, st: SampleType, s: usize) -> &mut Batch {
        let new_batch = |this: &mut HeadAppender| {
            this.types_in_batch.clear();
            if !matches!(st, SampleType::Float | SampleType::None) {
                this.types_in_batch.insert(s, st);
            }
            this.batches.push(Batch::default());
        };
        if self.batches.is_empty() {
            new_batch(self);
        } else if st != SampleType::None {
            match self.types_in_batch.get(&s).copied() {
                Some(prev) if prev == st => {}
                None if st == SampleType::Float => {}
                _ if st == SampleType::Float => new_batch(self),
                None => {
                    self.types_in_batch.insert(s, st);
                }
                Some(_) => new_batch(self),
            }
        }
        self.batches.last_mut().unwrap()
    }

    /// The core of Go `headAppenderV2.Append`.
    #[allow(clippy::too_many_arguments)]
    fn append_impl(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        reject_out_of_order: bool,
    ) -> Result<SeriesRef, Error> {
        if self.closed {
            return Err(ERR_APPENDER_CLOSED);
        }
        if let Some(fh) = fh {
            fh.validate()?;
        } else if let Some(h) = h {
            h.validate()?;
        }
        let state = self.state(t);
        if t < state.min_valid_time {
            return Err(storage::ERR_OUT_OF_BOUNDS);
        }
        let opts = &self.inner.opts;
        let st = if opts.enable_st_storage { st } else { 0 };
        let mut head = self.inner.head.write().unwrap();
        let idx = match head.get_by_id(r) {
            Some(i) => i,
            None => head.get_or_create(ls)?,
        };
        let s = &head.series[idx];
        // `reject_out_of_order` only rejects samples that would go to the out-of-order head, which is disabled.
        let _ = reject_out_of_order;
        if let Some(fh) = fh {
            s.appendable_fh(t, fh, state.min_valid_time)?;
            drop(head);
            let typ = if fh.uses_custom_buckets() {
                SampleType::CustomBucketFloatHistogram
            } else {
                SampleType::FloatHistogram
            };
            self.current_batch(typ, idx)
                .float_histograms
                .push((idx, st, t, fh.clone()));
        } else if let Some(h) = h {
            s.appendable_h(t, h, state.min_valid_time)?;
            drop(head);
            let typ = if h.uses_custom_buckets() {
                SampleType::CustomBucketHistogram
            } else {
                SampleType::Histogram
            };
            self.current_batch(typ, idx)
                .histograms
                .push((idx, st, t, h.clone()));
        } else {
            if is_stale_nan(v) {
                match self.types_in_batch.get(&idx) {
                    Some(SampleType::Histogram | SampleType::CustomBucketHistogram) => {
                        drop(head);
                        let sh = Histogram {
                            sum: v,
                            ..Default::default()
                        };
                        return self.append_impl(
                            (idx + 1) as SeriesRef,
                            ls,
                            st,
                            t,
                            0.0,
                            Some(&sh),
                            None,
                            reject_out_of_order,
                        );
                    }
                    Some(SampleType::FloatHistogram | SampleType::CustomBucketFloatHistogram) => {
                        drop(head);
                        let sfh = FloatHistogram {
                            sum: v,
                            ..Default::default()
                        };
                        return self.append_impl(
                            (idx + 1) as SeriesRef,
                            ls,
                            st,
                            t,
                            0.0,
                            None,
                            Some(&sfh),
                            reject_out_of_order,
                        );
                    }
                    _ => {}
                }
            }
            s.appendable(t, v, state.min_valid_time)?;
            drop(head);
            self.current_batch(SampleType::Float, idx)
                .floats
                .push((idx, st, t, v));
        }
        Ok((idx + 1) as SeriesRef)
    }

    fn chunk_opts(&self) -> ChunkOpts {
        let o = &self.inner.opts;
        ChunkOpts {
            chunk_range: o.chunk_range,
            samples_per_chunk: o.samples_per_chunk,
            use_xor2: o.use_xor2,
            use_histogram_st: o.enable_histogram_st_encoding,
            store_st: o.enable_st_storage,
        }
    }

    /// Go `headAppenderBase.Commit`.
    fn commit_impl(&mut self) -> Option<Error> {
        if self.closed {
            return Some(ERR_APPENDER_CLOSED);
        }
        self.closed = true;
        let Some(state) = self.state else { return None };
        let o = self.chunk_opts();
        let mut head = self.inner.head.write().unwrap();
        let (mut in_order_mint, mut in_order_maxt) = (i64::MAX, i64::MIN);
        let mut seen = |t: i64| {
            in_order_mint = in_order_mint.min(t);
            in_order_maxt = in_order_maxt.max(t);
        };
        for mut b in std::mem::take(&mut self.batches) {
            // commitFloats.
            for (idx, st, t, v) in std::mem::take(&mut b.floats) {
                let s = &mut head.series[idx];
                if is_stale_nan(v) {
                    if s.last_histogram.is_some() {
                        b.histograms.push((
                            idx,
                            st,
                            t,
                            Histogram {
                                sum: v,
                                ..Default::default()
                            },
                        ));
                        continue;
                    }
                    if s.last_float_histogram.is_some() {
                        b.float_histograms.push((
                            idx,
                            st,
                            t,
                            FloatHistogram {
                                sum: v,
                                ..Default::default()
                            },
                        ));
                        continue;
                    }
                }
                if s.appendable(t, v, state.min_valid_time).is_err() {
                    continue;
                }
                let (ok, _) = s.append(st, t, v, o);
                if ok {
                    seen(t);
                }
            }
            // commitHistograms.
            for (idx, st, t, h) in std::mem::take(&mut b.histograms) {
                let s = &mut head.series[idx];
                if s.appendable_h(t, &h, state.min_valid_time).is_err() {
                    continue;
                }
                let (ok, _) = s.append_histogram(st, t, &h, o);
                if ok {
                    seen(t);
                }
            }
            // commitFloatHistograms.
            for (idx, st, t, fh) in std::mem::take(&mut b.float_histograms) {
                let s = &mut head.series[idx];
                if s.appendable_fh(t, &fh, state.min_valid_time).is_err() {
                    continue;
                }
                let (ok, _) = s.append_float_histogram(st, t, &fh, o);
                if ok {
                    seen(t);
                }
            }
        }
        // Go `updateMinMaxTime`.
        if in_order_mint < head.min_time {
            head.min_time = in_order_mint;
        }
        if in_order_maxt > head.max_time {
            head.max_time = in_order_maxt;
        }
        None
    }

    fn rollback_impl(&mut self) -> Option<Error> {
        if self.closed {
            return Some(ERR_APPENDER_CLOSED);
        }
        self.closed = true;
        self.batches.clear();
        None
    }

    /// Go `AppendSTZeroSample` / `AppendHistogramSTZeroSample`: a zero sample at `st` before the sample at `t`.
    fn append_st_zero(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        if st >= t {
            return Err(storage::ERR_ST_NEWER_THAN_SAMPLE);
        }
        let state = self.state(st);
        let mut head = self.inner.head.write().unwrap();
        let idx = match head.get_by_id(r) {
            Some(i) => i,
            None => head.get_or_create(l)?,
        };
        let s = &head.series[idx];
        let map_ooo = |e: Error| {
            if e.is(&storage::ERR_OUT_OF_ORDER_SAMPLE) {
                storage::ERR_OUT_OF_ORDER_ST
            } else {
                e
            }
        };
        if let Some(h) = h {
            let z = Histogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: h.schema,
                zero_threshold: h.zero_threshold,
                custom_values: h.custom_values.clone(),
                ..Default::default()
            };
            s.appendable_h(st, &z, state.min_valid_time)
                .map_err(map_ooo)?;
            drop(head);
            let typ = if h.uses_custom_buckets() {
                SampleType::CustomBucketHistogram
            } else {
                SampleType::Histogram
            };
            self.current_batch(typ, idx)
                .histograms
                .push((idx, 0, st, z));
        } else if let Some(fh) = fh {
            let z = FloatHistogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: fh.schema,
                zero_threshold: fh.zero_threshold,
                custom_values: fh.custom_values.clone(),
                ..Default::default()
            };
            s.appendable_fh(st, &z, state.min_valid_time)
                .map_err(map_ooo)?;
            drop(head);
            let typ = if fh.uses_custom_buckets() {
                SampleType::CustomBucketFloatHistogram
            } else {
                SampleType::FloatHistogram
            };
            self.current_batch(typ, idx)
                .float_histograms
                .push((idx, 0, st, z));
        } else {
            s.appendable(st, 0.0, state.min_valid_time)?;
            drop(head);
            self.current_batch(SampleType::Float, idx)
                .floats
                .push((idx, 0, st, 0.0));
        }
        Ok((idx + 1) as SeriesRef)
    }
}

impl AppenderTransaction for HeadAppender {
    fn commit(&mut self) -> Option<Error> {
        self.commit_impl()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.rollback_impl()
    }
}

impl AppenderV2 for HeadAppender {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        self.append_impl(r, ls, st, t, v, h, fh, opts.reject_out_of_order)
    }

    fn type_name(&self) -> &'static str {
        "*tsdb.headAppenderV2"
    }
}

impl Appender for HeadAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.append_impl(r, l, 0, t, v, None, None, false)
    }
    fn set_options(&mut self, _opts: Option<&AppendOptions>) {}
    fn append_exemplar(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        _e: &Exemplar,
    ) -> Result<SeriesRef, Error> {
        let head = self.inner.head.read().unwrap();
        match head
            .get_by_id(r)
            .or_else(|| head.get_by_labels(&l.without_empty()))
        {
            Some(i) => Ok((i + 1) as SeriesRef),
            None => Err(Error::wrapped(
                format!(
                    "unknown HeadSeriesRef when trying to add exemplar: {}: not found",
                    r
                ),
                storage::ERR_NOT_FOUND,
            )),
        }
    }
    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.append_impl(r, l, 0, t, 0.0, h, fh, false)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.append_st_zero(r, l, t, st, h, fh)
    }
    fn update_metadata(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        _m: &Metadata,
    ) -> Result<SeriesRef, Error> {
        let head = self.inner.head.read().unwrap();
        match head
            .get_by_id(r)
            .or_else(|| head.get_by_labels(&l.without_empty()))
        {
            Some(i) => Ok((i + 1) as SeriesRef),
            None => Err(Error::wrapped(
                format!(
                    "unknown series when trying to add metadata with HeadSeriesRef: {} and labels: {}: not found",
                    r, l
                ),
                storage::ERR_NOT_FOUND,
            )),
        }
    }
    fn append_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
    ) -> Result<SeriesRef, Error> {
        self.append_st_zero(r, l, t, st, None, None)
    }
}
