//! Shared helpers of the tsdb tests: ports of the Go test helpers `mockIndex`, `mockChunkReader`
//! (querier_test.go), `genSeries`, `populateSeries`, `createBlock` (block_test.go), `query` (db_test.go).
//!
//! adapted: `create_block` builds the block without the head (the head is ported in parallel): the series are
//! encoded into chunks of at most 120 samples (`storage::series_to_chunks`, the same cut as the head for regular
//! float series), put into an in-memory block ([`MemBlock`]) and written with the leveled compactor, as Go's
//! `CreateBlock` writes its head. Once the head is merged, `create_block` should use the head as Go does.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chunkenc::{AnyIterator, Chunk};
use model::histogram::{CounterResetHint, Histogram, Span};
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::Matcher;
use storage::{Context, Error, LabelHints, Meta, Querier, Sample, SeriesArc, SeriesRef};
use tsdb::compact::{Compactor, new_leveled_compactor};
use tsdb::{BlockMeta, BlockReader, ChunkOrIterable, ChunkReader, IndexReader};
use tsdb_format::index::{self, BoxPostings, Postings, StringIter};
use tsdb_format::tombstones;

/// Go `DefaultBlockDuration`.
pub const DEFAULT_BLOCK_DURATION: i64 = 2 * 60 * 60 * 1000;
/// Go `defaultLabelName`.
pub const DEFAULT_LABEL_NAME: &str = "labelName";
/// Go `defaultLabelValue`.
pub const DEFAULT_LABEL_VALUE: &str = "labelValue";

/// Go `sample{t: t, f: f}`.
pub fn fs(t: i64, f: f64) -> Sample {
    Sample::float(0, t, f)
}

/// Go `sample{st, t, f, nil, nil}`.
pub fn fss(st: i64, t: i64, f: f64) -> Sample {
    Sample::float(st, t, f)
}

/// A fresh temporary directory under the agent's data folder (Go `t.TempDir()`); removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> TempDir {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w2-tsdb/tests");
        std::fs::create_dir_all(&base).unwrap();
        loop {
            let mut b = [0u8; 8];
            crypto::random_bytes(&mut b);
            let p = base.join(format!("t{}", u64::from_le_bytes(b)));
            if std::fs::create_dir(&p).is_ok() {
                return TempDir(p);
            }
        }
    }
    /// A fresh temporary directory whose name starts with `name`.
    pub fn named(name: &str) -> TempDir {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w2-tsdb/tests");
        let n = N.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = base.join(format!("{}-{}-{}", name, std::process::id(), n));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        TempDir(dir)
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// mockIndex.

#[derive(Clone, Default)]
pub struct MockSeries {
    pub l: Labels,
    pub chunks: Vec<Meta>,
}

/// Go `mockIndex`.
#[derive(Clone, Default)]
pub struct MockIndex {
    pub series: HashMap<SeriesRef, MockSeries>,
    pub postings: HashMap<(String, String), Vec<SeriesRef>>,
    pub symbols: HashSet<String>,
}

pub fn new_mock_index() -> MockIndex {
    MockIndex::default()
}

impl MockIndex {
    /// Go `AddSeries`.
    pub fn add_series(&mut self, r: SeriesRef, l: Labels, chunks: &[Meta]) -> Result<(), Error> {
        if self.series.contains_key(&r) {
            return Err(Error::msg(format!("series with reference {r} already added")));
        }
        l.range(|n, v| {
            self.symbols.insert(n.to_string());
            self.symbols.insert(v.to_string());
        });
        // Actual chunk data is not stored in the index.
        let chunks = chunks.iter().map(|c| Meta { chunk: None, ..c.clone() }).collect();
        self.series.insert(r, MockSeries { l, chunks });
        Ok(())
    }

    /// Go `WritePostings`.
    pub fn write_postings(&mut self, name: &str, value: &str, it: &mut dyn Postings) -> Result<(), Error> {
        let k = (name.to_string(), value.to_string());
        if self.postings.contains_key(&k) {
            return Err(Error::msg(format!("postings for {name}={} already added", model::unicode::quote(value))));
        }
        let ep = index::expand_postings(it).map_err(Error::other)?;
        self.postings.insert(k, ep);
        Ok(())
    }

    fn matches(&self, s: &MockSeries, matchers: &[Matcher]) -> bool {
        matchers.iter().all(|m| m.matches(s.l.get(&m.name)))
    }
}

impl IndexReader for MockIndex {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        let mut l: Vec<String> = self.symbols.iter().cloned().collect();
        l.sort();
        Box::new(index::new_string_list_iter(l))
    }
    fn sorted_label_values(
        &self,
        ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let mut v = self.label_values(ctx, name, hints, matchers)?;
        v.sort();
        Ok(v)
    }
    fn label_values(
        &self,
        _ctx: &Context,
        name: &str,
        hints: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> Result<Vec<String>, Error> {
        let limit = hints.map_or(0, |h| h.limit);
        let mut values: Vec<String> = Vec::new();
        if matchers.is_empty() {
            for (n, v) in self.postings.keys() {
                if n == name {
                    values.push(v.clone());
                    if limit > 0 && values.len() as i64 >= limit {
                        break;
                    }
                }
            }
            return Ok(values);
        }
        for s in self.series.values() {
            if self.matches(s, matchers) {
                let v = s.l.get(name).to_string();
                if !values.contains(&v) {
                    values.push(v);
                }
            }
            if limit > 0 && values.len() as i64 >= limit {
                break;
            }
        }
        Ok(values)
    }
    fn postings(&self, _ctx: &Context, name: &str, values: &[&str]) -> Result<BoxPostings<'_>, Error> {
        let res = values
            .iter()
            .map(|v| {
                let l = self.postings.get(&(name.to_string(), v.to_string())).cloned().unwrap_or_default();
                Box::new(index::new_list_postings(l)) as BoxPostings<'_>
            })
            .collect();
        Ok(index::merge(res))
    }
    fn postings_for_label_matching(
        &self,
        _ctx: &Context,
        name: &str,
        m: &(dyn Fn(&str) -> bool + Sync),
    ) -> BoxPostings<'_> {
        let res = self
            .postings
            .iter()
            .filter(|((n, v), _)| n == name && m(v))
            .map(|(_, srs)| Box::new(index::new_list_postings(srs.clone())) as BoxPostings<'_>)
            .collect();
        index::merge(res)
    }
    fn postings_for_all_label_values(&self, _ctx: &Context, name: &str) -> BoxPostings<'_> {
        let res = self
            .postings
            .iter()
            .filter(|((n, _), _)| n == name)
            .map(|(_, srs)| Box::new(index::new_list_postings(srs.clone())) as BoxPostings<'_>)
            .collect();
        index::merge(res)
    }
    fn sorted_postings<'a>(&'a self, mut p: BoxPostings<'a>) -> BoxPostings<'a> {
        let mut ep = match index::expand_postings(&mut *p) {
            Ok(ep) => ep,
            Err(e) => return index::err_postings(index::Error::wrap("expand postings", e)),
        };
        ep.sort_by(|a, b| labels::compare(&self.series[a].l, &self.series[b].l).cmp(&0));
        Box::new(index::new_list_postings(ep))
    }
    fn sharded_postings<'a>(&'a self, mut p: BoxPostings<'a>, shard_index: u64, shard_count: u64) -> BoxPostings<'a> {
        let mut out = Vec::new();
        while p.next() {
            let r = p.at();
            let Some(s) = self.series.get(&r) else { continue };
            // Check if the series belong to the shard.
            if s.l.hash() % shard_count != shard_index {
                continue;
            }
            out.push(r);
        }
        Box::new(index::new_list_postings(out))
    }
    fn series(&self, r: SeriesRef, builder: &mut ScratchBuilder, chks: &mut Vec<Meta>) -> Result<(), Error> {
        let Some(s) = self.series.get(&r) else {
            return Err(storage::ERR_NOT_FOUND);
        };
        builder.assign(s.l.clone());
        chks.clear();
        chks.extend(s.chunks.iter().cloned());
        Ok(())
    }
    fn label_names(&self, _ctx: &Context, matchers: &[Matcher]) -> Result<Vec<String>, Error> {
        let mut names: HashSet<String> = HashSet::new();
        if matchers.is_empty() {
            for (n, _) in self.postings.keys() {
                names.insert(n.clone());
            }
        } else {
            for s in self.series.values() {
                if self.matches(s, matchers) {
                    s.l.range(|n, _| {
                        names.insert(n.to_string());
                    });
                }
            }
        }
        let mut l: Vec<String> = names.into_iter().collect();
        l.sort();
        Ok(l)
    }
    fn label_names_for(&self, _ctx: &Context, postings: &mut dyn Postings) -> Result<Vec<String>, Error> {
        let mut names: HashSet<String> = HashSet::new();
        while postings.next() {
            if let Some(s) = self.series.get(&postings.at()) {
                s.l.range(|n, _| {
                    names.insert(n.to_string());
                });
            }
        }
        if let Some(e) = postings.err() {
            return Err(Error::other(e));
        }
        Ok(names.into_iter().collect())
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// An `IndexReader` that forwards to a shared one (so that a block can hand out boxed readers).
pub struct SharedIndex(pub Arc<dyn IndexReader>);

impl IndexReader for SharedIndex {
    fn symbols(&self) -> Box<dyn StringIter + '_> {
        self.0.symbols()
    }
    fn sorted_label_values(&self, c: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> Result<Vec<String>, Error> {
        self.0.sorted_label_values(c, n, h, m)
    }
    fn label_values(&self, c: &Context, n: &str, h: Option<&LabelHints>, m: &[Matcher]) -> Result<Vec<String>, Error> {
        self.0.label_values(c, n, h, m)
    }
    fn postings(&self, c: &Context, n: &str, v: &[&str]) -> Result<BoxPostings<'_>, Error> {
        self.0.postings(c, n, v)
    }
    fn postings_for_label_matching(&self, c: &Context, n: &str, m: &(dyn Fn(&str) -> bool + Sync)) -> BoxPostings<'_> {
        self.0.postings_for_label_matching(c, n, m)
    }
    fn postings_for_all_label_values(&self, c: &Context, n: &str) -> BoxPostings<'_> {
        self.0.postings_for_all_label_values(c, n)
    }
    fn sorted_postings<'a>(&'a self, p: BoxPostings<'a>) -> BoxPostings<'a> {
        self.0.sorted_postings(p)
    }
    fn sharded_postings<'a>(&'a self, p: BoxPostings<'a>, i: u64, c: u64) -> BoxPostings<'a> {
        self.0.sharded_postings(p, i, c)
    }
    fn series(&self, r: SeriesRef, b: &mut ScratchBuilder, c: &mut Vec<Meta>) -> Result<(), Error> {
        self.0.series(r, b, c)
    }
    fn label_names(&self, c: &Context, m: &[Matcher]) -> Result<Vec<String>, Error> {
        self.0.label_names(c, m)
    }
    fn label_names_for(&self, c: &Context, p: &mut dyn Postings) -> Result<Vec<String>, Error> {
        self.0.label_names_for(c, p)
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// mockChunkReader.

/// Go `mockChunkReader`.
#[derive(Clone, Default)]
pub struct MockChunkReader(pub HashMap<u64, Arc<dyn Chunk>>);

impl ChunkReader for MockChunkReader {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        match self.0.get(&meta.chunk_ref) {
            Some(c) => Ok(ChunkOrIterable::Chunk(c.clone())),
            None => Err(Error::msg("Chunk with ref not found")),
        }
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// A `ChunkReader` that forwards to a shared one.
pub struct SharedChunks(pub Arc<dyn ChunkReader>);

impl ChunkReader for SharedChunks {
    fn chunk_or_iterable(&self, meta: &Meta) -> Result<ChunkOrIterable, Error> {
        self.0.chunk_or_iterable(meta)
    }
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// In-memory block.

/// A block in memory: an index, chunks and tombstones (a `BlockReader` for the compactor and queriers).
pub struct MemBlock {
    pub index: Arc<dyn IndexReader>,
    pub chunks: Arc<dyn ChunkReader>,
    pub tombstones: Arc<dyn tombstones::Reader>,
    pub meta: BlockMeta,
}

impl BlockReader for MemBlock {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(SharedIndex(self.index.clone())))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        Ok(Box::new(SharedChunks(self.chunks.clone())))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Ok(self.tombstones.clone())
    }
    fn meta(&self) -> BlockMeta {
        self.meta.clone()
    }
    fn size(&self) -> i64 {
        0
    }
}

/// An in-memory block of the series (sorted by labels; series refs from 1), with chunks of at most 120 samples,
/// and its time range [min sample time, max sample time].
pub fn mem_block_from_series(series: &[SeriesArc]) -> MemBlock {
    let mut series: Vec<&SeriesArc> = series.iter().collect();
    series.sort_by(|a, b| labels::compare(a.labels(), b.labels()).cmp(&0));
    let mut mi = MockIndex::default();
    let mut cr = MockChunkReader::default();
    let mut postings: BTreeMap<(String, String), Vec<SeriesRef>> = BTreeMap::new();
    let (mut mint, mut maxt) = (i64::MAX, i64::MIN);
    let mut cref = 0u64;
    for (i, s) in series.iter().enumerate() {
        let r = i as SeriesRef + 1;
        let chks = storage::series_to_chunks(&***s, chunkenc::ENC_XOR).expect("encode series");
        let mut metas = Vec::with_capacity(chks.len());
        for c in chks {
            mint = mint.min(c.min_time);
            maxt = maxt.max(c.max_time);
            cr.0.insert(cref, c.chunk.clone().expect("chunk"));
            metas.push(Meta { chunk_ref: cref, chunk: None, min_time: c.min_time, max_time: c.max_time });
            cref += 1;
        }
        mi.add_series(r, s.labels().clone(), &metas).expect("add series");
        s.labels().range(|n, v| postings.entry((n.to_string(), v.to_string())).or_default().push(r));
        postings.entry(("".to_string(), "".to_string())).or_default().push(r);
    }
    for ((n, v), refs) in postings {
        mi.write_postings(&n, &v, &mut index::new_list_postings(refs)).expect("write postings");
    }
    MemBlock {
        index: Arc::new(mi),
        chunks: Arc::new(cr),
        tombstones: Arc::new(tombstones::MemTombstones::new()),
        meta: BlockMeta { min_time: mint, max_time: maxt, ..BlockMeta::default() },
    }
}

/// Go `createBlock` (adapted: without the head, see the module comment). Returns the block directory.
pub fn create_block(dir: &Path, series: &[SeriesArc]) -> PathBuf {
    let b = mem_block_from_series(series);
    let compactor =
        new_leveled_compactor(&Context::background(), None, vec![DEFAULT_BLOCK_DURATION], None, None).unwrap();
    std::fs::create_dir_all(dir).unwrap();
    // Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
    let ids = compactor.write(dir, &b, b.meta.min_time, b.meta.max_time + 1, None).unwrap();
    dir.join(ids.first().copied().unwrap_or_default().to_string())
}

// ---------------------------------------------------------------------------------------------------------------
// Series generators.

/// Go `rand.Float64()`.
pub fn rand_f64() -> f64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64
}

/// Go `genSeriesFromSampleGenerator`.
pub fn gen_series_from_sample_generator(
    total_series: usize,
    label_count: usize,
    mint: i64,
    maxt: i64,
    step: i64,
    mut generator: impl FnMut(i64) -> Sample,
) -> Vec<SeriesArc> {
    if total_series == 0 || label_count == 0 {
        return Vec::new();
    }
    let mut series = Vec::with_capacity(total_series);
    for i in 0..total_series {
        let mut lbls: BTreeMap<String, String> = BTreeMap::new();
        lbls.insert(DEFAULT_LABEL_NAME.to_string(), i.to_string());
        let mut j = 1;
        while lbls.len() < label_count {
            lbls.insert(format!("{DEFAULT_LABEL_NAME}{j}"), format!("{DEFAULT_LABEL_VALUE}{j}"));
            j += 1;
        }
        let mut samples = Vec::new();
        let mut t = mint;
        while t < maxt {
            samples.push(generator(t));
            t += step;
        }
        series.push(storage::new_list_series(labels::from_map(lbls), samples));
    }
    series
}

/// Go `genSeries`.
pub fn gen_series(total_series: usize, label_count: usize, mint: i64, maxt: i64) -> Vec<SeriesArc> {
    gen_series_from_sample_generator(total_series, label_count, mint, maxt, 1, |ts| fs(ts, rand_f64()))
}

fn gen_histogram(ts: i64) -> Histogram {
    Histogram {
        count: 7 + (ts * 5) as u64,
        zero_count: 2 + ts as u64,
        zero_threshold: 0.001,
        sum: 18.4 * rand_f64(),
        schema: 1,
        positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 1, length: 2 }],
        positive_buckets: vec![ts + 1, 1, -1, 0],
        ..Histogram::default()
    }
}

/// Go `genHistogramSeries`.
pub fn gen_histogram_series(
    total_series: usize,
    label_count: usize,
    mint: i64,
    maxt: i64,
    step: i64,
    float_histogram: bool,
) -> Vec<SeriesArc> {
    gen_series_from_sample_generator(total_series, label_count, mint, maxt, step, |ts| {
        let mut h = gen_histogram(ts);
        if ts != mint {
            h.counter_reset_hint = CounterResetHint::NotCounterReset;
        }
        if float_histogram { Sample::float_histogram(0, ts, h.to_float()) } else { Sample::histogram(0, ts, h) }
    })
}

/// Go `genHistogramAndFloatSeries`.
pub fn gen_histogram_and_float_series(
    total_series: usize,
    label_count: usize,
    mint: i64,
    maxt: i64,
    step: i64,
    float_histogram: bool,
) -> Vec<SeriesArc> {
    let mut float_sample = false;
    let mut count = 0;
    gen_series_from_sample_generator(total_series, label_count, mint, maxt, step, move |ts| {
        count += 1;
        let s = if float_sample {
            fs(ts, rand_f64())
        } else {
            let mut h = gen_histogram(ts);
            if count > 1 && count % 5 != 1 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            if float_histogram { Sample::float_histogram(0, ts, h.to_float()) } else { Sample::histogram(0, ts, h) }
        };
        if count % 5 == 0 {
            // Flip the sample type for every 5 samples.
            float_sample = !float_sample;
        }
        s
    })
}

/// Go `populateSeries`: series from the given labels, with samples at every ms in [mint, maxt].
pub fn populate_series(lbls: &[BTreeMap<String, String>], mint: i64, maxt: i64) -> Vec<SeriesArc> {
    let mut series = Vec::new();
    for lbl in lbls {
        if lbl.is_empty() {
            continue;
        }
        let samples = (mint..=maxt).map(|t| fs(t, rand_f64())).collect();
        series.push(storage::new_list_series(labels::from_map(lbl.clone()), samples));
    }
    series
}

// ---------------------------------------------------------------------------------------------------------------
// Queries.

/// Go `query` (db_test.go): runs a matcher query against the querier and fully expands its data (NaN values are
/// replaced by -42). Closes the querier.
pub fn query(mut q: Box<dyn Querier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    query_helper(&mut q, true, matchers)
}

/// Go `queryWithoutReplacingNaNs`.
pub fn query_without_replacing_nans(mut q: Box<dyn Querier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    query_helper(&mut q, false, matchers)
}

fn query_helper(q: &mut Box<dyn Querier>, nan: bool, matchers: &[Matcher]) -> BTreeMap<String, Vec<Sample>> {
    let mut result = BTreeMap::new();
    {
        let mut ss = q.select(&Context::background(), false, None, matchers);
        while ss.next() {
            let series = ss.at().clone();
            let mut it: AnyIterator<'_> = series.iterator(None);
            let samples = if nan {
                storage::expand_samples(&mut it).unwrap()
            } else {
                storage::expand_samples_without_replacing_nans(&mut it).unwrap()
            };
            let name = series.labels().to_string();
            if !samples.is_empty() {
                result.insert(name, samples);
            }
        }
        assert!(ss.err().is_none(), "{:?}", ss.err());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());
    result
}

// ---------------------------------------------------------------------------------------------------------------
// createIdxChkReaders (querier_test.go).

/// Go `seriesSamples`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SeriesSamples {
    pub lset: BTreeMap<String, String>,
    pub chunks: Vec<Vec<Sample>>,
}

pub fn lset(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
    kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// Go `createIdxChkReaders`: index: labels -> postings -> chunk metas -> chunk ref; chunk reader: ref -> values.
pub fn create_idx_chk_readers(tc: &[SeriesSamples]) -> (Arc<MockIndex>, Arc<MockChunkReader>, i64, i64) {
    let mut chk_reader = MockChunkReader::default();
    let mut mi = new_mock_index();
    let mut postings: BTreeMap<(String, String), Vec<u64>> = BTreeMap::new();
    let (mut block_mint, mut block_maxt) = (i64::MAX, i64::MIN);
    let mut chunk_ref = 0u64;
    for (i, s) in tc.iter().enumerate() {
        let i = i as u64 + 1; // 0 is not a valid posting.
        let mut metas = Vec::with_capacity(s.chunks.len());
        for chk in &s.chunks {
            block_mint = block_mint.min(chk[0].t);
            block_maxt = block_maxt.max(chk[chk.len() - 1].t);
            metas.push(Meta { min_time: chk[0].t, max_time: chk[chk.len() - 1].t, chunk_ref, chunk: None });
            let chunk: Box<dyn Chunk> = if chk[0].fh.is_some() {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_FLOAT_HISTOGRAM).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    let mut fh = smpl.fh.as_deref().expect("chunk can only contain one type of sample").clone();
                    app.append_float_histogram(&mut *c, None, 0, smpl.t, &mut fh, true).expect("chunk should be appendable");
                }
                c
            } else if chk[0].h.is_some() {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_HISTOGRAM).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    let mut h = smpl.h.as_deref().expect("chunk can only contain one type of sample").clone();
                    app.append_histogram(&mut *c, None, 0, smpl.t, &mut h, true).expect("chunk should be appendable");
                }
                c
            } else {
                let mut c = chunkenc::new_empty_chunk(chunkenc::ENC_XOR).unwrap();
                let mut app = c.appender().unwrap();
                for smpl in chk {
                    assert!(smpl.h.is_none() && smpl.fh.is_none(), "chunk can only contain one type of sample");
                    app.append(&mut *c, 0, smpl.t, smpl.f);
                }
                c
            };
            chk_reader.0.insert(chunk_ref, Arc::from(chunk));
            chunk_ref += 1;
        }
        let l = labels::from_map(s.lset.clone());
        mi.add_series(i, l.clone(), &metas).unwrap();
        l.range(|n, v| postings.entry((n.to_string(), v.to_string())).or_default().push(i));
        postings.entry((String::new(), String::new())).or_default().push(i);
    }
    for ((n, v), refs) in postings {
        mi.write_postings(&n, &v, &mut index::new_list_postings(refs)).unwrap();
    }
    (Arc::new(mi), Arc::new(chk_reader), block_mint, block_maxt)
}


// ---------------------------------------------------------------------------------------------------------------
// Test heads (Go newTestHead, newTestHeadWithOptions).

#[allow(unused_imports)]
use std::sync::atomic::Ordering;
use tsdb::head::{Head, HeadOptions};
use tsdb_format::wlog::{compression, WL};

/// A head with its WAL and directory; the head is closed on drop (Go `t.Cleanup(h.Close)`).
pub struct TestHead {
    pub head: Arc<Head>,
    pub wal: Option<Arc<WL>>,
    pub dir: TempDir,
}

impl std::ops::Deref for TestHead {
    type Target = Arc<Head>;
    fn deref(&self) -> &Arc<Head> {
        &self.head
    }
}

impl Drop for TestHead {
    fn drop(&mut self) {
        // Not while panicking: a second panic in close (for example a failing snapshot) would abort the test binary.
        if !self.head.is_closed() && !std::thread::panicking() {
            self.head.close().expect("close head");
        }
    }
}

/// Go `newTestHeadWithOptions`.
pub fn new_test_head_with_options(name: &str, compress_wal: compression::Type, mut opts: HeadOptions) -> TestHead {
    let dir = TempDir::named(name);
    let wal = Arc::new(WL::new_size(dir.path().join("wal"), 32768, compress_wal).expect("wal"));
    opts.chunk_dir_root = dir.path().to_path_buf();
    let head = Head::new(Some(wal.clone()), None, opts, None).expect("new head");
    head.chunk_disk_mapper.iterate_all_chunks(|_, _, _, _, _, _, _| Ok(())).expect("iterate chunks");
    TestHead {
        head,
        wal: Some(wal),
        dir,
    }
}

/// Go `newTestHead(t, chunkRange, compressWAL, oooEnabled)`.
pub fn new_test_head(name: &str, chunk_range: i64, compress_wal: compression::Type, ooo_enabled: bool) -> TestHead {
    let opts = HeadOptions {
        chunk_range,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    opts.max_exemplars.store(100_000, Ordering::Relaxed);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * 60 * 1000, Ordering::Relaxed);
    }
    new_test_head_with_options(name, compress_wal, opts)
}

/// A head without WAL in a temporary directory (Go `NewHead(nil, nil, nil, nil, opts, nil)`).
pub fn new_head_no_wal(name: &str, mut opts: HeadOptions) -> TestHead {
    let dir = TempDir::named(name);
    opts.chunk_dir_root = dir.path().to_path_buf();
    let head = Head::new(None, None, opts, None).expect("new head");
    TestHead { head, wal: None, dir }
}

// ---------------------------------------------------------------------------------------------------------------
// Go testutil.go: sample type scenarios.

use storage::tsdbutil as tu;
use storage::Appender;

pub const FLOAT: &str = "float";
pub const INT_HISTOGRAM: &str = "integer histogram";
pub const FLOAT_HISTOGRAM: &str = "float histogram";
pub const CUSTOM_BUCKETS_INT_HISTOGRAM: &str = "custom buckets int histogram";
pub const CUSTOM_BUCKETS_FLOAT_HISTOGRAM: &str = "custom buckets float histogram";
pub const GAUGE_INT_HISTOGRAM: &str = "gauge int histogram";
pub const GAUGE_FLOAT_HISTOGRAM: &str = "gauge float histogram";

/// Go `sampleTypeScenario`.
#[derive(Clone, Copy)]
pub struct SampleTypeScenario {
    pub sample_type: &'static str,
    pub sample_func: fn(i64, i64) -> Sample,
}

impl SampleTypeScenario {
    /// Go `appendFunc` (through a V1 appender).
    pub fn append(&self, app: &mut dyn Appender, lbls: &Labels, ts: i64, value: i64) -> (Result<SeriesRef, Error>, Sample) {
        let s = (self.sample_func)(ts, value);
        let r = if let Some(h) = &s.h {
            app.append_histogram(0, lbls, ts, Some(h), None)
        } else if let Some(fh) = &s.fh {
            app.append_histogram(0, lbls, ts, None, Some(fh))
        } else {
            app.append(0, lbls, ts, s.f)
        };
        (r, s)
    }
}

fn sc(sample_type: &'static str, sample_func: fn(i64, i64) -> Sample) -> SampleTypeScenario {
    SampleTypeScenario { sample_type, sample_func }
}

/// Go `sampleTypeScenarios`.
pub fn sample_type_scenarios() -> Vec<(&'static str, SampleTypeScenario)> {
    vec![
        (FLOAT, sc("float", |ts, v| Sample::float(0, ts, v as f64))),
        (INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_histogram(v)))),
        (FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_float_histogram(v)))),
        (CUSTOM_BUCKETS_INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_custom_buckets_histogram(v)))),
        (CUSTOM_BUCKETS_FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_custom_buckets_float_histogram(v)))),
        (GAUGE_INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_gauge_histogram(v)))),
        (GAUGE_FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_gauge_float_histogram(v)))),
    ]
}
