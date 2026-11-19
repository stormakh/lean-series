//! Compaction (port of `tsdb/compact.go`): the leveled compactor (plan, compact, write), the block populator
//! with vertical (overlapping) compaction through storage's chunk series merger, and `CompactBlockMetas`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chunkenc::Pool;
use model::labels::Labels;
use storage::{
    BoxChunkSeriesSet, Context, Error, FloatEncodingFn, Meta, SeriesRef, VerticalChunkSeriesMergeFunc, VerticalSeriesMergeFunc,
};
use tsdb_format::chunks;
use tsdb_format::index::{self, BoxPostings, PostingsEncoder, StringIter};
use tsdb_format::tombstones::{self, MemTombstones};

use crate::block::{Block, PostingsDecoderFactory, chunk_dir, open_block};
use crate::meta::{BlockDesc, BlockMeta, INDEX_FILENAME, Ulid, read_meta_file, write_meta_file};
use crate::metrics::{Counter, Gauge, Histogram};
use crate::querier::{OwnedPostings, new_block_chunk_series_set, new_merged_string_iter};
use crate::reader::{BlockReader, ChunkReader, ChunkWriter, IndexReader, IndexWriter};
use crate::{Logger, chunks_err, index_err, tomb_err};

/// Go `tmpForCreationBlockDirSuffix` (db.go).
pub const TMP_FOR_CREATION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-creation";

/// Go `ExponentialBlockRanges`: the time ranges based on the step size.
pub fn exponential_block_ranges(min_size: i64, steps: usize, step_size: i64) -> Vec<i64> {
    let mut ranges = Vec::with_capacity(steps);
    let mut cur = min_size;
    for _ in 0..steps {
        ranges.push(cur);
        cur *= step_size;
    }
    ranges
}

/// Go `Compactor`: compaction against an underlying storage of time series data.
pub trait Compactor: Send + Sync {
    /// A set of directories that can be compacted concurrently. The directories can be overlapping.
    fn plan(&self, dir: &Path) -> Result<Vec<PathBuf>, Error>;

    /// Persists one or more blocks into a directory. No block is written when the resulting block has 0
    /// samples; the result is then empty.
    fn write(
        &self,
        dest: &Path,
        b: &dyn BlockReader,
        mint: i64,
        maxt: i64,
        base: Option<&BlockMeta>,
    ) -> Result<Vec<Ulid>, Error>;

    /// Runs compaction against the provided directories. `open` are already open blocks, to avoid reopening
    /// them. When the resulting block has 0 samples, no block is written, the source dirs are marked deletable
    /// and the result is empty.
    fn compact(&self, dest: &Path, dirs: &[PathBuf], open: &[Block]) -> Result<Vec<Ulid>, Error>;
}

/// Go `CompactorMetrics`.
#[derive(Debug)]
pub struct CompactorMetrics {
    /// `prometheus_tsdb_compactions_total`
    pub ran: Counter,
    /// `prometheus_tsdb_compaction_populating_block`
    pub populating_blocks: Gauge,
    /// `prometheus_tsdb_vertical_compactions_total`
    pub overlapping_blocks: Counter,
    /// `prometheus_tsdb_compaction_duration_seconds`
    pub duration: Histogram,
    /// `prometheus_tsdb_compaction_chunk_size_bytes`
    pub chunk_size: Histogram,
    /// `prometheus_tsdb_compaction_chunk_samples`
    pub chunk_samples: Histogram,
    /// `prometheus_tsdb_compaction_chunk_range_seconds`
    pub chunk_range: Histogram,
}

/// Go `NewCompactorMetrics`.
pub fn new_compactor_metrics() -> Arc<CompactorMetrics> {
    Arc::new(CompactorMetrics {
        ran: Counter::new(),
        populating_blocks: Gauge::new(),
        overlapping_blocks: Counter::new(),
        duration: Histogram::new(Histogram::exponential_buckets(1.0, 2.0, 14)),
        chunk_size: Histogram::new(Histogram::exponential_buckets(32.0, 1.5, 12)),
        chunk_samples: Histogram::new(Histogram::exponential_buckets(4.0, 1.5, 12)),
        chunk_range: Histogram::new(Histogram::exponential_buckets(100.0, 4.0, 10)),
    })
}

/// Go `BlockExcludeFilterFunc`: decides which blocks are excluded from compactions.
pub type BlockExcludeFilterFunc = Arc<dyn Fn(&BlockMeta) -> bool + Send + Sync>;

/// Go `DefaultPostingsDecoderFactory`.
pub fn default_postings_decoder_factory(_: &BlockMeta) -> index::PostingsDecoder {
    index::decode_postings_raw
}

/// Go `LeveledCompactorOptions`.
#[derive(Default, Clone)]
pub struct LeveledCompactorOptions {
    /// The postings encoder (raw by default).
    pub pe: Option<PostingsEncoder>,
    /// The postings decoder factory (raw by default).
    pub pd: Option<PostingsDecoderFactory>,
    /// The max block chunk segment size; 0 means `chunks::DEFAULT_CHUNK_SEGMENT_SIZE`.
    pub max_block_chunk_segment_size: i64,
    /// Merges series in vertical compaction. Default: the compacting chunk series merger with
    /// `float_chunk_encoding`.
    pub merge_func: Option<VerticalChunkSeriesMergeFunc>,
    /// The encoding of float chunks re-encoded during vertical compaction (XOR when `None`). Ignored when
    /// `merge_func` is set.
    pub float_chunk_encoding: Option<FloatEncodingFn>,
    /// Decides which blocks are excluded from compactions.
    pub block_exclude_filter: Option<BlockExcludeFilterFunc>,
    /// Enables compaction of overlapping blocks (always on in Prometheus).
    pub enable_overlapping_compaction: bool,
    /// The metrics; new ones by default.
    pub metrics: Option<Arc<CompactorMetrics>>,
    /// Bypass the page cache when appropriate.
    pub use_uncached_io: bool,
}

/// Go `LeveledCompactor`.
pub struct LeveledCompactor {
    metrics: Arc<CompactorMetrics>,
    logger: Logger,
    ranges: Vec<i64>,
    chunk_pool: Arc<dyn Pool>,
    ctx: Context,
    max_block_chunk_segment_size: i64,
    use_uncached_io: bool,
    merge_func: VerticalChunkSeriesMergeFunc,
    block_exclude_func: Option<BlockExcludeFilterFunc>,
    postings_encoder: PostingsEncoder,
    postings_decoder_factory: Option<PostingsDecoderFactory>,
    enable_overlapping_compaction: bool,
}

/// Go `NewLeveledCompactor`.
pub fn new_leveled_compactor(
    ctx: &Context,
    logger: Option<Logger>,
    ranges: Vec<i64>,
    pool: Option<Arc<dyn Pool>>,
    merge_func: Option<VerticalChunkSeriesMergeFunc>,
) -> Result<LeveledCompactor, Error> {
    new_leveled_compactor_with_options(
        ctx,
        logger,
        ranges,
        pool,
        LeveledCompactorOptions { merge_func, enable_overlapping_compaction: true, ..Default::default() },
    )
}

/// Go `NewLeveledCompactorWithOptions`.
pub fn new_leveled_compactor_with_options(
    ctx: &Context,
    logger: Option<Logger>,
    ranges: Vec<i64>,
    pool: Option<Arc<dyn Pool>>,
    opts: LeveledCompactorOptions,
) -> Result<LeveledCompactor, Error> {
    if ranges.is_empty() {
        return Err(Error::msg("at least one range must be provided"));
    }
    let merge_func = opts.merge_func.unwrap_or_else(|| {
        let chained: VerticalSeriesMergeFunc = Arc::new(storage::chained_series_merge);
        storage::new_compacting_chunk_series_merger_with_float_encoding(chained, opts.float_chunk_encoding)
    });
    let max_block_chunk_segment_size = if opts.max_block_chunk_segment_size == 0 {
        chunks::DEFAULT_CHUNK_SEGMENT_SIZE
    } else {
        opts.max_block_chunk_segment_size
    };
    Ok(LeveledCompactor {
        ranges,
        chunk_pool: pool.unwrap_or_else(|| Arc::new(chunkenc::new_pool())),
        logger: logger.unwrap_or_else(Logger::nop),
        metrics: opts.metrics.unwrap_or_else(new_compactor_metrics),
        ctx: ctx.clone(),
        max_block_chunk_segment_size,
        use_uncached_io: opts.use_uncached_io,
        merge_func,
        postings_encoder: opts.pe.unwrap_or(index::encode_postings_raw),
        postings_decoder_factory: opts.pd,
        enable_overlapping_compaction: opts.enable_overlapping_compaction,
        block_exclude_func: opts.block_exclude_filter,
    })
}

/// Go `dirMeta`.
#[derive(Clone, Debug)]
pub struct DirMeta {
    pub dir: PathBuf,
    pub meta: BlockMeta,
}

/// Go `isBlockDir` (db.go): a directory whose name is a valid ULID (strict parsing).
pub fn is_block_dir(name: &str, is_dir: bool) -> bool {
    if !is_dir || name.len() != 26 {
        return false;
    }
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    if !name.bytes().all(|c| ALPHABET.contains(&c.to_ascii_uppercase())) {
        return false;
    }
    Ulid::parse(name).is_ok()
}

/// Go `blockDirs` (db.go): the block directories in `dir`, sorted by name.
pub fn block_dirs(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let rd = std::fs::read_dir(dir)
        .map_err(|e| Error::msg(format!("open {}: {}", dir.display(), crate::io_err_text(&e))))?;
    let mut names = Vec::new();
    for e in rd {
        let e = e.map_err(Error::other)?;
        let is_dir = e.file_type().map(|t| t.is_dir()).unwrap_or(false);
        let name = e.file_name().to_string_lossy().into_owned();
        if is_block_dir(&name, is_dir) {
            names.push(name);
        }
    }
    names.sort();
    Ok(names.into_iter().map(|n| dir.join(n)).collect())
}

impl LeveledCompactor {
    /// The block ranges of the compactor (Go `ranges`).
    pub fn ranges(&self) -> &[i64] {
        &self.ranges
    }

    /// The metrics of the compactor.
    pub fn metrics(&self) -> &Arc<CompactorMetrics> {
        &self.metrics
    }

    /// The chunk pool (kept for API compatibility; block chunks need no pool).
    pub fn chunk_pool(&self) -> &Arc<dyn Pool> {
        &self.chunk_pool
    }

    /// Go `plan` (on already read dir metas).
    pub fn plan_dir_metas(&self, dms: Vec<DirMeta>) -> Result<Vec<PathBuf>, Error> {
        if dms.is_empty() {
            return Ok(Vec::new());
        }
        // Blocks created from a partial view of the head (stale-series and selected-series compactions) carry
        // the from-stale-series or from-selected-series hint, which must survive compaction. A partial-view block
        // is only ever merged with blocks of the same class, so each class is planned independently, preferring
        // regular (no-hint) compaction. Out-of-order blocks are not segregated. See upstream issue #18379.
        let (mut stale, mut selected, mut non_hint) = (Vec::new(), Vec::new(), Vec::new());
        for dm in &dms {
            if dm.meta.compaction.from_stale_series() {
                stale.push(dm.clone());
            } else if dm.meta.compaction.from_selected_series() {
                selected.push(dm.clone());
            } else {
                non_hint.push(dm.clone());
            }
        }
        let classes = [&stale, &selected, &non_hint].iter().filter(|c| !c.is_empty()).count();
        if classes > 1 {
            // Prefer non-hint compaction so it is never starved by partial-view blocks.
            let res = self.plan_class(non_hint)?;
            if !res.is_empty() {
                return Ok(res);
            }
            let res = self.plan_class(stale)?;
            if !res.is_empty() {
                return Ok(res);
            }
            return self.plan_class(selected);
        }
        self.plan_class(dms)
    }

    /// Go `planClass`: plans compaction for a single class of blocks.
    fn plan_class(&self, mut dms: Vec<DirMeta>) -> Result<Vec<PathBuf>, Error> {
        if dms.is_empty() {
            return Ok(Vec::new());
        }
        dms.sort_by_key(|d| d.meta.min_time);

        let mut res = self.select_overlapping_dirs(&dms);
        if !res.is_empty() {
            return Ok(res);
        }
        // No overlapping blocks, do compaction the usual way. We do not include a recently created block with
        // max(minTime), so the block which was just created from WAL. This gives users a window of a full block
        // size to piece-wise backup new data without having to care about data overlap.
        dms.pop();

        for dm in self.select_dirs(&dms) {
            res.push(dm.dir);
        }
        if !res.is_empty() {
            return Ok(res);
        }

        // Compact any blocks with big enough time range that have >5% tombstones.
        for v in dms.iter().rev() {
            let meta = &v.meta;
            if meta.max_time - meta.min_time < self.ranges[self.ranges.len() / 2] {
                // If the block is entirely deleted, then we don't care about the block being big enough.
                if meta.stats.num_tombstones > 0 && meta.stats.num_tombstones >= meta.stats.num_series {
                    return Ok(vec![v.dir.clone()]);
                }
                break;
            }
            if meta.stats.num_tombstones as f64 / (meta.stats.num_series + 1) as f64 > 0.05 {
                return Ok(vec![v.dir.clone()]);
            }
        }
        Ok(Vec::new())
    }

    /// Go `selectDirs`: the dir metas that should be compacted into a single new block. If only a single block
    /// range is configured, the result is always empty.
    pub fn select_dirs(&self, ds: &[DirMeta]) -> Vec<DirMeta> {
        if self.ranges.len() < 2 || ds.is_empty() {
            return Vec::new();
        }
        let high_time = ds[ds.len() - 1].meta.min_time;
        for &iv in &self.ranges[1..] {
            let parts = split_by_range(ds, iv);
            'outer: for p in parts {
                // Do not select the range if it has a block whose compaction failed.
                for dm in &p {
                    if dm.meta.compaction.failed {
                        continue 'outer;
                    }
                }
                let mint = p[0].meta.min_time;
                let maxt = p[p.len() - 1].meta.max_time;
                // Pick the range of blocks if it spans the full range (potentially with gaps) or is before the
                // most recent block. This ensures we don't compact blocks prematurely when another one of the
                // same size still fits in the range.
                if (maxt - mint == iv || maxt <= high_time) && p.len() > 1 {
                    return p;
                }
            }
        }
        Vec::new()
    }

    /// Go `selectOverlappingDirs`: all dirs with overlapping time ranges. Expects input sorted by mint and returns
    /// the overlapping dirs in the same order.
    pub fn select_overlapping_dirs(&self, ds: &[DirMeta]) -> Vec<PathBuf> {
        if !self.enable_overlapping_compaction || ds.len() < 2 {
            return Vec::new();
        }
        let mut overlapping = Vec::new();
        let mut global_maxt = ds[0].meta.max_time;
        for (i, d) in ds[1..].iter().enumerate() {
            if d.meta.min_time < global_maxt {
                if overlapping.is_empty() {
                    // When it is the first overlap, need to add the last one as well.
                    overlapping.push(ds[i].dir.clone());
                }
                overlapping.push(d.dir.clone());
            } else if !overlapping.is_empty() {
                break;
            }
            if d.meta.max_time > global_maxt {
                global_maxt = d.meta.max_time;
            }
        }
        overlapping
    }

    /// Go `CompactWithBlockPopulator`.
    pub fn compact_with_block_populator(
        &self,
        dest: &Path,
        dirs: &[PathBuf],
        open: &[Block],
        block_populator: &dyn BlockPopulator,
    ) -> Result<Vec<Ulid>, Error> {
        let start = Instant::now();
        let mut bs: Vec<Block> = Vec::new();
        let mut to_close: Vec<Block> = Vec::new();
        let mut metas: Vec<BlockMeta> = Vec::new();
        let mut uids: Vec<String> = Vec::new();
        let close_opened = |to_close: &[Block]| {
            for b in to_close {
                let _ = b.close();
            }
        };

        for d in dirs {
            if let Some(e) = self.ctx.err() {
                close_opened(&to_close);
                return Err(e);
            }
            let meta = match read_meta_file(d) {
                Ok((m, _)) => m,
                Err(e) => {
                    close_opened(&to_close);
                    return Err(e);
                }
            };
            // Use already open blocks if we can, to avoid having the index data in memory twice.
            let b = match open.iter().find(|o| o.ulid() == meta.ulid) {
                Some(o) => o.clone(),
                None => match open_block(
                    Some(self.logger.clone()),
                    d,
                    Some(self.chunk_pool.clone()),
                    self.postings_decoder_factory.as_ref(),
                ) {
                    Ok(b) => {
                        to_close.push(b.clone());
                        b
                    }
                    Err(e) => {
                        close_opened(&to_close);
                        return Err(e);
                    }
                },
            };
            uids.push(meta.ulid.to_string());
            metas.push(meta);
            bs.push(b);
        }

        let uid = Ulid::new();
        let meta_refs: Vec<&BlockMeta> = metas.iter().collect();
        let mut meta = compact_block_metas(uid, &meta_refs);
        let blocks: Vec<&dyn BlockReader> = bs.iter().map(|b| b as &dyn BlockReader).collect();
        let res = self.write_block(dest, &mut meta, block_populator, &blocks);
        drop(blocks);
        close_opened(&to_close);

        match res {
            Ok(()) => {
                if meta.stats.num_samples == 0 {
                    for b in &bs {
                        if b.set_deletable().is_err() {
                            self.logger.error(
                                "Failed to write 'Deletable' to meta file after compaction",
                                &[("ulid", &b.ulid())],
                            );
                        }
                    }
                    self.logger.info(
                        "compact blocks resulted in empty block",
                        &[
                            ("count", &bs.len()),
                            ("sources", &format!("[{}]", uids.join(" "))),
                            ("duration", &format!("{:?}", start.elapsed())),
                        ],
                    );
                    return Ok(Vec::new());
                }
                self.logger.info(
                    "compact blocks",
                    &[
                        ("count", &bs.len()),
                        ("mint", &meta.min_time),
                        ("maxt", &meta.max_time),
                        ("ulid", &meta.ulid),
                        ("sources", &format!("[{}]", uids.join(" "))),
                        ("duration", &format!("{:?}", start.elapsed())),
                    ],
                );
                Ok(vec![uid])
            }
            Err(err) => {
                let canceled = err.is(&storage::ERR_CANCELED);
                let mut errs = vec![Some(err)];
                if !canceled {
                    for b in &bs {
                        if let Err(e) = b.set_compaction_failed() {
                            errs.push(Some(Error::wrap(
                                format_args!("setting compaction failed for block: {}", b.dir().display()),
                                e,
                            )));
                        }
                    }
                }
                Err(Error::join(errs).expect("an error"))
            }
        }
    }

    /// Go `write`: creates a new block that is the union of the provided blocks in `dest`.
    pub fn write_block(
        &self,
        dest: &Path,
        meta: &mut BlockMeta,
        block_populator: &dyn BlockPopulator,
        blocks: &[&dyn BlockReader],
    ) -> Result<(), Error> {
        let start = Instant::now();
        let dir = dest.join(meta.ulid.to_string());
        let tmp = PathBuf::from(format!("{}{}", dir.display(), TMP_FOR_CREATION_BLOCK_DIR_SUFFIX));

        let mut chunkw: Option<Box<dyn ChunkWriter>> = None;
        let mut indexw: Option<IndexFileWriter> = None;
        let mut res = self.write_inner(meta, block_populator, blocks, &dir, &tmp, &mut chunkw, &mut indexw);
        // Close the writers left open by an error.
        if res.is_err() {
            let mut errs = vec![res.err()];
            if let Some(mut w) = chunkw.take() {
                errs.push(w.close().err());
            }
            if let Some(mut w) = indexw.take() {
                errs.push(w.close().err());
            }
            res = Err(Error::join(errs).expect("an error"));
        }
        // Removing a missing tmp dir is not an error, so it is safe to always run it.
        if let Err(e) = remove_all(&tmp) {
            self.logger
                .error("removed tmp folder after failed compaction", &[("err", &crate::io_err_text(&e))]);
        }
        self.metrics.ran.inc();
        self.metrics.duration.observe(start.elapsed().as_secs_f64());
        res
    }

    #[allow(clippy::too_many_arguments)]
    fn write_inner(
        &self,
        meta: &mut BlockMeta,
        block_populator: &dyn BlockPopulator,
        blocks: &[&dyn BlockReader],
        dir: &Path,
        tmp: &Path,
        chunkw: &mut Option<Box<dyn ChunkWriter>>,
        indexw: &mut Option<IndexFileWriter>,
    ) -> Result<(), Error> {
        remove_all(tmp).map_err(Error::other)?;
        std::fs::create_dir_all(tmp).map_err(Error::other)?;

        // Populate chunk and index files into temporary directory with data of all blocks.
        let cw = chunks::new_writer(
            &chunk_dir(tmp),
            &[
                chunks::with_segment_size(self.max_block_chunk_segment_size),
                chunks::with_uncached_io(self.use_uncached_io),
            ],
        )
        .map_err(|e| Error::wrap("open chunk writer", chunks_err(e)))?;
        let mut cw: Box<dyn ChunkWriter> = Box::new(cw);
        // Record written chunk sizes on level 1 compactions.
        if meta.compaction.level == 1 {
            cw = Box::new(InstrumentedChunkWriter { w: cw, metrics: self.metrics.clone() });
        }
        *chunkw = Some(cw);

        let iw = index::Writer::new_with_encoder(&self.ctx, &tmp.join(INDEX_FILENAME), self.postings_encoder)
            .map_err(|e| Error::wrap("open index writer", index_err(e)))?;
        *indexw = Some(IndexFileWriter(Some(iw)));

        block_populator
            .populate_block(
                &self.ctx,
                &self.metrics,
                &self.logger,
                &self.chunk_pool,
                &self.merge_func,
                blocks,
                meta,
                indexw.as_mut().expect("index writer"),
                &mut **chunkw.as_mut().expect("chunk writer"),
                all_sorted_postings,
            )
            .map_err(|e| Error::wrap("populate block", e))?;

        if let Some(e) = self.ctx.err() {
            return Err(e);
        }

        // Close the writers here to check for errors.
        let mut errs = Vec::new();
        if let Some(mut w) = chunkw.take() {
            errs.push(w.close().err());
        }
        if let Some(mut w) = indexw.take() {
            errs.push(w.close().err());
        }
        if let Some(e) = Error::join(errs) {
            return Err(e);
        }

        // Populated block is empty, so exit early.
        if meta.stats.num_samples == 0 {
            return Ok(());
        }

        write_meta_file(tmp, meta).map_err(|e| Error::wrap("write merged meta", e))?;
        // Create an empty tombstones file.
        tombstones::write_file(&self.logger, tmp, &MemTombstones::new())
            .map_err(|e| Error::wrap("write new tombstones file", tomb_err(e)))?;

        let df = tsdb_format::fileutil::open_dir(tmp)
            .map_err(|e| Error::wrap("open temporary block dir", Error::other(e)))?;
        df.sync_all().map_err(|e| Error::wrap("sync temporary dir file", Error::other(e)))?;
        drop(df);

        // Block successfully written, make it visible in destination dir by moving it from tmp one.
        tsdb_format::fileutil::replace(tmp, dir).map_err(|e| Error::wrap("rename block dir", Error::other(e)))?;
        Ok(())
    }
}

impl Compactor for LeveledCompactor {
    /// Go `Plan`: a list of compactable blocks in the provided directory.
    fn plan(&self, dir: &Path) -> Result<Vec<PathBuf>, Error> {
        let dirs = block_dirs(dir)?;
        if dirs.is_empty() {
            return Ok(Vec::new());
        }
        let mut dms = Vec::new();
        for d in dirs {
            let (meta, _) = read_meta_file(&d)?;
            if let Some(f) = &self.block_exclude_func
                && f(&meta)
            {
                // Compactions work from oldest to newest; skipping one block would compact in a non-continuous
                // way, so stop here.
                break;
            }
            dms.push(DirMeta { dir: d, meta });
        }
        self.plan_dir_metas(dms)
    }

    /// Go `Write`.
    fn write(
        &self,
        dest: &Path,
        b: &dyn BlockReader,
        mint: i64,
        maxt: i64,
        base: Option<&BlockMeta>,
    ) -> Result<Vec<Ulid>, Error> {
        let start = Instant::now();
        let uid = Ulid::new();
        self.logger.info("write block started", &[("mint", &mint), ("maxt", &maxt), ("ulid", &uid)]);

        let mut meta = BlockMeta { ulid: uid, min_time: mint, max_time: maxt, ..BlockMeta::default() };
        meta.compaction.level = 1;
        meta.compaction.sources = vec![uid];
        if let Some(base) = base {
            meta.compaction.parents =
                vec![BlockDesc { ulid: base.ulid, min_time: base.min_time, max_time: base.max_time }];
            if base.compaction.from_out_of_order() {
                meta.compaction.set_out_of_order();
            }
            if base.compaction.from_stale_series() {
                meta.compaction.set_stale_series();
            }
            if base.compaction.from_selected_series() {
                meta.compaction.set_selected_series();
            }
        }

        self.write_block(dest, &mut meta, &DefaultBlockPopulator, &[b])?;

        if meta.stats.num_samples == 0 {
            self.logger.info(
                "write block resulted in empty block",
                &[
                    ("mint", &meta.min_time),
                    ("maxt", &meta.max_time),
                    ("duration", &format!("{:?}", start.elapsed())),
                ],
            );
            return Ok(Vec::new());
        }
        self.logger.info(
            "write block completed",
            &[
                ("mint", &meta.min_time),
                ("maxt", &meta.max_time),
                ("ulid", &meta.ulid),
                ("duration", &format!("{:?}", start.elapsed())),
                ("ooo", &meta.compaction.from_out_of_order()),
            ],
        );
        Ok(vec![uid])
    }

    /// Go `Compact`.
    fn compact(&self, dest: &Path, dirs: &[PathBuf], open: &[Block]) -> Result<Vec<Ulid>, Error> {
        self.compact_with_block_populator(dest, dirs, open, &DefaultBlockPopulator)
    }
}

/// Go `os.RemoveAll`.
fn remove_all(p: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(p) {
        Ok(md) if md.is_dir() => std::fs::remove_dir_all(p),
        Ok(_) => std::fs::remove_file(p),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Go `splitByRange`: splits the directories by the time range. The range sequence starts at 0.
///
/// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30 it returns
/// [0-10, 10-20], [50-60], [90-100].
pub fn split_by_range(ds: &[DirMeta], tr: i64) -> Vec<Vec<DirMeta>> {
    let mut split_dirs = Vec::new();
    let mut i = 0;
    while i < ds.len() {
        let m = &ds[i].meta;
        // Compute start of aligned time range of size tr closest to the current block's start.
        let t0 = if m.min_time >= 0 { tr * (m.min_time / tr) } else { tr * ((m.min_time - tr + 1) / tr) };
        // Skip blocks that don't fall into the range. This can happen via misalignment or by being a multiple of
        // the intended range.
        if m.max_time > t0 + tr {
            i += 1;
            continue;
        }
        // Add all dirs to the current group that are within [t0, t0+tr].
        let mut group = Vec::new();
        while i < ds.len() {
            // Either the block falls into the next range or doesn't fit at all (checked above).
            if ds[i].meta.max_time > t0 + tr {
                break;
            }
            group.push(ds[i].clone());
            i += 1;
        }
        if !group.is_empty() {
            split_dirs.push(group);
        }
    }
    split_dirs
}

/// Go `CompactBlockMetas`: merges many block metas into one, combining their source blocks and adjusting the
/// compaction level. The min/max time of the result covers all input blocks.
pub fn compact_block_metas(uid: Ulid, blocks: &[&BlockMeta]) -> BlockMeta {
    let mut res = BlockMeta { ulid: uid, ..BlockMeta::default() };
    let mut sources: Vec<Ulid> = Vec::new();
    let mut mint = blocks[0].min_time;
    let mut maxt = blocks[0].max_time;
    // The from-out-of-order hint is propagated only when every source block carries it; the from-stale-series
    // and from-selected-series hints when any source carries them. See upstream issue #18379.
    let mut all_out_of_order = true;
    for b in blocks {
        mint = mint.min(b.min_time);
        maxt = maxt.max(b.max_time);
        if b.compaction.level > res.compaction.level {
            res.compaction.level = b.compaction.level;
        }
        sources.extend_from_slice(&b.compaction.sources);
        if b.compaction.from_stale_series() {
            res.compaction.set_stale_series();
        }
        if b.compaction.from_selected_series() {
            res.compaction.set_selected_series();
        }
        if !b.compaction.from_out_of_order() {
            all_out_of_order = false;
        }
        res.compaction.parents.push(BlockDesc { ulid: b.ulid, min_time: b.min_time, max_time: b.max_time });
    }
    if all_out_of_order {
        res.compaction.set_out_of_order();
    }
    res.compaction.level += 1;
    sources.sort();
    sources.dedup();
    res.compaction.sources = sources;
    res.min_time = mint;
    res.max_time = maxt;
    res
}

// ---------------------------------------------------------------------------------------------------------------
// Writers.

/// The index file writer as an [`IndexWriter`] (Go `*index.Writer`).
pub struct IndexFileWriter(pub Option<index::Writer>);

impl IndexWriter for IndexFileWriter {
    fn add_symbol(&mut self, sym: &str) -> Result<(), Error> {
        match &mut self.0 {
            Some(w) => w.add_symbol(sym).map_err(index_err),
            None => Err(Error::msg("index writer closed")),
        }
    }
    fn add_series(&mut self, r: SeriesRef, l: &Labels, chunks: &[Meta]) -> Result<(), Error> {
        match &mut self.0 {
            Some(w) => w.add_series(r, l, chunks).map_err(index_err),
            None => Err(Error::msg("index writer closed")),
        }
    }
    fn close(&mut self) -> Result<(), Error> {
        match self.0.take() {
            Some(w) => w.close().map_err(index_err),
            None => Ok(()),
        }
    }
}

impl ChunkWriter for chunks::Writer {
    fn write_chunks(&mut self, chunks: &mut [Meta]) -> Result<(), Error> {
        chunks::Writer::write_chunks(self, chunks).map_err(chunks_err)
    }
    fn close(&mut self) -> Result<(), Error> {
        chunks::Writer::close(self).map_err(chunks_err)
    }
}

/// Go `instrumentedChunkWriter`: records statistics about compacted chunks on level 1 compactions.
struct InstrumentedChunkWriter {
    w: Box<dyn ChunkWriter>,
    metrics: Arc<CompactorMetrics>,
}

impl ChunkWriter for InstrumentedChunkWriter {
    fn write_chunks(&mut self, chunks: &mut [Meta]) -> Result<(), Error> {
        for c in chunks.iter() {
            let chk = c.chunk();
            self.metrics.chunk_size.observe(chk.bytes().len() as f64);
            self.metrics.chunk_samples.observe(chk.num_samples() as f64);
            self.metrics.chunk_range.observe((c.max_time - c.min_time) as f64);
        }
        self.w.write_chunks(chunks)
    }
    fn close(&mut self) -> Result<(), Error> {
        self.w.close()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Block populator.

/// Go `IndexReaderPostingsFunc`: sorted postings from an index reader.
pub type IndexReaderPostingsFunc = for<'a> fn(&Context, &'a dyn IndexReader) -> BoxPostings<'a>;

/// Go `AllSortedPostings`: all postings of the index reader, sorted.
pub fn all_sorted_postings<'a>(ctx: &Context, reader: &'a dyn IndexReader) -> BoxPostings<'a> {
    let (k, v) = index::all_postings_key();
    match reader.postings(ctx, k, &[v]) {
        Ok(all) => reader.sorted_postings(all),
        Err(e) => crate::querier::err_postings(&e),
    }
}

/// Go `BlockPopulator`.
pub trait BlockPopulator: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn populate_block(
        &self,
        ctx: &Context,
        metrics: &CompactorMetrics,
        logger: &Logger,
        chunk_pool: &Arc<dyn Pool>,
        merge_func: &VerticalChunkSeriesMergeFunc,
        blocks: &[&dyn BlockReader],
        meta: &mut BlockMeta,
        indexw: &mut dyn IndexWriter,
        chunkw: &mut dyn ChunkWriter,
        postings_func: IndexReaderPostingsFunc,
    ) -> Result<(), Error>;
}

/// Go `DefaultBlockPopulator`.
pub struct DefaultBlockPopulator;

/// Go `%+v` of a `BlockMeta` value.
pub fn fmt_block_meta(m: &BlockMeta) -> String {
    let mut s = String::new();
    let st = &m.stats;
    let c = &m.compaction;
    let _ = write!(
        s,
        "{{ULID:{} MinTime:{} MaxTime:{} Stats:{{NumSamples:{} NumFloatSamples:{} NumHistogramSamples:{} \
         NumSeries:{} NumChunks:{} NumTombstones:{}}} Compaction:{{Level:{} Sources:[",
        m.ulid,
        m.min_time,
        m.max_time,
        st.num_samples,
        st.num_float_samples,
        st.num_histogram_samples,
        st.num_series,
        st.num_chunks,
        st.num_tombstones,
        c.level
    );
    for (i, u) in c.sources.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        let _ = write!(s, "{u}");
    }
    let _ = write!(s, "] Deletable:{} Parents:[", c.deletable);
    for (i, p) in c.parents.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        let _ = write!(s, "{{ULID:{} MinTime:{} MaxTime:{}}}", p.ulid, p.min_time, p.max_time);
    }
    let _ = write!(s, "] Failed:{} Hints:[{}]}} Version:{}}}", c.failed, c.hints.join(" "), m.version);
    s
}

struct Readers {
    meta: BlockMeta,
    index: Arc<dyn IndexReader>,
    chunks: Arc<dyn ChunkReader>,
    tombs: Arc<dyn tombstones::Reader>,
}

impl BlockPopulator for DefaultBlockPopulator {
    /// Go `PopulateBlock`: fills the index and chunk writers with the union of the provided blocks, and the
    /// stats of `meta`. Expects the blocks sorted by mint.
    fn populate_block(
        &self,
        ctx: &Context,
        metrics: &CompactorMetrics,
        logger: &Logger,
        _chunk_pool: &Arc<dyn Pool>,
        merge_func: &VerticalChunkSeriesMergeFunc,
        blocks: &[&dyn BlockReader],
        meta: &mut BlockMeta,
        indexw: &mut dyn IndexWriter,
        chunkw: &mut dyn ChunkWriter,
        postings_func: IndexReaderPostingsFunc,
    ) -> Result<(), Error> {
        if blocks.is_empty() {
            return Err(Error::msg("cannot populate block from no readers"));
        }
        metrics.populating_blocks.set(1.0);
        let mut readers: Vec<Readers> = Vec::with_capacity(blocks.len());
        let res = populate(ctx, metrics, logger, merge_func, blocks, meta, indexw, chunkw, postings_func, &mut readers);
        // Close the readers.
        let mut errs = Vec::new();
        for r in &readers {
            errs.push(r.index.close().err());
            errs.push(r.chunks.close().err());
            errs.push(r.tombs.close().err().map(tomb_err));
        }
        metrics.populating_blocks.set(0.0);
        let cerr = Error::join(errs).map(|e| Error::wrap("close", e));
        match (res, cerr) {
            (Ok(()), None) => Ok(()),
            (Ok(()), Some(c)) => Err(c),
            (Err(e), None) => Err(e),
            (Err(e), Some(c)) => Err(Error::join([Some(e), Some(c)]).expect("errors")),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn populate(
    ctx: &Context,
    metrics: &CompactorMetrics,
    logger: &Logger,
    merge_func: &VerticalChunkSeriesMergeFunc,
    blocks: &[&dyn BlockReader],
    meta: &mut BlockMeta,
    indexw: &mut dyn IndexWriter,
    chunkw: &mut dyn ChunkWriter,
    postings_func: IndexReaderPostingsFunc,
    readers: &mut Vec<Readers>,
) -> Result<(), Error> {
    let mut overlapping = false;
    let mut global_maxt = blocks[0].meta().max_time;
    for (i, b) in blocks.iter().enumerate() {
        if let Some(e) = ctx.err() {
            return Err(e);
        }
        let bmeta = b.meta();
        if !overlapping {
            if i > 0 && bmeta.min_time < global_maxt {
                metrics.overlapping_blocks.inc();
                overlapping = true;
                logger.info("Found overlapping blocks during compaction", &[("ulid", &meta.ulid)]);
            }
            if bmeta.max_time > global_maxt {
                global_maxt = bmeta.max_time;
            }
        }
        let index: Arc<dyn IndexReader> = Arc::from(
            b.index()
                .map_err(|e| Error::wrap(format_args!("open index reader for block {}", fmt_block_meta(&bmeta)), e))?,
        );
        let chunks: Arc<dyn ChunkReader> = match b.chunks() {
            Ok(c) => Arc::from(c),
            Err(e) => {
                let _ = index.close();
                return Err(Error::wrap(format_args!("open chunk reader for block {}", fmt_block_meta(&bmeta)), e));
            }
        };
        // Enable the head-chunk cache for compaction.
        chunks.enable_chunk_cache();
        let tombs = match b.tombstones() {
            Ok(t) => t,
            Err(e) => {
                let _ = index.close();
                let _ = chunks.close();
                return Err(Error::wrap(
                    format_args!("open tombstone reader for block {}", fmt_block_meta(&bmeta)),
                    e,
                ));
            }
        };
        readers.push(Readers { meta: bmeta, index, chunks, tombs });
    }

    // Merge the symbols of all blocks.
    {
        let mut symbols: Option<Box<dyn StringIter + '_>> = None;
        for r in readers.iter() {
            let syms = r.index.symbols();
            symbols = Some(match symbols {
                None => syms,
                Some(s) => new_merged_string_iter(s, syms),
            });
        }
        let mut symbols = symbols.expect("symbols");
        while symbols.next() {
            indexw.add_symbol(symbols.at()).map_err(|e| Error::wrap("add symbol", e))?;
        }
        if let Some(e) = symbols.err() {
            return Err(Error::wrap("next symbol", index_err(e)));
        }
    }

    let mut sets: Vec<BoxChunkSeriesSet> = Vec::with_capacity(readers.len());
    for r in readers.iter() {
        let p = OwnedPostings::new(r.index.clone(), |ix| Ok(postings_func(ctx, ix)))?;
        // Blocks meta is half open: [min, max), so subtract 1 to ensure we don't hold samples with exact
        // meta.MaxTime timestamp.
        sets.push(new_block_chunk_series_set(
            r.meta.ulid,
            p,
            r.chunks.clone(),
            r.tombs.clone(),
            meta.min_time,
            meta.max_time - 1,
            false,
        ));
    }

    let mut set = if sets.len() > 1 {
        // Merge series using the specified chunk series merger. The default one is the compacting series merger.
        storage::new_merge_chunk_series_set(sets, 0, merge_func.clone())
    } else {
        sets.pop().expect("one set")
    };

    let mut r: SeriesRef = 0;
    let mut chks: Vec<Meta> = Vec::new();
    // Iterate over all sorted chunk series.
    while set.next() {
        if let Some(e) = ctx.err() {
            return Err(e);
        }
        let s = set.at().clone();
        chks.clear();
        {
            let mut it = s.iterator(None);
            while it.next() {
                // We are not iterating in a streaming way over chunks as it's more efficient to do bulk write for
                // index and chunk file purposes.
                chks.push(it.at().clone());
            }
            if let Some(e) = it.err() {
                return Err(Error::wrap("chunk iter", e));
            }
        }
        // Skip series with all deleted chunks.
        if chks.is_empty() {
            continue;
        }
        chunkw.write_chunks(&mut chks).map_err(|e| Error::wrap("write chunks", e))?;
        indexw.add_series(r, s.labels(), &chks).map_err(|e| Error::wrap("add series", e))?;

        meta.stats.num_chunks += chks.len() as u64;
        meta.stats.num_series += 1;
        for chk in &chks {
            let c = chk.chunk();
            let samples = c.num_samples() as u64;
            meta.stats.num_samples += samples;
            match c.encoding() {
                chunkenc::ENC_HISTOGRAM
                | chunkenc::ENC_FLOAT_HISTOGRAM
                | chunkenc::ENC_HISTOGRAM_ST
                | chunkenc::ENC_FLOAT_HISTOGRAM_ST => meta.stats.num_histogram_samples += samples,
                chunkenc::ENC_XOR | chunkenc::ENC_XOR2 => meta.stats.num_float_samples += samples,
                _ => {}
            }
        }
        r += 1;
    }
    if let Some(e) = set.err() {
        return Err(Error::wrap("iterate compaction set", e));
    }
    Ok(())
}
