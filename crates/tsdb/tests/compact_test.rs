//! Port of `tsdb/compact_test.go`.

#![allow(clippy::needless_update, clippy::type_complexity, clippy::unnecessary_to_owned, clippy::cloned_ref_to_slice_refs)]

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chunkenc::{ChunkIterator, ValueType};
use common::*;
use model::labels::Labels;
use storage::tsdbutil as tu;
use storage::{Meta, Sample};
use tsdb::compact::BlockPopulator;
use tsdb_format::index;
use storage::{Context, Error};
use tsdb::compact::{
    DefaultBlockPopulator, DirMeta, LeveledCompactorOptions, compact_block_metas, new_leveled_compactor,
    new_leveled_compactor_with_options, split_by_range, Compactor, TMP_FOR_CREATION_BLOCK_DIR_SUFFIX,
};
use tsdb::{BlockMeta, BlockReader, BlockStats, ChunkReader, IndexReader, Ulid, write_meta_file};
use tsdb_format::tombstones;

fn bg() -> Context {
    Context::background()
}

/// Go `metaRange`.
fn meta_range(name: &str, mint: i64, maxt: i64, stats: Option<BlockStats>) -> DirMeta {
    let mut meta = BlockMeta { min_time: mint, max_time: maxt, ..BlockMeta::default() };
    if let Some(s) = stats {
        meta.stats = s;
    }
    DirMeta { dir: PathBuf::from(name), meta }
}

/// Go `staleMetaRange`: like `meta_range` but tags the block with the from-stale-series hint.
fn stale_meta_range(name: &str, mint: i64, maxt: i64) -> DirMeta {
    let mut dm = meta_range(name, mint, maxt, None);
    dm.meta.compaction.set_stale_series();
    dm
}

fn paths(v: &[&str]) -> Vec<PathBuf> {
    v.iter().map(PathBuf::from).collect()
}

#[test]
fn test_split_by_range() {
    type R = Vec<[i64; 2]>;
    let cases: Vec<(i64, R, Vec<R>)> = vec![
        (60, vec![[0, 10]], vec![vec![[0, 10]]]),
        (60, vec![[0, 60]], vec![vec![[0, 60]]]),
        (60, vec![[0, 10], [9, 15], [30, 60]], vec![vec![[0, 10], [9, 15], [30, 60]]]),
        (
            60,
            vec![[70, 90], [125, 130], [130, 180], [1000, 1001]],
            vec![vec![[70, 90]], vec![[125, 130], [130, 180]], vec![[1000, 1001]]],
        ),
        // Mis-aligned or too-large blocks are ignored.
        (60, vec![[50, 70], [70, 80]], vec![vec![[70, 80]]]),
        (72, vec![[0, 144], [144, 216], [216, 288]], vec![vec![[144, 216]], vec![[216, 288]]]),
        // Various awkward edge cases easy to hit with negative numbers.
        (60, vec![[-10, -5]], vec![vec![[-10, -5]]]),
        (60, vec![[-60, -50], [-10, -5]], vec![vec![[-60, -50], [-10, -5]]]),
        (60, vec![[-60, -50], [-10, -5], [0, 15]], vec![vec![[-60, -50], [-10, -5]], vec![[0, 15]]]),
    ];
    for (trange, ranges, output) in cases {
        // Transform input range tuples into dirMetas.
        let blocks: Vec<DirMeta> = ranges.iter().map(|r| meta_range("", r[0], r[1], None)).collect();
        let got: Vec<Vec<(i64, i64)>> = split_by_range(&blocks, trange)
            .iter()
            .map(|g| g.iter().map(|d| (d.meta.min_time, d.meta.max_time)).collect())
            .collect();
        let exp: Vec<Vec<(i64, i64)>> = output.iter().map(|g| g.iter().map(|r| (r[0], r[1])).collect()).collect();
        assert_eq!(exp, got);
    }
}

// See https://github.com/prometheus/prometheus/issues/3064
#[test]
fn test_no_panic_for0_tombstones() {
    let metas = vec![meta_range("1", 0, 100, None), meta_range("2", 101, 200, None)];
    let c = new_leveled_compactor(&bg(), None, vec![50], None, None).unwrap();
    let _ = c.plan_dir_metas(metas);
}

// Verifies that the planner never groups a from-stale-series block with non-stale blocks. See
// https://github.com/prometheus/prometheus/issues/18379.
#[test]
fn test_plan_does_not_merge_stale_and_non_stale_blocks() {
    let compactor = new_leveled_compactor(&bg(), None, vec![20, 60, 180], None, None).unwrap();

    // Stale and non-stale in the same range are not merged.
    let metas = vec![
        meta_range("1", 0, 20, None),
        stale_meta_range("stale", 0, 20),
        meta_range("2", 20, 40, None),
        meta_range("3", 40, 60, None),
        // A fresh block so the last (most recent) block is excluded as usual.
        meta_range("fresh", 60, 80, None),
    ];
    let res = compactor.plan_dir_metas(metas).unwrap();
    assert!(!res.contains(&PathBuf::from("stale")), "stale block must not be merged with non-stale blocks");
    assert_eq!(paths(&["1", "2", "3"]), res);

    // Stale blocks are merged together.
    let metas = vec![
        stale_meta_range("s1", 0, 20),
        stale_meta_range("s2", 20, 40),
        stale_meta_range("s3", 40, 60),
        stale_meta_range("s4", 60, 80),
    ];
    assert_eq!(paths(&["s1", "s2", "s3"]), compactor.plan_dir_metas(metas).unwrap());

    // Stale blocks are planned when there is nothing to do for non-stale.
    let metas = vec![
        meta_range("1", 0, 20, None),
        stale_meta_range("s1", 0, 20),
        stale_meta_range("s2", 20, 40),
        stale_meta_range("s3", 40, 60),
        stale_meta_range("s4", 60, 80),
    ];
    assert_eq!(paths(&["s1", "s2", "s3"]), compactor.plan_dir_metas(metas).unwrap());
}

#[test]
fn test_leveled_compactor() {
    // Tests for the private plan() method.
    {
        // This mimics our default ExponentialBlockRanges with min block size equals to 20.
        let compactor = new_leveled_compactor(&bg(), None, vec![20, 60, 180, 540, 1620], None, None).unwrap();
        let ts = |n, t| Some(BlockStats { num_series: n, num_tombstones: t, ..Default::default() });
        let cases: Vec<(&str, Vec<DirMeta>, Vec<&str>)> = vec![
            ("Outside Range", vec![meta_range("1", 0, 20, None)], vec![]),
            (
                "We should wait for four blocks of size 20 to appear before compacting.",
                vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None)],
                vec![],
            ),
            (
                "We should wait for a next block of size 20 to appear before compacting the existing ones.",
                vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 40, 60, None)],
                vec![],
            ),
            (
                "Block to fill the entire parent range appeared – should be compacted",
                vec![
                    meta_range("1", 0, 20, None),
                    meta_range("2", 20, 40, None),
                    meta_range("3", 40, 60, None),
                    meta_range("4", 60, 80, None),
                ],
                vec!["1", "2", "3"],
            ),
            (
                "Block for the next parent range appeared with gap with size 20.",
                vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 60, 80, None)],
                vec![],
            ),
            (
                "Block for the next parent range appeared, and we have a gap with size 20 between second and third block.",
                vec![
                    meta_range("1", 0, 20, None),
                    meta_range("2", 20, 40, None),
                    meta_range("3", 60, 80, None),
                    meta_range("4", 80, 100, None),
                ],
                vec!["1", "2"],
            ),
            (
                "We have 20, 20, 20, 60, 60 range blocks. '5' is marked as fresh one",
                vec![
                    meta_range("1", 0, 20, None),
                    meta_range("2", 20, 40, None),
                    meta_range("3", 40, 60, None),
                    meta_range("4", 60, 120, None),
                    meta_range("5", 120, 180, None),
                ],
                vec!["1", "2", "3"],
            ),
            (
                "We have 20, 60, 20, 60, 240 range blocks. We can compact 20 + 60 + 60",
                vec![
                    meta_range("2", 20, 40, None),
                    meta_range("4", 60, 120, None),
                    meta_range("5", 960, 980, None), // Fresh one.
                    meta_range("6", 120, 180, None),
                    meta_range("7", 720, 960, None),
                ],
                vec!["2", "4", "6"],
            ),
            (
                "Do not select large blocks that have many tombstones when there is no fresh block",
                vec![meta_range("1", 0, 540, ts(10, 3))],
                vec![],
            ),
            (
                "Select large blocks that have many tombstones when fresh appears",
                vec![meta_range("1", 0, 540, ts(10, 3)), meta_range("2", 540, 560, None)],
                vec!["1"],
            ),
            (
                "For small blocks, do not compact tombstones, even when fresh appears.",
                vec![meta_range("1", 0, 60, ts(10, 3)), meta_range("2", 60, 80, None)],
                vec![],
            ),
            (
                "Regression test: we were stuck in a compact loop where we always recompacted the same block when \
                 tombstones and series counts were zero",
                vec![meta_range("1", 0, 540, ts(0, 0)), meta_range("2", 540, 560, None)],
                vec![],
            ),
            (
                "Regression test: we were wrongly assuming that new block is fresh from WAL when its ULID is newest.",
                vec![
                    meta_range("5", 0, 360, None),
                    meta_range("6", 540, 560, None), // Fresh one.
                    meta_range("7", 360, 420, None),
                    meta_range("8", 420, 540, None),
                ],
                vec!["7", "8"],
            ),
            (
                "Overlapping blocks 1",
                vec![meta_range("1", 0, 20, None), meta_range("2", 19, 40, None), meta_range("3", 40, 60, None)],
                vec!["1", "2"],
            ),
            (
                "Overlapping blocks 2",
                vec![meta_range("1", 0, 20, None), meta_range("2", 20, 40, None), meta_range("3", 30, 50, None)],
                vec!["2", "3"],
            ),
            (
                "Overlapping blocks 3",
                vec![meta_range("1", 0, 20, None), meta_range("2", 10, 40, None), meta_range("3", 30, 50, None)],
                vec!["1", "2", "3"],
            ),
            (
                "Overlapping blocks 4",
                vec![
                    meta_range("5", 0, 360, None),
                    meta_range("6", 340, 560, None),
                    meta_range("7", 360, 420, None),
                    meta_range("8", 420, 540, None),
                ],
                vec!["5", "6", "7", "8"],
            ),
            (
                "Overlapping blocks 5",
                vec![
                    meta_range("1", 0, 10, None),
                    meta_range("2", 9, 20, None),
                    meta_range("3", 30, 40, None),
                    meta_range("4", 39, 50, None),
                ],
                vec!["1", "2"],
            ),
        ];
        for (title, metas, expected) in cases {
            let res = compactor.plan_dir_metas(metas).unwrap();
            assert_eq!(paths(&expected), res, "{title}");
        }
    }

    // Tests for the public Plan() method: a BlockExcludeFilter that excludes a block in the middle of the list
    // stops the iteration.
    {
        let dir = TempDir::new();
        // Create 4 blocks with sequential ULIDs.
        let uids: Vec<Ulid> = (1..=4).map(|ms| Ulid::from_parts(ms, [0; 10])).collect();
        for (i, uid) in uids.iter().enumerate() {
            let block_dir = dir.path().join(uid.to_string());
            std::fs::create_dir_all(&block_dir).unwrap();
            let mut meta = BlockMeta {
                ulid: *uid,
                min_time: i as i64 * 10,
                max_time: (i as i64 + 1) * 10,
                ..BlockMeta::default()
            };
            meta.compaction.level = 1;
            write_meta_file(&block_dir, &mut meta).unwrap();
        }
        // Track which blocks were evaluated by the exclude function.
        let evaluated = Arc::new(Mutex::new(Vec::new()));
        let ev = evaluated.clone();
        let block2 = uids[1];
        let c = new_leveled_compactor_with_options(
            &bg(),
            None,
            vec![20],
            Some(Arc::new(chunkenc::new_pool())),
            LeveledCompactorOptions {
                block_exclude_filter: Some(Arc::new(move |meta: &BlockMeta| {
                    ev.lock().unwrap().push(meta.ulid);
                    meta.ulid == block2
                })),
                enable_overlapping_compaction: true,
                ..Default::default()
            },
        )
        .unwrap();
        c.plan(dir.path()).unwrap();
        let evaluated = evaluated.lock().unwrap();
        assert_eq!(evaluated.len(), 2, "Expected only 2 blocks to be evaluated");
        assert!(evaluated.contains(&uids[0]));
        assert!(evaluated.contains(&uids[1]));
    }
}

#[test]
fn test_range_with_failed_compaction_wont_get_selected() {
    let compactor = new_leveled_compactor(&bg(), None, vec![20, 60, 240, 720, 2160], None, None).unwrap();
    let cases: Vec<Vec<DirMeta>> = vec![
        vec![
            meta_range("1", 0, 20, None),
            meta_range("2", 20, 40, None),
            meta_range("3", 40, 60, None),
            meta_range("4", 60, 80, None),
        ],
        vec![
            meta_range("1", 0, 20, None),
            meta_range("2", 20, 40, None),
            meta_range("3", 60, 80, None),
            meta_range("4", 80, 100, None),
        ],
        vec![
            meta_range("1", 0, 20, None),
            meta_range("2", 20, 40, None),
            meta_range("3", 40, 60, None),
            meta_range("4", 60, 120, None),
            meta_range("5", 120, 180, None),
            meta_range("6", 180, 200, None),
        ],
    ];
    for mut metas in cases {
        metas[1].meta.compaction.failed = true;
        let res = compactor.plan_dir_metas(metas).unwrap();
        assert_eq!(Vec::<PathBuf>::new(), res);
    }
}

/// Go `erringBReader`.
struct ErringBReader;

impl BlockReader for ErringBReader {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Err(Error::msg("index"))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        Err(Error::msg("chunks"))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Err(Error::msg("tombstones"))
    }
    fn meta(&self) -> BlockMeta {
        BlockMeta::default()
    }
    fn size(&self) -> i64 {
        0
    }
}

#[test]
fn test_compaction_fail_will_clean_up_temp_dir() {
    let compactor = new_leveled_compactor(&bg(), None, vec![20, 60, 240, 720, 2160], None, None).unwrap();
    let tmpdir = TempDir::new();
    assert!(compactor.write_block(tmpdir.path(), &mut BlockMeta::default(), &DefaultBlockPopulator, &[&ErringBReader]).is_err());
    let p = format!("{}{}", tmpdir.path().join(Ulid::default().to_string()).display(), TMP_FOR_CREATION_BLOCK_DIR_SUFFIX);
    assert!(!std::path::Path::new(&p).exists(), "directory is not cleaned up");
}

/// Go `mockBReader`.
struct MockBReader {
    ir: Arc<dyn IndexReader>,
    cr: Arc<dyn ChunkReader>,
    mint: i64,
    maxt: i64,
}

impl BlockReader for MockBReader {
    fn index(&self) -> Result<Box<dyn IndexReader>, Error> {
        Ok(Box::new(SharedIndex(self.ir.clone())))
    }
    fn chunks(&self) -> Result<Box<dyn ChunkReader>, Error> {
        Ok(Box::new(SharedChunks(self.cr.clone())))
    }
    fn tombstones(&self) -> Result<Arc<dyn tombstones::Reader>, Error> {
        Ok(Arc::new(tombstones::MemTombstones::new()))
    }
    fn meta(&self) -> BlockMeta {
        BlockMeta { min_time: self.mint, max_time: self.maxt, ..BlockMeta::default() }
    }
    fn size(&self) -> i64 {
        0
    }
}

/// Go `mockIndexWriter`: records the series and copies of their chunks.
#[derive(Default)]
struct MockIndexWriter {
    series_chunks: Vec<(Labels, Vec<Meta>)>,
}

impl tsdb::IndexWriter for MockIndexWriter {
    fn add_symbol(&mut self, _: &str) -> Result<(), Error> {
        Ok(())
    }
    fn add_series(&mut self, _: u64, l: &Labels, chks: &[Meta]) -> Result<(), Error> {
        // Copy chunks as their bytes may be reused.
        let mut new = Vec::with_capacity(chks.len());
        for c in chks {
            let chk = c.chunk();
            let copy = chunkenc::from_data(chk.encoding(), chk.bytes().to_vec())
                .map_err(|e| Error::wrap("mockIndexWriter: copy chunk", Error::Chunk(e)))?;
            new.push(Meta { chunk_ref: 0, chunk: Some(Arc::from(copy)), min_time: c.min_time, max_time: c.max_time });
        }
        // We don't combine multiple same series together, by design as `AddSeries` requires full series to be saved.
        self.series_chunks.push((l.clone(), new));
        Ok(())
    }
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Go `nopChunkWriter`.
struct NopChunkWriter;

impl tsdb::ChunkWriter for NopChunkWriter {
    fn write_chunks(&mut self, _: &mut [Meta]) -> Result<(), Error> {
        Ok(())
    }
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Go `sample{t: t}`.
fn st(t: i64) -> Sample {
    Sample::float(0, t, 0.0)
}

fn sf(s: i64, t: i64, f: f64) -> Sample {
    Sample::float(s, t, f)
}

/// Go `samplesForRange`.
fn samples_for_range(min_time: i64, max_time: i64, max_samples_per_chunk: usize) -> Vec<Vec<Sample>> {
    let mut ret = Vec::new();
    let mut curr = Vec::new();
    for i in min_time..=max_time {
        curr.push(st(i));
        if curr.len() >= max_samples_per_chunk {
            ret.push(std::mem::take(&mut curr));
        }
    }
    if !curr.is_empty() {
        ret.push(curr);
    }
    ret
}

fn postings_a_c<'a>(ctx: &Context, reader: &'a dyn IndexReader) -> index::BoxPostings<'a> {
    match reader.postings(ctx, "a", &["c"]) {
        Ok(p) => reader.sorted_postings(p),
        Err(_) => index::empty_postings(),
    }
}

fn postings_a_cd<'a>(ctx: &Context, reader: &'a dyn IndexReader) -> index::BoxPostings<'a> {
    match reader.postings(ctx, "a", &["c", "d"]) {
        Ok(p) => reader.sorted_postings(p),
        Err(_) => index::empty_postings(),
    }
}

#[derive(Default)]
struct PCase {
    title: &'static str,
    input_series_samples: Vec<Vec<SeriesSamples>>,
    compact_min_time: i64,
    // When not defined the test runner sets a default of i64::MAX.
    compact_max_time: i64,
    ir_postings_func: Option<tsdb::compact::IndexReaderPostingsFunc>,
    exp_series_samples: Vec<SeriesSamples>,
    exp_err: Option<&'static str>,
}

#[test]
fn test_compaction_populate_block() {
    let cases: Vec<PCase> = vec![
		PCase {
			title:              "Populate block from empty input should return error.",
			input_series_samples: vec![],
			exp_err:             Some("cannot populate block from no readers"),
		..Default::default() },
		PCase {
			// Populate from single block without chunks. We expect these kind of series being ignored.
			input_series_samples: vec![
				vec![SeriesSamples {lset: lset(&[("a", "b")]), ..Default::default() }],
			],
		..Default::default() },
		PCase {
			title: "Populate from single block. We expect the same samples at the output.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from two blocks.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(1), st(9)], vec![st(10), st(19)]],
					..Default::default() },
					SeriesSamples {
						// no-chunk series should be dropped.
						lset: lset(&[("a", "empty")]),
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(21), st(30)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(40), st(45)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)], vec![st(21), st(30)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "c")]),
					chunks: vec![vec![st(1), st(9)], vec![st(10), st(19)], vec![st(40), st(45)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from two blocks; chunks with negative time.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(-11), st(-9)], vec![st(10), st(19)]],
					..Default::default() },
					SeriesSamples {
						// no-chunk series should be dropped.
						lset: lset(&[("a", "empty")]),
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(21), st(30)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(40), st(45)]],
					..Default::default() },
				],
			],
			compact_min_time: -11,
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)], vec![st(21), st(30)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "c")]),
					chunks: vec![vec![st(-11), st(-9)], vec![st(10), st(19)], vec![st(40), st(45)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from two blocks showing that order is maintained.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(1), st(9)], vec![st(10), st(19)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(21), st(30)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(40), st(45)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)], vec![st(21), st(30)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "c")]),
					chunks: vec![vec![st(1), st(9)], vec![st(10), st(19)], vec![st(40), st(45)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from two blocks showing that order of series is sorted.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "4")]),
						chunks: vec![vec![st(5), st(7)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "3")]),
						chunks: vec![vec![st(5), st(6)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "same")]),
						chunks: vec![vec![st(1), st(4)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "2")]),
						chunks: vec![vec![st(1), st(3)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "1")]),
						chunks: vec![vec![st(1), st(2)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "same")]),
						chunks: vec![vec![st(5), st(8)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "1")]),
					chunks: vec![vec![st(1), st(2)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "2")]),
					chunks: vec![vec![st(1), st(3)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "3")]),
					chunks: vec![vec![st(5), st(6)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "4")]),
					chunks: vec![vec![st(5), st(7)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "same")]),
					chunks: vec![vec![st(1), st(4)], vec![st(5), st(8)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from two blocks 1:1 duplicated chunks; with negative timestamps.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1")]),
						chunks: vec![vec![st(1), st(2)], vec![st(3), st(4)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "2")]),
						chunks: vec![vec![st(-3), st(-2)], vec![st(1), st(3), st(4)], vec![st(5), st(6)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1")]),
						chunks: vec![vec![st(3), st(4)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "2")]),
						chunks: vec![vec![st(1), st(3), st(4)], vec![st(7), st(8)]],
					..Default::default() },
				],
			],
			compact_min_time: -3,
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "1")]),
					chunks: vec![vec![st(1), st(2)], vec![st(3), st(4)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "2")]),
					chunks: vec![vec![st(-3), st(-2)], vec![st(1), st(3), st(4)], vec![st(5), st(6)], vec![st(7), st(8)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			// This should not happened because head block is making sure the chunks are not crossing block boundaries.
			// We used to return error, but now chunk is trimmed.
			title: "Populate from single block containing chunk outside of compact meta time range.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(1), st(2)], vec![st(10), st(30)]],
					..Default::default() },
				],
			],
			compact_min_time: 0,
			compact_max_time: 20,
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(1), st(2)], vec![st(10)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			// Introduced by https://github.com/prometheus/tsdb/issues/347. We used to return error, but now chunk is trimmed.
			title: "Populate from single block containing extra chunk",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "issue347")]),
						chunks: vec![vec![st(1), st(2)], vec![st(10), st(20)]],
					..Default::default() },
				],
			],
			compact_min_time: 0,
			compact_max_time: 10,
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "issue347")]),
					chunks: vec![vec![st(1), st(2)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			// Deduplication expected.
			// Introduced by pull/370 and pull/539.
			title: "Populate from two blocks containing duplicated chunk.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(1), st(2)], vec![st(10), st(20)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(10), st(20)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "b")]),
					chunks: vec![vec![st(1), st(2)], vec![st(10), st(20)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			// Introduced by https://github.com/prometheus/tsdb/pull/539.
			title: "Populate from three overlapping blocks.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "overlap-all")]),
						chunks: vec![vec![st(19), st(30)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "overlap-beginning")]),
						chunks: vec![vec![st(0), st(5)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "overlap-ending")]),
						chunks: vec![vec![st(21), st(30)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "overlap-all")]),
						chunks: vec![vec![st(0), st(10), st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "overlap-beginning")]),
						chunks: vec![vec![st(0), st(10), st(12), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "overlap-ending")]),
						chunks: vec![vec![st(0), st(10), st(13), st(20)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "overlap-all")]),
						chunks: vec![vec![st(27), st(35)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "overlap-ending")]),
						chunks: vec![vec![st(27), st(35)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "overlap-all")]),
					chunks: vec![vec![st(0), st(10), st(11), st(19), st(20), st(27), st(30), st(35)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "overlap-beginning")]),
					chunks: vec![vec![st(0), st(5), st(10), st(12), st(20)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "overlap-ending")]),
					chunks: vec![vec![st(0), st(10), st(13), st(20)], vec![st(21), st(27), st(30), st(35)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from three partially overlapping blocks with few full chunks.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "1")]),
						chunks: samples_for_range(0, 659, 120), // 5 chunks and half.
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: samples_for_range(0, 659, 120),
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: samples_for_range(480, 1199, 120), // two chunks overlapping with previous, two non overlapping and two overlapping with next block.
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "3")]),
						chunks: samples_for_range(480, 1199, 120),
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: samples_for_range(960, 1499, 120), // 5 chunks and half.
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "4")]),
						chunks: samples_for_range(960, 1499, 120),
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "1"), ("b", "1")]),
					chunks: samples_for_range(0, 659, 120),
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "1"), ("b", "2")]),
					chunks: samples_for_range(0, 1499, 120),
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "1"), ("b", "3")]),
					chunks: samples_for_range(480, 1199, 120),
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "1"), ("b", "4")]),
					chunks: samples_for_range(960, 1499, 120),
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from three partially overlapping blocks with chunks that are expected to merge into single big chunks.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: vec![vec![st(0), st(6902464)], vec![st(6961968), st(7080976)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: vec![vec![st(3600000), st(13953696)], vec![st(14042952), st(14221464)]],
					..Default::default() },
				],
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "1"), ("b", "2")]),
						chunks: vec![vec![st(10800000), st(14251232)], vec![st(14280984), st(14340488)]],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "1"), ("b", "2")]),
					chunks: vec![vec![st(0), st(3600000), st(6902464), st(6961968), st(7080976), st(10800000), st(13953696), st(14042952), st(14221464), st(14251232)], vec![st(14280984), st(14340488)]],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			// Regression test for populateWithDelChunkSeriesIterator failing to set minTime on chunks.
			title:          "Populate from mixed type series and expect sample inside the interval only.",
			compact_min_time: 1,
			compact_max_time: 11,
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset: lset(&[("a", "1")]),
						chunks: vec![
							vec![Sample::histogram(0, 0, tu::generate_test_histogram(0)), Sample::histogram(0, 1, tu::generate_test_histogram(1))],
							vec![sf(0, 10, 1.0), sf(0, 11, 2.0)],
						],
					..Default::default() },
				],
			],
			exp_series_samples: vec![
				SeriesSamples {
					lset: lset(&[("a", "1")]),
					chunks: vec![
						vec![Sample::histogram(0, 1, tu::generate_test_histogram(1))],
						vec![sf(0, 10, 1.0)],
					],
				..Default::default() },
			],
		..Default::default() },
		PCase {
			title: "Populate from single block with index reader postings function selecting different series. Expect empty block.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
				],
			],
			ir_postings_func: Some(postings_a_c as tsdb::compact::IndexReaderPostingsFunc),
		..Default::default() },
		PCase {
			title: "Populate from single block with index reader postings function selecting one series. Expect partial block.",
			input_series_samples: vec![
				vec![
					SeriesSamples {
						lset:   lset(&[("a", "b")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "c")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
					SeriesSamples {
						lset:   lset(&[("a", "d")]),
						chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
					..Default::default() },
				],
			],
			ir_postings_func: Some(postings_a_cd as tsdb::compact::IndexReaderPostingsFunc),
			exp_series_samples: vec![
				SeriesSamples {
					lset:   lset(&[("a", "c")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
				..Default::default() },
				SeriesSamples {
					lset:   lset(&[("a", "d")]),
					chunks: vec![vec![st(0), st(10)], vec![st(11), st(20)]],
				..Default::default() },
			],
		..Default::default() },

    ];
    for tc in cases {
        let mut blocks: Vec<MockBReader> = Vec::new();
        for b in &tc.input_series_samples {
            let (ir, cr, mint, maxt) = create_idx_chk_readers(b);
            blocks.push(MockBReader { ir, cr, mint, maxt });
        }
        let block_refs: Vec<&dyn BlockReader> = blocks.iter().map(|b| b as &dyn BlockReader).collect();
        let c = new_leveled_compactor(&bg(), None, vec![0], None, None).unwrap();
        let mut meta = BlockMeta { min_time: tc.compact_min_time, max_time: tc.compact_max_time, ..BlockMeta::default() };
        if meta.max_time == 0 {
            meta.max_time = i64::MAX;
        }
        let mut iw = MockIndexWriter::default();
        let ir_postings_func = tc.ir_postings_func.unwrap_or(tsdb::compact::all_sorted_postings);
        let merge_func = storage::new_compacting_chunk_series_merger(Arc::new(storage::chained_series_merge));
        let res = DefaultBlockPopulator.populate_block(
            &bg(),
            c.metrics(),
            &tsdb::Logger::nop(),
            c.chunk_pool(),
            &merge_func,
            &block_refs,
            &mut meta,
            &mut iw,
            &mut NopChunkWriter,
            ir_postings_func,
        );
        if let Some(e) = tc.exp_err {
            assert_eq!(e, res.unwrap_err().to_string(), "{}", tc.title);
            continue;
        }
        res.unwrap();

        // Check if response is expected and chunk is valid.
        let mut raw = Vec::new();
        for (l, chunks) in &iw.series_chunks {
            let mut ss = SeriesSamples { lset: l.map().into_iter().collect(), chunks: vec![] };
            for chk in chunks {
                let mut samples = Vec::new();
                let mut iter = chk.chunk().iterator(None);
                let mut first_ts = i64::MAX;
                let mut last_t = 0;
                loop {
                    let s = match iter.next() {
                        ValueType::None => break,
                        ValueType::Float => {
                            let (t, f) = iter.at();
                            Sample::float(0, t, f)
                        }
                        ValueType::Histogram => {
                            let (t, h) = iter.at_histogram(None);
                            Sample::histogram(0, t, h)
                        }
                        ValueType::FloatHistogram => {
                            let (t, fh) = iter.at_float_histogram(None);
                            Sample::float_histogram(0, t, fh)
                        }
                    };
                    if first_ts == i64::MAX {
                        first_ts = s.t;
                    }
                    last_t = s.t;
                    samples.push(s);
                }
                // Check if chunk has correct min, max times.
                assert_eq!(first_ts, chk.min_time, "chunk Meta does not match the first encoded sample timestamp: {}", tc.title);
                assert_eq!(last_t, chk.max_time, "chunk Meta does not match the last encoded sample timestamp: {}", tc.title);
                assert!(iter.err().is_none());
                ss.chunks.push(samples);
            }
            raw.push(ss);
        }
        assert_eq!(tc.exp_series_samples, raw, "{}", tc.title);

        // Check if stats are calculated properly.
        let mut s = BlockStats { num_series: tc.exp_series_samples.len() as u64, ..Default::default() };
        for series in &tc.exp_series_samples {
            s.num_chunks += series.chunks.len() as u64;
            for chk in &series.chunks {
                s.num_samples += chk.len() as u64;
                for smpl in chk {
                    if smpl.h.is_some() || smpl.fh.is_some() {
                        s.num_histogram_samples += 1;
                    } else {
                        s.num_float_samples += 1;
                    }
                }
            }
        }
        assert_eq!(s, meta.stats, "{}", tc.title);
    }
}

/// Go `blockPopulatorFunc`.
struct CanceledPopulator;

impl BlockPopulator for CanceledPopulator {
    fn populate_block(
        &self,
        _: &Context,
        _: &tsdb::compact::CompactorMetrics,
        _: &tsdb::Logger,
        _: &Arc<dyn chunkenc::Pool>,
        _: &storage::VerticalChunkSeriesMergeFunc,
        _: &[&dyn BlockReader],
        _: &mut BlockMeta,
        _: &mut dyn tsdb::IndexWriter,
        _: &mut dyn tsdb::ChunkWriter,
        _: tsdb::compact::IndexReaderPostingsFunc,
    ) -> Result<(), Error> {
        Err(storage::ERR_CANCELED)
    }
}

// Ensures that a compaction aborted via context cancellation does not mark its source blocks as Compaction.Failed.
#[test]
fn test_canceled_compaction_does_not_mark_blocks_failed() {
    let tmpdir = TempDir::new();
    let block_dirs = vec![
        create_block(tmpdir.path(), &gen_series(1, 1, 0, 100)),
        create_block(tmpdir.path(), &gen_series(1, 1, 100, 200)),
    ];
    let compactor = new_leveled_compactor(&bg(), None, vec![200], None, None).unwrap();
    let err = compactor.compact_with_block_populator(tmpdir.path(), &block_dirs, &[], &CanceledPopulator).unwrap_err();
    assert!(err.is(&storage::ERR_CANCELED));
    for dir in &block_dirs {
        let (meta, _) = tsdb::read_meta_file(dir).unwrap();
        assert!(!meta.compaction.failed, "block {} should not be marked as compaction failed", meta.ulid);
    }
}

fn ulid_ms(ms: u64) -> Ulid {
    Ulid::from_parts(ms, [0; 10])
}

#[test]
fn test_compact_block_metas() {
    use tsdb::{BlockDesc, BlockMetaCompaction};
    let (p1, p2, p3, p4) = (ulid_ms(100), ulid_ms(200), ulid_ms(300), ulid_ms(400));
    let bm = |u, mint, maxt, level, sources: Vec<Ulid>| BlockMeta {
        ulid: u,
        min_time: mint,
        max_time: maxt,
        compaction: BlockMetaCompaction { level, sources, ..Default::default() },
        ..BlockMeta::default()
    };
    let input = [
        bm(p1, 1000, 2000, 2, vec![ulid_ms(1), ulid_ms(10)]),
        bm(p2, 200, 500, 1, vec![]),
        bm(p3, 500, 2500, 3, vec![ulid_ms(5), ulid_ms(6)]),
        bm(p4, 100, 900, 1, vec![]),
    ];
    let out_ulid = ulid_ms(1000);
    let refs: Vec<&BlockMeta> = input.iter().collect();
    let output = compact_block_metas(out_ulid, &refs);
    let expected = BlockMeta {
        ulid: out_ulid,
        min_time: 100,
        max_time: 2500,
        stats: BlockStats::default(),
        compaction: BlockMetaCompaction {
            level: 4,
            sources: vec![ulid_ms(1), ulid_ms(5), ulid_ms(6), ulid_ms(10)],
            parents: vec![
                BlockDesc { ulid: p1, min_time: 1000, max_time: 2000 },
                BlockDesc { ulid: p2, min_time: 200, max_time: 500 },
                BlockDesc { ulid: p3, min_time: 500, max_time: 2500 },
                BlockDesc { ulid: p4, min_time: 100, max_time: 900 },
            ],
            ..Default::default()
        },
        ..BlockMeta::default()
    };
    assert_eq!(expected, output);
}

// Verifies that CompactBlockMetas propagates the compaction hints to the merged block. See #18379.
#[test]
fn test_compact_block_metas_hints() {
    let (p1, p2, out) = (ulid_ms(100), ulid_ms(200), ulid_ms(1000));
    let plain = |u| {
        let mut m = BlockMeta { ulid: u, min_time: 0, max_time: 1000, ..BlockMeta::default() };
        m.compaction.level = 1;
        m
    };
    let stale = |u| {
        let mut m = plain(u);
        m.compaction.set_stale_series();
        m
    };
    let ooo = |u| {
        let mut m = plain(u);
        m.compaction.set_out_of_order();
        m
    };
    // A single source carrying both the from-stale-series and the from-out-of-order hints.
    let stale_ooo = |u| {
        let mut m = plain(u);
        m.compaction.set_stale_series();
        m.compaction.set_out_of_order();
        m
    };
    let cbm = |v: Vec<BlockMeta>| compact_block_metas(out, &v.iter().collect::<Vec<_>>());

    // Single source carrying both hints keeps both.
    let o = cbm(vec![stale_ooo(p1)]);
    assert!(o.compaction.from_stale_series(), "merged block must keep the from-stale-series hint");
    assert!(o.compaction.from_out_of_order());
    // Stale+ooo source with an ooo-only source keeps both.
    let o = cbm(vec![stale_ooo(p1), ooo(p2)]);
    assert!(o.compaction.from_stale_series());
    assert!(o.compaction.from_out_of_order());
    // Stale+ooo source with a plain source keeps stale and drops ooo.
    let o = cbm(vec![stale_ooo(p1), plain(p2)]);
    assert!(o.compaction.from_stale_series());
    assert!(!o.compaction.from_out_of_order());
    // Stale hint is preserved when a source is stale.
    let o = cbm(vec![stale(p1), stale(p2)]);
    assert!(o.compaction.from_stale_series());
    assert!(!o.compaction.from_out_of_order());
    // Out-of-order hint is preserved when all sources are out-of-order.
    let o = cbm(vec![ooo(p1), ooo(p2)]);
    assert!(o.compaction.from_out_of_order());
    assert!(!o.compaction.from_stale_series());
    // Out-of-order hint is dropped when mixed with in-order.
    let o = cbm(vec![ooo(p1), plain(p2)]);
    assert!(!o.compaction.from_out_of_order());
    // Stale kept and out-of-order dropped when sources mix stale and out-of-order.
    let o = cbm(vec![stale(p1), ooo(p2)]);
    assert!(o.compaction.from_stale_series());
    assert!(!o.compaction.from_out_of_order());
    // No hint when no source carries one.
    let o = cbm(vec![plain(p1), plain(p2)]);
    assert!(!o.compaction.from_stale_series());
    assert!(!o.compaction.from_out_of_order());
}

#[test]
fn test_compact_empty_result_block_with_tombstone() {
    let ctx = bg();
    let tmpdir = TempDir::new();
    let block_dir = create_block(tmpdir.path(), &gen_series(1, 1, 0, 10));
    let block = tsdb::block::open_block(None, &block_dir, None, None).unwrap();
    // Write tombstone covering the whole block.
    block
        .delete(&ctx, 0, 10, &[model::matcher::Matcher::must_new(model::matcher::MatchType::Equal, DEFAULT_LABEL_NAME, "0")])
        .unwrap();
    let c = new_leveled_compactor(&ctx, None, vec![0], None, None).unwrap();
    let ulids = c.compact(tmpdir.path(), &[block_dir.clone()], std::slice::from_ref(&block)).unwrap();
    assert!(ulids.is_empty());
    block.close().unwrap();
}

#[test]
fn test_compaction_from_head() {
    {
        use model::matcher::{MatchType, Matcher};
        use tsdb::head::{Head, HeadOptions, HeadStats};
        // Multiple head chunks: verify compaction from a head with multiple head chunks per series produces correct
        // blocks (the chunk cache path of PopulateBlock).
        let chunk_dir = TempDir::new();
        let opts = HeadOptions { chunk_range: 100, chunk_dir_root: chunk_dir.path().to_path_buf(), ..HeadOptions::default() };
        let h = Head::new(None, None, opts, Some(HeadStats::new())).unwrap();
        let lbls = model::labels::from_strings(&["__name__", "test"]);
        let mut exp_samples = Vec::new();
        let mut app = h.appender(&bg());
        for i in (0..1000).step_by(5) {
            app.append(0, &lbls, i, i as f64).unwrap();
            exp_samples.push(sf(0, i, i as f64));
        }
        assert!(app.commit().is_none());
        // Verify we have multiple head chunks.
        assert!(h.series.get_by_id(1).unwrap().lock().head_chunk_count() > 1, "need multiple head chunks for this test");

        let dir = TempDir::new();
        let c = new_leveled_compactor(&bg(), None, vec![1000000], None, None).unwrap();
        let ulids = c.write(dir.path(), &*h, h.min_time(), h.max_time() + 1, None).unwrap();
        assert_eq!(ulids.len(), 1);
        let block = tsdb::block::open_block(None, &dir.path().join(ulids[0].to_string()), None, None).unwrap();
        let mut q = tsdb::querier::new_block_querier(&block, i64::MIN, i64::MAX).unwrap();
        {
            let mut ss = q.select(&bg(), false, None, &[Matcher::must_new(MatchType::Equal, "__name__", "test")]);
            assert!(ss.next(), "expected a series");
            let s = ss.at().clone();
            let mut it = s.iterator(None);
            let mut act = Vec::new();
            while it.next() == ValueType::Float {
                let (t, v) = it.at();
                act.push(sf(0, t, v));
            }
            assert!(it.err().is_none());
            assert!(!ss.next(), "expected only one series");
            assert!(ss.err().is_none());
            assert_eq!(exp_samples, act, "compacted block should contain all samples");
        }
        let _ = q.close();
        drop(q);
        block.close().unwrap();
        h.close().unwrap();
    }
}

fn compactor_ran(db: &tsdb::db::DB) -> f64 {
    db.leveled_compactor.as_ref().unwrap().metrics().ran.get()
}

fn block_range(db: &tsdb::db::DB) -> i64 {
    db.leveled_compactor.as_ref().unwrap().ranges()[0]
}

#[test]
fn test_disable_auto_compactions() {
    let db = new_test_db(vec![]);
    let block_range = block_range(&db);
    let label = model::labels::from_strings(&["foo", "bar"]);

    // Trigger a compaction to check that it was skipped and no new blocks were created when compaction is disabled.
    db.disable_compactions();
    let mut app = db.appender(&bg());
    for i in 0..3i64 {
        app.append(0, &label, i * block_range, 0.0).unwrap();
        app.append(0, &label, i * block_range + 1000, 0.0).unwrap();
    }
    assert!(app.commit().is_none());

    db.trigger_compaction();
    for _ in 0..10 {
        if db.metrics.compactions_skipped.get() > 0.0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(db.metrics.compactions_skipped.get() > 0.0, "No compaction was skipped after the set timeout.");
    assert!(db.mtx.read().unwrap().blocks.is_empty());

    // Enable the compaction, trigger it and check that the block is persisted.
    db.enable_compactions();
    db.trigger_compaction();
    for _ in 0..100 {
        if !db.blocks().is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(!db.blocks().is_empty(), "No block was persisted after the set timeout.");
}

/// A compactor of closures (Go `mockCompactorFn`).
struct MockCompactorFn {
    plan_fn: Box<dyn Fn() -> Result<Vec<PathBuf>, Error> + Send + Sync>,
    compact_fn: Box<dyn Fn() -> Result<Vec<Ulid>, Error> + Send + Sync>,
    write_fn: Box<dyn Fn() -> Result<Vec<Ulid>, Error> + Send + Sync>,
}

impl Compactor for MockCompactorFn {
    fn plan(&self, _: &std::path::Path) -> Result<Vec<PathBuf>, Error> {
        (self.plan_fn)()
    }
    fn write(
        &self,
        _: &std::path::Path,
        _: &dyn BlockReader,
        _: i64,
        _: i64,
        _: Option<&BlockMeta>,
    ) -> Result<Vec<Ulid>, Error> {
        (self.write_fn)()
    }
    fn compact(&self, _: &std::path::Path, _: &[PathBuf], _: &[tsdb::block::Block]) -> Result<Vec<Ulid>, Error> {
        (self.compact_fn)()
    }
}

// TestCancelCompactions ensures that when the db is closed any running compaction is cancelled to unblock closing
// the db.
#[test]
fn test_cancel_compactions() {
    use std::sync::mpsc;
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (canceled_tx, canceled_rx) = mpsc::channel::<()>();
    let chans = Arc::new((Mutex::new(Some(started_tx)), Mutex::new(Some(canceled_tx))));

    let mut opts = tsdb::db::default_options();
    opts.new_compactor_func = Some(Arc::new(move |ctx: &Context, _, _: &[i64], _, _: &tsdb::db::Options| {
        let ctx = ctx.clone();
        let chans = chans.clone();
        let c: Arc<dyn Compactor> = Arc::new(MockCompactorFn {
            plan_fn: Box::new(|| Ok(paths(&["block-a", "block-b"]))),
            compact_fn: Box::new(move || {
                drop(chans.0.lock().unwrap().take()); // close(compactionStarted)
                while !ctx.is_done() {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                drop(chans.1.lock().unwrap().take()); // close(compactionCanceled)
                Err(ctx.err().unwrap())
            }),
            // writeFn is unused: this test never reaches the head, OOO, or stale-series compaction paths that would
            // call Write.
            write_fn: Box::new(|| Ok(Vec::new())),
        });
        Ok(c)
    }));
    let db = new_test_db(vec![with_opts(opts)]);

    db.trigger_compaction();
    let _ = started_rx.recv();

    db.close().unwrap();
    let _ = canceled_rx.recv();

    // Wrapped context.Canceled must not be counted as a real compaction failure (verifies errors.Is at every
    // level of the chain).
    assert_eq!(0.0, db.metrics.compactions_failed.get());
}

// TestDeleteCompactionBlockAfterFailedReload ensures that a failed reloadBlocks immediately after a compaction
// deletes the resulting block to avoid creating blocks with the same time range.
#[test]
fn test_delete_compaction_block_after_failed_reload() {
    let head_compaction = |db: &tsdb::db::DB| -> usize {
        let range_to_trigger_compaction = block_range(db) / 2 * 3 - 1;
        let default_label = model::labels::from_strings(&["foo", "bar"]);
        // Add some data to the head that is enough to trigger a compaction.
        let mut app = db.appender(&bg());
        app.append(0, &default_label, 1, 0.0).unwrap();
        app.append(0, &default_label, 2, 0.0).unwrap();
        app.append(0, &default_label, 3 + range_to_trigger_compaction, 0.0).unwrap();
        assert!(app.commit().is_none());
        0
    };
    let block_compaction = |db: &tsdb::db::DB| -> usize {
        let blocks = [(0i64, 100i64), (100, 150), (150, 200)];
        for (mint, maxt) in blocks {
            create_block(db.dir(), &gen_series(1, 1, mint, maxt));
        }
        db.reload().unwrap();
        assert_eq!(blocks.len(), db.blocks().len(), "unexpected block count after a reloadBlocks");
        blocks.len()
    };
    let tests: Vec<(&str, &dyn Fn(&tsdb::db::DB) -> usize)> =
        vec![("Test Head Compaction", &head_compaction), ("Test Block Compaction", &block_compaction)];
    for (title, boot_strap) in tests {
        let db = new_test_db(vec![with_rngs(&[1, 100])]);
        db.disable_compactions();

        let exp_blocks = boot_strap(&db);

        // Create a block that will trigger the reloadBlocks to fail.
        let block_path = create_block(db.dir(), &gen_series(1, 1, 200, 300));
        let last_block_index = block_path.join(tsdb::INDEX_FILENAME);
        let act_blocks = tsdb::db::block_dirs(db.dir()).unwrap();
        assert_eq!(exp_blocks, act_blocks.len() - 1, "{title}"); // -1 to exclude the corrupted block.
        std::fs::remove_file(&last_block_index).unwrap(); // Corrupt the block by removing the index file.

        assert_eq!(0.0, db.metrics.reloads_failed.get(), "initial 'failed db reloadBlocks' count metrics mismatch");
        assert_eq!(0.0, compactor_ran(&db), "initial `compactions` count metric mismatch");
        assert_eq!(0.0, db.metrics.compactions_failed.get(), "initial `compactions failed` count metric mismatch");

        // Do the compaction and check the metrics. Compaction should succeed, but the reloadBlocks should fail and
        // the new block created from the compaction should be deleted.
        assert!(db.compact(&bg()).is_err());
        assert_eq!(1.0, db.metrics.reloads_failed.get(), "'failed db reloadBlocks' count metrics mismatch");
        assert_eq!(1.0, compactor_ran(&db), "`compaction` count metric mismatch");
        assert_eq!(1.0, db.metrics.compactions_failed.get(), "`compactions failed` count metric mismatch");

        let act_blocks = tsdb::db::block_dirs(db.dir()).unwrap();
        // -1 to exclude the corrupted block.
        assert_eq!(exp_blocks, act_blocks.len() - 1, "block count should be the same as before the compaction");
    }
}

#[test]
fn test_head_compaction_with_histograms() {
    use model::histogram::{CounterResetHint, Histogram, Span};
    use std::collections::BTreeMap;
    for float_test in [true, false] {
        let head = new_test_head("hcwh", DEFAULT_BLOCK_DURATION, tsdb_format::wlog::compression::Type::None, false);
        head.init(0).unwrap();

        let minute = |m: i64| m * 60 * 1000;
        let append_histogram = |lbls: &Labels, from: i64, to: i64, h: &Histogram, exp: &mut Vec<Sample>| {
            let mut app = head.appender(&bg());
            for ts_minute in from..=to {
                let hint = if ts_minute == from {
                    CounterResetHint::UnknownCounterReset
                } else {
                    CounterResetHint::NotCounterReset
                };
                if float_test {
                    let r = app.append_histogram(0, lbls, minute(ts_minute), None, Some(&h.to_float()));
                    let mut efh = h.to_float();
                    efh.counter_reset_hint = hint;
                    exp.push(Sample::float_histogram(0, minute(ts_minute), efh));
                    r.unwrap();
                } else {
                    let r = app.append_histogram(0, lbls, minute(ts_minute), Some(h), None);
                    let mut eh = h.copy();
                    eh.counter_reset_hint = hint;
                    exp.push(Sample::histogram(0, minute(ts_minute), eh));
                    r.unwrap();
                }
            }
            assert!(app.commit().is_none());
        };
        let append_float = |lbls: &Labels, from: i64, to: i64, exp: &mut Vec<Sample>| {
            let mut app = head.appender(&bg());
            for ts_minute in from..=to {
                app.append(0, lbls, minute(ts_minute), ts_minute as f64).unwrap();
                exp.push(Sample::float(0, minute(ts_minute), ts_minute as f64));
            }
            assert!(app.commit().is_none());
        };

        let series1 = model::labels::from_strings(&["foo", "bar1"]);
        let series2 = model::labels::from_strings(&["foo", "bar2"]);
        let series3 = model::labels::from_strings(&["foo", "bar3"]);
        let series4 = model::labels::from_strings(&["foo", "bar4"]);
        let (mut exp1, mut exp2, mut exp3, mut exp4) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let h = Histogram {
            count: 15,
            zero_count: 4,
            zero_threshold: 0.001,
            sum: 35.5,
            schema: 1,
            positive_spans: vec![Span { offset: 0, length: 2 }, Span { offset: 2, length: 2 }],
            positive_buckets: vec![1, 1, -1, 0],
            negative_spans: vec![Span { offset: 0, length: 1 }, Span { offset: 1, length: 2 }],
            negative_buckets: vec![1, 2, -1],
            ..Histogram::default()
        };

        // Series with only histograms.
        append_histogram(&series1, 100, 105, &h, &mut exp1);

        // Series starting with float and then getting histograms.
        append_float(&series2, 100, 102, &mut exp2);
        append_histogram(&series2, 103, 105, &h.copy(), &mut exp2);
        append_float(&series2, 106, 107, &mut exp2);
        append_histogram(&series2, 108, 109, &h.copy(), &mut exp2);

        // Series starting with histogram and then getting float.
        append_histogram(&series3, 101, 103, &h.copy(), &mut exp3);
        append_float(&series3, 104, 106, &mut exp3);
        append_histogram(&series3, 107, 108, &h.copy(), &mut exp3);
        append_float(&series3, 109, 110, &mut exp3);

        // A float only series.
        append_float(&series4, 100, 102, &mut exp4);

        // Compaction.
        let mint = head.min_time();
        let maxt = head.max_time() + 1; // Block intervals are half-open: [b.MinTime, b.MaxTime).
        let compactor =
            new_leveled_compactor(&bg(), None, vec![DEFAULT_BLOCK_DURATION], Some(Arc::new(chunkenc::new_pool())), None)
                .unwrap();
        let ids = compactor.write(&head.opts.chunk_dir_root, &**head, mint, maxt, None).unwrap();
        assert_eq!(1, ids.len());

        // Open the block and query it and check the histograms.
        let block =
            tsdb::block::open_block(None, &head.opts.chunk_dir_root.join(ids[0].to_string()), None, None).unwrap();
        let q = tsdb::querier::new_block_querier(&block, block.min_time(), block.max_time()).unwrap();
        let act_hists =
            query(q, &[model::matcher::Matcher::must_new(model::matcher::MatchType::Regexp, "foo", "bar.*")]);
        let mut exp = BTreeMap::new();
        exp.insert(series1.to_string(), exp1);
        exp.insert(series2.to_string(), exp2);
        exp.insert(series3.to_string(), exp3);
        exp.insert(series4.to_string(), exp4);
        assert_eq!(exp, act_hists);
        block.close().unwrap();
    }
}

#[test]
fn test_delayed_compaction() {
    let delay = std::time::Duration::from_secs(5);
    let label = model::labels::from_strings(&["foo", "bar"]);
    let append_samples = |db: &tsdb::db::DB, timestamps: &[i64]| {
        let mut app = db.appender(&bg());
        for &ts in timestamps {
            app.append(0, &label, ts, 0.0).unwrap();
        }
        assert!(app.commit().is_none());
    };

    // Delay not enabled.
    {
        let db = new_test_db(vec![with_rngs(&[10])]);
        let compact_and_check = || {
            let start = std::time::Instant::now();
            let _ = db.compact(&bg());
            assert!(!db.head().compactable());
            assert!(start.elapsed() < delay);
        };
        db.disable_compactions();
        append_samples(&db, &[0, 11, 21]);
        compact_and_check();
        assert_eq!(1.0, compactor_ran(&db));

        db.disable_compactions();
        append_samples(&db, &[31, 41]);
        compact_and_check();
        assert_eq!(3.0, compactor_ran(&db));
    }

    // Delay enabled.
    // adapted: Go runs this part in a synctest bubble (fake time). Here the DB clock is moved forward with
    // `test_time_offset_ms`, and `trigger_compaction_and_wait` waits until the background thread processed the
    // trigger (Go `db.compactc <- struct{}{}` and `synctest.Wait()`).
    {
        let mut opts = tsdb::db::zero_options();
        opts.compaction_delay = delay;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[10])]);
        let get_delay_start = || db.cmtx.lock().unwrap().time_when_compaction_delay_started;
        let trigger_auto_compaction = || assert!(db.trigger_compaction_and_wait());
        let sleep = |d: std::time::Duration| {
            db.test_time_offset_ms.fetch_add(d.as_millis() as i64, std::sync::atomic::Ordering::SeqCst);
        };

        // First compaction: expect 1 block.
        db.disable_compactions();
        append_samples(&db, &[0, 11, 21]);
        db.enable_compactions();

        // First trigger starts the delay period; no compaction yet.
        trigger_auto_compaction();
        assert!(get_delay_start().is_some());
        assert_eq!(0.0, compactor_ran(&db));

        // Compaction doesn't run before the delay expires.
        sleep(delay / 2);
        trigger_auto_compaction();
        assert_eq!(0.0, compactor_ran(&db));

        // Compaction runs once the delay expires.
        sleep(delay / 2);
        trigger_auto_compaction();
        // One compaction was run and one block was produced.
        assert_eq!(1.0, compactor_ran(&db));
        // The compaction delay doesn't block or wait on the trigger; 3 triggers were processed above without
        // blocking.
        assert!(db.metrics.compactions_triggered.get() - db.metrics.compactions_skipped.get() >= 3.0);
        // The delay doesn't change the head blocks alignment.
        assert_eq!(block_range(&db) + 1, db.head().min_time());

        // Second compaction: expect 2 more blocks. This ensures that the logic for compaction delay doesn't only
        // work for the first compaction, but also takes into account the future compactions. This also ensures
        // that no delay happens between consecutive compactions.
        db.disable_compactions();
        append_samples(&db, &[31, 41]);
        db.enable_compactions();

        trigger_auto_compaction();
        assert_eq!(1.0, compactor_ran(&db));

        sleep(delay / 2);
        trigger_auto_compaction();
        assert_eq!(1.0, compactor_ran(&db));

        sleep(delay / 2);
        trigger_auto_compaction();
        assert_eq!(3.0, compactor_ran(&db));

        // This test covers a special case. If auto compaction is in a delay period and a manual compaction is
        // triggered, auto compaction should stop waiting for the delay if the head is no longer compactable. Of
        // course, if the head is still compactable after the manual compaction, auto compaction will continue
        // waiting for the same delay.
        db.disable_compactions();
        append_samples(&db, &[51]);
        assert!(db.head().compactable());

        db.enable_compactions();
        // Trigger an auto compaction.
        trigger_auto_compaction();
        // That made auto compaction start waiting for the delay.
        assert!(get_delay_start().is_some());

        // Trigger a manual compaction.
        db.compact_head(&tsdb::head::RangeHead::new(db.head().clone(), 0, 50)).unwrap();
        assert_eq!(4.0, compactor_ran(&db));

        // Re-trigger an auto compaction.
        trigger_auto_compaction();
        // That made auto compaction stop waiting for the delay.
        assert!(get_delay_start().is_none());
    }
}

// TestDelayedCompactionDoesNotBlockUnrelatedOps makes sure that when delayed compaction is enabled, operations
// that don't directly derive from the Head compaction are not delayed, here we consider disk blocks compaction.
#[test]
fn test_delayed_compaction_does_not_block_unrelated_ops() {
    for (name, when_compactable) in [("Head is compactable", true), ("Head is not compactable", false)] {
        let tmpdir = TempDir::new();
        // Some blocks that need compaction are present.
        create_block(tmpdir.path(), &gen_series(1, 1, 0, 100));
        create_block(tmpdir.path(), &gen_series(1, 1, 100, 200));
        create_block(tmpdir.path(), &gen_series(1, 1, 200, 300));

        let mut options = tsdb::db::default_options();
        // This will make the test timeout if compaction really waits for it.
        options.compaction_delay = std::time::Duration::from_secs(3600);
        let db = tsdb::db::open_with_ranges(tmpdir.path(), Some(tsdb::Logger::nop()), options, vec![10, 200], None)
            .unwrap();

        db.disable_compactions();
        assert_eq!(3, db.blocks().len(), "{name}");

        if when_compactable {
            let label = model::labels::from_strings(&["foo", "bar"]);
            let mut app = db.appender(&bg());
            app.append(0, &label, 301, 0.0).unwrap();
            app.append(0, &label, 317, 0.0).unwrap();
            assert!(app.commit().is_none());
            // The Head is compactable and will still be at the end.
            assert!(db.head().compactable());
        }

        // The blocks were compacted.
        let _ = db.compact(&bg());
        assert_eq!(2, db.blocks().len(), "{name}");
        if when_compactable {
            assert!(db.head().compactable());
        }
        db.close().unwrap();
    }
}
