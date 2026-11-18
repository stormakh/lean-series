//! Port of `tsdb/compact_test.go`. Tests that need the head or the DB run their body with `--cfg head` and are
//! ignored until those are merged.

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::*;
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

//REST//
