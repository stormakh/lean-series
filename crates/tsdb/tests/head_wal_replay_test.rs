//! Port of `tsdb/head_wal_replay_test.go`.

use tsdb::head::MmappedChunk;
use tsdb::head_wal::merge_replay_mmapped_chunks;
use tsdb_format::chunks::ChunkDiskMapperRef;

fn mc(r: u64, min_time: i64, max_time: i64) -> MmappedChunk {
    MmappedChunk {
        r#ref: ChunkDiskMapperRef(r),
        num_samples: 0,
        encoding: chunkenc::ENC_NONE,
        min_time,
        max_time,
    }
}

#[test]
fn test_merge_replay_mmapped_chunks() {
    let a = mc(1, 100, 150);
    let b = mc(2, 200, 250);
    // Disk order can differ from time order after series references are merged.
    let c = mc(3, 0, 50);
    let replayed = mc(4, 300, 350);
    let overlap = mc(5, 125, 225);
    struct Tc {
        name: &'static str,
        existing: Vec<MmappedChunk>,
        incoming: Vec<MmappedChunk>,
        want: Vec<MmappedChunk>,
        last_ref: u64,
        ooo: bool,
    }
    let tc = |name, existing: &[MmappedChunk], incoming: &[MmappedChunk], want: &[MmappedChunk], last_ref, ooo| Tc {
        name,
        existing: existing.to_vec(),
        incoming: incoming.to_vec(),
        want: want.to_vec(),
        last_ref,
        ooo,
    };
    let cases = [
        tc("first definition", &[], &[a, b], &[a, b], 3, false),
        tc("empty later definition", &[a, b], &[], &[a, b], 3, false),
        tc("earlier chunks arrive later", &[b], &[c, a], &[c, a, b], 3, false),
        tc("later chunks arrive later", &[c, a], &[b], &[c, a, b], 3, false),
        tc("repeated definition", &[a, b], &[a, b], &[a, b], 3, false),
        tc("discard replayed chunks", &[c, a, replayed], &[b], &[c, a, b], 3, false),
        tc("discard all replayed chunks", &[a, b], &[], &[], 0, false),
        tc("overlapping in-order chunks", &[c, a, b], &[overlap], &[c, overlap], 5, false),
        tc("OOO sorted by disk reference", &[b], &[a, c], &[a, b, c], 3, true),
        tc("repeated OOO definition", &[a, b], &[a, b], &[a, b], 3, true),
        tc("overlapping OOO chunks", &[a, b], &[overlap], &[a, b, overlap], 5, true),
    ];
    for tc in cases {
        let existing = tc.existing.clone();
        let incoming = tc.incoming.clone();
        assert_eq!(
            tc.want,
            merge_replay_mmapped_chunks(&tc.existing, &tc.incoming, ChunkDiskMapperRef(tc.last_ref), tc.ooo),
            "{}",
            tc.name
        );
        // adapted: Go checks that the input slices are not modified; here they are shared references, so the
        // check is that their contents are unchanged.
        assert_eq!(existing, tc.existing, "do not modify the attached chunk list: {}", tc.name);
        assert_eq!(incoming, tc.incoming, "do not modify the replay inventory: {}", tc.name);
    }
}
