//! Port of cmd/prometheus/upload_test.go.

mod common;

use std::time::{Duration, SystemTime};

use server::helpers::{TSDB_DELAY_COMPACT_LAST_META, UploadMeta, exclude_blocks_pending_upload};
use tsdb::{BlockMeta, BlockMetaCompaction, Ulid};

/// Go `ulid.MustNew(ms, nil)`: the timestamp and zero entropy.
fn ulid(ms: u64) -> Ulid {
    let mut b = [0u8; 16];
    b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    Ulid(b)
}

fn meta(id: u64, level: i64) -> BlockMeta {
    BlockMeta { ulid: ulid(id), compaction: BlockMetaCompaction { level, ..Default::default() }, ..Default::default() }
}

fn set_cache(v: Option<(UploadMeta, SystemTime)>) {
    *TSDB_DELAY_COMPACT_LAST_META.lock().unwrap() = v;
}

#[test]
fn test_block_exclude_filter() {
    type Setup = Box<dyn Fn(&str)>;
    struct Case {
        summary: &'static str,
        uploaded: Vec<Ulid>,
        setup_fn: Option<Setup>,
        meta: BlockMeta,
        is_excluded: bool,
    }
    let cases = vec![
        Case {
            summary: "missing file",
            uploaded: vec![],
            setup_fn: Some(Box::new(|path: &str| {
                let _ = std::fs::remove_file(path);
            })),
            meta: meta(1, 0),
            is_excluded: false,
        },
        Case {
            summary: "corrupt file",
            uploaded: vec![],
            setup_fn: Some(Box::new(|path: &str| {
                let _ = std::fs::write(path, "{[");
            })),
            meta: meta(1, 0),
            is_excluded: false,
        },
        Case { summary: "empty uploaded list", uploaded: vec![], setup_fn: None, meta: meta(1, 0), is_excluded: true },
        Case {
            summary: "block meta not present in the uploaded list, level=1",
            uploaded: vec![ulid(1), ulid(3)],
            setup_fn: None,
            meta: meta(2, 1),
            is_excluded: true,
        },
        Case {
            summary: "block meta not present in the uploaded list, level=2",
            uploaded: vec![ulid(1), ulid(3)],
            setup_fn: None,
            meta: meta(2, 2),
            is_excluded: false,
        },
        Case {
            summary: "block meta present in the uploaded list",
            uploaded: vec![ulid(1), ulid(2), ulid(3)],
            setup_fn: None,
            meta: meta(2, 0),
            is_excluded: false,
        },
        Case {
            summary: "don't read the file if there's valid cache",
            uploaded: vec![],
            setup_fn: Some(Box::new(|path: &str| {
                // Remove the shipper file, cache should be used instead.
                std::fs::remove_file(path).unwrap();
                set_cache(Some((
                    UploadMeta { uploaded: vec![ulid(1).to_string(), ulid(2).to_string(), ulid(3).to_string()] },
                    SystemTime::now() - Duration::from_secs(1),
                )));
            })),
            meta: meta(2, 0),
            is_excluded: false,
        },
        Case {
            summary: "read the file if there's cache but expired",
            uploaded: vec![ulid(1), ulid(2), ulid(3)],
            setup_fn: Some(Box::new(|_: &str| {
                set_cache(Some((UploadMeta { uploaded: vec![] }, SystemTime::now() - Duration::from_secs(61))));
            })),
            meta: meta(2, 0),
            is_excluded: false,
        },
    ];
    for test in cases {
        let dir = common::TempDir::new();
        let shipper_path = dir.join("shipper.json");
        let uploaded: Vec<String> = test.uploaded.iter().map(|u| format!("{:?}", u.to_string())).collect();
        // Go json.Marshal(UploadMeta{Uploaded: uploaded}).
        let data = format!("{{\"uploaded\":[{}]}}", uploaded.join(","));
        std::fs::write(&shipper_path, data).expect("failed to write upload meta file");

        set_cache(None);
        if let Some(f) = &test.setup_fn {
            f(&shipper_path);
        }
        let f = exclude_blocks_pending_upload(discovery::Logger::nop(), shipper_path.clone());
        assert_eq!(test.is_excluded, f(&test.meta), "{}", test.summary);
    }
}
