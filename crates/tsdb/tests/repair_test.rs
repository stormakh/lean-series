//! Port of `tsdb/repair_test.go`.

mod common;

use std::path::Path;

use common::TempDir;
use model::labels::{self, ScratchBuilder};
use storage::{Context, Meta};
use tsdb::{Logger, META_VERSION_1, read_meta_file};
use tsdb_format::index::{self, Postings};

#[test]
fn test_repair_bad_index_version() {
    let tmp_dir = TempDir::new();
    let ctx = Context::background();
    let tmp_db_dir = tmp_dir.path().join("01BZJ9WJQPWHGNC2W4J9TA62KC");

    // Create a copy DB to run test against.
    let src = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../upstream/tsdb/testdata/repair_index_version/01BZJ9WJQPWHGNC2W4J9TA62KC");
    tsdb_format::fileutil::copy_dirs(&src, &tmp_db_dir).unwrap();

    // Check the current db. In its current state, lookups should fail with the fixed code.
    assert!(read_meta_file(&tmp_db_dir).is_err());

    // Touch chunks dir in block to imitate them.
    std::fs::create_dir_all(tmp_db_dir.join("chunks")).unwrap();

    // Read current index to check integrity.
    {
        let r = index::Reader::open(&tmp_db_dir.join("index"), index::decode_postings_raw).unwrap();
        let mut p = r.postings(&ctx, "b", &["1"]).unwrap();
        let mut builder = ScratchBuilder::default();
        while p.next() {
            assert!(r.series(p.at(), &mut builder, None).is_err());
        }
        assert!(p.err().is_none());
    }

    // adapted: Go opens a DB on the directory, which repairs all blocks in the base dir on opening (the DB is
    // ported separately); this runs that repair step directly.
    tsdb::repair::repair_bad_index_version(&Logger::nop(), tmp_dir.path()).unwrap();

    let r = index::Reader::open(&tmp_db_dir.join("index"), index::decode_postings_raw).unwrap();
    let mut p = r.postings(&ctx, "b", &["1"]).unwrap();
    let mut builder = ScratchBuilder::default();
    let mut res = Vec::new();
    while p.next() {
        let mut chks: Vec<Meta> = Vec::new();
        r.series(p.at(), &mut builder, Some(&mut chks)).unwrap();
        res.push(builder.labels());
    }
    assert!(p.err().is_none());
    assert_eq!(
        vec![labels::from_strings(&["a", "1", "b", "1"]), labels::from_strings(&["a", "2", "b", "1"])],
        res
    );

    let (meta, _) = read_meta_file(&tmp_db_dir).unwrap();
    assert_eq!(META_VERSION_1, meta.version, "unexpected meta version {}", meta.version);
}
