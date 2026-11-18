//! Port of `tsdb/blockwriter_test.go`. The block writer appends to a head, which is ported in parallel: the test
//! bodies compile with `--cfg head` and the tests are ignored until the head is merged.

mod common;
fn check_block(output_dir: &std::path::Path, id: tsdb::Ulid) {
    use common::*;
    use model::matcher::{MatchType, Matcher};
    let blockpath = output_dir.join(id.to_string());
    let b = tsdb::block::open_block(None, &blockpath, None, None).unwrap();
    let q = tsdb::querier::new_block_querier(&b, i64::MIN, i64::MAX).unwrap();
    let series = query(q, &[Matcher::must_new(MatchType::Regexp, "", ".*")]);
    let mut expected = std::collections::BTreeMap::new();
    expected.insert(r#"{a="b"}"#.to_string(), vec![fs(44, 7.0)]);
    expected.insert(r#"{c="d"}"#.to_string(), vec![fs(55, 12.0)]);
    assert_eq!(expected, series);
    b.close().unwrap();
}

#[test]
fn test_block_writer() {
    {
        use common::*;
        use model::labels;
        use storage::Context;
        let ctx = Context::background();
        let output_dir = TempDir::new();
        let w = tsdb::blockwriter::new_block_writer(tsdb::Logger::nop(), output_dir.path(), DEFAULT_BLOCK_DURATION)
            .unwrap();
        // Add some series.
        let mut app = w.appender(&ctx);
        let (ts1, v1) = (44i64, 7.0);
        app.append(0, &labels::from_strings(&["a", "b"]), ts1, v1).unwrap();
        let (ts2, v2) = (55i64, 12.0);
        app.append(0, &labels::from_strings(&["c", "d"]), ts2, v2).unwrap();
        assert!(app.commit().is_none());
        let id = w.flush(&ctx).unwrap();
        // Confirm the block has the correct data.
        check_block(output_dir.path(), id);
        w.close().unwrap();
    }
}

#[test]
fn test_block_writer_appender_v2() {
    {
        use common::*;
        use model::labels;
        use storage::{AppendV2Options, Context};
        let ctx = Context::background();
        let output_dir = TempDir::new();
        let w = tsdb::blockwriter::new_block_writer(tsdb::Logger::nop(), output_dir.path(), DEFAULT_BLOCK_DURATION)
            .unwrap();
        // Add some series.
        let mut app = w.appender_v2(&ctx);
        let (ts1, v1) = (44i64, 7.0);
        app.append(0, &labels::from_strings(&["a", "b"]), 0, ts1, v1, None, None, &AppendV2Options::default()).unwrap();
        let (ts2, v2) = (55i64, 12.0);
        app.append(0, &labels::from_strings(&["c", "d"]), 0, ts2, v2, None, None, &AppendV2Options::default()).unwrap();
        assert!(app.commit().is_none());
        let id = w.flush(&ctx).unwrap();
        // Confirm the block has the correct data.
        check_block(output_dir.path(), id);
        w.close().unwrap();
    }
}
