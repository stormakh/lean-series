//! Helpers shared by the tsdb and backfill tests of promtool (Go test helpers of tsdb_test.go and
//! util/teststorage).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use promtool::tsdb_cmd::{SeriesSetFormatter, dump_tsdb_data};
use storage::Context;

static N: AtomicU64 = AtomicU64::new(0);

/// Go `t.TempDir()`: a new directory under `bench/tmp/pt-tsdb`, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> TempDir {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/pt-tsdb").join(format!(
            "test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("create temp dir");
        TempDir(p)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn str(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Go `teststorage.TestStorage`: a real TSDB in a temporary directory.
pub struct TestStorage {
    pub db: Arc<tsdb::db::DB>,
    pub dir: TempDir,
}

impl TestStorage {
    /// Go `Dir()`.
    pub fn dir(&self) -> String {
        self.dir.str()
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        // Ignore errors, as it could be a double close.
        let _ = self.db.close();
    }
}

/// Go `teststorage.New(t)`.
pub fn new_test_storage() -> TestStorage {
    // Tests just load data for a series sequentially. Thus we need a long appendable window.
    let mut opts = tsdb::db::default_options();
    opts.min_block_duration = 24 * 3600 * 1000;
    opts.max_block_duration = 24 * 3600 * 1000;
    opts.retention_duration = 0;
    opts.out_of_order_time_window = 0;
    // Enable exemplars storage by default.
    opts.enable_exemplar_storage = true;
    opts.max_exemplars = 100_000;
    let dir = TempDir::new();
    let db = tsdb::db::open(dir.path(), None, Some(opts), Some(tsdb::db::DbStats::new())).expect("opening test storage");
    TestStorage { db, dir }
}

/// Go `promqltest.LoadedStorage(t, input)`.
pub fn loaded_storage(input: &str) -> TestStorage {
    let st = new_test_storage();
    promql::promqltest::load_into(input, st.db.clone()).expect("load");
    st
}

/// Go `getDumpedSamples`: dumps samples and returns them.
pub fn get_dumped_samples(
    database_path: &str,
    sandbox_dir_root: &str,
    mint: i64,
    maxt: i64,
    match_: &[&str],
    formatter: SeriesSetFormatter,
) -> String {
    let mut out: Vec<u8> = Vec::new();
    let match_: Vec<String> = match_.iter().map(|s| s.to_string()).collect();
    let res = dump_tsdb_data(
        &Context::background(),
        database_path,
        sandbox_dir_root,
        mint,
        maxt,
        &match_,
        formatter,
        &promql::parser::Parser::new(promql::parser::Options::default()),
        &mut out,
    );
    if let Err(e) = res {
        panic!("unexpected error: {e}");
    }
    String::from_utf8(out).expect("utf8")
}

/// Go `normalizeNewLine` (only Windows changes anything).
pub fn normalize_new_line(b: Vec<u8>) -> Vec<u8> {
    b
}

/// Go `sortLines`.
pub fn sort_lines(buf: &str) -> String {
    let mut lines: Vec<&str> = buf.split('\n').collect();
    lines.sort();
    lines.join("\n")
}

/// A file of upstream cmd/promtool/testdata.
pub fn testdata(name: &str) -> String {
    format!("{}/../../upstream/cmd/promtool/testdata/{}", env!("CARGO_MANIFEST_DIR"), name)
}
