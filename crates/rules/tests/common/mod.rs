//! Port of `util/teststorage/storage.go` (`teststorage.New`): the real TSDB in a temporary directory, with the
//! options upstream tests use, and `promqltest.LoadedStorage` on top of it.
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use storage::Storage;
use tsdb::db::{Options, DB};

/// Go `teststorage.TestStorage`: a TSDB that is closed and removed on drop.
pub struct TestStorage {
    pub db: Arc<DB>,
    dir: PathBuf,
}

impl std::ops::Deref for TestStorage {
    type Target = Arc<DB>;
    fn deref(&self) -> &Arc<DB> {
        &self.db
    }
}

impl TestStorage {
    /// The DB as a storage (Go passes the `*TestStorage` where a `storage.Storage` is expected).
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.db.clone()
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        // Errors are ignored, as it could be a double close.
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

static N: AtomicU64 = AtomicU64::new(0);

/// Go `teststorage.New(t, opts...)`.
pub fn new_with(f: impl FnOnce(&mut Options)) -> TestStorage {
    // Tests load data for a series sequentially, so they need a long appendable window.
    let mut opts = tsdb::db::default_options();
    opts.min_block_duration = 24 * 3600 * 1000;
    opts.max_block_duration = 24 * 3600 * 1000;
    opts.retention_duration = 0;
    opts.out_of_order_time_window = 0;
    // Exemplar storage is on by default.
    opts.enable_exemplar_storage = true;
    opts.max_exemplars = 100_000;
    f(&mut opts);
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("test_storage-{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("opening test directory");
    let db = tsdb::db::open(&dir, None, Some(opts), None).expect("opening test storage");
    TestStorage { db, dir }
}

/// Go `teststorage.New(t)`.
pub fn new() -> TestStorage {
    new_with(|_| {})
}

/// Go `promqltest.LoadedStorage(t, input)`.
pub fn loaded_storage(input: &str) -> TestStorage {
    let st = new();
    promql::promqltest::load_into(input, st.storage()).expect("loading storage");
    st
}
