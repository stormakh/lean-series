//! Port of `util/teststorage/storage.go`: a TSDB in a temporary directory with test options (long blocks, no
//! retention, no OOO window, exemplar storage on). The directory is under the agent data folder
//! (`bench/tmp/w2-tsdb/teststorage`) and removed when the storage is dropped.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use tsdb::db::{DB, Options};

/// Go `teststorage.TestStorage`: the DB and its directory. Closes the DB and removes the directory on drop.
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

impl Drop for TestStorage {
    fn drop(&mut self) {
        // Ignore errors, as it could be a double close.
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Go `teststorage.New`: options are applied after the test defaults.
pub fn new(opts: &[&dyn Fn(&mut Options)]) -> TestStorage {
    // Tests just load data for a series sequentially. Thus we need a long appendable window.
    let mut o = tsdb::db::default_options();
    o.min_block_duration = 24 * 60 * 60 * 1000;
    o.max_block_duration = 24 * 60 * 60 * 1000;
    o.retention_duration = 0;
    o.out_of_order_time_window = 0;
    // Enable exemplars storage by default.
    o.enable_exemplar_storage = true;
    o.max_exemplars = 100_000;
    for f in opts {
        f(&mut o);
    }
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w2-tsdb/teststorage");
    std::fs::create_dir_all(&base).expect("create test storage base dir");
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = loop {
        let n = N.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let p = base.join(format!("test_storage{}-{}", std::process::id(), n));
        if std::fs::create_dir(&p).is_ok() {
            break p;
        }
    };
    let db = tsdb::db::open(&dir, None, Some(o), Some(tsdb::db::DbStats::new())).expect("opening test storage");
    TestStorage { db, dir }
}
