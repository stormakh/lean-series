//! Port of `util/teststorage/storage.go`: a real TSDB in a temporary directory for tests.
//!
//! It lives here and not in the `util` crate because `tsdb` must not depend on `util`. The appender mock of the
//! same Go package is `util::teststorage`.

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use storage::{
    Appender, AppenderV2, BoxChunkQuerier, BoxQuerier, Context, Error, SampleAndChunkQueryable,
};

use crate::db::{self, DbStats, Options, DB};

/// Go `teststorage.Option`.
pub type StorageOption = Box<dyn FnOnce(&mut Options)>;

/// Go `teststorage.TestStorage`: a DB in a temporary directory. Dropping it closes the DB and removes the
/// directory (Go does that through `t.Cleanup` or `Close`).
pub struct TestStorage {
    db: Arc<DB>,
    dir: PathBuf,
}

/// Go `teststorage.New`: panics on error (Go `require.NoError`).
#[track_caller]
pub fn new(o: Vec<StorageOption>) -> TestStorage {
    match new_with_error(o) {
        Ok(s) => s,
        Err(e) => panic!("{e}"),
    }
}

/// Go `teststorage.New` without options.
#[track_caller]
pub fn new_default() -> TestStorage {
    new(Vec::new())
}

/// A shared [`TestStorage`] that is closed (and its directory removed) when this handle is dropped, even if
/// clones of the `Arc` still live elsewhere, for example in a scrape thread that has not ended yet (Go
/// `teststorage.New(t)` closes it in `t.Cleanup`).
pub struct Shared(Arc<TestStorage>);

impl Deref for Shared {
    type Target = Arc<TestStorage>;
    fn deref(&self) -> &Arc<TestStorage> {
        &self.0
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Ignore errors, as it could be a double close.
        let _ = self.0.close();
    }
}

/// Go `teststorage.New(t, o...)` for a storage that is shared with appendables (see [`Shared`]).
#[track_caller]
pub fn new_shared(o: Vec<StorageOption>) -> Shared {
    Shared(Arc::new(new(o)))
}

/// Go `teststorage.New(t)` for a storage that is shared with appendables (see [`Shared`]).
#[track_caller]
pub fn new_shared_default() -> Shared {
    new_shared(Vec::new())
}

/// Go `NewWithError`.
pub fn new_with_error(o: Vec<StorageOption>) -> Result<TestStorage, Error> {
    // Tests just load data for a series sequentially. Thus we need a long appendable window.
    let mut opts = db::default_options();
    opts.min_block_duration = 24 * 60 * 60 * 1000;
    opts.max_block_duration = 24 * 60 * 60 * 1000;
    opts.retention_duration = 0;
    opts.out_of_order_time_window = 0;

    // Enable exemplars storage by default.
    opts.enable_exemplar_storage = true;
    opts.max_exemplars = 100_000;

    for opt in o {
        opt(&mut opts);
    }

    let dir = mkdir_temp("test_storage")
        .map_err(|e| Error::msg(format!("opening test directory: {}", crate::io_err_text(&e))))?;

    let db = db::open(&dir, None, Some(opts), Some(DbStats::new()))
        .map_err(|e| Error::wrap("opening test storage", e))?;
    Ok(TestStorage { db, dir })
}

/// Go `os.MkdirTemp("", pattern)`.
fn mkdir_temp(pattern: &str) -> std::io::Result<PathBuf> {
    static N: AtomicU64 = AtomicU64::new(0);
    let base = std::env::temp_dir();
    loop {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let d = base.join(format!("{pattern}{}{nanos}{n}", std::process::id()));
        match std::fs::create_dir(&d) {
            Ok(()) => return Ok(d),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

impl TestStorage {
    /// The DB (Go embeds `*tsdb.DB`).
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    /// The temporary directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Go `TestStorage.Close`: closes the DB and removes the directory. Closing twice is fine.
    pub fn close(&self) -> Result<(), Error> {
        self.db.close()?;
        match std::fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::msg(crate::io_err_text(&e))),
        }
    }
}

impl Deref for TestStorage {
    type Target = DB;
    fn deref(&self) -> &DB {
        &self.db
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        // Ignore errors, as it could be a double close.
        let _ = self.close();
    }
}

impl storage::Queryable for TestStorage {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        self.db.querier(mint, maxt)
    }
}

impl storage::ChunkQueryable for TestStorage {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        self.db.chunk_querier(mint, maxt)
    }
}

impl storage::Appendable for TestStorage {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        self.db.appender(ctx)
    }
}

impl storage::AppendableV2 for TestStorage {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        self.db.appender_v2(ctx)
    }
}

impl storage::ExemplarQueryable for TestStorage {
    fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn storage::ExemplarQuerier>, Error> {
        self.db.exemplar_querier(ctx)
    }
}

impl storage::CostSamplingQueryable for TestStorage {
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        storage::CostSamplingQueryable::cost_sampling_queryable(&*self.db)
    }
}

impl storage::Storage for TestStorage {
    fn start_time(&self) -> Result<i64, Error> {
        self.db.start_time()
    }
    fn close(&self) -> Option<Error> {
        TestStorage::close(self).err()
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn storage::CostSamplingQueryable> {
        Some(self)
    }
}
