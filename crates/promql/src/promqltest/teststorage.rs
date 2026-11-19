//! The storage of promqltest: the real TSDB in a temporary directory (Go `util/teststorage.New`, here
//! `tsdb::teststorage`), with the options that promqltest passes.

use std::ops::Deref;
use std::sync::Arc;

use storage::{
    Appendable, AppendableV2, Appender, AppenderV2, BoxChunkQuerier, BoxQuerier, ChunkQueryable, Context, Error,
    Queryable, Storage,
};

/// The options of the test storage (the parts of `tsdb.Options` that promqltest and its users set).
#[derive(Clone, Debug)]
pub struct Options {
    /// The head chunk range in milliseconds (`MinBlockDuration` and `MaxBlockDuration`; teststorage sets 24h).
    pub chunk_range: i64,
    /// Go `SamplesPerChunk` (default 120).
    pub samples_per_chunk: usize,
    /// Go `EnableSTStorage`: keep start timestamps.
    pub enable_st_storage: bool,
    /// Go `FloatChunkEncoding == chunkenc.EncXOR2`.
    pub use_xor2: bool,
    /// Go `EnableHistogramSTEncoding`.
    pub enable_histogram_st_encoding: bool,
}

impl Default for Options {
    /// The options of Go `teststorage.New(t)` without options.
    fn default() -> Options {
        Options {
            chunk_range: 24 * 3600 * 1000,
            samples_per_chunk: 120,
            enable_st_storage: false,
            use_xor2: false,
            enable_histogram_st_encoding: false,
        }
    }
}

impl Options {
    /// The options of Go `promqltest.NewBuiltinTestStorage` (ST storage, XOR2 floats, histogram ST chunks).
    pub fn builtin() -> Options {
        Options { enable_st_storage: true, use_xor2: true, enable_histogram_st_encoding: true, ..Options::default() }
    }
}

/// Go `*teststorage.TestStorage`: a TSDB in a temporary directory. Cheap to clone; clones share the DB. The DB is
/// closed and its directory removed when the last clone is dropped.
#[derive(Clone)]
pub struct TestStorage {
    inner: Arc<tsdb::teststorage::TestStorage>,
    opts: Arc<Options>,
}

impl Default for TestStorage {
    fn default() -> TestStorage {
        TestStorage::new()
    }
}

impl TestStorage {
    /// Go `teststorage.New(t)`.
    pub fn new() -> TestStorage {
        TestStorage::with_options(Options::default())
    }

    /// Go `teststorage.New(t, opts...)`.
    pub fn with_options(opts: Options) -> TestStorage {
        let o = opts.clone();
        let inner = tsdb::teststorage::new(vec![Box::new(move |t: &mut tsdb::db::Options| {
            t.min_block_duration = o.chunk_range;
            t.max_block_duration = o.chunk_range;
            t.samples_per_chunk = o.samples_per_chunk as i64;
            t.enable_st_storage = o.enable_st_storage;
            if o.use_xor2 {
                t.float_chunk_encoding = chunkenc::ENC_XOR2;
            }
            t.enable_histogram_st_encoding = o.enable_histogram_st_encoding;
        })]);
        TestStorage { inner: Arc::new(inner), opts: Arc::new(opts) }
    }

    /// Go `promqltest.NewBuiltinTestStorage`.
    pub fn new_builtin() -> TestStorage {
        TestStorage::with_options(Options::builtin())
    }

    /// The options of the storage.
    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// A new V2 appender (Go `DB.AppenderV2`).
    pub fn appender_v2(&self) -> Box<dyn AppenderV2> {
        self.inner.db().appender_v2(&Context::background())
    }

    /// A new V1 appender (Go `DB.Appender`).
    pub fn appender_v1(&self) -> Box<dyn Appender> {
        self.inner.db().appender(&Context::background())
    }
}

impl Deref for TestStorage {
    type Target = tsdb::teststorage::TestStorage;
    fn deref(&self) -> &tsdb::teststorage::TestStorage {
        &self.inner
    }
}

impl Queryable for TestStorage {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        Queryable::querier(&*self.inner, mint, maxt)
    }
}

impl ChunkQueryable for TestStorage {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        ChunkQueryable::chunk_querier(&*self.inner, mint, maxt)
    }
}

impl Appendable for TestStorage {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        Appendable::appender(&*self.inner, ctx)
    }
}

impl AppendableV2 for TestStorage {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        AppendableV2::appender_v2(&*self.inner, ctx)
    }
}

impl storage::ExemplarQueryable for TestStorage {
    fn exemplar_querier(&self, ctx: &Context) -> Result<Box<dyn storage::ExemplarQuerier>, Error> {
        storage::ExemplarQueryable::exemplar_querier(&*self.inner, ctx)
    }
}

impl Storage for TestStorage {
    fn start_time(&self) -> Result<i64, Error> {
        Storage::start_time(&*self.inner)
    }
    fn close(&self) -> Option<Error> {
        Storage::close(&*self.inner)
    }
    fn as_cost_sampling_queryable(&self) -> Option<&dyn storage::CostSamplingQueryable> {
        Some(self)
    }
}

impl storage::CostSamplingQueryable for TestStorage {
    fn cost_sampling_queryable(&self) -> (Option<&dyn storage::SampleAndChunkQueryable>, bool) {
        storage::CostSamplingQueryable::cost_sampling_queryable(&*self.inner)
    }
}
