//! Port of `tsdb/blockwriter.go`: a block writer that appends series to a head and flushes them to a block.
//!
//! Needs the head (`crate::head`), which is ported in parallel: this module is compiled with `--cfg head` until
//! the head is merged (see lib.rs). The head API used: `Head::new(wal, wbl, HeadOptions, stats) -> Arc<Head>`,
//! `HeadOptions::default()` with `chunk_range` and `chunk_dir_root`, `Head::init`, `appender`, `appender_v2`,
//! `min_time`, `max_time`, `num_series`, `close`, and `Head: BlockReader`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use storage::{Appender, AppenderV2, Context, Error, Sentinel};

use crate::Logger;
use crate::compact::{Compactor, new_leveled_compactor};
use crate::head::{Head, HeadOptions, HeadStats};
use crate::meta::Ulid;

static NO_SERIES_APPENDED: Sentinel = Sentinel::new("no series appended, aborting");
/// Go `ErrNoSeriesAppended`: returned if the series count is zero while flushing blocks.
pub const ERR_NO_SERIES_APPENDED: Error = Error::Sentinel(&NO_SERIES_APPENDED);

/// Go `BlockWriter`: appends series to an in-memory head and flushes them to a block on disk.
pub struct BlockWriter {
    logger: Logger,
    destination_dir: PathBuf,
    head: Arc<Head>,
    // In milliseconds.
    block_size: i64,
    chunk_dir: PathBuf,
}

/// Go `NewBlockWriter`: the writer accumulates all the series in the head until `flush` is called. It does not
/// check the target directory; the caller ensures that the resulting blocks do not overlap. The flush is atomic
/// (through a rename).
pub fn new_block_writer(logger: Logger, dir: &Path, block_size: i64) -> Result<BlockWriter, Error> {
    // Go `initHead`: creates and initialises a new TSDB head in a temporary directory.
    let chunk_dir = make_temp_dir().map_err(|e| Error::wrap("create temp dir", Error::other(e)))?;
    let opts = HeadOptions { chunk_range: block_size, chunk_dir_root: chunk_dir.clone(), ..HeadOptions::default() };
    let head = Head::new(None, None, opts, Some(HeadStats::new()))
        .map_err(|e| Error::wrap("tsdb.NewHead", e))?;
    head.init(i64::MIN)?;
    Ok(BlockWriter { logger, destination_dir: dir.to_path_buf(), head, block_size, chunk_dir })
}

fn make_temp_dir() -> std::io::Result<PathBuf> {
    let base = std::env::temp_dir();
    loop {
        let mut b = [0u8; 8];
        crypto::random_bytes(&mut b);
        let p = base.join(format!("head{}", u64::from_le_bytes(b)));
        match std::fs::create_dir(&p) {
            Ok(()) => return Ok(p),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

impl BlockWriter {
    /// Go `Appender`: a new appender on the head.
    pub fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        self.head.appender(ctx)
    }

    /// Go `AppenderV2`.
    pub fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        self.head.appender_v2(ctx)
    }

    /// Go `Flush`: writes the block. After flush completes, no writes can be done. Returns the zero ULID when no
    /// block was produced.
    pub fn flush(&self, ctx: &Context) -> Result<Ulid, Error> {
        let mint = self.head.min_time();
        // Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
        let maxt = self.head.max_time() + 1;
        self.logger.info(
            "flushing",
            &[("series_count", &self.head.num_series()), ("mint", &mint), ("maxt", &maxt)],
        );
        let compactor = new_leveled_compactor(
            ctx,
            Some(self.logger.clone()),
            vec![self.block_size],
            Some(Arc::new(chunkenc::new_pool())),
            None,
        )
        .map_err(|e| Error::wrap("create leveled compactor", e))?;
        let ids = compactor
            .write(&self.destination_dir, &*self.head, mint, maxt, None)
            .map_err(|e| Error::wrap("compactor write", e))?;
        // No block was produced. The caller checks the zero ULID.
        Ok(ids.first().copied().unwrap_or_default())
    }

    /// Go `Close`.
    pub fn close(&self) -> Result<(), Error> {
        let r = self.head.close();
        if let Err(e) = std::fs::remove_dir_all(&self.chunk_dir) {
            self.logger.error("error in deleting BlockWriter files", &[("err", &e)]);
        }
        r
    }
}
