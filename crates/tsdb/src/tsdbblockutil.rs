//! Port of `tsdb/tsdbblockutil.go`: creates a block from series. Needs the head (through [`crate::blockwriter`]);
//! compiled with `--cfg head` until the head is merged.

use std::path::{Path, PathBuf};

use chunkenc::{ChunkIterator, ValueType};
use storage::{AppendV2Options, Context, Error, SeriesArc, SeriesRef, Sentinel};

use crate::Logger;
use crate::blockwriter::new_block_writer;

static INVALID_TIMES: Sentinel = Sentinel::new("max time is lesser than min time");
/// Go `ErrInvalidTimes`.
pub const ERR_INVALID_TIMES: Error = Error::Sentinel(&INVALID_TIMES);

/// Go `DefaultBlockDuration` (db.go): 2 hours in milliseconds.
pub const DEFAULT_BLOCK_DURATION: i64 = 2 * 60 * 60 * 1000;

/// Go `CreateBlock`: creates a chunk range block from the series and writes it to `dir`. Returns the block
/// directory.
pub fn create_block(series: &[SeriesArc], dir: &Path, mut chunk_range: i64, logger: Logger) -> Result<PathBuf, Error> {
    if chunk_range == 0 {
        chunk_range = DEFAULT_BLOCK_DURATION;
    }
    if chunk_range < 0 {
        return Err(ERR_INVALID_TIMES);
    }
    let w = new_block_writer(logger.clone(), dir, chunk_range)?;
    let res = create_block_with(&w, series);
    if let Err(e) = w.close() {
        logger.error("err closing blockwriter", &[("err", &e)]);
    }
    let ulid = res?;
    Ok(dir.join(ulid.to_string()))
}

fn create_block_with(w: &crate::blockwriter::BlockWriter, series: &[SeriesArc]) -> Result<crate::meta::Ulid, Error> {
    const COMMIT_AFTER: usize = 10000;
    let ctx = Context::background();
    let mut sample_count = 0usize;
    let mut app = w.appender_v2(&ctx);
    let opts = AppendV2Options::default();
    for s in series {
        let mut r: SeriesRef = 0;
        let mut it = s.iterator(None);
        let lset = s.labels();
        let mut typ = it.next();
        let mut last_typ = typ;
        while typ != ValueType::None {
            if last_typ != typ {
                // The behaviour of appender is undefined if samples of different types are appended to the same
                // series in a single Commit().
                if let Some(e) = app.commit() {
                    return Err(e);
                }
                app = w.appender_v2(&ctx);
                sample_count = 0;
            }
            r = match typ {
                ValueType::Float => {
                    let (t, v) = it.at();
                    app.append(r, lset, 0, t, v, None, None, &opts)?
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    app.append(r, lset, 0, t, 0.0, Some(&h), None, &opts)?
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    app.append(r, lset, 0, t, 0.0, None, Some(&fh), &opts)?
                }
                ValueType::None => return Err(Error::msg(format!("unknown sample type {typ}"))),
            };
            sample_count += 1;
            last_typ = typ;
            typ = it.next();
        }
        if let Some(e) = it.err() {
            return Err(Error::Chunk(e.clone()));
        }
        // Commit and make a new appender periodically, to avoid building up data in memory.
        if sample_count > COMMIT_AFTER {
            if let Some(e) = app.commit() {
                return Err(e);
            }
            app = w.appender_v2(&ctx);
            sample_count = 0;
        }
    }
    if let Some(e) = app.commit() {
        return Err(e);
    }
    w.flush(&ctx)
}
