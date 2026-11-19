//! Port of `tsdb/agent/checkpoint.go`: a checkpoint written from the in-memory series, without re-reading the
//! previous checkpoint and the WAL segments.

use std::fs;
use std::path::Path;

use model::labels::Labels;
use model::metadata::{self, Handle};
use tsdb_format::chunks::HeadSeriesRef;
use tsdb_format::fileutil;
use tsdb_format::record::{self, Encoder, RefMetadata, RefSample, RefSeries};
use tsdb_format::wlog::{self, Error, Result, WL};

use crate::Logger;

/// Go `defaultBatchSize`.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Go `ActiveSeries` and `ActiveSeriesWithMetadata`: a live series to be written by [`checkpoint`].
pub trait ActiveSeries {
    fn r#ref(&self) -> HeadSeriesRef;
    fn labels(&self) -> &Labels;
    fn last_sample_timestamp(&self) -> i64;
    /// The metadata handle of the series (`Some(zero handle)` if it has none); `None` when the type does not
    /// provide metadata (Go: does not implement `ActiveSeriesWithMetadata`).
    fn metadata(&self) -> Option<&Handle> {
        None
    }
}

/// Go `DeletedSeries` and `DeletedSeriesWithMetadata`: a deleted series to be written by [`checkpoint`].
pub trait DeletedSeries {
    fn r#ref(&self) -> HeadSeriesRef;
    fn labels(&self) -> &Labels;
    /// Like [`ActiveSeries::metadata`].
    fn metadata(&self) -> Option<&Handle> {
        None
    }
}

/// Go `Checkpoint`: creates an unindexed checkpoint `checkpoint.<at_index>` containing series records, metadata
/// records (when the series provide non-empty metadata) and the last timestamp (as a sample with value 0) of
/// the active series, and series and metadata records of the deleted series.
///
/// Unlike `wlog::checkpoint`, it does not re-read the current checkpoint and segments; it relies on the data
/// in memory.
pub fn checkpoint<A: ActiveSeries, D: DeletedSeries>(
    logger: &Logger,
    w: &WL,
    at_index: i64,
    batch_size: usize,
    active_series: impl Iterator<Item = A>,
    deleted_series: impl Iterator<Item = D>,
) -> Result<()> {
    let batch_size = if batch_size == 0 { DEFAULT_BATCH_SIZE } else { batch_size };
    logger.info("Creating checkpoint", &[("atIndex", &at_index)]);

    match wlog::last_checkpoint(w.dir()) {
        Ok((dir, idx)) if idx >= at_index => {
            logger.info(
                "checkpoint already exists",
                &[("dir", &dir.display()), ("index", &idx), ("requested_index", &at_index)],
            );
            return Ok(());
        }
        Ok(_) => {}
        Err(e) if e.is_not_found() => {}
        Err(e) => return Err(e.wrap("can't find last checkpoint")),
    }

    wlog::delete_temp_checkpoints(logger, w.dir()).map_err(|e| e.wrap("failed to cleanup temporary checkpoints"))?;

    let cp_dir = wlog::checkpoint_dir(w.dir(), at_index);
    let mut tmp = cp_dir.clone().into_os_string();
    tmp.push(wlog::CHECKPOINT_TEMP_FILE_SUFFIX);
    let cp_tmp_dir = std::path::PathBuf::from(tmp);

    fs::create_dir_all(&cp_tmp_dir).map_err(|e| Error::from(e).wrap("create checkpoint dir"))?;

    // Removes the temporary directory on every return path (Go's deferred cleanup); closes the checkpoint WAL
    // unless it was closed successfully.
    struct Cleanup<'a> {
        dir: &'a Path,
        cp: Option<WL>,
    }
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            if let Some(cp) = self.cp.take() {
                let _ = cp.close();
            }
            let _ = fs::remove_dir_all(self.dir);
        }
    }

    let cp = WL::new(&cp_tmp_dir, w.compression_type()).map_err(|e| e.wrap("open checkpoint"))?;
    let mut cleanup = Cleanup {
        dir: &cp_tmp_dir,
        cp: Some(cp),
    };

    {
        let mut flusher = CheckpointWriter::new(cleanup.cp.as_ref().expect("checkpoint"), batch_size);
        flusher.write_series(active_series)?;
        flusher.write_deleted_records(deleted_series)?;
    }

    let cp = cleanup.cp.take().expect("checkpoint");
    cp.close().map_err(|e| e.wrap("close checkpoint"))?;

    let df = fileutil::open_dir(&cp_tmp_dir).map_err(|e| Error::from(e).wrap("open temporary checkpoint directory"))?;
    df.sync_all().map_err(|e| Error::from(e).wrap("sync temporary checkpoint directory"))?;
    drop(df);

    fileutil::replace(&cp_tmp_dir, &cp_dir).map_err(|e| Error::from(e).wrap("rename checkpoint directory"))?;
    drop(cleanup);
    Ok(())
}

/// Go `checkpointWriter`.
struct CheckpointWriter<'a> {
    enc: Encoder,
    checkpoint: &'a WL,
    series_buff: Vec<u8>,
    samples_buff: Vec<u8>,
    meta_buff: Vec<u8>,

    series_records: Vec<RefSeries>,
    sample_records: Vec<RefSample>,
    meta_records: Vec<RefMetadata>,
    batch_size: usize,
}

impl<'a> CheckpointWriter<'a> {
    fn new(checkpoint: &'a WL, batch_size: usize) -> CheckpointWriter<'a> {
        CheckpointWriter {
            enc: Encoder::default(),
            checkpoint,
            series_buff: Vec::new(),
            samples_buff: Vec::new(),
            meta_buff: Vec::new(),
            series_records: Vec::with_capacity(batch_size),
            sample_records: Vec::with_capacity(batch_size),
            meta_records: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    fn flush_records(&mut self) -> Result<()> {
        let with_samples = !self.sample_records.is_empty();
        let with_meta = !self.meta_records.is_empty();
        self.series_buff.clear();
        self.enc.series(&self.series_records, &mut self.series_buff);

        let r = match (with_meta, with_samples) {
            (true, true) => {
                self.meta_buff.clear();
                self.enc.metadata(&self.meta_records, &mut self.meta_buff);
                self.samples_buff.clear();
                self.enc.samples(&self.sample_records, &mut self.samples_buff);
                self.checkpoint.log_batch(&[&self.series_buff, &self.meta_buff, &self.samples_buff])
            }
            (true, false) => {
                self.meta_buff.clear();
                self.enc.metadata(&self.meta_records, &mut self.meta_buff);
                self.checkpoint.log_batch(&[&self.series_buff, &self.meta_buff])
            }
            (false, true) => {
                self.samples_buff.clear();
                self.enc.samples(&self.sample_records, &mut self.samples_buff);
                self.checkpoint.log_batch(&[&self.series_buff, &self.samples_buff])
            }
            (false, false) => self.checkpoint.log(&self.series_buff),
        };
        r.map_err(|e| e.wrap("flush records"))?;

        self.series_buff.clear();
        self.samples_buff.clear();
        self.meta_buff.clear();
        self.series_records.clear();
        self.sample_records.clear();
        self.meta_records.clear();
        Ok(())
    }

    fn push_meta(&mut self, r#ref: HeadSeriesRef, mh: Option<&Handle>) {
        if let Some(mh) = mh
            && !metadata::is_zero_handle(mh)
        {
            let m = mh.value();
            if !m.is_empty() {
                self.meta_records.push(RefMetadata {
                    r#ref,
                    r#type: record::get_metric_type(m.typ),
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                });
            }
        }
    }

    fn write_series<A: ActiveSeries>(&mut self, series_iter: impl Iterator<Item = A>) -> Result<()> {
        for series in series_iter {
            // If we filled the buffers, write them out and reset.
            if self.series_records.len() == self.batch_size {
                self.flush_records().map_err(|e| e.wrap("flush active series"))?;
            }
            self.series_records.push(RefSeries {
                r#ref: series.r#ref(),
                labels: series.labels().clone(),
            });
            self.push_meta(series.r#ref(), series.metadata());
            // The sample value is irrelevant, we only need the timestamp.
            self.sample_records.push(RefSample {
                r#ref: series.r#ref(),
                st: 0,
                t: series.last_sample_timestamp(),
                v: 0.0,
            });
        }
        // Flush the last batch if we have one.
        if !self.series_records.is_empty() {
            return self.flush_records();
        }
        Ok(())
    }

    fn write_deleted_records<D: DeletedSeries>(&mut self, series_iter: impl Iterator<Item = D>) -> Result<()> {
        for series in series_iter {
            if self.series_records.len() == self.batch_size {
                self.flush_records().map_err(|e| e.wrap("flush deleted series"))?;
            }
            // We don't care about timestamps here, so no samples.
            self.series_records.push(RefSeries {
                r#ref: series.r#ref(),
                labels: series.labels().clone(),
            });
            self.push_meta(series.r#ref(), series.metadata());
        }
        if !self.series_records.is_empty() {
            return self.flush_records();
        }
        Ok(())
    }
}
