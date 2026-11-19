//! Port of `checkpoint.go`: compacted checkpoints of WAL segments.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::segment::{SegmentRange, go_atoi, new_segments_range_reader, new_segments_reader};
use super::{Error, Reader, Result, WL};
use crate::chunks::HeadSeriesRef;
use crate::fileutil;
use crate::record::{self, Decoder, Encoder, Logger, RefMetadata};

/// Go `CheckpointStats`: stats about a created checkpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    pub dropped_series: usize,
    pub dropped_samples: usize, // Includes histograms.
    pub dropped_tombstones: usize,
    pub dropped_exemplars: usize,
    pub dropped_metadata: usize,
    pub total_series: usize,     // Processed series including dropped ones.
    pub total_samples: usize,    // Processed float and histogram samples including dropped ones.
    pub total_tombstones: usize, // Processed tombstones including dropped ones.
    pub total_exemplars: usize,  // Processed exemplars including dropped ones.
    pub total_metadata: usize,   // Processed metadata including dropped ones.
}

/// Go `LastCheckpoint`: the directory name and index of the most recent checkpoint. If `dir` does not contain
/// any checkpoints, `Error::NotFound` is returned.
pub fn last_checkpoint(dir: &Path) -> Result<(PathBuf, i64)> {
    let checkpoints = list_checkpoints(dir)?;
    let Some(checkpoint) = checkpoints.last() else {
        return Err(Error::NotFound);
    };
    Ok((dir.join(&checkpoint.name), checkpoint.index))
}

/// Go `ReadMinValidTime`: the mint recorded by the most recent checkpoint in `dir`, i.e. the highest mint any
/// WAL truncation has used so far. `None`, with no error, if `dir` has no checkpoint yet, or its most recent
/// checkpoint predates this record (e.g. it was written by an older Prometheus version).
pub fn read_min_valid_time(dir: &Path) -> Result<Option<i64>> {
    let cpdir = match last_checkpoint(dir) {
        Ok((d, _)) => d,
        Err(Error::NotFound) => return Ok(None),
        Err(e) => return Err(e.wrap("find last checkpoint")),
    };

    let sr = new_segments_reader(&cpdir).map_err(|e| e.wrap("open checkpoint"))?;

    let mut r = Reader::new(sr);
    if !r.next() {
        return match r.err() {
            Some(e) => Err(e),
            None => Ok(None),
        };
    }

    let dec = Decoder::new(Logger::nop());
    let rec = r.record();
    if dec.r#type(rec) != record::Type::MinValidTime {
        // An older checkpoint, written before this record existed.
        return Ok(None);
    }
    let mint = dec
        .min_valid_time(rec)
        .map_err(|e| rec_err(e).wrap("decode min valid time"))?;
    Ok(Some(mint))
}

/// Go `DeleteCheckpoints`: deletes all checkpoints in a directory below a given index.
pub fn delete_checkpoints(dir: &Path, max_index: i64) -> Result<()> {
    let checkpoints = list_checkpoints(dir)?;

    let mut errs: Vec<String> = Vec::new();
    for checkpoint in checkpoints {
        if checkpoint.index >= max_index {
            break;
        }
        if let Err(e) = remove_all(&dir.join(&checkpoint.name)) {
            errs.push(e.to_string());
        }
    }
    if errs.is_empty() {
        Ok(())
    } else {
        Err(Error::Msg(errs.join("\n")))
    }
}

/// Go `os.RemoveAll`: a missing path is not an error.
fn remove_all(p: &Path) -> io::Result<()> {
    let r = match fs::symlink_metadata(p) {
        Ok(md) if md.is_dir() => fs::remove_dir_all(p),
        Ok(_) => fs::remove_file(p),
        Err(e) => Err(e),
    };
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        r => r,
    }
}

/// Go `CheckpointTempFileSuffix`: the suffix used when creating temporary checkpoint files.
pub const CHECKPOINT_TEMP_FILE_SUFFIX: &str = ".tmp";

/// Go `DeleteTempCheckpoints`: deletes all temporary checkpoint directories in the given directory.
pub fn delete_temp_checkpoints(logger: &Logger, dir: &Path) -> Result<()> {
    remove_tmp_dirs(logger, dir)
        .map_err(|e| Error::from(e).wrap("remove previous temporary checkpoint dirs"))
}

/// Go `tsdbutil.RemoveTmpDirs` with `isTempDir`.
fn remove_tmp_dirs(logger: &Logger, dir: &Path) -> io::Result<()> {
    let files = match fs::read_dir(dir) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for f in files {
        let f = f?;
        let name = f.file_name();
        let Some(name) = name.to_str() else { continue };
        if is_temp_dir(name) {
            let p = dir.join(name);
            if let Err(e) = remove_all(&p) {
                logger.error("failed to delete tmp dir", &[("dir", &p.display()), ("err", &e)]);
                continue;
            }
            logger.info("Found and deleted tmp dir", &[("dir", &p.display())]);
        }
    }
    Ok(())
}

fn is_temp_dir(name: &str) -> bool {
    name.starts_with(CHECKPOINT_PREFIX) && name.ends_with(CHECKPOINT_TEMP_FILE_SUFFIX)
}

/// Go `metadataBatchSize`: how many series a single checkpoint metadata record covers.
pub const METADATA_BATCH_SIZE: usize = 1000;
/// Go `flushThreshold`: how many encoded bytes accumulate before they are written out.
pub const FLUSH_THRESHOLD: usize = 1024 * 1024;

fn rec_err(e: record::Error) -> Error {
    Error::Msg(e.to_string())
}

/// Removes the temporary checkpoint directory on every return path (Go's deferred cleanup).
struct TmpCheckpoint<'a> {
    cp: &'a WL,
    dir: &'a Path,
}

impl Drop for TmpCheckpoint<'_> {
    fn drop(&mut self) {
        let _ = self.cp.close();
        let _ = remove_all(self.dir);
    }
}

/// Go `Checkpoint`: creates a compacted checkpoint of segments in range `[from, to]` in the given WAL.
/// It includes the most recent checkpoint if it exists.
/// All series not satisfying `keep`, samples/exemplars below `mint`, tombstones not satisfying `keep` or with
/// all intervals below `mint`, and metadata that are not the latest are dropped.
///
/// `keep` is evaluated per record as segments are read, so its result for a given ref must not change while
/// `checkpoint` runs.
///
/// The checkpoint is stored in a directory named `checkpoint.N` in the same segmented format as the original
/// WAL itself. This makes it easy to read it through the WAL package and concatenate it with the original WAL.
///
/// `write_min_valid_time` controls whether the checkpoint also carries a record of `mint`, readable back with
/// `read_min_valid_time`.
#[allow(clippy::too_many_arguments)]
pub fn checkpoint(
    w: &WL,
    mut from: i64,
    to: i64,
    keep: &dyn Fn(HeadSeriesRef) -> bool,
    mint: i64,
    enable_st_storage: bool,
    write_min_valid_time: bool,
) -> Result<CheckpointStats> {
    let mut stats = CheckpointStats::default();

    let logger = w.logger();
    logger.info(
        "Creating checkpoint",
        &[("from_segment", &from), ("to_segment", &to), ("mint", &mint)],
    );

    let sgm_reader = {
        let mut sgm_range = Vec::new();
        match last_checkpoint(w.dir()) {
            Ok((dir, idx)) => {
                let last = idx + 1;
                if from > last {
                    return Err(Error::Msg(format!(
                        "unexpected gap to last checkpoint. expected:{last}, requested:{from}"
                    )));
                }
                // Ignore WAL files below the checkpoint. They shouldn't exist to begin with.
                from = last;

                sgm_range.push(SegmentRange {
                    dir,
                    first: 0,
                    last: i32::MAX as i64,
                });
            }
            Err(Error::NotFound) => {}
            Err(e) => return Err(e.wrap("find last checkpoint")),
        }

        sgm_range.push(SegmentRange {
            dir: w.dir().to_path_buf(),
            first: from,
            last: to,
        });
        new_segments_range_reader(&sgm_range).map_err(|e| e.wrap("create segment reader"))?
    };

    delete_temp_checkpoints(logger, w.dir())?;

    let cpdir = checkpoint_dir(w.dir(), to);
    let mut cpdirtmp = cpdir.clone().into_os_string();
    cpdirtmp.push(CHECKPOINT_TEMP_FILE_SUFFIX);
    let cpdirtmp = PathBuf::from(cpdirtmp);

    fs::create_dir_all(&cpdirtmp).map_err(|e| Error::from(e).wrap("create checkpoint dir"))?;
    let cp = WL::new(&cpdirtmp, w.compression_type()).map_err(|e| e.wrap("open checkpoint"))?;

    // Ensures that an early return caused by an error doesn't leave any tmp files.
    let _guard = TmpCheckpoint {
        cp: &cp,
        dir: &cpdirtmp,
    };

    if write_min_valid_time {
        // Persist mint as the checkpoint's first record, so a restart can recover it. Read back
        // whatever the previous checkpoint recorded and keep the higher of the two, so the
        // persisted value never regresses across restarts.
        let mut persisted_min_valid_time = mint;
        let previous =
            read_min_valid_time(w.dir()).map_err(|e| e.wrap("read previous min valid time"))?;
        if let Some(previous) = previous
            && previous > persisted_min_valid_time
        {
            persisted_min_valid_time = previous;
        }
        let mut b = Vec::new();
        Encoder::default().min_valid_time(persisted_min_valid_time, &mut b);
        cp.log(&b)
            .map_err(|e| e.wrap("write min valid time record"))?;
    }

    let mut r = Reader::new(sgm_reader);

    let mut series = Vec::new();
    let mut samples = Vec::new();
    let mut histogram_samples = Vec::new();
    let mut float_histogram_samples = Vec::new();
    let mut tstones = Vec::new();
    let mut exemplars = Vec::new();
    let mut metadata: Vec<RefMetadata> = Vec::new();
    let mut dec = Decoder::new(logger.clone());
    let enc = Encoder { enable_st_storage };
    let mut buf: Vec<u8> = Vec::new();
    // Records waiting in `buf`, as byte ranges (Go keeps sub-slices of buf).
    let mut recs: Vec<(usize, usize)> = Vec::new();
    // The V1 histogram encoders clear their whole output buffer when every sample uses custom buckets (Go
    // `buf.Reset()`); encoding into this scratch buffer keeps the records already batched in `buf`.
    let mut scratch: Vec<u8> = Vec::new();

    let mut latest_metadata_map: HashMap<HeadSeriesRef, RefMetadata> = HashMap::new();

    while r.next() {
        series.clear();
        samples.clear();
        histogram_samples.clear();
        float_histogram_samples.clear();
        tstones.clear();
        exemplars.clear();
        metadata.clear();

        // We don't reset the buffer since we batch up multiple records
        // before writing them to the checkpoint.
        // Remember where the record for this iteration starts.
        let mut start = buf.len();
        let rec = r.record();

        match dec.r#type(rec) {
            record::Type::Series => {
                dec.series(rec, &mut series)
                    .map_err(|e| rec_err(e).wrap("decode series"))?;
                let total = series.len();
                // Drop irrelevant series in place.
                series.retain(|s| keep(s.r#ref));
                if !series.is_empty() {
                    enc.series(&series, &mut buf);
                }
                stats.total_series += total;
                stats.dropped_series += total - series.len();
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                dec.samples(rec, &mut samples)
                    .map_err(|e| rec_err(e).wrap("decode samples"))?;
                let total = samples.len();
                // Drop irrelevant samples in place.
                samples.retain(|s| s.t >= mint);
                if !samples.is_empty() {
                    enc.samples(&samples, &mut buf);
                }
                stats.total_samples += total;
                stats.dropped_samples += total - samples.len();
            }
            record::Type::HistogramSamples | record::Type::HistogramSamplesV2 => {
                dec.histogram_samples(rec, &mut histogram_samples)
                    .map_err(|e| rec_err(e).wrap("decode histogram samples"))?;
                let total = histogram_samples.len();
                // Drop irrelevant histogram samples in place.
                histogram_samples.retain(|h| h.t >= mint);
                if !histogram_samples.is_empty() {
                    scratch.clear();
                    let leftover = enc.histogram_samples(&histogram_samples, &mut scratch);
                    buf.extend_from_slice(&scratch);
                    if !leftover.is_empty() {
                        // Flush the exponential-histogram record before
                        // appending the custom-bucket record so they are
                        // written as two separate WAL records.
                        let exp_end = buf.len();
                        if exp_end > start {
                            recs.push((start, exp_end));
                            start = exp_end;
                        }
                        enc.custom_buckets_histogram_samples(&leftover, &mut buf);
                    }
                }
                stats.total_samples += total;
                stats.dropped_samples += total - histogram_samples.len();
            }
            record::Type::CustomBucketsHistogramSamples => {
                dec.histogram_samples(rec, &mut histogram_samples)
                    .map_err(|e| rec_err(e).wrap("decode histogram samples"))?;
                let total = histogram_samples.len();
                histogram_samples.retain(|h| h.t >= mint);
                if !histogram_samples.is_empty() {
                    enc.custom_buckets_histogram_samples(&histogram_samples, &mut buf);
                }
                stats.total_samples += total;
                stats.dropped_samples += total - histogram_samples.len();
            }
            record::Type::FloatHistogramSamples | record::Type::FloatHistogramSamplesV2 => {
                dec.float_histogram_samples(rec, &mut float_histogram_samples)
                    .map_err(|e| rec_err(e).wrap("decode float histogram samples"))?;
                let total = float_histogram_samples.len();
                float_histogram_samples.retain(|fh| fh.t >= mint);
                if !float_histogram_samples.is_empty() {
                    scratch.clear();
                    let float_leftover =
                        enc.float_histogram_samples(&float_histogram_samples, &mut scratch);
                    buf.extend_from_slice(&scratch);
                    if !float_leftover.is_empty() {
                        // Flush the exponential-float-histogram record before
                        // appending the custom-bucket record so they are
                        // written as two separate WAL records.
                        let exp_end = buf.len();
                        if exp_end > start {
                            recs.push((start, exp_end));
                            start = exp_end;
                        }
                        enc.custom_buckets_float_histogram_samples(&float_leftover, &mut buf);
                    }
                }
                stats.total_samples += total;
                stats.dropped_samples += total - float_histogram_samples.len();
            }
            record::Type::CustomBucketsFloatHistogramSamples => {
                dec.float_histogram_samples(rec, &mut float_histogram_samples)
                    .map_err(|e| rec_err(e).wrap("decode float histogram samples"))?;
                let total = float_histogram_samples.len();
                float_histogram_samples.retain(|fh| fh.t >= mint);
                if !float_histogram_samples.is_empty() {
                    enc.custom_buckets_float_histogram_samples(&float_histogram_samples, &mut buf);
                }
                stats.total_samples += total;
                stats.dropped_samples += total - float_histogram_samples.len();
            }
            record::Type::Tombstones => {
                dec.tombstones(rec, &mut tstones)
                    .map_err(|e| rec_err(e).wrap("decode deletes"))?;
                let total = tstones.len();
                // Drop irrelevant tombstones in place. A tombstone is dropped together with
                // its series record, or once all its intervals age out of the WAL.
                tstones.retain(|s| {
                    keep(s.r#ref as HeadSeriesRef) && s.intervals.iter().any(|iv| iv.maxt >= mint)
                });
                if !tstones.is_empty() {
                    enc.tombstones(&tstones, &mut buf);
                }
                stats.total_tombstones += total;
                stats.dropped_tombstones += total - tstones.len();
            }
            record::Type::Exemplars => {
                dec.exemplars(rec, &mut exemplars)
                    .map_err(|e| rec_err(e).wrap("decode exemplars"))?;
                let total = exemplars.len();
                exemplars.retain(|e| e.t >= mint);
                if !exemplars.is_empty() {
                    enc.exemplars(&exemplars, &mut buf);
                }
                stats.total_exemplars += total;
                stats.dropped_exemplars += total - exemplars.len();
            }
            record::Type::Metadata => {
                dec.metadata(rec, &mut metadata)
                    .map_err(|e| rec_err(e).wrap("decode metadata"))?;
                // Only keep reference to the latest found metadata for each refID.
                let total = metadata.len();
                let mut repl = 0;
                for m in metadata.drain(..) {
                    if keep(m.r#ref) && latest_metadata_map.insert(m.r#ref, m).is_none() {
                        repl += 1;
                    }
                }
                stats.total_metadata += total;
                stats.dropped_metadata += total - repl;
            }
            // The previous checkpoint's own copy of MinValidTime is safe to drop unconditionally: its value
            // was already folded into the record written above. Unknown record types, probably from a
            // future Prometheus version, are dropped too.
            _ => continue,
        }
        if buf.len() == start {
            continue; // All contents discarded.
        }
        recs.push((start, buf.len()));

        if buf.len() > FLUSH_THRESHOLD {
            log_ranges(&cp, &buf, &recs).map_err(|e| e.wrap("flush records"))?;
            buf.clear();
            recs.clear();
        }
    }
    // If we hit any corruption during checkpointing, repairing is not an option.
    // The head won't know which series records are lost.
    if let Some(e) = r.err() {
        return Err(e.wrap("read segments"));
    }

    // Flush remaining records.
    log_ranges(&cp, &buf, &recs).map_err(|e| e.wrap("flush records"))?;

    // Flush the latest metadata record for each series. Batching caps the size of
    // a single record, which bounds both the buffer built here and the buffer a
    // Reader reassembles the record into on the way back out.
    buf.clear();
    recs.clear();
    metadata.clear();
    let mut remaining = latest_metadata_map.len();
    for (_, m) in latest_metadata_map.drain() {
        metadata.push(m);
        remaining -= 1;
        if metadata.len() < METADATA_BATCH_SIZE && remaining > 0 {
            continue;
        }

        let start = buf.len();
        enc.metadata(&metadata, &mut buf);
        recs.push((start, buf.len()));
        metadata.clear();

        if buf.len() > FLUSH_THRESHOLD {
            log_ranges(&cp, &buf, &recs).map_err(|e| e.wrap("flush metadata records"))?;
            buf.clear();
            recs.clear();
        }
    }
    if !recs.is_empty() {
        log_ranges(&cp, &buf, &recs).map_err(|e| e.wrap("flush metadata records"))?;
    }

    cp.close().map_err(|e| e.wrap("close checkpoint"))?;

    // Sync temporary directory before rename.
    let df = fileutil::open_dir(&cpdirtmp)
        .map_err(|e| Error::from(e).wrap("open temporary checkpoint directory"))?;
    df.sync_all()
        .map_err(|e| Error::from(e).wrap("sync temporary checkpoint directory"))?;
    drop(df);

    fileutil::replace(&cpdirtmp, &cpdir)
        .map_err(|e| Error::from(e).wrap("rename checkpoint directory"))?;

    Ok(stats)
}

fn log_ranges(cp: &WL, buf: &[u8], recs: &[(usize, usize)]) -> Result<()> {
    if recs.is_empty() {
        return Ok(());
    }
    let slices: Vec<&[u8]> = recs.iter().map(|&(s, e)| &buf[s..e]).collect();
    cp.log_batch(&slices)
}

/// Go `checkpointPrefix`: the prefix used for checkpoint files.
pub const CHECKPOINT_PREFIX: &str = "checkpoint.";

/// Go `CheckpointDir`.
pub fn checkpoint_dir(dir: &Path, i: i64) -> PathBuf {
    dir.join(format!("{CHECKPOINT_PREFIX}{i:08}"))
}

struct CheckpointRef {
    name: String,
    index: i64,
}

fn list_checkpoints(dir: &Path) -> Result<Vec<CheckpointRef>> {
    let mut refs = Vec::new();
    // Go `os.ReadDir` error text: "open <dir>: no such file or directory" (Rust prints "No such file or
    // directory (os error 2)" without the operation and path). The error kind is kept.
    let entries = fs::read_dir(dir).map_err(|e| {
        let s = e.to_string();
        let msg = s.find(" (os error ").map_or(s.as_str(), |i| &s[..i]);
        let mut c = msg.chars();
        let msg: String = c.next().map(|f| f.to_lowercase().chain(c).collect()).unwrap_or_default();
        Error::from(io::Error::new(e.kind(), format!("open {}: {}", dir.display(), msg)))
    })?;
    for fi in entries {
        let fi = fi?;
        let name = fi.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(CHECKPOINT_PREFIX) {
            continue;
        }
        if !fi.file_type()?.is_dir() {
            return Err(Error::Msg(format!("checkpoint {name} is not a directory")));
        }
        let Some(idx) = go_atoi(&name[CHECKPOINT_PREFIX.len()..]) else {
            continue;
        };
        refs.push(CheckpointRef {
            name: name.to_string(),
            index: idx,
        });
    }
    refs.sort_by_key(|r| r.index);
    Ok(refs)
}
