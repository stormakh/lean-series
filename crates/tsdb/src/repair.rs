//! Port of `tsdb/repair.go`: repairs blocks written by Prometheus 2.1 with a bad index version.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use json::Json;
use storage::Error;

use crate::Logger;
use crate::compact::block_dirs;
use crate::meta::{BlockMeta, INDEX_FILENAME, META_FILENAME, META_VERSION_1, write_meta_file};

fn io_wrap(msg: String, e: std::io::Error) -> Error {
    Error::msg(format!("{msg}: {}", crate::io_err_text(&e)))
}

/// Go `repairBadIndexVersion`: repairs an issue in index and meta.json persistence introduced in commit
/// 129773b41a565fde5156301e37f9a87158030443. All blocks written by Prometheus 2.1 with a meta.json version of 2
/// are affected: the index file version is set to 2 and the meta.json version back to 1.
pub fn repair_bad_index_version(logger: &Logger, dir: &Path) -> Result<(), Error> {
    let dirs = block_dirs(dir).map_err(|e| Error::wrap(format_args!("list block dirs in {}", quote_path(dir)), e))?;
    let mut tmp_files: Vec<PathBuf> = Vec::with_capacity(dirs.len());
    let res = repair_dirs(logger, &dirs, &mut tmp_files);
    for tmp in tmp_files {
        if let Err(e) = remove_all(&tmp) {
            logger.error("remove tmp file", &[("err", &crate::io_err_text(&e))]);
        }
    }
    res
}

fn repair_dirs(logger: &Logger, dirs: &[PathBuf], tmp_files: &mut Vec<PathBuf>) -> Result<(), Error> {
    for d in dirs {
        let mut meta = match read_bogus_meta_file(d) {
            Ok(m) => m,
            Err(e) => {
                logger.error(
                    "failed to read meta.json for a block during repair process; skipping",
                    &[("dir", &d.display()), ("err", &e)],
                );
                continue;
            }
        };
        if meta.version == META_VERSION_1 {
            logger.info(
                "Found healthy block",
                &[("mint", &meta.min_time), ("maxt", &meta.max_time), ("ulid", &meta.ulid)],
            );
            continue;
        }
        logger.info("Fixing broken block", &[("mint", &meta.min_time), ("maxt", &meta.max_time), ("ulid", &meta.ulid)]);

        let repl_path = d.join("index.repaired");
        let mut repl = std::fs::File::create(&repl_path)
            .map_err(|e| io_wrap(format!("create index.repaired for block dir: {}", d.display()), e))?;
        tmp_files.push(repl_path.clone());

        let broken_path = d.join(INDEX_FILENAME);
        let mut broken = std::fs::File::open(&broken_path)
            .map_err(|e| io_wrap(format!("open broken index for block dir: {}", d.display()), e))?;
        std::io::copy(&mut broken, &mut repl).map_err(|e| {
            io_wrap(format!("copy content of index to index.repaired for block dir: {}", d.display()), e)
        })?;

        // Set the 5th byte to 2 to indicate the correct file format version.
        repl.seek(SeekFrom::Start(4))
            .and_then(|_| repl.write_all(&[2]))
            .map_err(|e| io_wrap(format!("rewrite of index.repaired for block dir: {}", d.display()), e))?;
        repl.sync_all().map_err(|e| io_wrap(format!("sync of index.repaired for block dir: {}", d.display()), e))?;
        drop(repl);
        drop(broken);
        // Go then closes the already closed replacement file, which fails with "file already closed", and
        // returns that error.
        let closed = || format!("close {}: file already closed", repl_path.display());
        if tsdb_format::fileutil::replace(&repl_path, &broken_path).is_err() {
            return Err(Error::msg(format!(
                "replaced broken index with index.repaired for block dir: {}: {}",
                d.display(),
                closed()
            )));
        }
        // Reset version of meta.json to 1.
        meta.version = META_VERSION_1;
        if write_meta_file(d, &mut meta).is_err() {
            return Err(Error::msg(format!("write meta for block dir: {}: {}", d.display(), closed())));
        }
    }
    Ok(())
}

/// Go `readBogusMetaFile`: reads a meta file of version 1 or 2.
pub fn read_bogus_meta_file(dir: &Path) -> Result<BlockMeta, Error> {
    let p = dir.join(META_FILENAME);
    let b = std::fs::read(&p).map_err(|e| Error::msg(format!("open {}: {}", p.display(), crate::io_err_text(&e))))?;
    let v = Json::parse_bytes(&b).map_err(|e| Error::msg(e.to_string()))?;
    let m = BlockMeta::from_json(&v)?;
    if m.version != META_VERSION_1 && m.version != 2 {
        return Err(Error::msg(format!("unexpected meta file version {}", m.version)));
    }
    Ok(m)
}

fn quote_path(p: &Path) -> String {
    model::unicode::quote(&p.display().to_string())
}

fn remove_all(p: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(p) {
        Ok(md) if md.is_dir() => std::fs::remove_dir_all(p),
        Ok(_) => std::fs::remove_file(p),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
