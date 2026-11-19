//! Port of cmd/promtool/backfill.go: creates TSDB blocks from OpenMetrics input
//! (`tsdb create-blocks-from openmetrics`; the entry point is [`crate::tsdb_cmd::backfill_open_metrics`]).

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use model::labels::{Builder, Labels};
use storage::Context;
use textparse::{Entry, OpenMetricsOptions, OpenMetricsParser, Parser};
use tsdb::{BlockReader, Ulid};
use tsdb::blockwriter::{ERR_NO_SERIES_APPENDED, new_block_writer};
use tsdb::db::{DEFAULT_BLOCK_DURATION, DbReadOnly};

use crate::Error;
use crate::tsdb_cmd::print_blocks;

fn err(s: impl Into<String>) -> Error {
    s.into().into()
}

/// Go `getMinAndMaxTimestamps`: returns (maxt, mint) of the series samples; 0 for each when there are none.
pub fn get_min_and_max_timestamps(p: &mut dyn Parser) -> Result<(i64, i64), Error> {
    let (mut maxt, mut mint) = (i64::MIN, i64::MAX);

    loop {
        let entry = match p.next() {
            Ok(e) => e,
            Err(e) if e.is_eof() => break,
            Err(e) => return Err(err(format!("next: {}", e))),
        };

        if entry != Entry::Series {
            continue;
        }

        let (_, ts, _) = p.series();
        let Some(ts) = ts else {
            return Err(err("expected timestamp for series got none"));
        };

        if ts > maxt {
            maxt = ts;
        }
        if ts < mint {
            mint = ts;
        }
    }

    if maxt == i64::MIN {
        maxt = 0;
    }
    if mint == i64::MAX {
        mint = 0;
    }

    Ok((maxt, mint))
}

/// Go `getCompatibleBlockDuration`: the largest Prometheus compaction range that is not above
/// `max_block_duration` (milliseconds); the default block duration when that is not above it.
pub fn get_compatible_block_duration(max_block_duration: i64) -> i64 {
    let mut block_duration = DEFAULT_BLOCK_DURATION;
    if max_block_duration > DEFAULT_BLOCK_DURATION {
        let ranges = tsdb::compact::exponential_block_ranges(DEFAULT_BLOCK_DURATION, 10, 3);
        let mut idx = ranges.len() as isize - 1; // Use largest range if user asked for something enormous.
        for (i, &v) in ranges.iter().enumerate() {
            if v > max_block_duration {
                idx = i as isize - 1;
                break;
            }
        }
        block_duration = ranges[idx as usize];
    }
    block_duration
}

/// Go `createBlocks`.
#[allow(clippy::too_many_arguments)]
pub fn create_blocks(
    input: &[u8],
    mint: i64,
    maxt: i64,
    block_duration: i64,
    max_samples_in_appender: usize,
    output_dir: &str,
    human_readable: bool,
    quiet: bool,
    custom_labels: &HashMap<String, String>,
    out: &mut dyn Write,
) -> Result<(), Error> {
    let mint = block_duration * (mint / block_duration);

    let mut db = DbReadOnly::open(Path::new(output_dir), None, None)?;
    let res = create_blocks_in(
        &mut db,
        input,
        mint,
        maxt,
        block_duration,
        max_samples_in_appender,
        output_dir,
        human_readable,
        quiet,
        custom_labels,
        out,
    );
    match (res, db.close().err()) {
        (r, None) => r,
        (Ok(()), Some(c)) => Err(c.into()),
        (Err(e), Some(c)) => Err(err(format!("{}\n{}", e, c))),
    }
}

#[allow(clippy::too_many_arguments)]
fn create_blocks_in(
    db: &mut DbReadOnly,
    input: &[u8],
    mint: i64,
    maxt: i64,
    block_duration: i64,
    max_samples_in_appender: usize,
    output_dir: &str,
    human_readable: bool,
    quiet: bool,
    custom_labels: &HashMap<String, String>,
    out: &mut dyn Write,
) -> Result<(), Error> {
    let mut wrote_header = false;
    let mut next_sample_ts = i64::MAX;

    let mut lb = Builder::new(Labels::empty());

    let mut t = mint;
    while t <= maxt {
        let ts_upper = t + block_duration;
        if next_sample_ts != i64::MAX && next_sample_ts >= ts_upper {
            // The next sample is not in this timerange, we can avoid parsing the file for this timerange.
            t += block_duration;
            continue;
        }
        next_sample_ts = i64::MAX;

        let r = (|| -> Result<(), Error> {
            // To prevent races with compaction, a block writer only allows appending samples that are at most half
            // a block size older than the most recent sample appended so far. However, in the way we use the block
            // writer here, compaction doesn't happen, while we also need to append samples throughout the whole
            // block range. To allow that, we pretend that the block is twice as large here, but only really add
            // sample in the original interval later.
            let w = new_block_writer(tsdb::Logger::nop(), Path::new(output_dir), 2 * block_duration)
                .map_err(|e| err(format!("block writer: {}", e)))?;
            // Go defers w.Close() into an unnamed result, so its error is dropped.
            let res = (|| -> Result<(), Error> {
                let ctx = Context::background();
                let mut app = w.appender_v2(&ctx);
                let mut p = OpenMetricsParser::new(input, OpenMetricsOptions::default());
                let mut samples_count = 0usize;
                loop {
                    let e = match p.next() {
                        Ok(e) => e,
                        Err(e) if e.is_eof() => break,
                        Err(e) => return Err(err(format!("parse: {}", e))),
                    };
                    if e != Entry::Series {
                        continue;
                    }

                    let (_, ts, v) = p.series();
                    let Some(ts) = ts else {
                        let mut l = Labels::empty();
                        p.labels(&mut l);
                        return Err(err(format!("expected timestamp for series {}, got none", l)));
                    };
                    if ts < t {
                        continue;
                    }
                    if ts >= ts_upper {
                        if ts < next_sample_ts {
                            next_sample_ts = ts;
                        }
                        continue;
                    }

                    let mut l = Labels::empty();
                    p.labels(&mut l);

                    lb.reset(l);
                    for (name, value) in custom_labels {
                        lb.set(name, value);
                    }
                    let lbls = lb.labels();

                    if let Err(e) = app.append(0, &lbls, 0, ts, v, None, None, &storage::AOptions::default()) {
                        return Err(err(format!("add sample: {}", e)));
                    }

                    samples_count += 1;
                    if samples_count < max_samples_in_appender {
                        continue;
                    }

                    // If we arrive here, the samples count is greater than the maxSamplesInAppender. Therefore the
                    // old appender is committed and a new one is created. This prevents keeping too many samples
                    // lined up in an appender and thus in RAM.
                    if let Some(e) = app.commit() {
                        return Err(err(format!("commit: {}", e)));
                    }

                    app = w.appender_v2(&ctx);
                    samples_count = 0;
                }

                if let Some(e) = app.commit() {
                    return Err(err(format!("commit: {}", e)));
                }

                match w.flush(&ctx) {
                    Ok(block) => {
                        if quiet {
                            return Ok(());
                        }
                        // Empty block, don't print.
                        if block == Ulid::default() {
                            return Ok(());
                        }
                        let blocks = db.blocks().map_err(|e| err(format!("get blocks: {}", e)))?;
                        for b in &blocks {
                            if b.meta().ulid == block {
                                print_blocks(out, std::slice::from_ref(b), !wrote_header, human_readable);
                                wrote_header = true;
                                break;
                            }
                        }
                    }
                    Err(e) if e.is(&ERR_NO_SERIES_APPENDED) => {}
                    Err(e) => return Err(err(format!("flush: {}", e))),
                }

                Ok(())
            })();
            let _ = w.close();
            res
        })();
        if let Err(e) = r {
            return Err(err(format!("process blocks: {}", e)));
        }
        t += block_duration;
    }
    Ok(())
}

/// Go `backfill`: creates blocks of `block_duration` from the OpenMetrics `input` in `output_dir`.
#[allow(clippy::too_many_arguments)]
pub fn backfill(
    max_samples_in_appender: usize,
    input: &[u8],
    output_dir: &str,
    human_readable: bool,
    quiet: bool,
    block_duration: Duration,
    custom_labels: &HashMap<String, String>,
    out: &mut dyn Write,
) -> Result<(), Error> {
    // Don't need a SymbolTable to get max and min timestamps.
    let mut p = OpenMetricsParser::new(input, OpenMetricsOptions::default());
    let (maxt, mint) =
        get_min_and_max_timestamps(&mut p).map_err(|e| err(format!("getting min and max timestamp: {}", e)))?;
    if let Err(e) = create_blocks(
        input,
        mint,
        maxt,
        block_duration.as_millis() as i64,
        max_samples_in_appender,
        output_dir,
        human_readable,
        quiet,
        custom_labels,
        out,
    ) {
        return Err(err(format!("block creation: {}", e)));
    }
    Ok(())
}
