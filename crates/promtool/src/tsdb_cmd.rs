//! Port of cmd/promtool/tsdb.go: `tsdb bench write`, `tsdb analyze`, `tsdb list`, `tsdb dump`,
//! `tsdb dump-openmetrics` and the entry point of `tsdb create-blocks-from openmetrics`.
//!
//! What Go prints to `os.Stdout` goes to the `out` writer; `checkErr` output goes to `stderr`. Write errors are
//! ignored, as Go's `fmt.Printf` results are.
//!
//! Differences from Go:
//! - `tsdb bench write` does not write the pprof files (cpu.prof, mem.prof, block.prof, mutex.prof): Rust has no
//!   pprof. The rest (stages, output text, TSDB writes) is the same.
//! - Lists that Go builds from maps (`tsdb analyze`: churn and most common label pairs) are in label order before
//!   the sort; Go's map order is random, so ties may print in another order than one Go run.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chunkenc::{Chunk, ChunkIterator, ValueType};
use model::labels::{self, Label, Labels, ScratchBuilder};
use model::matcher::Matcher;
use promql::parser::Parser;
use storage::{Context, Meta, SeriesRef, SeriesSet};
use tsdb::block::Block;
use tsdb::db::{self, DbReadOnly};
use tsdb::{BlockReader, ChunkOrIterable, IndexReader};
use tsdb_format::index::{self, BoxPostings, Postings};

use crate::Error;

const TIME_DELTA: i64 = 30000;

/// Go `SeriesSetFormatter`: prints a series set to `out`.
pub type SeriesSetFormatter = fn(&mut dyn SeriesSet, &mut dyn Write) -> Result<(), Error>;

fn err(s: impl Into<String>) -> Error {
    s.into().into()
}

/// The Go text of an I/O error: the OS message in lower case without Rust's " (os error N)" suffix.
pub(crate) fn go_io_text(e: &std::io::Error) -> String {
    go_err_text(&e.to_string())
}

/// Removes Rust's " (os error N)" from an error text and writes the OS message that precedes it in lower case,
/// as Go's syscall errors are.
pub(crate) fn go_err_text(s: &str) -> String {
    let Some(i) = s.find(" (os error ") else { return s.to_string() };
    let Some(j) = s[i..].find(')') else { return s.to_string() };
    let head = &s[..i];
    let start = head.rfind(": ").map_or(0, |k| k + 2);
    let mut out = String::with_capacity(s.len());
    out.push_str(&head[..start]);
    let mut msg = head[start..].chars();
    if let Some(c) = msg.next() {
        out.extend(c.to_lowercase());
        out.push_str(msg.as_str());
    }
    out.push_str(&s[i + j + 1..]);
    out
}

/// Go `time.Duration(ns).String()`.
pub(crate) fn go_duration_string(ns: i64) -> String {
    let neg = ns < 0;
    let mut u = ns.unsigned_abs();
    let mut out = String::new();
    if u < 1_000_000_000 {
        // Less than one second: use a smaller unit, like "1.2ms".
        if u == 0 {
            return "0s".to_string();
        }
        let (unit, prec) = if u < 1_000 {
            ("ns", 0)
        } else if u < 1_000_000 {
            ("µs", 3)
        } else {
            ("ms", 6)
        };
        let s = fmt_frac_int(u, prec);
        if neg {
            out.push('-');
        }
        out.push_str(&s);
        out.push_str(unit);
        return out;
    }
    let secs_frac = fmt_frac_int(u % 60_000_000_000, 9);
    u /= 60_000_000_000;
    let mut s = String::new();
    if u > 0 {
        let m = u % 60;
        u /= 60;
        if u > 0 {
            s.push_str(&u.to_string());
            s.push('h');
        }
        s.push_str(&m.to_string());
        s.push('m');
    }
    if neg {
        out.push('-');
    }
    out.push_str(&s);
    out.push_str(&secs_frac);
    out.push('s');
    out
}

/// Go `fmtFrac` + `fmtInt`: `v / 10^prec` with the fraction digits, trailing zeros removed.
fn fmt_frac_int(v: u64, prec: u32) -> String {
    let p = 10u64.pow(prec);
    let (int, frac) = (v / p, v % p);
    let mut s = int.to_string();
    if frac != 0 {
        let mut f = format!("{:0width$}", frac, width = prec as usize);
        while f.ends_with('0') {
            f.pop();
        }
        s.push('.');
        s.push_str(&f);
    }
    s
}

/// Go `time.Unix(sec, 0).UTC().String()`: "2006-01-02 15:04:05 +0000 UTC".
pub(crate) fn go_utc_string(sec: i64) -> String {
    let days = sec.div_euclid(86400);
    let rem = sec.rem_euclid(86400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} +0000 UTC",
        y,
        m,
        d,
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

// ---------------------------------------------------------------------------------------------------------------
// tsdb bench write.

struct WriteBenchmark {
    out_path: String,
    samples_file: String,
    #[allow(dead_code)]
    cleanup: bool,
    num_metrics: usize,
    storage: Option<Arc<db::DB>>,
}

/// Go `benchmarkWrite` (`tsdb bench write`): `out_path` is `--out`, `samples_file` the `file` argument,
/// `num_metrics` `--metrics` and `num_scrapes` `--scrapes`. The TSDB logs to `stderr`.
pub fn benchmark_write(
    out_path: &str,
    samples_file: &str,
    num_metrics: usize,
    num_scrapes: usize,
    out: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<(), Error> {
    let _ = stderr;
    let mut b = WriteBenchmark {
        out_path: out_path.to_string(),
        samples_file: samples_file.to_string(),
        cleanup: false,
        num_metrics,
        storage: None,
    };
    if b.out_path.is_empty() {
        let dir = make_temp_dir("tsdb_bench").map_err(|e| err(go_io_text(&e)))?;
        b.out_path = dir;
        b.cleanup = true;
    }
    if let Err(e) = std::fs::remove_dir_all(&b.out_path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(err(format!("unlinkat {}: {}", b.out_path, go_io_text(&e))));
        }
    }
    std::fs::create_dir_all(&b.out_path).map_err(|e| err(format!("mkdir {}: {}", b.out_path, go_io_text(&e))))?;

    let dir = Path::new(&b.out_path).join("storage");

    let mut opts = db::zero_options();
    opts.retention_duration = 15 * 24 * 3600 * 1000;
    opts.min_block_duration = 2 * 3600 * 1000;
    let logger = tsdb::Logger::new(|line| {
        eprintln!("time={} {}", go_rfc3339_now(), line);
    });
    let st = db::open(&dir, Some(logger), Some(opts), Some(db::DbStats::new()))?;
    st.disable_compactions();
    b.storage = Some(st);

    let mut lbs: Vec<Labels> = Vec::new();

    measure_time(out, "readData", &mut |_| {
        let f = std::fs::File::open(&b.samples_file)
            .map_err(|e| err(format!("open {}: {}", b.samples_file, go_io_text(&e))))?;
        lbs = read_prometheus_labels(std::io::BufReader::new(f), b.num_metrics)?;
        Ok(())
    })?;

    let mut total = 0u64;

    let dur = measure_time(out, "ingestScrapes", &mut |out| {
        total = b.ingest_scrapes(&lbs, num_scrapes, out)?;
        Ok(())
    })?;

    let _ = writeln!(out, " > total samples: {}", total);
    let _ = writeln!(out, " > samples/sec: {}", model::histogram::gofmt::g(total as f64 / dur.as_secs_f64()));

    measure_time(out, "stopStorage", &mut |_| {
        if let Some(st) = &b.storage {
            st.close()?;
        }
        Ok(())
    })?;

    Ok(())
}

fn go_rfc3339_now() -> String {
    let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let s = go_utc_string(d.as_secs() as i64);
    // "2006-01-02 15:04:05 +0000 UTC" -> "2006-01-02T15:04:05.000Z".
    format!("{}T{}.{:03}Z", &s[..10], &s[11..19], d.subsec_millis())
}

fn make_temp_dir(prefix: &str) -> std::io::Result<String> {
    let base = std::env::temp_dir();
    let mut n = std::process::id() as u64 ^ Instant::now().elapsed().as_nanos() as u64;
    loop {
        n = n.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let p = base.join(format!("{}{}", prefix, n % 10_000_000_000));
        match std::fs::create_dir(&p) {
            Ok(()) => return Ok(p.to_string_lossy().into_owned()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

impl WriteBenchmark {
    fn ingest_scrapes(&self, lbls: &[Labels], scrape_count: usize, out: &mut dyn Write) -> Result<u64, Error> {
        let mut total = 0u64;
        let mut i = 0;
        while i < scrape_count {
            let results: Vec<(u64, Option<String>)> = std::thread::scope(|s| {
                let mut handles = Vec::new();
                let mut rest = lbls;
                while !rest.is_empty() {
                    let l = rest.len().min(1000);
                    let batch = &rest[..l];
                    rest = &rest[l..];
                    let base = TIME_DELTA * i as i64;
                    handles.push(s.spawn(move || self.ingest_scrapes_shard(batch, 100, base)));
                }
                handles.into_iter().map(|h| h.join().expect("ingest shard panicked")).collect()
            });
            for (n, e) in results {
                if let Some(e) = e {
                    let _ = writeln!(out, " err {}", e);
                }
                total += n;
            }
            i += 100;
        }
        let _ = writeln!(out, "ingestion completed");
        Ok(total)
    }

    fn ingest_scrapes_shard(&self, lbls: &[Labels], scrape_count: usize, baset: i64) -> (u64, Option<String>) {
        let st = self.storage.as_ref().expect("storage");
        let mut ts = baset;

        struct Sample<'a> {
            labels: &'a Labels,
            value: i64,
            r: Option<SeriesRef>,
        }

        let mut scrape: Vec<Sample<'_>> = lbls.iter().map(|m| Sample { labels: m, value: 123456789, r: None }).collect();
        let mut total = 0u64;
        let ctx = Context::background();

        for _ in 0..scrape_count {
            let mut app = st.appender_v2(&ctx);
            ts += TIME_DELTA;

            for s in scrape.iter_mut() {
                s.value += 1000;
                let r = s.r.unwrap_or(0);
                let r = match app.append(r, s.labels, 0, ts, s.value as f64, None, None, &storage::AOptions::default())
                {
                    Ok(r) => r,
                    Err(e) => panic!("{}", e),
                };
                if s.r.is_none() {
                    s.r = Some(r);
                }
                total += 1;
            }
            if let Some(e) = app.commit() {
                return (total, Some(e.to_string()));
            }
        }
        (total, None)
    }
}

fn measure_time(
    out: &mut dyn Write,
    stage: &str,
    f: &mut dyn FnMut(&mut dyn Write) -> Result<(), Error>,
) -> Result<Duration, Error> {
    let _ = writeln!(out, ">> start stage={}", stage);
    let start = Instant::now();
    f(out)?;
    let _ = writeln!(
        out,
        ">> completed stage={} duration={}",
        stage,
        go_duration_string(start.elapsed().as_nanos() as i64)
    );
    Ok(start.elapsed())
}

/// Go `readPrometheusLabels`: up to `n` distinct label sets, one per line in the form `{"a":"b","c":"d"}`.
pub fn read_prometheus_labels(r: impl std::io::BufRead, n: usize) -> Result<Vec<Labels>, Error> {
    let mut mets = Vec::new();
    let mut hashes: HashSet<u64> = HashSet::new();
    let mut i = 0;

    let mut lines = r.split(b'\n');
    while i < n {
        let Some(line) = lines.next() else { break };
        let mut line = line.map_err(|e| err(go_io_text(&e)))?;
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let text = String::from_utf8_lossy(&line);
        let s: String = text.chars().filter(|c| !matches!(c, '"' | '{' | '}')).collect();

        let mut m: Vec<Label> = Vec::new();
        for label_chunk in s.split(',') {
            let split: Vec<&str> = label_chunk.split(':').collect();
            m.push(Label::new(split[0], split[1]));
        }
        // This sorts by name - order of the k/v labels matters, don't assume we'll always receive them sorted.
        let ml = labels::new(&mut m);
        let h = ml.hash();
        if hashes.contains(&h) {
            continue;
        }
        mets.push(ml);
        hashes.insert(h);
        i += 1;
    }
    Ok(mets)
}

// ---------------------------------------------------------------------------------------------------------------
// tsdb list.

/// Go `listBlocks` (`tsdb list`): `path` is the `db path` argument, `human_readable` `--human-readable`/`-r`.
pub fn list_blocks(path: &str, human_readable: bool, out: &mut dyn Write) -> Result<(), Error> {
    let mut db = DbReadOnly::open(Path::new(path), None, None)?;
    // Go defers db.Close() into an unnamed result, so its error is dropped.
    let res = (|| -> Result<(), Error> {
        let blocks = db.blocks()?;
        print_blocks(out, &blocks, true, human_readable);
        Ok(())
    })();
    let _ = db.close();
    res
}

/// Go `printBlocks`: a table of the blocks (Go `text/tabwriter` with min width 13, padding 2).
pub fn print_blocks(out: &mut dyn Write, blocks: &[Block], write_header: bool, human_readable: bool) {
    let mut rows: Vec<Vec<String>> = Vec::new();
    if write_header {
        rows.push(
            ["BLOCK ULID", "MIN TIME", "MAX TIME", "DURATION", "NUM SAMPLES", "NUM CHUNKS", "NUM SERIES", "SIZE"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        );
    }
    for b in blocks {
        let meta = b.meta();
        rows.push(vec![
            meta.ulid.to_string(),
            get_formatted_time(meta.min_time, human_readable),
            get_formatted_time(meta.max_time, human_readable),
            go_duration_string((meta.max_time.wrapping_sub(meta.min_time)).wrapping_mul(1_000_000)),
            meta.stats.num_samples.to_string(),
            meta.stats.num_chunks.to_string(),
            meta.stats.num_series.to_string(),
            get_formatted_bytes(BlockReader::size(b), human_readable),
        ]);
    }
    let _ = out.write_all(tabwrite(&rows, 13, 2).as_bytes());
}

/// Go `text/tabwriter` (flags 0, pad char ' ') for lines with the same number of cells: every cell but the last
/// of a line is padded to the column width (the widest cell plus `padding`, at least `minwidth`).
fn tabwrite(rows: &[Vec<String>], minwidth: usize, padding: usize) -> String {
    let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let mut widths = vec![minwidth; ncols];
    for r in rows {
        for (i, c) in r.iter().enumerate().take(r.len().saturating_sub(1)) {
            let w = c.chars().count() + padding;
            if w > widths[i] {
                widths[i] = w;
            }
        }
    }
    let mut s = String::new();
    for r in rows {
        for (i, c) in r.iter().enumerate() {
            s.push_str(c);
            if i + 1 < r.len() {
                for _ in c.chars().count()..widths[i] {
                    s.push(' ');
                }
            }
        }
        s.push('\n');
    }
    s
}

/// Go `getFormattedTime`.
pub fn get_formatted_time(timestamp: i64, human_readable: bool) -> String {
    if human_readable {
        return go_utc_string(timestamp / 1000);
    }
    timestamp.to_string()
}

/// Go `getFormattedBytes`.
pub fn get_formatted_bytes(bytes: i64, human_readable: bool) -> String {
    if human_readable {
        return yaml::Base2Bytes(bytes).go_string();
    }
    bytes.to_string()
}

fn open_block(path: &str, block_id: &str) -> Result<(DbReadOnly, Block), Error> {
    let mut db = DbReadOnly::open(Path::new(path), None, None)?;
    let mut block_id = block_id.to_string();
    if block_id.is_empty() {
        block_id = db.last_block_id()?;
    }
    let b = db.block(&block_id, None)?;
    Ok((db, b))
}

// ---------------------------------------------------------------------------------------------------------------
// tsdb analyze.

struct PostingInfo {
    key: String,
    metric: u64,
}

fn print_info(out: &mut dyn Write, posting_infos: &mut [PostingInfo], limit: usize) {
    promql::gosort::sort_func(posting_infos, |a, b| b.metric.cmp(&a.metric));
    for (i, pc) in posting_infos.iter().enumerate() {
        if i >= limit {
            break;
        }
        let _ = writeln!(out, "{} {}", pc.metric, pc.key);
    }
}

/// Go `analyzeBlock` (`tsdb analyze`): `path` is `db path`, `block_id` `block id` (empty: the last block),
/// `limit` `--limit`, `run_extended` `--extended`, `matchers` `--match`.
#[allow(clippy::too_many_arguments)]
pub fn analyze_block(
    ctx: &Context,
    path: &str,
    block_id: &str,
    limit: i64,
    run_extended: bool,
    matchers: &str,
    p: &Parser,
    out: &mut dyn Write,
) -> Result<(), Error> {
    let mut selectors: Vec<Matcher> = Vec::new();
    if !matchers.is_empty() {
        selectors = p.parse_metric_selector(matchers)?;
    }
    let (mut db, block) = open_block(path, block_id)?;
    // Go defers db.Close() into an unnamed result, so its error is dropped.
    let res = analyze_open_block(ctx, &block, limit, run_extended, matchers, &selectors, out);
    let _ = db.close();
    res
}

fn analyze_open_block(
    ctx: &Context,
    block: &Block,
    limit: i64,
    run_extended: bool,
    matchers: &str,
    selectors: &[Matcher],
    out: &mut dyn Write,
) -> Result<(), Error> {
    let limit = if limit < 0 { 0 } else { limit as usize };
    let meta = block.meta();
    let _ = writeln!(out, "Block ID: {}", meta.ulid);
    // Presume 1ms resolution that Prometheus uses.
    let _ = writeln!(out, "Duration: {}", go_duration_string((meta.max_time - meta.min_time).wrapping_mul(1_000_000)));
    let _ = writeln!(out, "Total Series: {}", meta.stats.num_series);
    if !matchers.is_empty() {
        let _ = writeln!(out, "Matcher: {}", matchers);
    }
    let ir = block.index()?;
    let res = analyze_index(ctx, block, &*ir, &meta, limit, run_extended, matchers, selectors, out);
    let _ = ir.close();
    res
}

#[allow(clippy::too_many_arguments)]
fn analyze_index(
    ctx: &Context,
    block: &Block,
    ir: &dyn IndexReader,
    meta: &tsdb::BlockMeta,
    limit: usize,
    run_extended: bool,
    matchers: &str,
    selectors: &[Matcher],
    out: &mut dyn Write,
) -> Result<(), Error> {
    let all_label_names = ir.label_names(ctx, selectors)?;
    let _ = writeln!(out, "Label names: {}", all_label_names.len());

    let mut labels_uncovered: BTreeMap<String, u64> = BTreeMap::new();
    let mut labelpairs_uncovered: BTreeMap<String, u64> = BTreeMap::new();
    let mut labelpairs_count: BTreeMap<String, u64> = BTreeMap::new();
    let mut entries = 0usize;
    let mut refs: Vec<SeriesRef> = Vec::new();
    let mut postings: BoxPostings<'_> = if !matchers.is_empty() {
        let mut p = tsdb::querier::postings_for_matchers(ctx, ir, selectors)?;
        // Expand refs first and cache in memory, so later we don't have to expand again.
        refs = index::expand_postings(&mut p)?;
        let _ = writeln!(out, "Matched series: {}", refs.len());
        Box::new(index::new_list_postings(refs.clone()))
    } else {
        // The special all key.
        ir.postings(ctx, "", &[""])?
    };

    let mut chks: Vec<Meta> = Vec::new();
    let mut builder = ScratchBuilder::new(0);
    let block_range = (meta.max_time - meta.min_time) as u64;
    while postings.next() {
        ir.series(postings.at(), &mut builder, &mut chks)?;
        // Amount of the block time range not covered by this series.
        let uncovered = block_range.wrapping_sub((chks[chks.len() - 1].max_time - chks[0].min_time) as u64);
        for (name, value) in builder.labels().iter() {
            let key = format!("{}={}", name, value);
            let e = labels_uncovered.entry(name.to_string()).or_insert(0);
            *e = e.wrapping_add(uncovered);
            let e = labelpairs_uncovered.entry(key.clone()).or_insert(0);
            *e = e.wrapping_add(uncovered);
            *labelpairs_count.entry(key).or_insert(0) += 1;
            entries += 1;
        }
    }
    if let Some(e) = postings.err() {
        return Err(e.into());
    }
    drop(postings);
    let _ = writeln!(out, "Postings (unique label pairs): {}", labelpairs_uncovered.len());
    let _ = writeln!(out, "Postings entries (total label pairs): {}", entries);

    let dur = (meta.max_time - meta.min_time) as f64;
    let mut posting_infos: Vec<PostingInfo> = labelpairs_uncovered
        .iter()
        .map(|(k, m)| PostingInfo { key: k.clone(), metric: (*m as f64 / dur) as u64 })
        .collect();
    let _ = write!(out, "\nLabel pairs most involved in churning:\n");
    print_info(out, &mut posting_infos, limit);

    let mut posting_infos: Vec<PostingInfo> = labels_uncovered
        .iter()
        .map(|(k, m)| PostingInfo { key: k.clone(), metric: (*m as f64 / dur) as u64 })
        .collect();
    let _ = write!(out, "\nLabel names most involved in churning:\n");
    print_info(out, &mut posting_infos, limit);

    let mut posting_infos: Vec<PostingInfo> =
        labelpairs_count.iter().map(|(k, m)| PostingInfo { key: k.clone(), metric: *m }).collect();
    let _ = write!(out, "\nMost common label pairs:\n");
    print_info(out, &mut posting_infos, limit);

    let mut posting_infos: Vec<PostingInfo> = Vec::new();
    for n in &all_label_names {
        let values = ir.sorted_label_values(ctx, n, None, selectors)?;
        let cumulative_length: u64 = values.iter().map(|s| s.len() as u64).sum();
        posting_infos.push(PostingInfo { key: n.clone(), metric: cumulative_length });
    }
    let _ = write!(out, "\nLabel names with highest cumulative label value length:\n");
    print_info(out, &mut posting_infos, limit);

    let mut posting_infos: Vec<PostingInfo> = Vec::new();
    for n in &all_label_names {
        let lv = ir.sorted_label_values(ctx, n, None, selectors)?;
        posting_infos.push(PostingInfo { key: n.clone(), metric: lv.len() as u64 });
    }
    let _ = write!(out, "\nHighest cardinality labels:\n");
    print_info(out, &mut posting_infos, limit);

    let mut posting_infos: Vec<PostingInfo> = Vec::new();
    let lv = ir.sorted_label_values(ctx, "__name__", None, selectors)?;
    for n in &lv {
        let mut postings = ir.postings(ctx, "__name__", &[n.as_str()])?;
        // Only intersect postings if matchers are specified.
        if !matchers.is_empty() {
            postings = index::intersect(vec![postings, Box::new(index::new_list_postings(refs.clone()))]);
        }
        let mut count = 0u64;
        while postings.next() {
            count += 1;
        }
        if let Some(e) = postings.err() {
            return Err(e.into());
        }
        posting_infos.push(PostingInfo { key: n.clone(), metric: count });
    }
    let _ = write!(out, "\nHighest cardinality metric names:\n");
    print_info(out, &mut posting_infos, limit);

    if run_extended {
        return analyze_compaction(ctx, block, ir, selectors, out);
    }
    Ok(())
}

/// Go `analyzeCompaction` (`tsdb analyze --extended`).
pub fn analyze_compaction(
    ctx: &Context,
    block: &dyn BlockReader,
    indexr: &dyn IndexReader,
    matchers: &[Matcher],
    out: &mut dyn Write,
) -> Result<(), Error> {
    let mut postingsr = if !matchers.is_empty() {
        tsdb::querier::postings_for_matchers(ctx, indexr, matchers)?
    } else {
        let (n, v) = index::all_postings_key();
        indexr.postings(ctx, n, &[v])?
    };

    let chunkr = block.chunks()?;
    let res = (|| -> Result<(), Error> {
        let mut total_chunks = 0i64;
        let mut float_chunk_samples_count: Vec<i64> = Vec::new();
        let mut float_chunk_size: Vec<i64> = Vec::new();
        let mut histogram_chunk_samples_count: Vec<i64> = Vec::new();
        let mut histogram_chunk_size: Vec<i64> = Vec::new();
        let mut histogram_chunk_buckets_count: Vec<i64> = Vec::new();
        let mut builder = ScratchBuilder::new(0);
        let mut chks: Vec<Meta> = Vec::new();
        while postingsr.next() {
            indexr.series(postingsr.at(), &mut builder, &mut chks)?;

            for chk in &chks {
                // Load the actual data of the chunk.
                // Chunks within blocks should not need to be re-written, so an iterable is not expected to be
                // returned from the chunk reader.
                let chk: Arc<dyn Chunk> = match chunkr.chunk_or_iterable(chk)? {
                    ChunkOrIterable::Chunk(c) => c,
                    ChunkOrIterable::Iterable(_) => {
                        return Err(err("ChunkOrIterable should not return an iterable when reading a block"));
                    }
                };
                // Go also checks the concrete chunk type; a Rust block chunk is a typed view of its encoding.
                match chk.encoding() {
                    chunkenc::ENC_XOR | chunkenc::ENC_XOR2 => {
                        float_chunk_samples_count.push(chk.num_samples() as i64);
                        float_chunk_size.push(chk.bytes().len() as i64);
                    }
                    chunkenc::ENC_FLOAT_HISTOGRAM | chunkenc::ENC_FLOAT_HISTOGRAM_ST => {
                        histogram_chunk_samples_count.push(chk.num_samples() as i64);
                        histogram_chunk_size.push(chk.bytes().len() as i64);
                        let mut it = chk.iterator(None);
                        let mut bucket_count = 0i64;
                        while it.next() == ValueType::FloatHistogram {
                            let (_, f) = it.at_float_histogram(None);
                            bucket_count += f.positive_buckets.len() as i64;
                            bucket_count += f.negative_buckets.len() as i64;
                        }
                        histogram_chunk_buckets_count.push(bucket_count);
                    }
                    chunkenc::ENC_HISTOGRAM | chunkenc::ENC_HISTOGRAM_ST => {
                        histogram_chunk_samples_count.push(chk.num_samples() as i64);
                        histogram_chunk_size.push(chk.bytes().len() as i64);
                        let mut it = chk.iterator(None);
                        let mut bucket_count = 0i64;
                        while it.next() == ValueType::Histogram {
                            let (_, f) = it.at_histogram(None);
                            bucket_count += f.positive_buckets.len() as i64;
                            bucket_count += f.negative_buckets.len() as i64;
                        }
                        histogram_chunk_buckets_count.push(bucket_count);
                    }
                    _ => {}
                }
                total_chunks += 1;
            }
        }

        let _ = write!(out, "\nCompaction analysis:\n");
        let _ = writeln!(out);
        display_histogram(out, "samples per float chunk", &mut float_chunk_samples_count, total_chunks);
        display_histogram(out, "bytes per float chunk", &mut float_chunk_size, total_chunks);
        display_histogram(out, "samples per histogram chunk", &mut histogram_chunk_samples_count, total_chunks);
        display_histogram(out, "bytes per histogram chunk", &mut histogram_chunk_size, total_chunks);
        display_histogram(out, "buckets per histogram chunk", &mut histogram_chunk_buckets_count, total_chunks);
        Ok(())
    })();
    let close = chunkr.close().err();
    match (res, close) {
        (Ok(()), None) => Ok(()),
        (Ok(()), Some(c)) => Err(c.into()),
        (Err(e), None) => Err(e),
        (Err(e), Some(c)) => Err(err(format!("{}\n{}", e, c))),
    }
}

/// Go `displayHistogram`.
pub fn display_histogram(out: &mut dyn Write, data_type: &str, datas: &mut [i64], total: i64) {
    if datas.is_empty() {
        let _ = write!(out, "{}: N/A\n\n", data_type);
        return;
    }
    datas.sort();
    let (start, end, step) = generate_bucket(datas[0], datas[datas.len() - 1]);
    let mut sum = 0i64;
    let mut buckets = vec![0i64; ((end - start) / step + 1) as usize];
    let mut max_count = 0i64;
    for &c in datas.iter() {
        sum += c;
        let i = ((c - start) / step) as usize;
        buckets[i] += 1;
        if buckets[i] > max_count {
            max_count = buckets[i];
        }
    }
    let avg = sum / datas.len() as i64;
    let _ = writeln!(out, "{} (min/avg/max): {}/{}/{}", data_type, datas[0], avg, datas[datas.len() - 1]);
    let max_left_len = end.to_string().len();
    let max_right_len = (end + step).to_string().len();
    let max_count_len = max_count.to_string().len();
    for (bucket, &count) in buckets.iter().enumerate() {
        let bucket = bucket as i64;
        let percentage = 100 * count / total;
        let _ = writeln!(
            out,
            "[{:>lw$}, {:>rw$}]: {:>cw$} {}",
            bucket * step + start + 1,
            (bucket + 1) * step + start,
            count,
            "#".repeat(percentage.max(0) as usize),
            lw = max_left_len,
            rw = max_right_len,
            cw = max_count_len
        );
    }
    let _ = writeln!(out);
}

/// Go `generateBucket`: returns (start, end, step).
pub fn generate_bucket(min_val: i64, max_val: i64) -> (i64, i64, i64) {
    let s = (max_val - min_val) / 10;

    let mut step = 10;
    while step < s && step <= 10000 {
        step *= 10;
    }

    let start = min_val - min_val % step;
    let end = max_val - max_val % step + step;

    (start, end, step)
}

// ---------------------------------------------------------------------------------------------------------------
// tsdb dump, dump-openmetrics.

/// Go `dumpTSDBData` (`tsdb dump` and `tsdb dump-openmetrics`): `db_dir` is `db path`, `sandbox_dir_root`
/// `--sandbox-dir-root` (empty: the db path), `mint`/`maxt` `--min-time`/`--max-time`, `match_` the `--match`
/// values, `formatter` [`format_series_set`] (`--format=prom`), [`format_series_set_labels_to_json`]
/// (`--format=seriesjson`) or [`format_series_set_open_metrics`] (`dump-openmetrics`).
#[allow(clippy::too_many_arguments)]
pub fn dump_tsdb_data(
    ctx: &Context,
    db_dir: &str,
    sandbox_dir_root: &str,
    mint: i64,
    maxt: i64,
    match_: &[String],
    formatter: SeriesSetFormatter,
    p: &Parser,
    out: &mut dyn Write,
) -> Result<(), Error> {
    let root = if sandbox_dir_root.is_empty() { None } else { Some(Path::new(sandbox_dir_root)) };
    let mut db = DbReadOnly::open(Path::new(db_dir), root, None)?;
    let res = (|| -> Result<(), Error> {
        let mut q = db.querier(mint, maxt)?;
        let res = (|| -> Result<(), Error> {
            let matcher_sets = p.parse_metric_selectors(match_)?;

            let mut ss: storage::BoxSeriesSet = if matcher_sets.len() > 1 {
                let mut sets = Vec::new();
                for mset in &matcher_sets {
                    sets.push(q.select(ctx, true, None, mset));
                }
                storage::new_merge_series_set(sets, 0, Arc::new(storage::chained_series_merge))
            } else {
                q.select(ctx, false, None, &matcher_sets[0])
            };

            formatter(&mut *ss, out)?;

            let ws = ss.warnings();
            if !ws.is_empty() {
                let texts: Vec<String> = ws.as_errors().iter().map(|e| e.to_string()).collect();
                return Err(err(texts.join("\n")));
            }

            if let Some(e) = ss.err() {
                return Err(e.into());
            }
            Ok(())
        })();
        let _ = q.close();
        res
    })();
    match (res, db.close().err()) {
        (r, None) => r,
        (Ok(()), Some(c)) => Err(c.into()),
        (Err(e), Some(c)) => Err(err(format!("{}\n{}", e, c))),
    }
}

/// Go `formatSeriesSet` (`tsdb dump --format=prom`).
pub fn format_series_set(ss: &mut dyn SeriesSet, out: &mut dyn Write) -> Result<(), Error> {
    while ss.next() {
        let series = ss.at().clone();
        let lbs = series.labels();
        let mut it = series.iterator(None);
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Float => {
                    let (ts, val) = it.at();
                    let _ = writeln!(out, "{} {} {}", lbs, model::histogram::gofmt::g(val), ts);
                }
                ValueType::FloatHistogram => {
                    let (ts, fh) = it.at_float_histogram(None);
                    let _ = writeln!(out, "{} {} {}", lbs, fh.string(), ts);
                }
                ValueType::Histogram => {
                    let (ts, h) = it.at_histogram(None);
                    let _ = writeln!(out, "{} {} {}", lbs, h.string(), ts);
                }
            }
        }
        if it.err().is_some() {
            // Go returns ss.Err() here (not the iterator error).
            return match ss.err() {
                Some(e) => Err(e.into()),
                None => Ok(()),
            };
        }
    }
    Ok(())
}

/// Go `CondensedString`: `labels.Labels.String()` without spaces after the commas.
#[allow(non_snake_case)]
pub fn CondensedString(ls: &Labels) -> String {
    condensed_string(ls)
}

/// Go `CondensedString` in snake case.
pub fn condensed_string(ls: &Labels) -> String {
    let mut b = String::with_capacity(ls.data().len() + 16);
    b.push('{');
    for (i, (name, value)) in ls.iter().enumerate() {
        if i > 0 {
            b.push(',');
        }
        b.push_str(name);
        b.push('=');
        b.push_str(&model::unicode::quote(value));
    }
    b.push('}');
    b
}

/// Go `formatSeriesSetLabelsToJSON` (`tsdb dump --format=seriesjson`).
pub fn format_series_set_labels_to_json(ss: &mut dyn SeriesSet, out: &mut dyn Write) -> Result<(), Error> {
    let mut series_cache: HashSet<String> = HashSet::new();
    while ss.next() {
        let s = ss.at().labels().marshal_json();
        if s.is_empty() {
            continue;
        }
        if !series_cache.contains(&s) {
            let _ = writeln!(out, "{}", s);
            series_cache.insert(s);
        }
    }
    Ok(())
}

/// Go `formatSeriesSetOpenMetrics` (`tsdb dump-openmetrics`).
pub fn format_series_set_open_metrics(ss: &mut dyn SeriesSet, out: &mut dyn Write) -> Result<(), Error> {
    while ss.next() {
        let series = ss.at().clone();
        let lbs = series.labels();
        let metric_name = lbs.get(labels::METRIC_NAME).to_string();
        let lbs = lbs.drop_reserved(|n| n == labels::METRIC_NAME);
        let mut it = series.iterator(None);
        while it.next() == ValueType::Float {
            let (ts, val) = it.at();
            let _ = writeln!(
                out,
                "{}{} {} {:.3}",
                metric_name,
                condensed_string(&lbs),
                model::histogram::gofmt::g(val),
                ts as f64 / 1000.0
            );
        }
        if it.err().is_some() {
            // Go returns ss.Err() here (not the iterator error).
            return match ss.err() {
                Some(e) => Err(e.into()),
                None => Ok(()),
            };
        }
    }
    let _ = writeln!(out, "# EOF");
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// tsdb create-blocks-from openmetrics.

/// Go `backfillOpenMetrics` (`tsdb create-blocks-from openmetrics`): `path` is `input file`, `output_dir`
/// `output directory`, `human_readable` `--human-readable`/`-r`, `quiet` `--quiet`/`-q`, `block_duration` the
/// block duration that main.go derives from `--max-block-duration` and `--allow-incompatible-block-duration`,
/// `custom_labels` the `--label` values. Returns the exit code; errors go to `stderr`.
#[allow(clippy::too_many_arguments)]
pub fn backfill_open_metrics(
    path: &str,
    output_dir: &str,
    human_readable: bool,
    quiet: bool,
    block_duration: Duration,
    custom_labels: &HashMap<String, String>,
    out: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    use std::os::unix::fs::FileTypeExt;

    let info = match std::fs::metadata(path) {
        Ok(i) => i,
        Err(e) => return crate::check_err::<()>(Err(err(format!("stat {}: {}", path, go_io_text(&e)))), stderr),
    };
    let ft = info.file_type();
    let mmapped;
    let read;
    let buf: &[u8] = if ft.is_fifo() || ft.is_char_device() {
        // Read the pipe chunks by chunks as it cannot be mmap-ed.
        read = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => return crate::check_err::<()>(Err(err(format!("read {}: {}", path, go_io_text(&e)))), stderr),
        };
        &read
    } else {
        mmapped = match tsdb_format::fileutil::MmapFile::open(Path::new(path)) {
            Ok(f) => f,
            Err(e) => {
                let mut t = go_io_text(&e);
                // Go's os.Open error names the operation and the path.
                if let Some(rest) = t.strip_prefix("try lock file: ") {
                    t = format!("try lock file: open {}: {}", path, rest);
                }
                return crate::check_err::<()>(Err(err(t)), stderr);
            }
        };
        mmapped.bytes()
    };

    if let Err(e) = std::fs::create_dir_all(output_dir) {
        return crate::check_err::<()>(
            Err(err(format!("create output dir: mkdir {}: {}", output_dir, go_io_text(&e)))),
            stderr,
        );
    }

    crate::check_err(
        crate::backfill::backfill(5000, buf, output_dir, human_readable, quiet, block_duration, custom_labels, out),
        stderr,
    )
}
